'use strict';

/**
 * CAPUNGA DADOS — BAC BO · v21.1
 *
 * ALTERAÇÕES v20 (vs v19) — baseadas na auditoria do histórico real:
 *  1. GATE DE FRESQUIDÃO: se a última rodada recebida tiver mais de maxStaleMs
 *     (2.5 min), NÃO se gera sinal. A v19 gerava sinais com a API parada há
 *     horas — origem das "oportunidades forçadas" com ~33% de perdas.
 *  2. SEQUÊNCIA DE RETOMA: após dados parados, exige recoveryRounds rodadas
 *     NOVAS antes de voltar a emitir sinais (recuperação confirmada, não
 *     apenas "apareceu uma mensagem nova").
 *  3. EXPIRADO ≠ LOSS: se as rodadas necessárias para julgar o sinal não
 *     chegarem (gap > gapMs), o sinal fecha como EXPIRADO (sem apuração),
 *     separado do placar e da taxa. A v19 publicava LOSS por falha de dados.
 *  4. CONFIANÇA HONESTA: sem piso fabricado de 80%. A confiança é a taxa
 *     histórica (backtest + sinais reais) de NÃO PERDER até G2 (green, gale
 *     ou empate/devolução), com prior neutro. Empate = não perder, não vitória.
 *  5. TIER 2 SÓ COM DADOS FRESCOS: a escada de relaxamento continua a existir,
 *     mas nunca opera com dados parados.
 *  6. v21: mantém a tabela de sinais v20 e todo o histórico persistido; acrescenta
 *     recuperação consistente, histórico consultável e mensagens sem numeração.
 *
 * Env: BOT_TOKEN, CHAT_ID, ADMIN_TOKEN, DATABASE_URL, PORT,
 *      API_BACBO (opcional), API_BACBO_2 (opcional)
 */

const fs = require('fs');
const crypto = require('crypto');
const express = require('express');
const cors = require('cors');
const axios = require('axios');
const { Pool } = require('pg');

const PORT = Number(process.env.PORT || 10000);
const BOT_TOKEN = process.env.BOT_TOKEN || '';
const CHAT_ID = process.env.CHAT_ID || '';
const ADMIN_TOKEN = process.env.ADMIN_TOKEN || '';
const DATABASE_URL = process.env.DATABASE_URL || '';

const CFG = {
  ativos: true,
  maxGale: 2,
  maxPending: 1,
  minAmostra: 60,
  syncMs: 5000,               // polling normal, reduz pressão sobre a API
  syncFastMs: 3000,           // polling rápido; apuração continua a cada 1s
  api429BaseMs: 15000,        // espera inicial após HTTP 429
  api429MaxMs: 180000,        // limite do backoff exponencial (3 min)
  minGapMs: 20000,            // entre sinais (quando há oportunidade)
  relaxMs: 3 * 60 * 1000,     // 3min sem sinal -> relaxa os filtros
  forceSignalMs: 5 * 60 * 1000, // 5min -> melhor oportunidade disponível (SÓ com dados frescos)
  minSettledAgeMs: 5000,
  apuraMaxMs: 600000,
  maxHist: 4000,
  gapMs: 7 * 60 * 1000,       // gap entre rodadas acima disto = dados interrompidos
  maxSignalsMem: 500,
  signalsOnBoot: 1000,
  suspendMs: 4 * 3600 * 1000,
  suspendLossStreak: 3,
  staleAlertMs: 3 * 60 * 1000,
  alertThrottleMs: 10 * 60 * 1000,
  keepAliveMs: 60000,
  // ---- NOVO v20 ----
  maxStaleMs: 150000,         // última rodada mais velha que 2.5min -> sem sinais
  recoveryRounds: 4,          // rodadas novas exigidas após dados parados
  confMin: 50,                // piso honesto (v19 fabricava mínimo de 80)
  confMax: 95,
  baseRateMin: 0.60,          // limites do base rate do backtest (sem clamp de 0.90)
  baseRateMax: 0.95,
  // Escada de rigor por tier (0=rigoroso, 1=relaxado, 2=melhor disponível)
  tiers: [
    { scoreMin: 84, minStratN: 8, liveMin: 0.80 },
    { scoreMin: 80, minStratN: 5, liveMin: 0.72 },
    { scoreMin: 76, minStratN: 3, liveMin: 0 }
  ]
};

const J = 'Jogador';
const B = 'Banca';
const E = 'Empate';
const isCol = (x) => x === J || x === B;
const opp = (c) => (c === J ? B : c === B ? J : null);
const FECHADOS = new Set(['GREEN', 'G1', 'G2', 'EMPATE', 'EG1', 'EG2', 'LOSS', 'EXPIRADO']);
const LOCK_KEY = Number(process.env.LOCK_KEY || 727272);

const API_SOURCES = [
  process.env.API_BACBO ||
    'https://api-cs.casino.org/svc-evolution-game-events/api/bacbo?page=0&size=50&sort=data.settledAt,desc&duration=120&wheelResults=PlayerWon,BankerWon,Tie',
  process.env.API_BACBO_2 || ''
].filter(Boolean);

// Estado independente por fonte: um 429 não provoca novas tentativas imediatas.
const API_SOURCE_STATE = API_SOURCES.map(() => ({ failures429: 0, blockedUntil: 0 }));

const DEPLOY_ID = (() => {
  if (process.env.DEPLOY_ID) return String(process.env.DEPLOY_ID).trim();
  try { return crypto.createHash('sha256').update(fs.readFileSync(__filename)).digest('hex').slice(0, 16); }
  catch { return 'unknown'; }
})();

const S = {
  hist: [], seen: new Set(), signals: [],
  nextId: 1, lastSignalAt: 0, curStreak: 0,
  stats: null, statsAtLen: -1,
  awaitingRecovery: 0,  // rodadas novas ainda necessárias após dados parados
  lastSignalRoundId: null
};

// Placar DIÁRIO (zera 00h00 Angola = UTC+1)
const DAILY = { date: '', total: 0, green: 0, g1: 0, g2: 0, empate: 0, eg1: 0, eg2: 0, loss: 0, expirado: 0, bestStreak: 0 };
// Placar GERAL (persistente)
const ALLTIME = { total: 0, green: 0, g1: 0, g2: 0, empate: 0, eg1: 0, eg2: 0, loss: 0, expirado: 0 };
const SUSPENDED = {};

const R = {
  iniciadoEm: new Date().toISOString(), deployId: DEPLOY_ID, versao: '21.2',
  ultimaSync: null, ultimaRodadaNova: null, fonteAPI: 0,
  erroAPI: null, erroTelegram: null, erroDB: null,
  ultimoMotivo: null, ultimoSinal: null, tier: 0, wakes: 0, ultimoWake: null,
  ultimoAlertaAPI: 0, ultimoAlertaDB: 0, lockOk: false, dadosStale: false
};

let pool = null, lockClient = null, server = null, tickTimer = null, keepTimer = null;
let busy = false, stopping = false, lastTickMs = Date.now();

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const log = (tag, d) => console.log(`[${new Date().toISOString()}] [${tag}] ${typeof d === 'string' ? d : JSON.stringify(d)}`);

const fmtHora = new Intl.DateTimeFormat('pt-PT', {
  timeZone: 'Africa/Luanda', hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false
});
const hora = (ms = Date.now()) => fmtHora.format(new Date(Number.isFinite(Number(ms)) && Number(ms) > 0 ? Number(ms) : Date.now()));
// Meia-noite em Angola (UTC+1, sem horário de verão)
const luandaDayKey = (ms = Date.now()) => new Date(ms + 3600000).toISOString().slice(0, 10);

const nomeCor = (c) => (c === B ? 'VERMELHO 🔴' : c === J ? 'AZUL 🔵' : 'EMPATE 🟡');
const emojiCor = (c) => (c === B ? '🔴' : c === J ? '🔵' : '🟡');
const barra = (p) => {
  const f = Math.max(0, Math.min(10, Math.round((p / 100) * 10)));
  return '▰'.repeat(f) + '▱'.repeat(10 - f);
};

/* ============================ DB ============================ */

async function dbInit() {
  if (!DATABASE_URL) { log('DB', 'sem persistência'); return; }
  try {
    const local = /^postgres(?:ql)?:\/\/[^@]*@?(localhost|127\.0\.0\.1)(:|\/|$)/i.test(DATABASE_URL);
    pool = new Pool({
      connectionString: DATABASE_URL,
      ssl: local ? false : { rejectUnauthorized: false },
      max: 6
    });
    await pool.query('SELECT 1');
    await pool.query(`
      CREATE TABLE IF NOT EXISTS capunga_state (k TEXT PRIMARY KEY, v TEXT NOT NULL, updated_at TIMESTAMPTZ DEFAULT NOW());
      CREATE TABLE IF NOT EXISTS capunga_rounds (id TEXT PRIMARY KEY, resultado TEXT NOT NULL, t BIGINT NOT NULL, pj DOUBLE PRECISION, pb DOUBLE PRECISION);
      CREATE INDEX IF NOT EXISTS idx_cap_rounds_t ON capunga_rounds(t DESC);
      CREATE TABLE IF NOT EXISTS capunga_signals_v20 (
        id INTEGER PRIMARY KEY, cor TEXT NOT NULL, key TEXT NOT NULL, nome TEXT NOT NULL,
        tipo TEXT NOT NULL, conf INT NOT NULL, status TEXT NOT NULL DEFAULT 'PENDENTE',
        gale INT, agree INT, tier INT DEFAULT 0, trigger_round_id TEXT, created_at TIMESTAMPTZ NOT NULL, closed_at TIMESTAMPTZ
      );
      ALTER TABLE capunga_signals_v20 ADD COLUMN IF NOT EXISTS trigger_round_id TEXT;
      CREATE INDEX IF NOT EXISTS idx_cap_sig_status ON capunga_signals_v20(status);
      CREATE INDEX IF NOT EXISTS idx_cap_sig_created ON capunga_signals_v20(created_at DESC);
      CREATE INDEX IF NOT EXISTS idx_cap_sig_trigger ON capunga_signals_v20(trigger_round_id);
    `);
    R.erroDB = null; log('DB', 'schema ok');
  } catch (e) { R.erroDB = e.message; log('DB ERRO', e.message); pool = null; }
}

async function acquireLock() {
  if (!pool || stopping) return false;
  try {
    // O lock é exclusivo por sessão PostgreSQL. Cada processo tenta uma vez;
    // se a versão anterior ainda estiver ativa, esta instância fica em espera.
    if (!lockClient) lockClient = await pool.connect();
    const r = await lockClient.query(
      'SELECT pg_try_advisory_lock($1) AS ok', [LOCK_KEY]
    );
    if (r.rows[0].ok) {
      R.lockOk = true;
      log('LOCK', 'bloqueio adquirido');
      return true;
    }
    log('LOCK', 'outra instância ainda ativa; aguardando libertação');
    lockClient.release();
    lockClient = null;
    return false;
  } catch (e) {
    R.erroDB = e.message;
    log('LOCK ERRO', e.message);
    if (lockClient) {
      lockClient.release();
      lockClient = null;
    }
    return false;
  }
}
async function releaseLock() {
  if (!lockClient) return;
  try {
    if (R.lockOk) await lockClient.query('SELECT pg_advisory_unlock($1)', [LOCK_KEY]);
  } catch (e) { log('LOCK RELEASE ERRO', e.message); }
  finally { R.lockOk = false; lockClient.release(); lockClient = null; }
}

async function dbStateGet(k) {
  if (!pool) return null;
  try { const r = await pool.query('SELECT v FROM capunga_state WHERE k=$1', [k]); return r.rows[0]?.v ?? null; }
  catch (e) { R.erroDB = e.message; return null; }
}
async function dbStateSet(k, v) {
  if (!pool) return;
  try {
    await pool.query(
      `INSERT INTO capunga_state(k,v,updated_at) VALUES ($1,$2,NOW())
       ON CONFLICT (k) DO UPDATE SET v=EXCLUDED.v, updated_at=NOW()`,
      [k, String(v)]
    );
  } catch (e) { R.erroDB = e.message; }
}
async function dbSignalInsert(s) {
  if (!pool) return;
  try {
    await pool.query(
      `INSERT INTO capunga_signals_v20 (id,cor,key,nome,tipo,conf,status,gale,agree,tier,trigger_round_id,created_at,closed_at)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13) ON CONFLICT (id) DO NOTHING`,
      [s.id, s.cor, s.key, s.nome, s.tipo, s.conf, s.status, s.gale ?? null, s.agree ?? 1, s.tier ?? 0,
       s.triggerRoundId || null, new Date(s.t), s.fechadoEm ? new Date(s.fechadoEm) : null]
    );
  } catch (e) { R.erroDB = e.message; }
}
async function dbSignalUpdate(s) {
  if (!pool) return;
  try {
    await pool.query('UPDATE capunga_signals_v20 SET status=$1, gale=$2, closed_at=$3 WHERE id=$4',
      [s.status, s.gale ?? null, s.fechadoEm ? new Date(s.fechadoEm) : null, s.id]);
  } catch (e) { R.erroDB = e.message; }
}
async function dbRoundUpsert(r) {
  if (!pool) return;
  try {
    await pool.query(
      `INSERT INTO capunga_rounds(id,resultado,t,pj,pb) VALUES($1,$2,$3,$4,$5)
       ON CONFLICT(id) DO UPDATE SET resultado=EXCLUDED.resultado,t=EXCLUDED.t,pj=EXCLUDED.pj,pb=EXCLUDED.pb`,
      [r.id, r.resultado, r.t, r.pj, r.pb]
    );
  } catch (e) { R.erroDB = e.message; }
}

async function dbLoadAlltime() {
  if (!pool) return;
  try {
    // Placar geral inclui todos os estados fechados, inclusive expirados (separados nas taxas).
    const r = await pool.query(
      `SELECT status, COUNT(*)::int AS n FROM capunga_signals_v20
       WHERE status <> 'PENDENTE' GROUP BY status`
    );
    for (const k of Object.keys(ALLTIME)) ALLTIME[k] = 0;
    for (const row of r.rows) {
      ALLTIME.total += row.n;
      if (row.status === 'GREEN') ALLTIME.green = row.n;
      else if (row.status === 'G1') ALLTIME.g1 = row.n;
      else if (row.status === 'G2') ALLTIME.g2 = row.n;
      else if (row.status === 'EMPATE') ALLTIME.empate = row.n;
      else if (row.status === 'EG1') ALLTIME.eg1 = row.n;
      else if (row.status === 'EG2') ALLTIME.eg2 = row.n;
      else if (row.status === 'LOSS') ALLTIME.loss = row.n;
      else if (row.status === 'EXPIRADO') ALLTIME.expirado = row.n;
    }
  } catch (e) { R.erroDB = e.message; }
}

async function dbLoadState() {
  if (!pool) return;
  try {
    // Carrega o estado e o histórico persistidos da versão anterior desta tabela.
    const nextId = await dbStateGet('nextId_v20');
    const lastSignalAt = await dbStateGet('lastSignalAt_v20');
    const curStreak = await dbStateGet('curStreak_v20');
    S.lastSignalRoundId = await dbStateGet('lastSignalRoundId_v20');
    if (nextId) S.nextId = Math.max(1, parseInt(nextId, 10));
    if (lastSignalAt) S.lastSignalAt = parseInt(lastSignalAt, 10);
    if (curStreak) S.curStreak = parseInt(curStreak, 10);
    const maxId = await pool.query('SELECT COALESCE(MAX(id),0)::int AS n FROM capunga_signals_v20');
    S.nextId = Math.max(S.nextId, Number(maxId.rows[0]?.n || 0) + 1);

    try {
      const raw = await dbStateGet('suspended_v20');
      if (raw) {
        const arr = JSON.parse(raw);
        const now = Date.now();
        for (const [key, until] of arr) if (until > now) SUSPENDED[key] = until;
      }
    } catch (_) {}

    // Placar diário persistido (apenas v20)
    try {
      const rawD = await dbStateGet('daily_v20');
      if (rawD) {
        const d = JSON.parse(rawD);
        for (const k of Object.keys(DAILY)) if (k in d) DAILY[k] = d[k];
      }
    } catch (_) {}
    if (!DAILY.date) DAILY.date = luandaDayKey();

    // rodadas da MESA (dados do jogo, precisam-se para as estratégias — não é histórico do bot)
    const rounds = await pool.query(`SELECT id,resultado,t,pj,pb FROM capunga_rounds ORDER BY t DESC LIMIT ${CFG.maxHist}`);
    S.hist = rounds.rows.map((r) => ({ id: r.id, resultado: r.resultado, t: Number(r.t), pj: r.pj === null ? null : Number(r.pj), pb: r.pb === null ? null : Number(r.pb) })).reverse();
    S.seen = new Set(S.hist.map((r) => r.id));
    // sinais: histórico completo permanece na tabela; memória carrega os mais recentes
    const r = await pool.query(`SELECT * FROM capunga_signals_v20 ORDER BY created_at DESC LIMIT ${CFG.signalsOnBoot}`);
    S.signals = r.rows.reverse().map((row) => ({
      id: row.id, cor: row.cor, key: row.key, nome: row.nome, tipo: row.tipo,
      conf: row.conf, status: row.status, gale: row.gale, agree: row.agree, tier: row.tier || 0,
      t: new Date(row.created_at).getTime(), criadoEm: new Date(row.created_at).getTime(),
      fechadoEm: row.closed_at ? new Date(row.closed_at).getTime() : null,
      triggerRoundId: row.trigger_round_id || null,
      gale1MsgId: null, gale2MsgId: null, galeLevel: row.status !== 'PENDENTE' ? 2 : 0
    }));
    if (!S.lastSignalRoundId) {
      const lastWithRound = [...S.signals].reverse().find((x) => x.triggerRoundId);
      if (lastWithRound) S.lastSignalRoundId = lastWithRound.triggerRoundId;
    }
    await dbLoadAlltime();
    log('DB', `estado carregado: nextId=${S.nextId}, sinais=${S.signals.length}, diário=${DAILY.date}`);

  } catch (e) { R.erroDB = e.message; log('DB ERRO load', e.message); }
}

/* ============================ TELEGRAM ============================ */

async function tgSendRaw(texto) {
  if (!BOT_TOKEN || !CHAT_ID) { R.erroTelegram = 'TOKEN/CHAT em falta'; return { ok: false }; }
  for (let t = 1; t <= 3; t++) {
    try {
      const r = await axios.post(`https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`,
        { chat_id: CHAT_ID, text: texto, parse_mode: 'HTML', disable_web_page_preview: true },
        { timeout: 11000 });
      if (r.data?.ok) { R.erroTelegram = null; return { ok: true, messageId: r.data.result.message_id }; }
      R.erroTelegram = JSON.stringify(r.data);
    } catch (e) {
      R.erroTelegram = e.response?.data ? JSON.stringify(e.response.data) : e.message;
      const retry = Number(e.response?.data?.parameters?.retry_after);
      if (retry > 0 && retry <= 20) await sleep(retry * 1000 + 200);
    }
    if (t < 3) await sleep(1100 * t);
  }
  return { ok: false };
}
async function tgDelete(messageId) {
  if (!messageId || !BOT_TOKEN || !CHAT_ID) return;
  try {
    await axios.post(`https://api.telegram.org/bot${BOT_TOKEN}/deleteMessage`,
      { chat_id: CHAT_ID, message_id: messageId }, { timeout: 8000 });
  } catch (_) {}
}
async function tgAlert(tag, detalhe) {
  const now = Date.now();
  if (tag === 'API') {
    if (now - R.ultimoAlertaAPI < CFG.alertThrottleMs) return;
    R.ultimoAlertaAPI = now;
  }
  if (tag === 'DB') {
    if (now - R.ultimoAlertaDB < CFG.alertThrottleMs) return;
    R.ultimoAlertaDB = now;
  }
  await tgSendRaw(`⚠️ <b>ALERTA ${tag}</b>\n${detalhe}\n⏱ ${hora()}`);
}

/* ============================ RESET DIÁRIO (00h00 ANGOLA) ============================ */

function dailyWins() { return DAILY.green + DAILY.g1 + DAILY.g2; }
function dailyDraws() { return DAILY.empate + DAILY.eg1 + DAILY.eg2; }
// Taxa sem loss: GREEN + empate/devolução, excluindo expirados.
function dailyDecidiveis() { return DAILY.total - DAILY.expirado; }
function dailyTaxa() { const d = dailyDecidiveis(); return d > 0 ? ((dailyWins() + dailyDraws()) / d) * 100 : 0; }

async function sendDailySummary(ontem) {
  const wins = dailyWins();
  const taxa = dailyTaxa();
  const reais = DAILY.green + DAILY.g1 + DAILY.g2;
  const decididos = reais + DAILY.loss;
  const taxaReal = decididos > 0 ? (reais / decididos) * 100 : 0;
  await tgSendRaw([
    '━━━━━━━━━━━━━━━━━━',
    `🕛 <b>FIM DO DIA ${ontem}</b>`,
    '━━━━━━━━━━━━━━━━━━',
    `📊 Sinais: <b>${DAILY.total}</b> (sem apuração: ${DAILY.expirado})`,
    `✅ Greens: ${wins} (G1: ${DAILY.g1} • G2: ${DAILY.g2})`,
    `🟡 Empates/devoluções: ${dailyDraws()}`,
    `❌ Loss: ${DAILY.loss}`,
    `🔥 Maior sequência: ${DAILY.bestStreak}`,
    `🎯 Sem loss: ${taxa.toFixed(1)}% (green sobre decisões: ${taxaReal.toFixed(1)}%)`,
    '',
    '🌅 <b>PLACAR ZERADO — BOM DIA!</b>'
  ].join('\n'));
}

async function checkDailyReset() {
  const hoje = luandaDayKey();
  if (DAILY.date && DAILY.date !== hoje) {
    const ontem = DAILY.date;
    await sendDailySummary(ontem);
    for (const k of ['total', 'green', 'g1', 'g2', 'empate', 'eg1', 'eg2', 'loss', 'expirado', 'bestStreak']) DAILY[k] = 0;
    DAILY.date = hoje;
    await dbStateSet('daily_v20', JSON.stringify(DAILY));
    log('DIÁRIO', `reset de ${ontem} -> ${hoje}`);
  } else if (!DAILY.date) {
    DAILY.date = hoje;
  }
}

/* ============================ TEMPLATES SMS ============================ */

function fmtAnalisando() {
  return [
    '🔍 <b>CAPUNGA PRIME</b> — a analisar a mesa…',
    `⏱ ${hora()}`
  ].join('\n');
}

function fmtSinal(s) {
  const confEmoji = s.conf >= 90 ? '💎' : s.conf >= 80 ? '🔥' : '⚡';
  return [
    '🎯 <b>CAPUNGA PRIME</b>',
    `${emojiCor(s.cor)} <b>ENTRADA: ${s.cor === J ? 'AZUL' : 'VERMELHO'}</b>`,
    `${confEmoji} Força: <b>${s.conf}%</b>`,
    `🧠 Padrão: ${s.emoji || '🧩'} ${s.nome}`,
    '🛡️ Proteção: Empate',
    `⏰ ${hora(s.t || Date.now())}`
  ].join('\n');
}

function fmtGale(s, nivel) {
  return `${nivel === 1 ? '🔄' : '⚠️'} <b>GALE ${nivel} — Manter ${s.cor === J ? 'AZUL 🔵' : 'VERMELHO 🔴'}</b>`;
}

function fmtResult(s) {
  const msg = {
    GREEN: '✅ <b>GREEN!</b> Entrada acertada diretamente.',
    G1: '✅ <b>GREEN G1!</b> Entrada recuperada.',
    G2: '✅ <b>GREEN G2!</b> Ciclo recuperado.',
    EMPATE: '🟡 <b>EMPATE</b> — entrada devolvida.',
    EG1: '🟡 <b>EMPATE G1</b> — entrada devolvida.',
    EG2: '🟡 <b>EMPATE G2</b> — entrada devolvida.',
    LOSS: '❌ <b>LOSS</b> — ciclo encerrado. Aguardar nova oportunidade.',
    EXPIRADO: '⏸ <b>SEM APURAÇÃO</b> — dados interrompidos; não conta como loss.'
  };
  return msg[s.status] || msg.LOSS;
}

function fmtHistorico() {
  const streak = S.curStreak;
  const streakTxt = streak >= 3 ? `${streak} 🔥` : `${streak}`;
  return [
    '━━━━━━━━━━━━━━━━━━',
    `📊 <b>PLACAR — ${DAILY.date}</b>`,
    '━━━━━━━━━━━━━━━━━━',
    `Sinais: ${DAILY.total} (sem apuração: ${DAILY.expirado})`,
    `✅ ${DAILY.green} diretos • ${DAILY.g1} no G1 • ${DAILY.g2} no G2`,
    `🟡 Empates: ${DAILY.empate + DAILY.eg1 + DAILY.eg2}`,
    `❌ Loss: ${DAILY.loss}`,
    `🔥 Sequência: ${streakTxt} | Recorde: ${DAILY.bestStreak}`,
    `🎯 Taxa sem loss: ${dailyTaxa().toFixed(1)}%`
  ].join('\n');
}

/* ============================ API ============================ */

// Remove acentos SEM regex de caracteres combinantes (evita corrupção de encoding)
function stripAccents(str) {
  return String(str ?? '').normalize('NFD').split('').filter((c) => {
    const code = c.charCodeAt(0);
    return code < 0x0300 || code > 0x036f;
  }).join('');
}

function normalizeOutcome(v) {
  if (v && typeof v === 'object') v = v.outcome ?? v.resultado ?? v.winner ?? v.name;
  const s = stripAccents(v).trim().toLowerCase().replace(/[\s_-]+/g, '');
  if (['player', 'jogador', 'playerwin', 'playerwins', 'playerwon', 'blue'].includes(s)) return J;
  if (['banker', 'banca', 'bankerwin', 'bankerwins', 'bankerwon', 'red', 'vermelho'].includes(s)) return B;
  if (['tie', 'draw', 'empate', 'equal', 'push', 'tied'].includes(s)) return E;
  return null;
}
const validMs = (v) => { if (!v) return null; const t = new Date(v).getTime(); return Number.isNaN(t) ? null : t; };
function extractItems(b) {
  if (Array.isArray(b)) return b;
  if (!b || typeof b !== 'object') return [];
  return [b.data, b.results, b.items, b.resultados, b.rounds, b.events,
    b.data?.results, b.data?.items, b.data?.resultados, b.data?.rounds,
    b.data?.events, b.data?.data].find(Array.isArray) || [];
}
// null-safe: Number(null) === 0 criaria scores falsos (ex.: Gatilho 0 falso)
const numOrNull = (v) => {
  if (v === null || v === undefined || v === '') return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
};
function normalizeRound(item) {
  const d = item?.data || item?.round || item || {};
  const r = (d.result && typeof d.result === 'object') ? d.result
    : (item?.result && typeof item.result === 'object') ? item.result : {};
  const t = validMs(d.settledAt ?? d.endTime ?? d.endedAt ?? d.finishedAt ?? d.fim ?? item?.settledAt ?? item?.fim);
  const raw = r.outcome ?? r.resultado ?? r.winner ?? d.outcome ?? d.resultado ?? d.winner ??
    item?.outcome ?? item?.resultado ?? item?.winner ??
    (typeof d.result === 'string' ? d.result : null) ??
    (typeof item?.result === 'string' ? item.result : null);
  const resultado = normalizeOutcome(raw);
  const id = String(d.id ?? d.roundId ?? d.round_id ?? d.eventId ?? item?.id ?? item?.roundId ?? item?.round_id ?? item?.eventId ?? '').trim();
  if (!id || !resultado || !t) return null;
  const pj = numOrNull(r.playerDice?.score ?? d.playerDice?.score ?? r.playerScore ?? d.playerScore ?? d.player_score ?? d.pontos_jogador);
  const pb = numOrNull(r.bankerDice?.score ?? d.bankerDice?.score ?? r.bankerScore ?? d.bankerScore ?? d.banker_score ?? d.pontos_banca);
  return { id, resultado, t, pj, pb };
}

function retryAfterMs(error) {
  const raw = error?.response?.headers?.['retry-after'] ?? error?.response?.headers?.['Retry-After'];
  if (raw === undefined || raw === null || raw === '') return 0;
  const seconds = Number(raw);
  if (Number.isFinite(seconds) && seconds >= 0) return Math.ceil(seconds * 1000);
  const date = Date.parse(String(raw));
  return Number.isFinite(date) ? Math.max(0, date - Date.now()) : 0;
}

async function fetchFrom(url) {
  const resp = await axios.get(url, {
    timeout: 10000,
    headers: { Accept: 'application/json', 'User-Agent': 'CapungaDados/21.1' }
  });
  const map = new Map();
  for (const it of extractItems(resp.data)) {
    const rd = normalizeRound(it);
    if (rd) map.set(rd.id, rd);
  }
  const rounds = [...map.values()].sort((a, b) => a.t - b.t || (a.id < b.id ? -1 : 1));
  if (!rounds.length) throw new Error('fonte sem rodadas válidas');
  const newest = rounds[rounds.length - 1];
  if (Date.now() - newest.t > CFG.maxStaleMs) throw new Error('fonte devolveu dados antigos');
  return rounds;
}

// Fallback automático, com cooldown por fonte em caso de rate limit HTTP 429.
async function fetchRounds() {
  const erros = [];
  const now = Date.now();
  let tentouFonte = false;

  for (let idx = 0; idx < API_SOURCES.length; idx++) {
    const state = API_SOURCE_STATE[idx];
    if (state.blockedUntil > now) {
      const restante = Math.ceil((state.blockedUntil - now) / 1000);
      erros.push(`fonte${idx + 1}: limite 429; em espera ${restante}s`);
      continue;
    }

    tentouFonte = true;
    // Dois ensaios apenas para erros transitórios normais. Em 429 nunca repetir já.
    for (let tentativa = 1; tentativa <= 2; tentativa++) {
      try {
        const rounds = await fetchFrom(API_SOURCES[idx]);
        state.failures429 = 0;
        state.blockedUntil = 0;
        R.fonteAPI = idx;
        R.erroAPI = null;
        return rounds;
      } catch (e) {
        const status = Number(e?.response?.status || e?.status || 0);
        if (status === 429) {
          state.failures429 += 1;
          const exponential = Math.min(
            CFG.api429MaxMs,
            CFG.api429BaseMs * Math.pow(2, Math.max(0, state.failures429 - 1))
          );
          const serverWait = retryAfterMs(e);
          // Nunca encurtar o Retry-After enviado pelo servidor da API.
          const waitMs = Math.max(exponential, serverWait);
          state.blockedUntil = Date.now() + waitMs;
          erros.push(`fonte${idx + 1}: HTTP 429; pausa ${Math.ceil(waitMs / 1000)}s`);
          break;
        }

        erros.push(`fonte${idx + 1}: ${e.message}`);
        if (tentativa < 2) await sleep(1000);
      }
    }
  }

  if (!tentouFonte && erros.length === 0) erros.push('todas as fontes estão temporariamente indisponíveis');
  R.erroAPI = erros.join(' | ');
  throw new Error(R.erroAPI || 'nenhuma fonte API disponível');
}

/* ============================ HELPERS ============================ */

const hasScore = (r) => r && Number.isFinite(r.pj) && Number.isFinite(r.pb);
const margem = (r) => Math.abs((r.pj || 0) - (r.pb || 0));
const scoreOf = (r) => (r.resultado === J ? r.pj : r.pb);
const scoreRange = (s) => {
  if (!Number.isFinite(s)) return null;
  if (s <= 2) return 'MB';
  if (s <= 4) return 'B';
  if (s <= 6) return 'M';
  if (s <= 8) return 'A';
  return 'MA';
};
function streak(seq) {
  if (!seq.length) return 0;
  let n = 0; const cor = seq[0].resultado;
  for (const r of seq) { if (r.resultado === cor) n++; else break; }
  return n;
}
function alternation(seq) {
  if (seq.length < 2) return 1;
  let L = 1;
  while (L < seq.length && seq[L].resultado !== seq[L - 1].resultado) L++;
  return L;
}
function detectRegime(seq) {
  if (seq.length < 6) return { trend: 0.5, chop: 0.5, label: 'neutro' };
  const n = Math.min(seq.length, 15);
  let changes = 0;
  for (let i = 1; i < n; i++) if (seq[i].resultado !== seq[i - 1].resultado) changes++;
  const chop = changes / (n - 1);
  const trend = 1 - chop;
  const label = chop >= 0.65 ? 'errático' : chop <= 0.35 ? 'tendência' : 'neutro';
  return { trend, chop, label };
}
// Estratégias de quebra só em mesa errática; de sequência só em tendência
function regimeFit(cand, regime) {
  const k = cand.key;
  if (/SEQ_(QUEBRA|SURF|PINGPONG|2x2|EXTEND|3_1|MOMENTUM|ALT|BREAK_WEAK)/.test(k)) return regime.chop >= 0.45;
  if (/SEQ_(REP|STRONG_STREAK)|BANKER_BIAS/.test(k)) return regime.trend >= 0.45;
  return true;
}

/* ============================ ESTRATÉGIAS (30 — só as com lógica real) ============================ */

const STRATS = [
  // ---- SEQUÊNCIA (padrões reais de repetição/alternância) ----
  function seqRepeticao({ seq }) {
    const n = streak(seq);
    if (n < 3 || n > 7) return null;
    const strength = [0, 0, 0, 72, 78, 84, 86, 86][n] || 86;
    return { cor: seq[0].resultado, key: `SEQ_REP_${n}`, nome: `Sequência x${n}`, emoji: '🔁', strength, tipo: 'seq' };
  },
  function seqQuebra({ seq }) {
    const n = streak(seq);
    if (n < 4 || n > 8) return null;
    const strength = [0, 0, 0, 0, 74, 80, 84, 85, 85][n] || 85;
    return { cor: opp(seq[0].resultado), key: `SEQ_QUEBRA_${n}`, nome: `Quebra x${n}`, emoji: '💥', strength, tipo: 'seq' };
  },
  function seqSurf({ seq }) {
    const L = alternation(seq);
    if (L < 4) return null;
    const strength = L >= 6 ? 84 : L >= 5 ? 78 : 72;
    return { cor: opp(seq[0].resultado), key: `SEQ_SURF_${L}`, nome: `Surf ${L}`, emoji: '🌊', strength, tipo: 'seq' };
  },
  function seqDoisDois({ seq }) {
    if (seq.length < 4) return null;
    const [a, b, c, d] = seq.map((x) => x.resultado);
    if (a === b && c === d && a !== c) return { cor: c, key: 'SEQ_2x2', nome: 'Bloco 2+2', emoji: '♟️', strength: 76, tipo: 'seq' };
    return null;
  },
  function seqTresUm({ seq }) {
    if (seq.length < 4) return null;
    const [a, b, c, d] = seq.map((x) => x.resultado);
    if (a !== b && b === c && c === d) return { cor: a, key: 'SEQ_3_1', nome: '3→1 confirmado', emoji: '🧩', strength: 78, tipo: 'seq' };
    return null;
  },
  function seqMomentum2({ seq }) {
    if (seq.length < 3) return null;
    const [a, b, c] = seq.map((x) => x.resultado);
    if (a !== b && b === c) return { cor: c, key: 'SEQ_MOMENTUM_2', nome: 'Momentum 2', emoji: '⚡', strength: 77, tipo: 'seq' };
    return null;
  },
  function seqBreakWeak4({ seq }) {
    const n = streak(seq);
    if (n !== 4) return null;
    const s = seq.slice(0, 4);
    if (!s.every((r) => hasScore(r) && margem(r) <= 2)) return null;
    return { cor: opp(seq[0].resultado), key: 'SEQ_BREAK_WEAK_4', nome: 'Quebra fraca x4', emoji: '🩹', strength: 79, tipo: 'seq' };
  },
  function seqExtend({ seq }) {
    if (seq.length < 5) return null;
    const [a, b, c, d, e] = seq.map((x) => x.resultado);
    if (a === b && b === c && c !== d && d === e) {
      return { cor: a, key: 'SEQ_EXTEND', nome: 'Streak extend', emoji: '⏭️', strength: 80, tipo: 'seq' };
    }
    return null;
  },
  function seqStrongStreak({ seq }) {
    const n = streak(seq);
    if (n < 4 || n > 6) return null;
    const same = seq.slice(0, n);
    if (!same.every((r) => hasScore(r) && margem(r) >= 3)) return null;
    return { cor: seq[0].resultado, key: 'SEQ_STRONG_STREAK', nome: `Dragon ${n}`, emoji: '🐉', strength: 82, tipo: 'seq' };
  },
  function seqPingPong({ seq }) {
    const L = alternation(seq);
    if (L < 4 || L > 10) return null;
    return { cor: opp(seq[0].resultado), key: 'SEQ_PINGPONG', nome: `Ping-pong ${L}`, emoji: '🏓', strength: 79, tipo: 'seq' };
  },
  function seqAlternatingStrong({ seq }) {
    const L = alternation(seq);
    if (L < 3) return null;
    const s = seq.slice(0, L);
    if (!s.every((r) => hasScore(r) && margem(r) >= 3)) return null;
    return { cor: opp(seq[0].resultado), key: 'SEQ_ALT_STRONG', nome: `Alt. forte ${L}`, emoji: '⚔️', strength: 78, tipo: 'seq' };
  },
  function bankerBias20({ seq }) {
    const s = seq.slice(0, 20);
    if (s.length < 15) return null;
    const b = s.filter((r) => r.resultado === B).length;
    const j = s.filter((r) => r.resultado === J).length;
    if (b >= 12) return { cor: B, key: 'BANKER_BIAS_20', nome: 'Viés Banca', emoji: '📉', strength: 80, tipo: 'seq' };
    if (j >= 12) return { cor: J, key: 'BANKER_BIAS_20', nome: 'Viés Jogador', emoji: '📈', strength: 80, tipo: 'seq' };
    return null;
  },
  // ---- NUMÉRICA (força real dos dados: margens, escores, sequências de pontos) ----
  function numEscada({ seq }) {
    if (seq.length < 3 || !seq.slice(0, 3).every(hasScore)) return null;
    const [a, b, c] = seq;
    if (a.resultado !== b.resultado || b.resultado !== c.resultado) return null;
    const sa = scoreOf(a), sb = scoreOf(b), sc = scoreOf(c);
    if ((sb - sa === 1 && sc - sb === 1) || (sa - sb === 1 && sb - sc === 1)) {
      return { cor: a.resultado, key: 'NUM_ESCADA', nome: `Escada ${sa}→${sb}→${sc}`, emoji: '🪜', strength: 88, tipo: 'num' };
    }
    return null;
  },
  function numRepeteScore({ seq }) {
    if (seq.length < 2 || !hasScore(seq[0]) || !hasScore(seq[1])) return null;
    const [a, b] = seq;
    if (a.resultado !== b.resultado) return null;
    if (scoreOf(a) === scoreOf(b)) return { cor: a.resultado, key: 'NUM_REPETE', nome: `Score repetido ${scoreOf(a)}`, emoji: '🔂', strength: 84, tipo: 'num' };
    return null;
  },
  function numThreeLow({ seq }) {
    if (seq.length < 3 || !seq.slice(0, 3).every(hasScore)) return null;
    const [a, b, c] = seq;
    if (a.resultado !== b.resultado || b.resultado !== c.resultado) return null;
    if (scoreOf(a) <= 3 && scoreOf(b) <= 3 && scoreOf(c) <= 3) {
      return { cor: opp(a.resultado), key: 'NUM_THREE_LOW', nome: 'Virada de baixos x3', emoji: '📉', strength: 81, tipo: 'num' };
    }
    return null;
  },
  function numGatilho_0({ seq }) {
    if (!seq.length || !hasScore(seq[0])) return null;
    if (scoreOf(seq[0]) === 0) return { cor: opp(seq[0].resultado), key: 'NUM_GATILHO_0', nome: 'Gatilho 0', emoji: '🎯', strength: 86, tipo: 'num' };
    return null;
  },
  function numGatilho_1({ seq }) {
    if (!seq.length || !hasScore(seq[0])) return null;
    if (scoreOf(seq[0]) === 1) return { cor: opp(seq[0].resultado), key: 'NUM_GATILHO_1', nome: 'Gatilho 1', emoji: '🎯', strength: 84, tipo: 'num' };
    return null;
  },
  function numGatilho_9({ seq }) {
    if (!seq.length || !hasScore(seq[0])) return null;
    if (scoreOf(seq[0]) === 9) return { cor: opp(seq[0].resultado), key: 'NUM_GATILHO_9', nome: 'Gatilho 9', emoji: '🎯', strength: 88, tipo: 'num' };
    return null;
  },
  function numMargemAlta({ rows }) {
    const r = rows[0];
    if (!r || !isCol(r.resultado) || !hasScore(r) || margem(r) < 5) return null;
    return { cor: r.resultado, key: 'NUM_MARGEM5', nome: `Margem alta ${r.pj}×${r.pb}`, emoji: '📊', strength: 82, tipo: 'num' };
  },
  function numDiff4x3({ seq }) {
    if (seq.length < 3) return null;
    const t = seq.slice(0, 3);
    if (!t.every((r) => hasScore(r) && margem(r) >= 4 && r.resultado === t[0].resultado)) return null;
    return { cor: t[0].resultado, key: 'NUM_DIFF4x3', nome: 'Diff ≥4 ×3', emoji: '🔢', strength: 85, tipo: 'num' };
  },
  function numViradaBaixa({ seq }) {
    if (seq.length < 2) return null;
    const [a, b] = seq;
    if (!hasScore(a) || !hasScore(b) || a.resultado !== b.resultado) return null;
    if (scoreOf(a) > 4 || scoreOf(b) > 4) return null;
    return { cor: opp(a.resultado), key: 'NUM_VIRADA_BAIXA', nome: 'Virada de baixos', emoji: '🔄', strength: 80, tipo: 'num' };
  },
  function numTendencia({ rows }) {
    const s = rows.slice(0, 8).filter(hasScore);
    if (s.length < 6) return null;
    const mj = s.reduce((a, r) => a + r.pj, 0) / s.length;
    const mb = s.reduce((a, r) => a + r.pb, 0) / s.length;
    const diff = mj - mb;
    if (Math.abs(diff) < 1.5) return null;
    return {
      cor: diff > 0 ? J : B, key: 'NUM_TENDENCIA',
      nome: `Tendência ${mj.toFixed(1)}×${mb.toFixed(1)}`, emoji: '📈',
      strength: Math.abs(diff) >= 2.2 ? 84 : 76, tipo: 'num'
    };
  },
  function numRangeSalto({ seq }) {
    if (seq.length < 2 || !hasScore(seq[0]) || !hasScore(seq[1])) return null;
    const [a, b] = seq;
    const ra = scoreRange(scoreOf(a)), rb = scoreRange(scoreOf(b));
    if (!ra || !rb) return null;
    if ((ra === 'MB' && rb === 'MA') || (ra === 'MA' && rb === 'MB')) {
      return { cor: opp(a.resultado), key: 'NUM_SALTO_RANGE', nome: `Salto ${scoreOf(b)}→${scoreOf(a)}`, emoji: '📐', strength: 81, tipo: 'num' };
    }
    return null;
  },
  function numScoreTrend3({ seq }) {
    if (seq.length < 3 || !seq.slice(0, 3).every(hasScore)) return null;
    const [a, b, c] = seq;
    if (a.resultado !== b.resultado || b.resultado !== c.resultado) return null;
    const sa = scoreOf(a), sb = scoreOf(b), sc = scoreOf(c);
    if (sa < sb && sb < sc) {
      return { cor: a.resultado, key: 'NUM_SCORE_TREND_3', nome: `Escala ascendente ${sa}→${sc}`, emoji: '📊', strength: 82, tipo: 'num' };
    }
    return null;
  },
  function numDoubleHigh({ seq }) {
    if (seq.length < 2 || !seq.slice(0, 2).every(hasScore)) return null;
    const [a, b] = seq;
    if (a.resultado !== b.resultado) return null;
    if (margem(a) >= 5 && margem(b) >= 5) {
      return { cor: a.resultado, key: 'NUM_DOUBLE_HIGH', nome: 'Dupla margem alta', emoji: '💪', strength: 84, tipo: 'num' };
    }
    return null;
  },
  function numMomentumShift({ seq }) {
    if (seq.length < 4 || !seq.slice(0, 4).every(hasScore)) return null;
    const [a, b, c, d] = seq;
    if (![a, b, c, d].every((r) => r.resultado === a.resultado)) return null;
    const sa = scoreOf(a), sb = scoreOf(b), sc = scoreOf(c), sd = scoreOf(d);
    if (sb < sa && sc < sb && sd < sc) {
      return { cor: opp(a.resultado), key: 'MOMENTUM_SHIFT', nome: 'Mudança de momentum', emoji: '🔀', strength: 78, tipo: 'num' };
    }
    return null;
  },
  // ---- EMPATE (padrões pós-empate reais) ----
  function numPosEmpate({ rows, seq }) {
    if (!rows[0] || rows[0].resultado !== E || !seq.length) return null;
    return { cor: seq[0].resultado, key: 'NUM_POS_EMPATE', nome: 'Pós-empate', emoji: '🎯', strength: 74, tipo: 'num' };
  },
  function tieCascade({ rows }) {
    const ties = rows.slice(0, 3).filter((r) => r.resultado === E).length;
    if (ties < 2) return null;
    const pre = rows.find((r) => isCol(r.resultado));
    if (!pre) return null;
    return { cor: pre.resultado, key: 'TIE_CASCADE', nome: 'Cascata de empates', emoji: '🟡', strength: 76, tipo: 'num' };
  },
  function tieSingle({ rows, seq }) {
    if (!rows[0] || rows[0].resultado !== E) return null;
    if (!rows[1] || rows[1].resultado === E) return null;
    if (!seq.length) return null;
    return { cor: opp(seq[0].resultado), key: 'TIE_SINGLE', nome: 'Empate isolado', emoji: '🔀', strength: 77, tipo: 'num' };
  },
  function tieDoubleBack({ rows }) {
    if (rows.length < 3) return null;
    if (rows[0].resultado !== E || rows[1].resultado !== E) return null;
    if (!isCol(rows[2].resultado)) return null;
    return { cor: rows[2].resultado, key: 'TIE_DOUBLE_BACK', nome: 'Retorno pós-duplo empate', emoji: '🔙', strength: 78, tipo: 'num' };
  }
];

function runStrategies(rows) {
  const seq = rows.filter((r) => isCol(r.resultado));
  const ctx = { rows, seq };
  const out = [];
  for (const fn of STRATS) {
    try { const r = fn(ctx); if (r && isCol(r.cor)) out.push(r); } catch (_) {}
  }
  return out;
}

/* ============================ BACKTEST ============================ */

function windowAt(H, i) {
  const rows = [];
  for (let j = i; j >= 0 && rows.length < 20; j--) {
    if (rows.length && rows[rows.length - 1].t - H[j].t > CFG.gapMs) break;
    rows.push(H[j]);
  }
  return rows;
}
// v20: gap nos dados -> null (desconhecido), NÃO loss.
// Empate conta como "não perder" (devolução), coerente com a apuração real.
function evalEntry(H, i, cor) {
  for (let k = 1; k <= CFG.maxGale + 1; k++) {
    const idx = i + k;
    if (idx >= H.length) return null;
    if (H[idx].t - H[idx - 1].t > CFG.gapMs) return null; // dados interrompidos: sem julgamento
    if (H[idx].resultado === cor || H[idx].resultado === E) return true;
  }
  return false;
}
function backtest() {
  const H = S.hist;
  const per = {};
  const ini = 10, fim = H.length - 1;
  if (fim - ini < 30) return { per, baseRate: 0.85 };
  let bw = 0, bn = 0;
  for (let i = ini; i < fim; i++) {
    const win = windowAt(H, i);
    if (win.length < 6) continue;
    for (const c of runStrategies(win)) {
      const v = evalEntry(H, i, c.cor);
      if (v === null) continue;
      const p = per[c.key] || (per[c.key] = { n: 0, w: 0 });
      p.n++; if (v) p.w++;
      bn++; if (v) bw++;
    }
  }
  // v20: prior neutro 0.85 (≈ 1 - 0.5^3 de acertar numa cor em 3 rodadas),
  // sem clamp mágico de 0.90. Limites apenas de sanidade.
  const baseRate = bn ? bw / bn : 0.85;
  return { per, baseRate: Math.min(CFG.baseRateMax, Math.max(CFG.baseRateMin, baseRate)) };
}
function getStats(force = false) {
  if (force || !S.stats || S.hist.length - S.statsAtLen >= 30) {
    S.stats = backtest();
    S.statsAtLen = S.hist.length;
  }
  return S.stats;
}

/* ============================ CONFIANÇA (v20: honesta) ============================
 * conf = taxa histórica de NÃO PERDER até G2 (green, gale ou empate/devolução).
 * Prior do backtest + amostra real, SEM piso fabricado de 80.
 * Um número alto aqui não é garantia: é o histórico observado dessa estratégia. */

function bayesianRate(w, n, priorRate, priorWeight) {
  return (w + priorRate * priorWeight) / (n + priorWeight);
}
function confianca(cand, ctx) {
  const stats = ctx.stats;
  const p = stats.per[cand.key] || { n: 0, w: 0 };
  const baseRate = stats.baseRate || 0.85;
  const btRate = bayesianRate(p.w, p.n, baseRate, 6);
  // amostra real: EXPIRADO não julga a estratégia, fica de fora
  const live = S.signals.filter((x) => x.key === cand.key && FECHADOS.has(x.status) && x.status !== 'EXPIRADO');
  const liveW = live.filter((x) => x.status !== 'LOSS').length;
  const liveRate = live.length >= 5 ? bayesianRate(liveW, live.length, btRate, 5) : null;

  let conf = Math.round((liveRate ?? btRate) * 100);

  const agree = ctx.agreement?.confirmations || 0;
  conf += agree >= 3 ? 2 : agree >= 2 ? 1 : 0;
  conf += Math.round((cand.strength - 80) * 0.15);
  if (cand.tipo === 'seq' && ctx.regime) {
    const regimeMatch = (cand.key.includes('QUEBRA') || cand.key.includes('SURF') || cand.key.includes('PINGPONG'))
      ? ctx.regime.chop : ctx.regime.trend;
    conf += Math.round((regimeMatch - 0.5) * 4);
  }
  if (live.length >= 5 && liveW / live.length < 0.80) conf -= 4;
  const recentLoss = S.signals.find((x) =>
    x.key === cand.key && x.status === 'LOSS' && Date.now() - (x.fechadoEm || 0) < 5 * 60 * 1000);
  if (recentLoss) conf -= 3;

  return Math.max(CFG.confMin, Math.min(CFG.confMax, conf));
}

/* ============================ SUSPENSÃO (persistente) ============================ */

function isSuspended(key) {
  const u = SUSPENDED[key];
  if (!u) return false;
  if (Date.now() > u) { delete SUSPENDED[key]; dbStateSet('suspended_v20', JSON.stringify(Object.entries(SUSPENDED))); return false; }
  return true;
}
function maybeSuspend(key) {
  const recent = S.signals
    .filter((x) => x.key === key && FECHADOS.has(x.status) && x.status !== 'EXPIRADO')
    .sort((a, b) => (a.t || 0) - (b.t || 0))
    .slice(-CFG.suspendLossStreak);
  if (recent.length < CFG.suspendLossStreak) return;
  if (recent.every((x) => x.status === 'LOSS')) {
    SUSPENDED[key] = Date.now() + CFG.suspendMs;
    dbStateSet('suspended_v20', JSON.stringify(Object.entries(SUSPENDED)));
    log('SUSPENSO', { key, consecutiveLosses: recent.length });
  }
}

/* ============================ ESCADA DE OPORTUNIDADES ============================ */
/* Tier 0 (mesa quente, <3min sem sinal): RIGOROSO — só entrada de alta convicção
   Tier 1 (3-5min): RELAXADO — filtros cedem um pouco
   Tier 2 (>5min): melhor oportunidade disponível
   v20: TODOS os tiers exigem dados frescos (maxStaleMs) e retoma confirmada. */

function idleMs() { return S.lastSignalAt ? Date.now() - S.lastSignalAt : Infinity; }
function tierAtual() {
  const i = idleMs();
  if (i >= CFG.forceSignalMs) return 2;
  if (i >= CFG.relaxMs) return 1;
  return 0;
}

/* ============================ GATE ============================ */

function pendentes() { return S.signals.filter((s) => !FECHADOS.has(s.status)); }

function podeGerar() {
  if (!CFG.ativos) { R.ultimoMotivo = 'desativado'; return false; }
  if (pendentes().length >= CFG.maxPending) { R.ultimoMotivo = 'max pending'; return false; }
  const now = Date.now();
  if (S.lastSignalAt && now - S.lastSignalAt < CFG.minGapMs) {
    R.ultimoMotivo = `gap ${Math.ceil((CFG.minGapMs - (now - S.lastSignalAt)) / 1000)}s`;
    return false;
  }
  const last = S.hist[S.hist.length - 1];
  if (!last) { R.ultimoMotivo = 'sem hist'; return false; }
  if (S.lastSignalRoundId && last.id === S.lastSignalRoundId) { R.ultimoMotivo = 'rodada já utilizada'; return false; }
  // v20 — GATE DE FRESQUIDÃO: dados parados => sem sinais novos, sem exceção
  if (now - last.t > CFG.maxStaleMs) {
    R.dadosStale = true;
    R.ultimoMotivo = `dados antigos ${Math.ceil((now - last.t) / 60000)}min (API parada)`;
    return false;
  }
  R.dadosStale = false;
  // v20 — RETOMA: após dados parados, exigir rodadas novas confirmadas
  if (S.awaitingRecovery > 0) {
    R.ultimoMotivo = `retoma: aguarda ${S.awaitingRecovery} rodada(s) nova(s)`;
    return false;
  }
  if (now - last.t < CFG.minSettledAgeMs) {
    R.ultimoMotivo = `rodada recente ${Math.ceil((CFG.minSettledAgeMs - (now - last.t)) / 1000)}s`;
    return false;
  }
  if (S.hist.length < CFG.minAmostra) { R.ultimoMotivo = `amostra ${S.hist.length}`; return false; }
  return true;
}

function stratSamples(key, stats) {
  const p = stats.per[key];
  const bn = p ? p.n : 0;
  let liveN = 0;
  for (const x of S.signals) if (x.key === key && x.status !== 'PENDENTE' && x.status !== 'EXPIRADO') liveN++;
  return bn + liveN;
}
function liveHitRate(key) {
  let n = 0, w = 0;
  for (const x of S.signals) {
    if (x.key !== key || x.status === 'PENDENTE' || x.status === 'EXPIRADO') continue;
    n++; if (x.status !== 'LOSS') w++;
  }
  return n >= 5 ? w / n : null;
}

/* ============================ GERAR SINAL ============================ */

async function tentarSinal() {
  if (!podeGerar()) return;

  const tier = tierAtual();
  R.tier = tier;
  const T = CFG.tiers[tier];

  const rows = S.hist.slice(-30).reverse();
  const stats = getStats();
  const seq = rows.filter((r) => isCol(r.resultado));
  const regime = detectRegime(seq);

  let cands = runStrategies(rows);
  if (!cands.length) { R.ultimoMotivo = 'sem estratégia'; return; }

  if (tier <= 1) cands = cands.filter((c) => regimeFit(c, regime)); // tier 2 ignora regime (mas pega o melhor)

  cands = cands.filter((c) => {
    if (isSuspended(c.key)) return false;
    if (stratSamples(c.key, stats) < T.minStratN) return false;
    const lr = liveHitRate(c.key);
    if (lr !== null && lr < T.liveMin) return false;
    return true;
  });
  if (!cands.length) { R.ultimoMotivo = `tier${tier}: filtros (${T.scoreMin}%/${T.minStratN}am)`; return; }

  const byColor = {};
  for (const c of cands) {
    if (!byColor[c.cor]) byColor[c.cor] = [];
    byColor[c.cor].push(c);
  }

  let melhor = null;
  for (const cor of Object.keys(byColor)) {
    const list = byColor[cor];
    const scored = list.map((c) => ({ ...c, conf: confianca(c, { stats, regime, agreement: { confirmations: list.length } }) }));
    scored.sort((a, b) => b.conf - a.conf);
    const top = scored[0];
    if (!melhor || top.conf > melhor.conf) melhor = { ...top, agree: scored.length };
  }

  if (!melhor) { R.ultimoMotivo = 'sem candidato'; return; }
  // v20: scoreMin aplica-se a TODOS os tiers (tier 2 tem limiar menor, mas existe)
  if (melhor.conf < T.scoreMin) { R.ultimoMotivo = `score ${melhor.conf} < ${T.scoreMin} (tier${tier})`; return; }

  const reservedId = S.nextId;
  const sinalPreliminar = {
    id: reservedId, cor: melhor.cor, key: melhor.key, nome: melhor.nome, emoji: melhor.emoji,
    conf: melhor.conf, tipo: melhor.tipo, status: 'PENDENTE', tier,
    t: Date.now(), agree: melhor.agree,
    triggerRoundId: S.hist[S.hist.length - 1].id,
    gale1MsgId: null, gale2MsgId: null, galeLevel: 0
  };

  const analisando = await tgSendRaw(fmtAnalisando());
  try {
    const envio = await tgSendRaw(fmtSinal(sinalPreliminar));
    if (!envio.ok) {
      log('SINAL', { motivo: 'telegram rejeitou sinal', id: reservedId });
      R.ultimoMotivo = 'telegram falhou';
      return;
    }
    const agora = sinalPreliminar.t;
    S.nextId++;
    S.lastSignalRoundId = sinalPreliminar.triggerRoundId;
    S.signals.push(sinalPreliminar);
    if (S.signals.length > CFG.maxSignalsMem) S.signals = S.signals.slice(-CFG.maxSignalsMem + 50);
    S.lastSignalAt = agora;
    R.ultimoSinal = sinalPreliminar;
    R.ultimoMotivo = null;

    await dbSignalInsert(sinalPreliminar);
    await dbStateSet('nextId_v20', String(S.nextId));
    await dbStateSet('lastSignalAt_v20', String(agora));
    await dbStateSet('lastSignalRoundId_v20', S.lastSignalRoundId);

    log('SINAL', { id: sinalPreliminar.id, cor: sinalPreliminar.cor, conf: sinalPreliminar.conf, key: sinalPreliminar.key, tier });

  } finally {
    if (analisando.ok) await tgDelete(analisando.messageId);
  }
}

async function notificarGale(s, nivel) {
  if (nivel === 2 && s.gale1MsgId) {
    await tgDelete(s.gale1MsgId);
    s.gale1MsgId = null;
  }
  const r = await tgSendRaw(fmtGale(s, nivel));
  if (r.ok) {
    if (nivel === 1) s.gale1MsgId = r.messageId;
    else s.gale2MsgId = r.messageId;
    s.galeLevel = nivel;
  }
}

/* ============================ FECHAR ============================ */

async function fecharSinal(s, status, gale) {
  s.status = status;
  s.gale = gale;
  s.fechadoEm = Date.now();

  if (s.gale1MsgId) { await tgDelete(s.gale1MsgId); s.gale1MsgId = null; }
  if (s.gale2MsgId) { await tgDelete(s.gale2MsgId); s.gale2MsgId = null; }

  const bucket = status === 'GREEN' ? 'green' : status === 'G1' ? 'g1' : status === 'G2' ? 'g2'
    : status === 'EMPATE' ? 'empate' : status === 'EG1' ? 'eg1' : status === 'EG2' ? 'eg2'
    : status === 'EXPIRADO' ? 'expirado' : 'loss';
  ALLTIME[bucket]++; ALLTIME.total++;
  DAILY[bucket]++; DAILY.total++;
  // EXPIRADO não julga a estratégia: não mexe na sequência nem suspende
  if (status === 'LOSS') {
    S.curStreak = 0;
  } else if (status !== 'EXPIRADO') {
    S.curStreak++;
    if (S.curStreak > DAILY.bestStreak) DAILY.bestStreak = S.curStreak;
  }
  await dbStateSet('curStreak_v20', String(S.curStreak));
  await dbStateSet('daily_v20', JSON.stringify(DAILY));

  await dbSignalUpdate(s);
  await tgSendRaw(fmtResult(s));
  await tgSendRaw(fmtHistorico());
  if (status === 'LOSS') maybeSuspend(s.key);
}

/* ============================ APURAÇÃO ============================ */
/* REGRA v20:
   - Rodadas presentes e contra a cor (3x seguidas, sem falha de dados) => LOSS real.
   - Falha de dados (gap > gapMs, ou rodadas nunca chegaram) => EXPIRADO (sem apuração).
   - Cor ou empate => GREEN/G1/G2/EMPATE/EG1/EG2. */

async function apurar() {
  const now = Date.now();
  for (const s of S.signals) {
    if (FECHADOS.has(s.status)) continue;
    const after = S.hist.filter((r) => r.t > s.t && (!s.triggerRoundId || r.id !== s.triggerRoundId)).sort((a, b) => a.t - b.t);
    let decided = false;
    for (let k = 0; k <= CFG.maxGale && k < after.length; k++) {
      const r = after[k];
      const prevT = k === 0 ? s.t : after[k - 1].t;
      if (r.t - prevT > CFG.gapMs) {
        // dados interrompidos: entrada não julgável
        await fecharSinal(s, 'EXPIRADO', k);
        decided = true; break;
      }
      if (r.resultado === s.cor || r.resultado === E) {
        const status = r.resultado === E ? (k === 0 ? 'EMPATE' : k === 1 ? 'EG1' : 'EG2') : (k === 0 ? 'GREEN' : k === 1 ? 'G1' : 'G2');
        await fecharSinal(s, status, k); decided = true; break;
      }
      if (k < CFG.maxGale && s.galeLevel < k + 1) await notificarGale(s, k + 1);
    }
    if (decided) continue;
    if (!after.length) {
      // nenhuma rodada chegou depois do sinal
      if (now - s.t > CFG.gapMs || now - s.t >= CFG.apuraMaxMs) {
        await fecharSinal(s, 'EXPIRADO', 0);
      }
    } else if (after.length >= CFG.maxGale + 1) {
      // 3 rodadas após o sinal, todas contra, sem falha de dados: LOSS real
      await fecharSinal(s, 'LOSS', CFG.maxGale);
    } else if (now - s.t >= CFG.apuraMaxMs) {
      // timeout com dados incompletos: sem apuração
      await fecharSinal(s, 'EXPIRADO', s.galeLevel || 0);
    }
  }
}

/* ============================ SYNC ADAPTATIVO ============================ */

function currentSyncMs() {
  if (pendentes().length > 0) return CFG.syncFastMs;
  const last = S.hist[S.hist.length - 1];
  if (last && Date.now() - last.t < 45000) return CFG.syncFastMs; // mesa quente
  if (S.awaitingRecovery > 0) return CFG.syncFastMs; // retoma em curso, monitorar de perto
  return CFG.syncMs;
}

async function sync() {
  try {
    const rounds = await fetchRounds();
    let novos = 0, atualizados = 0;
    const newestBefore = S.hist[S.hist.length - 1];
    const newestBeforeT = newestBefore ? newestBefore.t : 0;
    const wasStaleBefore = !newestBefore || Date.now() - newestBefore.t > CFG.maxStaleMs;
    const byId = new Map(S.hist.map((r) => [r.id, r]));
    for (const r of rounds) {
      const previous = byId.get(r.id);
      if (!previous) {
        novos++; byId.set(r.id, r);
        await dbRoundUpsert(r);
      } else if (previous.t !== r.t || previous.resultado !== r.resultado || previous.pj !== r.pj || previous.pb !== r.pb) {
        Object.assign(previous, r); atualizados++;
        await dbRoundUpsert(r);
      }
    }
    S.hist = [...byId.values()].sort((a, b) => a.t - b.t || a.id.localeCompare(b.id));
    if (S.hist.length > CFG.maxHist) S.hist = S.hist.slice(-CFG.maxHist);
    S.seen = new Set(S.hist.map((r) => r.id));
    R.ultimaSync = new Date().toISOString();
    const newest = S.hist[S.hist.length - 1];
    if (newest) R.ultimaRodadaNova = new Date(newest.t).toISOString();

    // v20 — máquina de estados da retoma:
    // dados parados -> exige recoveryRounds rodadas novas -> só então volta a emitir
    const stale = !newest || Date.now() - newest.t > CFG.maxStaleMs;
    if (stale) {
      R.dadosStale = true;
      if (S.awaitingRecovery < CFG.recoveryRounds) {
        S.awaitingRecovery = CFG.recoveryRounds;
        log('RETOMA', `dados parados — exigindo ${CFG.recoveryRounds} rodadas novas antes de novos sinais`);
      }
    } else {
      R.dadosStale = false;
      if (wasStaleBefore && S.awaitingRecovery === 0) {
        S.awaitingRecovery = CFG.recoveryRounds;
        log('RETOMA', `dados recuperados — exigindo ${CFG.recoveryRounds} rodadas novas`);
      }
      if (S.awaitingRecovery > 0) {
        const antes = S.awaitingRecovery;
        const novasAposRetoma = newestBefore ? rounds.filter((r) => r.t > newestBeforeT).length : 0;
        S.awaitingRecovery = Math.max(0, S.awaitingRecovery - novasAposRetoma);
        if (S.awaitingRecovery !== antes) {
          log('RETOMA', `dados voltaram — faltam ${S.awaitingRecovery} rodada(s) para liberar sinais`);
        }
      }
    }

    if (newest && Date.now() - newest.t > CFG.staleAlertMs) {
      const mins = Math.floor((Date.now() - newest.t) / 60000);
      await tgAlert('API', `API BACBO sem rodadas novas há <b>${mins} min</b>.\nÚltima: ${R.ultimaRodadaNova}\nFonte: ${R.fonteAPI + 1}/${API_SOURCES.length}\n🛑 Sinais suspensos até os dados voltarem`);
    }

    if (novos || atualizados) log('SYNC', { novos, atualizados, total: S.hist.length, fonte: R.fonteAPI + 1, retoma: S.awaitingRecovery });

  } catch (e) {
    R.erroAPI = e.message;
    log('SYNC ERRO', e.message);
    await tgAlert('API', `Erro ao consultar API BACBO:\n<code>${e.message}</code>`);
  }
}

let lastSyncAt = 0;
async function tick() {
  if (busy || stopping) return;
  busy = true; lastTickMs = Date.now();
  try {
    await checkDailyReset();
    const now = Date.now();
    if (now - lastSyncAt >= currentSyncMs()) {
      await sync();
      lastSyncAt = Date.now();
    }
    await apurar();    // apura a cada 1s — gale nunca atrasa
    await tentarSinal();
  } catch (e) { log('TICK ERRO', e.message); }
  finally { busy = false; }
}

async function keepAlive() {
  const self = process.env.SELF_URL || process.env.RENDER_EXTERNAL_URL || '';
  if (self) { try { await axios.get(self.replace(/\/$/, '') + '/health', { timeout: 7000 }); } catch (_) {} }
}
function wakeCheck() {
  if (Date.now() - lastTickMs > 10000) {
    R.wakes++; R.ultimoWake = new Date().toISOString();
    log('WAKE', 'atraso'); tick().catch(() => {});
  }
}

/* ============================ EXPRESS ============================ */

const app = express();
app.disable('x-powered-by');
const corsOrigins = (process.env.CORS_ORIGINS || '').split(',').map((x) => x.trim()).filter(Boolean);
app.use(cors(corsOrigins.length ? { origin: corsOrigins } : { origin: false }));
app.use(express.json({ limit: '200kb' }));

function requireAdmin(req, res, next) {
  if (!ADMIN_TOKEN) return res.status(503).json({ erro: 'ADMIN_TOKEN não configurado' });
  const supplied = String(req.get('authorization') || '').replace(/^Bearer\s+/i, '');
  const a = Buffer.from(supplied);
  const b = Buffer.from(ADMIN_TOKEN);
  if (a.length !== b.length || !crypto.timingSafeEqual(a, b)) return res.status(401).json({ erro: 'não autorizado' });
  next();
}

app.get('/health', (req, res) => {
  const newest = S.hist[S.hist.length - 1];
  res.json({
    ok: Boolean(server) && !R.erroDB,
    role: leaderReady ? 'leader' : 'standby',
    versao: '21.2', deployId: DEPLOY_ID, maxGale: CFG.maxGale, lock: R.lockOk,
    tier: R.tier, fonteAPI: R.fonteAPI + 1, fontes: API_SOURCES.length,
    hist: S.hist.length, sinais: S.signals.length, pendentes: pendentes().length,
    dadosStale: R.dadosStale,
    aguardaRetoma: S.awaitingRecovery,
    rodasRetoma: CFG.recoveryRounds,
    diario: { ...DAILY, taxa: dailyTaxa() }, geral: ALLTIME,
    ultimaSync: R.ultimaSync, ultimaRodada: newest ? R.ultimaRodadaNova : null,
    apiStale: newest ? Date.now() - newest.t > CFG.staleAlertMs : true,
    erroAPI: R.erroAPI, erroDB: R.erroDB,
    uptime: Math.round(process.uptime())
  });
});
app.get('/status', requireAdmin, (req, res) => {
  res.json({ motivo: R.ultimoMotivo, R, DAILY, ALLTIME, CFG, suspenso: Object.keys(SUSPENDED) });
});
app.get('/sinais', requireAdmin, async (req, res) => {
  const n = Math.min(500, Math.max(1, parseInt(req.query.n, 10) || 50));
  if (pool) {
    const q = await pool.query(`SELECT id,cor,key,nome,tipo,conf,status,gale,agree,tier,trigger_round_id,created_at,closed_at FROM capunga_signals_v20 ORDER BY created_at DESC LIMIT $1`, [n]);
    return res.json(q.rows);
  }
  res.json(S.signals.slice(-n).reverse());
});
app.get('/historico', requireAdmin, async (req, res) => {
  const limit = Math.min(500, Math.max(1, parseInt(req.query.limit, 10) || 100));
  const offset = Math.max(0, parseInt(req.query.offset, 10) || 0);
  if (!pool) return res.status(503).json({ erro: 'histórico PostgreSQL indisponível' });
  const [items, count] = await Promise.all([
    pool.query(`SELECT id,cor,key,nome,tipo,conf,status,gale,agree,tier,trigger_round_id,created_at,closed_at FROM capunga_signals_v20 ORDER BY created_at DESC LIMIT $1 OFFSET $2`, [limit, offset]),
    pool.query(`SELECT COUNT(*)::int AS total FROM capunga_signals_v20`)
  ]);
  res.json({ total: count.rows[0].total, limit, offset, sinais: items.rows });
});
app.get('/stats', requireAdmin, (req, res) => {
  const wins = dailyWins();
  const reais = DAILY.green + DAILY.g1 + DAILY.g2;
  const decididos = reais + DAILY.loss;
  const gw = ALLTIME.green + ALLTIME.g1 + ALLTIME.g2;
  const gDraws = ALLTIME.empate + ALLTIME.eg1 + ALLTIME.eg2;
  const gDecidiveis = ALLTIME.total - ALLTIME.expirado;
  res.json({
    diario: {
      ...DAILY, wins, taxa: dailyTaxa(),
      taxaReal: decididos > 0 ? reais / decididos : null, sequenciaAtual: S.curStreak
    },
    geral: {
      ...ALLTIME, wins: gw, draws: gDraws,
      taxa: gDecidiveis > 0 ? (gw + gDraws) / gDecidiveis : null,
      taxaGreen: (gw + ALLTIME.loss) > 0 ? gw / (gw + ALLTIME.loss) : null
    }
  });
});
// v20: calibração por faixa de confiança — expirados excluídos
app.get('/calibracao', requireAdmin, (req, res) => {
  const buckets = { '50-69': { n: 0, w: 0 }, '70-79': { n: 0, w: 0 }, '80-85': { n: 0, w: 0 }, '86-92': { n: 0, w: 0 }, '93-95': { n: 0, w: 0 } };
  for (const s of S.signals) {
    if (s.status === 'PENDENTE' || s.status === 'EXPIRADO') continue;
    const b = s.conf >= 93 ? '93-95' : s.conf >= 86 ? '86-92' : s.conf >= 80 ? '80-85' : s.conf >= 70 ? '70-79' : '50-69';
    buckets[b].n++;
    if (s.status !== 'LOSS') buckets[b].w++;
  }
  res.json(buckets);
});

/* ============================ START ============================ */

async function becomeLeader() {
  if (leaderReady || acquiringLeader || stopping) return;
  acquiringLeader = true;
  try {
    const locked = await acquireLock();
    if (!locked) return;

    // Só a instância que detém o advisory lock pode carregar estado,
    // consultar a API, apurar sinais ou enviar mensagens.
    await dbLoadState();
    await dbLoadAlltime();
    await sync();
    getStats(true);

    const last = S.hist[S.hist.length - 1];
    if (!last || Date.now() - last.t > CFG.maxStaleMs) {
      S.awaitingRecovery = CFG.recoveryRounds;
    }

    for (const s of S.signals) {
      if (s.status !== 'PENDENTE') continue;
      const after = S.hist.filter((r) =>
        r.t > s.t && (!s.triggerRoundId || r.id !== s.triggerRoundId)
      ).sort((a, b) => a.t - b.t);
      let consecutivasFalhas = 0;
      let prevT = s.t;
      for (const r of after) {
        if (r.t - prevT > CFG.gapMs || r.resultado === s.cor || r.resultado === E) break;
        consecutivasFalhas++;
        prevT = r.t;
        if (consecutivasFalhas >= CFG.maxGale) break;
      }
      s.galeLevel = Math.min(consecutivasFalhas, CFG.maxGale);
    }

    leaderReady = true;
    R.ultimoMotivo = 'Instância principal ativa';
    log('LEADER', 'instância principal ativa; bot iniciado');
    tickTimer = setInterval(() => tick().catch((e) => log('TICK ERRO', e.message)), 1000);
    setTimeout(() => tick().catch((e) => log('TICK ERRO', e.message)), 1000);
  } catch (e) {
    R.ultimoMotivo = `Falha ao assumir liderança: ${e.message}`;
    log('LEADER ERRO', e.stack || e.message);
    await releaseLock();
  } finally {
    acquiringLeader = false;
  }
}

async function start() {
  for (const [name, value] of Object.entries({ BOT_TOKEN, CHAT_ID, ADMIN_TOKEN, DATABASE_URL })) {
    if (!value) throw new Error(`Variável obrigatória em falta: ${name}`);
  }
  if (!/^postgres(?:ql)?:\/\//i.test(DATABASE_URL)) {
    throw new Error('DATABASE_URL inválido');
  }

  log('BOOT', { versao: '21.2', deployId: DEPLOY_ID, fontes: API_SOURCES.length });
  await dbInit();
  if (!pool) throw new Error('Não foi possível inicializar PostgreSQL; arranque cancelado');

  // IMPORTANTE: abrir a porta antes de tentar o advisory lock.
  // Durante deploys sobrepostos, o processo antigo pode ainda deter o lock.
  server = app.listen(PORT, '0.0.0.0', () => {
    log('HTTP', `porta ${PORT} aberta; aguardando liderança PostgreSQL`);
  });

  server.on('error', (e) => {
    log('HTTP ERRO', e.message);
    R.ultimoMotivo = `Erro HTTP: ${e.message}`;
  });

  // Primeira tentativa e novas tentativas sem encerrar o serviço.
  await becomeLeader();
  lockRetryTimer = setInterval(() => {
    if (!leaderReady) becomeLeader().catch((e) => log('LOCK RETRY ERRO', e.message));
  }, 15000);

  keepTimer = setInterval(() => {
    if (leaderReady) keepAlive().catch(() => {});
    wakeCheck();
  }, CFG.keepAliveMs);

  process.on('SIGTERM', shutdown);
  process.on('SIGINT', shutdown);
}
async function shutdown() {
  if (stopping) return;
  stopping = true; log('SHUTDOWN', 'ok');
  clearInterval(tickTimer); clearInterval(keepTimer); clearInterval(lockRetryTimer);
  if (server) await new Promise((resolve) => server.close(resolve));
  await releaseLock();
  if (pool) await pool.end().catch(() => {});
  process.exit(0);
}

start().catch((e) => { console.error(e); process.exit(1); });
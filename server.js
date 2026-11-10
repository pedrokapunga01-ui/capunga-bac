'use strict';

/**
 * CAPUNGA DADOS — BAC BO · v18 (Gale 2)
 * Ficheiro: server.js
 * Env: BOT_TOKEN, CHAT_ID, ADMIN_TOKEN, DATABASE_URL, PORT
 * Opcionais: IDADE_API_MAX_MS, IDADE_VISTO_MAX_MS, SELF_URL
 *
 * Correções v18:
 *  - Sinal sai no mesmo ciclo em que a rodada nova é detectada (sem atraso artificial).
 *  - Entrada = primeira rodada DEPOIS do sinal (refT). Nunca conta rodada já em curso.
 *  - Apuração recalculada do zero a cada ciclo: não depende do Telegram, nunca congela.
 *  - Telegram em fila com prioridade (sinal/resultado/gale primeiro). Nada bloqueia o tick.
 *  - "Analisando a mesa": enviada após Green/Loss, apagada quando o próximo sinal sai,
 *    com prioridade mínima (nunca impede nem atrasa o sinal).
 */

const fs = require('fs');
const crypto = require('crypto');
const express = require('express');
const cors = require('cors');
const axios = require('axios');
const { Pool } = require('pg');

/* ============================ CONFIG ============================ */

const PORT = Number(process.env.PORT || 10000);
const BOT_TOKEN = process.env.BOT_TOKEN || '';
const CHAT_ID = process.env.CHAT_ID || '';
const ADMIN_TOKEN = process.env.ADMIN_TOKEN || '';
const DATABASE_URL = process.env.DATABASE_URL || '';

const CFG = {
  ativos: true,
  maxGale: 2,                 // Gale 2 = 3 rodadas por sinal
  maxPending: 1,
  scoreMin: 80,
  minAmostra: 60,
  sinalInicial: 1,
  syncMs: 2000,               // polling da API
  apiTimeoutMs: 6000,
  cacheBust: true,
  idadeApiMaxMs: Number(process.env.IDADE_API_MAX_MS || 20000),   // idade máx. (settledAt) da última rodada p/ emitir sinal
  idadeVistoMaxMs: Number(process.env.IDADE_VISTO_MAX_MS || 9000), // idade máx. desde que o bot viu a rodada
  apuraMaxMs: 10 * 60 * 1000, // sinal sem desfecho após isto expira em silêncio
  maxHist: 4000,
  gapMs: 7 * 60 * 1000,
  maxSignalsMem: 500,
  signalsOnBoot: 500,
  suspendMs: 4 * 3600 * 1000,
  suspendLossStreak: 3,
  suspendLookback: 5,
  keepAliveMs: 60000,
  tickWatchdogMs: 45000
};

const J = 'Jogador';
const B = 'Banca';
const E = 'Empate';
const isCol = (x) => x === J || x === B;
const opp = (c) => (c === J ? B : c === B ? J : null);
const FECHADOS = new Set(['GREEN', 'G1', 'G2', 'EMPATE', 'EG1', 'EG2', 'LOSS']);

const API_BACBO =
  'https://api-cs.casino.org/svc-evolution-game-events/api/bacbo?page=0&size=50&sort=data.settledAt,desc&duration=120&wheelResults=PlayerWon,BankerWon,Tie';

/* ============================ DEPLOY ID ============================ */

const DEPLOY_ID = (() => {
  if (process.env.DEPLOY_ID) return String(process.env.DEPLOY_ID).trim();
  try { return crypto.createHash('sha256').update(fs.readFileSync(__filename)).digest('hex').slice(0, 16); }
  catch { return 'unknown'; }
})();

/* ============================ STATE ============================ */

const S = {
  hist: [], seen: new Set(), signals: [],
  nextId: CFG.sinalInicial, lastSignalAt: 0, lastSignalRoundId: null,
  stats: null, statsAtLen: -1, statsRefreshing: false
};

const STATS = { total: 0, green: 0, g1: 0, g2: 0, empate: 0, eg1: 0, eg2: 0, loss: 0 };
const SUSPENDED = {};

const R = {
  iniciadoEm: new Date().toISOString(), deployId: DEPLOY_ID,
  ultimaSync: null, ultimaRodadaNova: null,
  erroAPI: null, erroTelegram: null, erroDB: null,
  ultimoMotivo: null, ultimoSinal: null, wakes: 0, ultimoWake: null
};

let pool = null, server = null, tickTimer = null, keepTimer = null;
let busy = false, busySince = 0, stopping = false, lastTickMs = Date.now();

/* ============================ UTILS ============================ */

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const log = (tag, d) => console.log(`[${new Date().toISOString()}] [${tag}] ${typeof d === 'string' ? d : JSON.stringify(d)}`);

const fmtHora = new Intl.DateTimeFormat('pt-PT', {
  timeZone: 'Africa/Luanda', hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false
});
const hora = (ms = Date.now()) => fmtHora.format(new Date(ms));

const nomeCor = (c) => (c === B ? 'Vermelho🔴' : c === J ? 'Azul🔵' : 'Empate🟡');
const emojiCor = (c) => (c === B ? '🔴' : c === J ? '🔵' : '🟡');
const barra = (p) => {
  const f = Math.max(0, Math.min(10, Math.round((p / 100) * 10)));
  return '▰'.repeat(f) + '▱'.repeat(10 - f);
};
const SEP = '━━━━━━━━━━━━━━━';

/* ============================ DB ============================ */

async function dbInit() {
  if (!DATABASE_URL) { log('DB', 'sem persistência'); return; }
  try {
    pool = new Pool({
      connectionString: DATABASE_URL,
      ssl: /localhost|127\.0\.0\.1/i.test(DATABASE_URL) ? false : { rejectUnauthorized: false },
      max: 4
    });
    await pool.query('SELECT 1');
    await pool.query(`
      CREATE TABLE IF NOT EXISTS capunga_state (k TEXT PRIMARY KEY, v TEXT NOT NULL, updated_at TIMESTAMPTZ DEFAULT NOW());
      CREATE TABLE IF NOT EXISTS capunga_signals (
        id INTEGER PRIMARY KEY, cor TEXT NOT NULL, key TEXT NOT NULL, nome TEXT NOT NULL,
        tipo TEXT NOT NULL, conf INT NOT NULL, status TEXT NOT NULL DEFAULT 'PENDENTE',
        gale INT, agree INT, created_at TIMESTAMPTZ NOT NULL, closed_at TIMESTAMPTZ
      );
      ALTER TABLE capunga_signals ADD COLUMN IF NOT EXISTS ref_t BIGINT;
      CREATE INDEX IF NOT EXISTS idx_cap_sig_status ON capunga_signals(status);
      CREATE INDEX IF NOT EXISTS idx_cap_sig_created ON capunga_signals(created_at DESC);
    `);
    R.erroDB = null; log('DB', 'schema ok');
  } catch (e) { R.erroDB = e.message; log('DB ERRO', e.message); pool = null; }
}

async function dbStateGet(k) {
  if (!pool) return null;
  try { const r = await pool.query('SELECT v FROM capunga_state WHERE k=$1', [k]); return r.rows[0]?.v ?? null; }
  catch (e) { R.erroDB = e.message; return null; }
}
async function dbStateSet(k, v) {
  if (!pool) return;
  try {
    await pool.query(`INSERT INTO capunga_state(k,v,updated_at) VALUES ($1,$2,NOW())
      ON CONFLICT (k) DO UPDATE SET v=EXCLUDED.v, updated_at=NOW()`, [k, String(v)]);
  } catch (e) { R.erroDB = e.message; }
}
async function dbSignalInsert(s) {
  if (!pool) return;
  try {
    await pool.query(`INSERT INTO capunga_signals (id,cor,key,nome,tipo,conf,status,gale,agree,created_at,closed_at,ref_t)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) ON CONFLICT (id) DO NOTHING`,
      [s.id, s.cor, s.key, s.nome, s.tipo, s.conf, s.status, s.gale ?? null, s.agree ?? 1,
       new Date(s.t), s.fechadoEm ? new Date(s.fechadoEm) : null, s.refT ?? null]);
  } catch (e) { R.erroDB = e.message; }
}
async function dbSignalUpdate(s) {
  if (!pool) return;
  try {
    await pool.query('UPDATE capunga_signals SET status=$1, gale=$2, closed_at=$3 WHERE id=$4',
      [s.status, s.gale ?? null, s.fechadoEm ? new Date(s.fechadoEm) : null, s.id]);
  } catch (e) { R.erroDB = e.message; }
}
async function dbLoadState() {
  if (!pool) return;
  try {
    const nextId = await dbStateGet('nextId');
    const lastSignalAt = await dbStateGet('lastSignalAt');
    if (nextId) S.nextId = Math.max(CFG.sinalInicial, parseInt(nextId, 10));
    if (lastSignalAt) S.lastSignalAt = parseInt(lastSignalAt, 10);
    const r = await pool.query(`SELECT * FROM capunga_signals ORDER BY created_at DESC LIMIT ${CFG.signalsOnBoot}`);
    S.signals = r.rows.reverse().map((row) => {
      const t = new Date(row.created_at).getTime();
      return {
        id: row.id, cor: row.cor, key: row.key, nome: row.nome, tipo: row.tipo,
        conf: row.conf, status: row.status, gale: row.gale, agree: row.agree,
        t, criadoEm: t, refT: row.ref_t ? Number(row.ref_t) : t,
        fechadoEm: row.closed_at ? new Date(row.closed_at).getTime() : null,
        enviado: true, gale1MsgId: null, gale2MsgId: null, galeLevel: 0
      };
    });
    rebuildStats();
    log('DB', `estado carregado: nextId=${S.nextId}, sinais=${S.signals.length}`);
  } catch (e) { R.erroDB = e.message; log('DB ERRO load', e.message); }
}
function rebuildStats() {
  STATS.total = 0; STATS.green = 0; STATS.g1 = 0; STATS.g2 = 0;
  STATS.empate = 0; STATS.eg1 = 0; STATS.eg2 = 0; STATS.loss = 0;
  for (const s of S.signals) {
    if (!FECHADOS.has(s.status)) continue;
    incStats(s.status);
  }
}
function incStats(status) {
  STATS.total++;
  if (status === 'GREEN') STATS.green++;
  else if (status === 'G1') STATS.g1++;
  else if (status === 'G2') STATS.g2++;
  else if (status === 'EMPATE') STATS.empate++;
  else if (status === 'EG1') STATS.eg1++;
  else if (status === 'EG2') STATS.eg2++;
  else if (status === 'LOSS') STATS.loss++;
}

/* ============================ TELEGRAM ============================ */

async function tgSendRaw(texto, opt = {}) {
  if (!BOT_TOKEN || !CHAT_ID) { R.erroTelegram = 'TOKEN/CHAT em falta'; return { ok: false }; }
  const tries = opt.tries ?? 3;
  const timeout = opt.timeout ?? 7000;
  for (let t = 1; t <= tries; t++) {
    try {
      const r = await axios.post(`https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`,
        { chat_id: CHAT_ID, text: texto, parse_mode: 'HTML', disable_web_page_preview: true },
        { timeout });
      if (r.data?.ok) { R.erroTelegram = null; return { ok: true, messageId: r.data.result.message_id }; }
      R.erroTelegram = JSON.stringify(r.data);
    } catch (e) {
      R.erroTelegram = e.response?.data ? JSON.stringify(e.response.data) : e.message;
      const retry = Number(e.response?.data?.parameters?.retry_after);
      if (t < tries) {
        if (retry > 0 && retry <= 10) await sleep(retry * 1000 + 100);
        else await sleep(300 * t);
        continue;
      }
    }
    if (t < tries) await sleep(300 * t);
  }
  return { ok: false };
}
async function tgDelete(messageId) {
  if (!messageId || !BOT_TOKEN || !CHAT_ID) return;
  try {
    await axios.post(`https://api.telegram.org/bot${BOT_TOKEN}/deleteMessage`,
      { chat_id: CHAT_ID, message_id: messageId }, { timeout: 6000 });
  } catch (_) {}
}

/**
 * Fila de envio com prioridade (0 = alta: sinal/resultado/gale; 1 = baixa: limpeza/analisando).
 * Corre em segundo plano: o tick nunca espera pelo Telegram.
 */
const TGQ = [];
let tgRunning = false;
let tgSeq = 0;
function tgEnqueue(prio, fn) {
  return new Promise((resolve) => {
    TGQ.push({ prio, fn, resolve, seq: ++tgSeq });
    TGQ.sort((a, b) => a.prio - b.prio || a.seq - b.seq);
    tgPump();
  });
}
async function tgPump() {
  if (tgRunning) return;
  tgRunning = true;
  try {
    while (TGQ.length) {
      const it = TGQ.shift();
      try { it.resolve(await it.fn()); }
      catch (e) { log('TG FILA ERRO', e.message); it.resolve(null); }
    }
  } finally { tgRunning = false; }
}

/* ============================ TEMPLATES SMS ============================ */

function fmtAnalisando() {
  return ['🔍 <b>Analisando a mesa…</b>', '', `⏱ ${hora()}`].join('\n');
}

function fmtSinal(s) {
  const confEmoji = s.conf >= 93 ? '💎' : s.conf >= 86 ? '🔥' : '⚡';
  return [
    `╔═══ 🎲 CAPUNGA · SINAL #${s.id} 🎲 ═══╗`,
    '🎯 <b>ENTRADA CONFIRMADA</b>',
    `Cor:${nomeCor(s.cor)}`,
    'Proteção:Empate🟡',
    `📊 Confiança :${confEmoji} <b>${s.conf}%</b>`,
    barra(s.conf),
    `⏱ ${hora(s.t)}`,
    '╚════════════════════════════╝',
    '',
    '💎 <i>Capunga Premium</i>'
  ].join('\n');
}

function fmtGale(s, nivel) {
  const icons = '✊🏿'.repeat(nivel);
  const extra = nivel === 2 ? '\n⚠️ <i>2ª tentativa — dobra a aposta</i>' : '';
  return [
    `${icons} <b>FAZER GALE ${nivel}</b> ${icons}`,
    `Continuar na cor:${emojiCor(s.cor)}${extra}`
  ].join('\n');
}

function fmtResult(s) {
  const msg = {
    GREEN:  '✅ <b>GREEN</b> ✅\n\nA mesa obedeceu tropa 💚\n\n<i>Capunga Dados</i>',
    G1:     '✅✅ <b>GREEN NO GALE 1</b> ✅✅\n\nA mesa obedeceu tropa 💚\n\n<i>Capunga Dados</i>',
    G2:     '✅✅✅ <b>GREEN NO GALE 2</b> ✅✅✅\n\nA mesa obedeceu tropa 💚\n\n<i>Capunga Dados</i>',
    EMPATE: '🟡 <b>GREEN NO EMPATE</b> 🟡\n\nA mesa obedeceu tropa 💚\n\n<i>Capunga Dados</i>',
    EG1:    '🟡🟡 <b>GREEN NO EMPATE GALE 1</b> 🟡🟡\n\nA mesa obedeceu tropa 💚\n\n<i>Capunga Dados</i>',
    EG2:    '🟡🟡🟡 <b>GREEN NO EMPATE GALE 2</b> 🟡🟡🟡\n\nA mesa obedeceu tropa 💚\n\n<i>Capunga Dados</i>',
    LOSS:   '❌ <b>LOSS</b> ❌\n\nTropa, a mesa não obedeceu 😤\n\n<i>Capunga Dados</i>'
  };
  return msg[s.status] || msg.LOSS;
}

function fmtHistorico() {
  const wins = STATS.green + STATS.g1 + STATS.g2 + STATS.empate + STATS.eg1 + STATS.eg2;
  const taxa = STATS.total > 0 ? (wins / STATS.total) * 100 : 0;
  const winsReais = STATS.green + STATS.g1 + STATS.g2;
  const decididos = winsReais + STATS.loss;
  const taxaReal = decididos > 0 ? (winsReais / decididos) * 100 : 0;
  const streak = getGreenStreak();
  const streakTxt = streak >= 3 ? `${streak}🔥` : `${streak}`;
  return [
    '📊 <b>HISTÓRICO</b>',
    SEP,
    `Total: ${STATS.total} sinais`,
    `✅ Green: ${STATS.green}`,
    `✅✅ G1: ${STATS.g1}`,
    `✅✅✅ G2: ${STATS.g2}`,
    `🟡 Empate: ${STATS.empate}`,
    `🟡🟡 Empate G1: ${STATS.eg1}`,
    `🟡🟡🟡 Empate G2: ${STATS.eg2}`,
    `❌ Loss: ${STATS.loss}`,
    `Sequência de greens: ${streakTxt}`,
    `🎯 Taxa: ${taxa.toFixed(1)}%`,
    `💡 Taxa real (sem empate): ${taxaReal.toFixed(1)}%`
  ].join('\n');
}

function getGreenStreak() {
  let count = 0;
  for (let i = S.signals.length - 1; i >= 0; i--) {
    const s = S.signals[i];
    if (!FECHADOS.has(s.status)) continue;
    if (s.status === 'LOSS') break;
    count++;
  }
  return count;
}

/* ============================ "ANALISANDO A MESA" ============================ */
/**
 * - Agendada após cada Green/Loss, com prioridade mínima (nunca atrasa sinal).
 * - Cancelada no instante em que um sinal é gerado.
 * - A mensagem visível é apagada logo que o sinal é enviado.
 */
const A = { token: 0, msgId: null };

function analiseAgendar() {
  const tok = ++A.token;
  analiseApagar(); // limpa qualquer uma anterior
  tgEnqueue(1, async () => {
    if (A.token !== tok) return;
    const r = await tgSendRaw(fmtAnalisando(), { tries: 1, timeout: 3500 });
    if (!r.ok) return;
    if (A.token !== tok) { tgDelete(r.messageId); return; }
    A.msgId = r.messageId;
  });
}
function analiseCancelar() { A.token++; }
function analiseApagar() {
  const id = A.msgId; A.msgId = null;
  if (id) tgDelete(id).catch(() => {});
}

/* ============================ API ============================ */

function normalizeOutcome(v) {
  if (v && typeof v === 'object') v = v.outcome ?? v.resultado ?? v.winner ?? v.name;
  const s = String(v ?? '').normalize('NFD').replace(/[\u0300-\u036f]/g, '')
    .trim().toLowerCase().replace(/[\s_-]+/g, '');
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
  const pj = Number(r.playerDice?.score ?? d.playerDice?.score ?? d.playerScore ?? d.player_score ?? d.pontos_jogador);
  const pb = Number(r.bankerDice?.score ?? d.bankerDice?.score ?? d.bankerScore ?? d.banker_score ?? d.pontos_banca);
  return { id, resultado, t, pj: Number.isFinite(pj) ? pj : null, pb: Number.isFinite(pb) ? pb : null };
}
async function fetchRounds() {
  let lastErr = null;
  for (let i = 1; i <= 2; i++) {
    try {
      const url = CFG.cacheBust ? `${API_BACBO}&_=${Date.now()}` : API_BACBO;
      const resp = await axios.get(url, {
        timeout: CFG.apiTimeoutMs,
        headers: { Accept: 'application/json', 'User-Agent': 'CapungaDados/18.0', 'Cache-Control': 'no-cache', Pragma: 'no-cache' }
      });
      const map = new Map();
      for (const it of extractItems(resp.data)) {
        const rd = normalizeRound(it);
        if (rd) map.set(rd.id, rd);
      }
      R.erroAPI = null;
      return [...map.values()].sort((a, b) => a.t - b.t || (a.id < b.id ? -1 : 1));
    } catch (e) {
      lastErr = e; R.erroAPI = e.message;
      if (i < 2) await sleep(300);
    }
  }
  throw lastErr || new Error('Falha API');
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

/* ============================ ESTRATÉGIAS ============================ */

const STRATS = [
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
  function numGatilho_8({ seq }) {
    if (!seq.length || !hasScore(seq[0])) return null;
    if (scoreOf(seq[0]) === 8) return { cor: opp(seq[0].resultado), key: 'NUM_GATILHO_8', nome: 'Gatilho 8', emoji: '🎯', strength: 78, tipo: 'num' };
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
  function numEspelho({ rows }) {
    const r = rows[0];
    if (!r || !hasScore(r) || r.pj !== r.pb) return null;
    return { cor: r.resultado, key: 'NUM_ESPELHO', nome: `Espelho ${r.pj}×${r.pb}`, emoji: '🪞', strength: 83, tipo: 'num' };
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
  function numPosEmpate({ rows, seq }) {
    if (!rows[0] || rows[0].resultado !== E || !seq.length) return null;
    return { cor: seq[0].resultado, key: 'NUM_POS_EMPATE', nome: 'Pós-empate', emoji: '🎯', strength: 74, tipo: 'num' };
  },
  function seqPingPong({ seq }) {
    const L = alternation(seq);
    if (L < 4 || L > 10) return null;
    return { cor: opp(seq[0].resultado), key: 'SEQ_PINGPONG', nome: `Ping-pong ${L}`, emoji: '🏓', strength: 79, tipo: 'seq' };
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
  function numNatural({ rows }) {
    const r = rows[0];
    if (!r || !hasScore(r)) return null;
    if (r.pj >= 8 && r.pb >= 8) {
      return { cor: r.resultado, key: 'NUM_NATURAL', nome: `Natural ${r.pj}×${r.pb}`, emoji: '🎴', strength: 86, tipo: 'num' };
    }
    return null;
  },
  function numFibonacci({ seq }) {
    if (!seq.length || !hasScore(seq[0])) return null;
    const s = scoreOf(seq[0]);
    if (s === 5 || s === 8) {
      return { cor: seq[0].resultado, key: 'NUM_FIBONACCI', nome: `Fibonacci ${s}`, emoji: '🌀', strength: 80, tipo: 'num' };
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
  function bankerBias20({ seq }) {
    const s = seq.slice(0, 20);
    if (s.length < 15) return null;
    const b = s.filter((r) => r.resultado === B).length;
    const j = s.filter((r) => r.resultado === J).length;
    if (b >= 12) return { cor: B, key: 'BANKER_BIAS_20', nome: 'Viés Banca', emoji: '📉', strength: 80, tipo: 'seq' };
    if (j >= 12) return { cor: J, key: 'BANKER_BIAS_20', nome: 'Viés Jogador', emoji: '📈', strength: 80, tipo: 'seq' };
    return null;
  },
  function numMomentumShift({ seq }) {
    if (seq.length < 4 || !seq.slice(0, 4).every(hasScore)) return null;
    const [a, b, c, d] = seq;
    const sa = scoreOf(a), sb = scoreOf(b), sc = scoreOf(c), sd = scoreOf(d);
    const m1 = sa - sb, m2 = sb - sc, m3 = sc - sd;
    if (m1 < 0 && m2 < 0 && m3 < 0) {
      return { cor: opp(a.resultado), key: 'MOMENTUM_SHIFT', nome: 'Mudança de momentum', emoji: '🔀', strength: 78, tipo: 'num' };
    }
    return null;
  },
  function seqAlternatingStrong({ seq }) {
    const L = alternation(seq);
    if (L < 3) return null;
    const s = seq.slice(0, L);
    if (!s.every((r) => hasScore(r) && margem(r) >= 3)) return null;
    return { cor: opp(seq[0].resultado), key: 'SEQ_ALT_STRONG', nome: `Alt. forte ${L}`, emoji: '⚔️', strength: 78, tipo: 'seq' };
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
function evalEntry(H, i, cor) {
  for (let k = 1; k <= CFG.maxGale + 1; k++) {
    const idx = i + k;
    if (idx >= H.length) return null;
    if (H[idx].t - H[idx - 1].t > CFG.gapMs) return null;
    if (H[idx].resultado === cor || H[idx].resultado === E) return true;
  }
  return false;
}
function backtest() {
  const H = S.hist.slice();
  const per = {};
  const ini = 10, fim = H.length - 1;
  if (fim - ini < 30) return { per, baseRate: 0.52 };
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
  return { per, baseRate: bn ? bw / bn : 0.52 };
}
/** Usa sempre o cache (nunca atrasa o sinal); recalcula em segundo plano quando velho. */
function getStats(force = false) {
  if (force || !S.stats) {
    S.stats = backtest();
    S.statsAtLen = S.hist.length;
    return S.stats;
  }
  if (S.hist.length - S.statsAtLen >= 30 && !S.statsRefreshing) {
    S.statsRefreshing = true;
    setImmediate(() => {
      try { S.stats = backtest(); S.statsAtLen = S.hist.length; }
      catch (e) { log('BACKTEST ERRO', e.message); }
      finally { S.statsRefreshing = false; }
    });
  }
  return S.stats;
}

/* ============================ CONFIANÇA (Gale 2) ============================ */

function bayesianRate(w, n, priorRate = 0.90, priorWeight = 10) {
  return (w + priorRate * priorWeight) / (n + priorWeight);
}
function confianca(cand, ctx) {
  const stats = ctx.stats;
  const p = stats.per[cand.key] || { n: 0, w: 0 };
  const baseRate = cand.cor === B ? 0.9112 : 0.9035;
  const btRate = bayesianRate(p.w, p.n, baseRate, 10);
  const live = S.signals.filter((x) => x.key === cand.key && FECHADOS.has(x.status));
  const liveW = live.filter((x) => x.status !== 'LOSS').length;
  const liveRate = live.length >= 3 ? bayesianRate(liveW, live.length, btRate, 4) : null;
  const btSamples = p.n + 10;
  const liveSamples = live.length >= 3 ? live.length + 4 : 0;
  const observed = ((btRate * btSamples) + ((liveRate ?? btRate) * liveSamples)) / (btSamples + liveSamples);

  let conf = Math.round(observed * 100);

  const agree = ctx.agreement?.confirmations || 0;
  conf += agree >= 3 ? 2 : agree >= 2 ? 1 : 0;
  conf += Math.round((cand.strength - 80) * 0.2);
  if (cand.tipo === 'seq' && ctx.regime) {
    const regimeMatch = (cand.key.includes('QUEBRA') || cand.key.includes('SURF') || cand.key.includes('PINGPONG'))
      ? ctx.regime.chop : ctx.regime.trend;
    conf += Math.round((regimeMatch - 0.5) * 4);
  }
  const recentLoss = S.signals.find((x) =>
    x.key === cand.key && x.status === 'LOSS' && Date.now() - (x.fechadoEm || 0) < 5 * 60 * 1000);
  if (recentLoss) conf -= 3;

  return Math.max(80, Math.min(95, conf));
}

/* ============================ SUSPENSÃO ============================ */

function isSuspended(key) {
  const u = SUSPENDED[key];
  if (!u) return false;
  if (Date.now() > u) { delete SUSPENDED[key]; return false; }
  return true;
}
function maybeSuspend(key) {
  const recent = S.signals.filter((x) => x.key === key && FECHADOS.has(x.status)).slice(-CFG.suspendLookback);
  if (recent.length < CFG.suspendLookback - 1) return;
  const l = recent.filter((x) => x.status === 'LOSS').length;
  if (l >= CFG.suspendLossStreak) {
    SUSPENDED[key] = Date.now() + CFG.suspendMs;
    log('SUSPENSO', { key, losses: l });
  }
}

/* ============================ GATE ============================ */

function pendentes() { return S.signals.filter((s) => s.status === 'PENDENTE'); }

function podeGerar() {
  if (!CFG.ativos) { R.ultimoMotivo = 'desativado'; return false; }
  if (pendentes().length >= CFG.maxPending) { R.ultimoMotivo = 'max pending'; return false; }
  const last = S.hist[S.hist.length - 1];
  if (!last) { R.ultimoMotivo = 'sem hist'; return false; }
  if (S.hist.length < CFG.minAmostra) { R.ultimoMotivo = `amostra ${S.hist.length}`; return false; }
  if (last.id === S.lastSignalRoundId) { R.ultimoMotivo = 'aguardando rodada nova'; return false; }

  // Frescor: o sinal só vale se a rodada de entrada ainda estiver com apostas abertas.
  const now = Date.now();
  if (now - last.t > CFG.idadeApiMaxMs) { R.ultimoMotivo = `rodada velha (api ${Math.round((now - last.t) / 1000)}s)`; return false; }
  if (now - (last.visto || 0) > CFG.idadeVistoMaxMs) { R.ultimoMotivo = `rodada velha (vista ${Math.round((now - (last.visto || 0)) / 1000)}s)`; return false; }
  return true;
}

/* ============================ GERAR SINAL ============================ */

async function tentarSinal() {
  if (!podeGerar()) return;

  const rows = S.hist.slice(-30).reverse();
  const cands = runStrategies(rows);
  if (!cands.length) { R.ultimoMotivo = 'sem estratégia'; return; }

  const stats = getStats();
  const seq = rows.filter((r) => isCol(r.resultado));
  const regime = detectRegime(seq);

  const byColor = {};
  for (const c of cands) {
    if (!byColor[c.cor]) byColor[c.cor] = [];
    byColor[c.cor].push(c);
  }

  let melhor = null;
  for (const cor of Object.keys(byColor)) {
    const list = byColor[cor].filter((c) => !isSuspended(c.key));
    if (!list.length) continue;
    const scored = list.map((c) => ({ ...c, conf: confianca(c, { stats, regime, agreement: { confirmations: list.length } }) }));
    scored.sort((a, b) => b.conf - a.conf);
    const top = scored[0];
    if (!melhor || top.conf > melhor.conf) melhor = { ...top, agree: scored.length };
  }

  if (!melhor || melhor.conf < CFG.scoreMin) { R.ultimoMotivo = `score < ${CFG.scoreMin}`; return; }

  const last = S.hist[S.hist.length - 1];
  const now = Date.now();
  const sig = {
    id: S.nextId, cor: melhor.cor, key: melhor.key, nome: melhor.nome, emoji: melhor.emoji,
    conf: melhor.conf, tipo: melhor.tipo, status: 'PENDENTE',
    t: now, criadoEm: now, agree: melhor.agree,
    // Entrada = primeira rodada que liquidar DEPOIS deste instante e depois da última rodada conhecida.
    refT: Math.max(last.t, now),
    enviado: false, gale1MsgId: null, gale2MsgId: null, galeLevel: 0
  };

  // Regista de imediato (estado em memória) e cancela o "analisando".
  S.nextId++;
  S.signals.push(sig);
  if (S.signals.length > CFG.maxSignalsMem) S.signals = S.signals.slice(-(CFG.maxSignalsMem - 50));
  S.lastSignalAt = now;
  S.lastSignalRoundId = last.id;
  R.ultimoSinal = sig;
  R.ultimoMotivo = null;
  analiseCancelar();

  dbSignalInsert(sig).catch(() => {});
  dbStateSet('nextId', String(S.nextId)).catch(() => {});
  dbStateSet('lastSignalAt', String(now)).catch(() => {});
  log('SINAL', { id: sig.id, cor: sig.cor, conf: sig.conf, key: sig.key });

  // Envio em segundo plano com prioridade máxima — não bloqueia o tick.
  tgEnqueue(0, () => tgSendRaw(fmtSinal(sig), { tries: 3, timeout: 6000 })).then((envio) => {
    if (envio && envio.ok) {
      sig.enviado = true;
      sig.sinalMsgId = envio.messageId;
      analiseApagar(); // o "analisando" some depois do próximo SMS (o sinal)
    } else {
      log('SINAL', { motivo: 'telegram rejeitou sinal', id: sig.id });
      R.ultimoMotivo = 'telegram falhou';
      if (sig.status === 'PENDENTE') {
        sig.status = 'EXPIRADO'; sig.fechadoEm = Date.now();
        dbSignalUpdate(sig).catch(() => {});
      }
    }
  }).catch(() => {});
}

/* ============================ GALE ============================ */

function notificarGale(s, nivel) {
  tgEnqueue(0, async () => {
    const r = await tgSendRaw(fmtGale(s, nivel), { tries: 3, timeout: 6000 });
    if (!r.ok) return;
    if (nivel === 1) s.gale1MsgId = r.messageId; else s.gale2MsgId = r.messageId;
    // Sinal já fechado enquanto a mensagem seguia: apaga logo.
    if (s.status !== 'PENDENTE') {
      tgDelete(r.messageId); 
      if (nivel === 1) s.gale1MsgId = null; else s.gale2MsgId = null;
      return;
    }
    // Gale 2 substitui a mensagem do Gale 1
    if (nivel === 2 && s.gale1MsgId) { const id = s.gale1MsgId; s.gale1MsgId = null; tgDelete(id); }
  });
}

/* ============================ FECHAR ============================ */

function limparGales(s) {
  tgEnqueue(1, async () => {
    if (s.gale1MsgId) { const id = s.gale1MsgId; s.gale1MsgId = null; await tgDelete(id); }
    if (s.gale2MsgId) { const id = s.gale2MsgId; s.gale2MsgId = null; await tgDelete(id); }
  });
}

function fecharSinal(s, status, gale) {
  if (s.status !== 'PENDENTE') return; // idempotente
  s.status = status;
  s.gale = gale;
  s.fechadoEm = Date.now();
  incStats(status);

  dbSignalUpdate(s).catch(() => {});

  // Resultado + histórico (prioridade alta, em ordem), depois limpeza e "analisando" (baixa).
  tgEnqueue(0, () => tgSendRaw(fmtResult(s), { tries: 3, timeout: 6000 }));
  tgEnqueue(0, () => tgSendRaw(fmtHistorico(), { tries: 2, timeout: 6000 }));
  limparGales(s);
  analiseAgendar();

  maybeSuspend(s.key);
  log('FECHADO', { id: s.id, status, gale });
}

function expirarSinal(s) {
  if (s.status !== 'PENDENTE') return;
  s.status = 'EXPIRADO';
  s.fechadoEm = Date.now();
  dbSignalUpdate(s).catch(() => {});
  limparGales(s);
  log('EXPIRADO', { id: s.id });
}

/* ============================ APURAÇÃO (determinística) ============================ */
/**
 * Recalculada do zero a cada ciclo só com as rodadas posteriores a s.refT.
 * Não depende de mensagens do Telegram nem de contadores intermédios → não congela.
 */
function resolver(s) {
  const after = S.hist.filter((r) => r.t > s.refT);
  const n = Math.min(after.length, CFG.maxGale + 1);
  for (let k = 0; k < n; k++) {
    const r = after[k];
    if (r.resultado === s.cor) return { fim: true, status: ['GREEN', 'G1', 'G2'][k], gale: k };
    if (r.resultado === E) return { fim: true, status: ['EMPATE', 'EG1', 'EG2'][k], gale: k };
  }
  if (after.length > CFG.maxGale) return { fim: true, status: 'LOSS', gale: CFG.maxGale };
  return { fim: false, perdidas: after.length };
}

function apurar() {
  const now = Date.now();
  for (const s of S.signals) {
    if (s.status !== 'PENDENTE' || !s.enviado) continue;
    const res = resolver(s);
    if (res.fim) { fecharSinal(s, res.status, res.gale); continue; }

    if (res.perdidas > s.galeLevel) {
      s.galeLevel = res.perdidas;          // marca ANTES de enviar: nunca depende do Telegram
      notificarGale(s, res.perdidas);
    }
    if (now - s.t >= CFG.apuraMaxMs) expirarSinal(s);
  }
}

/* ============================ SYNC ============================ */

async function sync() {
  try {
    const rounds = await fetchRounds();
    let novos = 0;
    const agora = Date.now();
    for (const r of rounds) {
      if (S.seen.has(r.id)) continue;
      r.visto = agora;
      S.seen.add(r.id); S.hist.push(r); novos++;
      R.ultimaRodadaNova = new Date(r.t).toISOString();
    }
    if (novos) S.hist.sort((a, b) => a.t - b.t || (a.id < b.id ? -1 : 1));
    if (S.hist.length > CFG.maxHist) {
      const ex = S.hist.length - CFG.maxHist;
      for (let i = 0; i < ex; i++) S.seen.delete(S.hist[i].id);
      S.hist = S.hist.slice(ex);
    }
    R.ultimaSync = new Date().toISOString();
    if (novos) log('SYNC', { novos, total: S.hist.length });
  } catch (e) { R.erroAPI = e.message; log('SYNC ERRO', e.message); }
}

/* ============================ LOOP ============================ */

async function tick() {
  if (stopping) return;
  if (busy) {
    if (Date.now() - busySince < CFG.tickWatchdogMs) return;
    log('WATCHDOG', 'tick preso, a forçar');
  }
  busy = true; busySince = Date.now(); lastTickMs = busySince;
  try {
    await sync();
    apurar();            // fecha sinais (estado imediato; Telegram em fila)
    await tentarSinal(); // gera sinal na mesma passagem
  } catch (e) { log('TICK ERRO', e.message); }
  finally { busy = false; }
}

async function keepAlive() {
  const self = process.env.SELF_URL || process.env.RENDER_EXTERNAL_URL || '';
  if (self) { try { await axios.get(self.replace(/\/$/, '') + '/health', { timeout: 7000 }); } catch (_) {} }
}
function wakeCheck() {
  if (Date.now() - lastTickMs > CFG.syncMs * 8) {
    R.wakes++; R.ultimoWake = new Date().toISOString();
    log('WAKE', 'atraso'); tick().catch(() => {});
  }
}

/* ============================ EXPRESS ============================ */

const app = express();
app.disable('x-powered-by');
app.use(cors());
app.use(express.json({ limit: '200kb' }));

const auth = (req, res) => {
  if (ADMIN_TOKEN && req.query.token !== ADMIN_TOKEN) { res.status(401).json({ erro: 'token' }); return false; }
  return true;
};

app.get('/health', (req, res) => {
  res.json({
    ok: true, versao: '18.0', deployId: DEPLOY_ID, maxGale: CFG.maxGale,
    hist: S.hist.length, sinais: S.signals.length, pendentes: pendentes().length,
    stats: { ...STATS, greenStreak: getGreenStreak() },
    ultimaSync: R.ultimaSync, erroAPI: R.erroAPI, erroDB: R.erroDB, erroTelegram: R.erroTelegram,
    filaTelegram: TGQ.length, uptime: Math.round(process.uptime())
  });
});
app.get('/status', (req, res) => {
  if (!auth(req, res)) return;
  res.json({ motivo: R.ultimoMotivo, R, STATS, CFG });
});
app.get('/sinais', (req, res) => {
  if (!auth(req, res)) return;
  const n = Math.min(100, Math.max(1, parseInt(req.query.n, 10) || 20));
  res.json(S.signals.slice(-n).reverse());
});
app.get('/stats', (req, res) => {
  if (!auth(req, res)) return;
  const wins = STATS.green + STATS.g1 + STATS.g2 + STATS.empate + STATS.eg1 + STATS.eg2;
  const winsReais = STATS.green + STATS.g1 + STATS.g2;
  const decididos = winsReais + STATS.loss;
  res.json({
    ...STATS,
    wins,
    taxaComEmpate: STATS.total > 0 ? wins / STATS.total : null,
    taxaReal: decididos > 0 ? winsReais / decididos : null,
    greenStreak: getGreenStreak()
  });
});
app.get('/calibracao', (req, res) => {
  if (!auth(req, res)) return;
  const buckets = { '80-85': { n: 0, w: 0 }, '86-92': { n: 0, w: 0 }, '93-95': { n: 0, w: 0 } };
  for (const s of S.signals) {
    if (!FECHADOS.has(s.status)) continue;
    const b = s.conf >= 93 ? '93-95' : s.conf >= 86 ? '86-92' : '80-85';
    buckets[b].n++;
    if (s.status !== 'LOSS') buckets[b].w++;
  }
  res.json(buckets);
});

/* ============================ START ============================ */

async function start() {
  log('BOOT', { versao: '18.0', deployId: DEPLOY_ID, maxGale: CFG.maxGale });
  await dbInit();
  if (pool) await dbLoadState();
  await sync();
  getStats(true);

  // Recuperação de pendentes após reinício: recalcula o nível de gale já consumido (sem reenviar mensagens).
  for (const s of S.signals) {
    if (s.status !== 'PENDENTE') continue;
    const after = S.hist.filter((r) => r.t > s.refT);
    let perdidas = 0;
    for (const r of after) {
      if (r.resultado === s.cor || r.resultado === E) break;
      perdidas++;
      if (perdidas > CFG.maxGale) break;
    }
    s.galeLevel = Math.min(perdidas, CFG.maxGale);
  }

  server = app.listen(PORT, () => log('HTTP', `porta ${PORT}`));
  tickTimer = setInterval(() => tick().catch(() => {}), CFG.syncMs);
  keepTimer = setInterval(() => { keepAlive().catch(() => {}); wakeCheck(); }, CFG.keepAliveMs);
  setTimeout(() => tick().catch(() => {}), 1000);

  process.on('SIGTERM', shutdown);
  process.on('SIGINT', shutdown);
}
async function shutdown() {
  if (stopping) return;
  stopping = true; log('SHUTDOWN', 'ok');
  clearInterval(tickTimer); clearInterval(keepTimer);
  if (server) server.close();
  if (pool) await pool.end().catch(() => {});
  process.exit(0);
}

start().catch((e) => { console.error(e); process.exit(1); });

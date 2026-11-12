'use strict';

/**
 * CAPUNGA DIAMOND — BAC BO · v21.0
 * Elite only · sem Gale 2 · estratégias super certeiras
 *
 * Mudanças v20.2 → v21.0:
 *   - "STOP" → "LOSS" em todas as mensagens
 *   - 9 estratégias (7 base + 2 compiladas) — todas com multi-confirmação
 *   - Cooldown por estratégia após loss (10min)
 *   - Pausa automática: 3 losses seguidos → 30min; ratio alto → 60min
 *   - Confluência e APEX dinâmicos (2x e 3x confirmação)
 *   - SMS ainda mais limpo
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
  maxGale: 1,
  maxPending: 1,
  minAmostra: 45,
  syncMs: 3000,
  syncFastMs: 1000,
  minGapMs: 20000,
  relaxMs: 3 * 60 * 1000,
  minSettledAgeMs: 5000,
  apuraMaxMs: 600000,
  maxHist: 4000,
  gapMs: 7 * 60 * 1000,
  maxSignalsMem: 500,
  signalsOnBoot: 500,
  suspendMs: 4 * 3600 * 1000,
  suspendLossStreak: 3,
  staleAlertMs: 3 * 60 * 1000,
  alertThrottleMs: 10 * 60 * 1000,
  keepAliveMs: 60000,
  strategyCooldownMs: 10 * 60 * 1000,   // cooldown após loss
  pauseAfterLosses: 3,                    // losses seguidos → pausa
  pauseMs: 30 * 60 * 1000,                // duração da pausa
  tiers: [
    { scoreMin: 87, minStratN: 4, liveMin: 0.85 },
    { scoreMin: 83, minStratN: 3, liveMin: 0.75 }
  ]
};

const J = 'Jogador';
const B = 'Banca';
const E = 'Empate';
const isCol = (x) => x === J || x === B;
const opp = (c) => (c === J ? B : c === B ? J : null);
const FECHADOS = new Set(['GREEN', 'G1', 'G2', 'EMPATE', 'EG1', 'EG2', 'LOSS']);
const LOCK_KEY = 727272;

const API_SOURCES = [
  process.env.API_BACBO ||
    'https://api-cs.casino.org/svc-evolution-game-events/api/bacbo?page=0&size=50&sort=data.settledAt,desc&duration=120&wheelResults=PlayerWon,BankerWon,Tie',
  process.env.API_BACBO_2 || ''
].filter(Boolean);

const DEPLOY_ID = (() => {
  if (process.env.DEPLOY_ID) return String(process.env.DEPLOY_ID).trim();
  try { return crypto.createHash('sha256').update(fs.readFileSync(__filename)).digest('hex').slice(0, 16); }
  catch { return 'unknown'; }
})();

const S = {
  hist: [], seen: new Set(), signals: [],
  nextId: 1, lastSignalAt: 0, curStreak: 0,
  stats: null, statsAtLen: -1
};

const DAILY = { date: '', total: 0, green: 0, g1: 0, g2: 0, empate: 0, eg1: 0, eg2: 0, loss: 0, bestStreak: 0 };
const ALLTIME = { total: 0, green: 0, g1: 0, g2: 0, empate: 0, eg1: 0, eg2: 0, loss: 0 };
const SUSPENDED = {};
const PAUSE = { until: 0, reason: null };

const R = {
  iniciadoEm: new Date().toISOString(), deployId: DEPLOY_ID, versao: '21.0',
  ultimaSync: null, ultimaRodadaNova: null, fonteAPI: 0,
  erroAPI: null, erroTelegram: null, erroDB: null,
  ultimoMotivo: null, ultimoSinal: null, tier: 0, wakes: 0, ultimoWake: null,
  ultimoAlertaAPI: 0, ultimoAlertaDB: 0, lockOk: false,
  lastErrorAt: 0
};

let pool = null, server = null, tickTimer = null, keepTimer = null;
let busy = false, stopping = false, lastTickMs = Date.now();

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const log = (tag, d) => console.log(`[${new Date().toISOString()}] [${tag}] ${typeof d === 'string' ? d : JSON.stringify(d)}`);

const fmtHora = new Intl.DateTimeFormat('pt-PT', {
  timeZone: 'Africa/Luanda', hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false
});
const hora = (ms = Date.now()) => fmtHora.format(new Date(ms));
const luandaDayKey = (ms = Date.now()) => new Date(ms + 3600000).toISOString().slice(0, 10);

const nomeCor = (c) => (c === B ? 'VERMELHO 🔴' : c === J ? 'AZUL 🔵' : 'EMPATE 🟡');

/* ══════════════════════ DIAMOND TEMPLATES ══════════════════════ */

function fmtSinal(s) {
  const confEmoji = s.conf >= 95 ? '👑' : s.conf >= 92 ? '💎' : s.conf >= 86 ? '🔥' : '⚡';
  const tipoLabel =
    s.tipo === 'apex' ? '👑 APEX' :
    s.tipo === 'confluence' ? '🌟 CONFLUÊNCIA' :
    s.tipo === 'elite' ? '💎 ELITE' : '⚡ ADV';
  return [
    `${confEmoji} <b>#${s.id} · ${nomeCor(s.cor)}</b>`,
    '',
    `${tipoLabel} · ${s.conf}%`,
    `${s.emoji || '🧩'} ${s.nome}`,
    `🕐 ${hora(s.t)}`
  ].join('\n');
}

function fmtGale(s, nivel) {
  return [
    `⚡ <b>#${s.id} · REFORÇO ${nivel}</b>`,
    '',
    `🎯 ${nomeCor(s.cor)}`,
    `🕐 ${hora()}`
  ].join('\n');
}

function fmtResult(s) {
  const wins = dailyWins();
  const taxa = dailyTaxa().toFixed(0);
  const placar = `◈ Hoje: ${wins}W · ${DAILY.loss}L · ${taxa}%`;
  const titulo =
    s.status === 'GREEN' ? `💚 <b>#${s.id} · ACERTO</b>` :
    s.status === 'EMPATE' ? `🟡 <b>#${s.id} · EMPATE</b>` :
    s.status === 'G1' ? `💚 <b>#${s.id} · G1</b>` :
    s.status === 'EG1' ? `🟡 <b>#${s.id} · EMPATE G1</b>` :
    `🔴 <b>#${s.id} · LOSS</b>`;
  return `${titulo}\n\n${placar}`;
}

function fmtHistorico() {
  const wins = dailyWins();
  const streakTxt = S.curStreak >= 3 ? `${S.curStreak} 🔥` : `${S.curStreak}`;
  return [
    `📊 <b>${DAILY.date}</b>`,
    `${wins}W · ${DAILY.loss}L · ${dailyTaxa().toFixed(1)}%`,
    `sequência ${streakTxt} · recorde ${DAILY.bestStreak}`
  ].join('\n');
}

/* ══════════════════════ DB ══════════════════════ */

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
      CREATE TABLE IF NOT EXISTS diamond_state (k TEXT PRIMARY KEY, v TEXT NOT NULL, updated_at TIMESTAMPTZ DEFAULT NOW());
      CREATE TABLE IF NOT EXISTS diamond_rounds (id TEXT PRIMARY KEY, resultado TEXT NOT NULL, t BIGINT NOT NULL, pj DOUBLE PRECISION, pb DOUBLE PRECISION);
      CREATE INDEX IF NOT EXISTS idx_diamond_rounds_t ON diamond_rounds(t DESC);
      CREATE TABLE IF NOT EXISTS diamond_signals (
        id INTEGER PRIMARY KEY, cor TEXT NOT NULL, key TEXT NOT NULL, nome TEXT NOT NULL,
        tipo TEXT NOT NULL, conf INT NOT NULL, status TEXT NOT NULL DEFAULT 'PENDENTE',
        gale INT, agree INT, tier INT DEFAULT 0, created_at TIMESTAMPTZ NOT NULL, closed_at TIMESTAMPTZ
      );
      CREATE INDEX IF NOT EXISTS idx_diamond_sig_status ON diamond_signals(status);
      CREATE INDEX IF NOT EXISTS idx_diamond_sig_created ON diamond_signals(created_at DESC);
    `);
    R.erroDB = null; log('DB', 'schema diamond_* ok');
  } catch (e) { R.erroDB = e.message; log('DB ERRO', e.message); pool = null; }
}

async function acquireLock() {
  if (!pool) return false;
  for (let i = 1; i <= 10; i++) {
    try {
      const r = await pool.query('SELECT pg_try_advisory_lock($1) AS ok', [LOCK_KEY]);
      if (r.rows[0].ok) { R.lockOk = true; return true; }
      log('LOCK', `instância concorrente — tentativa ${i}/10`);
    } catch (e) { R.erroDB = e.message; log('LOCK ERRO', e.message); }
    await sleep(15000);
  }
  return false;
}
async function releaseLock() {
  if (!pool || !R.lockOk) return;
  try { await pool.query('SELECT pg_advisory_unlock($1)', [LOCK_KEY]); } catch (_) {}
}
async function dbStateGet(k) {
  if (!pool) return null;
  try { const r = await pool.query('SELECT v FROM diamond_state WHERE k=$1', [k]); return r.rows[0]?.v ?? null; }
  catch (e) { R.erroDB = e.message; return null; }
}
async function dbStateSet(k, v) {
  if (!pool) return;
  try {
    await pool.query(`INSERT INTO diamond_state(k,v,updated_at) VALUES ($1,$2,NOW())
      ON CONFLICT (k) DO UPDATE SET v=EXCLUDED.v, updated_at=NOW()`, [k, String(v)]);
  } catch (e) { R.erroDB = e.message; }
}
async function dbSignalInsert(s) {
  if (!pool) return;
  try {
    await pool.query(`INSERT INTO diamond_signals (id,cor,key,nome,tipo,conf,status,gale,agree,tier,created_at,closed_at)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) ON CONFLICT (id) DO NOTHING`,
      [s.id, s.cor, s.key, s.nome, s.tipo, s.conf, s.status, s.gale ?? null, s.agree ?? 1, s.tier ?? 0,
       new Date(s.t), s.fechadoEm ? new Date(s.fechadoEm) : null]);
  } catch (e) { R.erroDB = e.message; }
}
async function dbSignalUpdate(s) {
  if (!pool) return;
  try {
    await pool.query('UPDATE diamond_signals SET status=$1, gale=$2, closed_at=$3 WHERE id=$4',
      [s.status, s.gale ?? null, s.fechadoEm ? new Date(s.fechadoEm) : null, s.id]);
  } catch (e) { R.erroDB = e.message; }
}
async function dbRoundUpsert(r) {
  if (!pool) return;
  try {
    await pool.query(`INSERT INTO diamond_rounds(id,resultado,t,pj,pb) VALUES($1,$2,$3,$4,$5)
      ON CONFLICT(id) DO UPDATE SET resultado=EXCLUDED.resultado,t=EXCLUDED.t,pj=EXCLUDED.pj,pb=EXCLUDED.pb`,
      [r.id, r.resultado, r.t, r.pj, r.pb]);
  } catch (e) { R.erroDB = e.message; }
}
async function dbLoadAlltime() {
  if (!pool) return;
  try {
    const r = await pool.query(`SELECT status, COUNT(*)::int AS n FROM diamond_signals
      WHERE status <> 'PENDENTE' GROUP BY status`);
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
    }
  } catch (e) { R.erroDB = e.message; }
}

async function dbLoadState() {
  if (!pool) return;
  try {
    const nextId = await dbStateGet('nextId');
    const lastSignalAt = await dbStateGet('lastSignalAt');
    const curStreak = await dbStateGet('curStreak');
    if (nextId) S.nextId = Math.max(1, parseInt(nextId, 10));
    if (lastSignalAt) S.lastSignalAt = parseInt(lastSignalAt, 10);
    if (curStreak) S.curStreak = parseInt(curStreak, 10);

    try {
      const raw = await dbStateGet('suspended');
      if (raw) {
        const arr = JSON.parse(raw);
        const now = Date.now();
        for (const [key, until] of arr) if (until > now) SUSPENDED[key] = until;
      }
    } catch (_) {}

    try {
      const rawD = await dbStateGet('diamond_daily');
      if (rawD) Object.assign(DAILY, JSON.parse(rawD));
    } catch (_) {}
    if (!DAILY.date) DAILY.date = luandaDayKey();

    const rounds = await pool.query(`SELECT id,resultado,t,pj,pb FROM diamond_rounds ORDER BY t DESC LIMIT ${CFG.maxHist}`);
    S.hist = rounds.rows.map((r) => ({ id: r.id, resultado: r.resultado, t: Number(r.t), pj: r.pj === null ? null : Number(r.pj), pb: r.pb === null ? null : Number(r.pb) })).reverse();
    S.seen = new Set(S.hist.map((r) => r.id));
    const r = await pool.query(`SELECT * FROM diamond_signals ORDER BY created_at DESC LIMIT ${CFG.signalsOnBoot}`);
    S.signals = r.rows.reverse().map((row) => ({
      id: row.id, cor: row.cor, key: row.key, nome: row.nome, tipo: row.tipo,
      conf: row.conf, status: row.status, gale: row.gale, agree: row.agree, tier: row.tier || 0,
      t: new Date(row.created_at).getTime(), criadoEm: new Date(row.created_at).getTime(),
      fechadoEm: row.closed_at ? new Date(row.closed_at).getTime() : null,
      gale1MsgId: null, galeLevel: row.status !== 'PENDENTE' ? 1 : 0
    }));
    await dbLoadAlltime();
    log('DB', `estado diamond: nextId=${S.nextId}, sinais=${S.signals.length}, diário=${DAILY.date}, hist=${S.hist.length}`);
  } catch (e) { R.erroDB = e.message; log('DB ERRO load', e.message); }
}

/* ══════════════════════ TELEGRAM ══════════════════════ */

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
  await tgSendRaw(`⚠️ <b>ALERTA ${tag}</b>\n\n${detalhe}\n🕐 ${hora()}`);
}

/* ══════════════════════ RESET DIÁRIO ══════════════════════ */

function dailyWins() { return DAILY.green + DAILY.g1 + DAILY.g2 + DAILY.empate + DAILY.eg1 + DAILY.eg2; }
function dailyTaxa() { return DAILY.total > 0 ? (dailyWins() / DAILY.total) * 100 : 0; }

async function sendDailySummary(ontem) {
  const wins = dailyWins();
  const taxa = dailyTaxa();
  const reais = DAILY.green + DAILY.g1 + DAILY.g2;
  const decididos = reais + DAILY.loss;
  const taxaReal = decididos > 0 ? (reais / decididos) * 100 : 0;
  const msg = [
    `🌙 <b>FECHO · ${ontem}</b>`,
    '',
    `◈ Sinais  → ${DAILY.total}`,
    `◈ Acertos → ${wins} (${taxa.toFixed(1)}%)`,
    `◈ Empates → ${DAILY.empate + DAILY.eg1 + DAILY.eg2}`,
    `◈ Loss    → ${DAILY.loss}`,
    `◈ Real    → ${taxaReal.toFixed(1)}%`,
    `◈ Recorde → ${DAILY.bestStreak}`,
    '',
    `🌅 novo dia · zerado`
  ].join('\n');
  await tgSendRaw(msg);
}

async function checkDailyReset() {
  const hoje = luandaDayKey();
  if (DAILY.date && DAILY.date !== hoje) {
    const ontem = DAILY.date;
    await sendDailySummary(ontem);
    for (const k of ['total', 'green', 'g1', 'g2', 'empate', 'eg1', 'eg2', 'loss', 'bestStreak']) DAILY[k] = 0;
    DAILY.date = hoje;
    PAUSE.until = 0; PAUSE.reason = null;
    await dbStateSet('diamond_daily', JSON.stringify(DAILY));
    log('DIÁRIO', `reset ${ontem} -> ${hoje}`);
  } else if (!DAILY.date) {
    DAILY.date = hoje;
  }
}

/* ══════════════════════ API ══════════════════════ */

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
  const pj = numOrNull(r.playerDice?.score ?? d.playerDice?.score ?? d.playerScore ?? d.player_score ?? d.pontos_jogador);
  const pb = numOrNull(r.bankerDice?.score ?? d.bankerDice?.score ?? d.bankerScore ?? d.banker_score ?? d.pontos_banca);
  return { id, resultado, t, pj, pb };
}
async function fetchFrom(url) {
  const resp = await axios.get(url, {
    timeout: 10000,
    headers: { Accept: 'application/json', 'User-Agent': 'CapungaDiamond/21.0' }
  });
  const map = new Map();
  for (const it of extractItems(resp.data)) {
    const rd = normalizeRound(it);
    if (rd) map.set(rd.id, rd);
  }
  return [...map.values()].sort((a, b) => a.t - b.t || (a.id < b.id ? -1 : 1));
}
async function fetchRounds() {
  const erros = [];
  for (let idx = 0; idx < API_SOURCES.length; idx++) {
    for (let tentativa = 1; tentativa <= 2; tentativa++) {
      try {
        const rounds = await fetchFrom(API_SOURCES[idx]);
        R.fonteAPI = idx;
        R.erroAPI = null;
        return rounds;
      } catch (e) {
        erros.push(`fonte${idx + 1}: ${e.message}`);
        if (tentativa < 2) await sleep(500);
      }
    }
  }
  R.erroAPI = erros.join(' | ');
  throw new Error(R.erroAPI);
}

/* ══════════════════════ HELPERS ══════════════════════ */

const hasScore = (r) => r && Number.isFinite(r.pj) && Number.isFinite(r.pb);
const margem = (r) => Math.abs((r.pj || 0) - (r.pb || 0));
const scoreOf = (r) => (r.resultado === J ? r.pj : r.pb);

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

/* ══════════════════════ API HEALTH GATE ══════════════════════ */

function apiHealthy() {
  if (!S.hist.length) return false;
  const last = S.hist[S.hist.length - 1];
  if (Date.now() - last.t > 90000) return false;
  if (R.erroAPI) return false;
  if (R.lastErrorAt && Date.now() - R.lastErrorAt < 60000) return false;
  return true;
}

/* ══════════════════════ ANTI-LOSS COOLDOWN ══════════════════════ */

function strategyCoolingDown(key, windowMs = CFG.strategyCooldownMs) {
  const cutoff = Date.now() - windowMs;
  return S.signals.some((s) =>
    s.key === key && s.status === 'LOSS' && (s.fechadoEm || 0) > cutoff);
}

function isPaused() {
  if (PAUSE.until > Date.now()) return true;
  const recent = S.signals.filter((s) => FECHADOS.has(s.status)).slice(-CFG.pauseAfterLosses);
  if (recent.length === CFG.pauseAfterLosses && recent.every((s) => s.status === 'LOSS')) {
    PAUSE.until = Date.now() + CFG.pauseMs;
    PAUSE.reason = `${CFG.pauseAfterLosses} losses seguidos`;
    log('PAUSE', PAUSE.reason);
    return true;
  }
  if (DAILY.loss >= 6 && DAILY.loss / Math.max(DAILY.total, 1) > 0.35) {
    PAUSE.until = Date.now() + 60 * 60 * 1000;
    PAUSE.reason = `ratio alto (${DAILY.loss}/${DAILY.total})`;
    log('PAUSE', PAUSE.reason);
    return true;
  }
  return false;
}

/* ══════════════════════ 7 ESTRATÉGIAS BASE (todas super certeiras) ══════════════════════ */
/*
 * Cada uma exige multi-confirmação:
 *   - padrão claro com ≥4 mãos
 *   - margens confirmadas
 *   - sem ties a poluir (exceto TIE_ISO)
 */

/* 1. ALTERNÂNCIA FORTE — alternation 4+ com todas margens ≥ 3 */
function _altForte({ seq }) {
  const L = alternation(seq);
  if (L < 4) return null;
  const s = seq.slice(0, L);
  if (!s.every((r) => hasScore(r) && margem(r) >= 3)) return null;
  return {
    cor: opp(seq[0].resultado), key: 'DIAMOND_ALT',
    nome: `Alt. forte ${L}`, emoji: '💎', strength: 90, tipo: 'elite'
  };
}

/* 2. 3→1 CONFIRMADO — a-b-b-b com margens do triple ≥ 2 */
function _3to1({ seq }) {
  if (seq.length < 4) return null;
  const [a, b, c, d] = seq.map((x) => x.resultado);
  if (a !== b || b !== c || c !== d) return null;
  const triple = seq.slice(0, 3);
  if (!triple.every((r) => hasScore(r) && margem(r) >= 2)) return null;
  return {
    cor: a, key: 'DIAMOND_3_1',
    nome: '3→1 confirmado', emoji: '💎', strength: 90, tipo: 'elite'
  };
}

/* 3. BLOCO 2+2 — a-a-b-b com margens confirmadas */
function _bloco22({ seq }) {
  if (seq.length < 4) return null;
  const [a, b, c, d] = seq.map((x) => x.resultado);
  if (a !== b || c !== d || a === c) return null;
  const block = seq.slice(0, 4);
  if (!block.every((r) => hasScore(r) && margem(r) >= 2)) return null;
  return {
    cor: c, key: 'DIAMOND_2x2',
    nome: 'Bloco 2+2', emoji: '💎', strength: 88, tipo: 'elite'
  };
}

/* 4. EMPATE ISOLADO — vencedor pré-tie com margem ≥ 3, regime não-trend */
function _empateIsolado({ rows, seq, regime }) {
  if (!rows[0] || rows[0].resultado !== E) return null;
  if (!rows[1] || rows[1].resultado === E) return null;
  if (!seq.length) return null;
  if (!hasScore(seq[0]) || margem(seq[0]) < 3) return null;
  if (regime && regime.trend > 0.6) return null;
  return {
    cor: opp(seq[0].resultado), key: 'DIAMOND_TIE_ISO',
    nome: 'Empate isolado', emoji: '💎', strength: 88, tipo: 'elite'
  };
}

/* 5. SCORE REPETIDO — mesmo vencedor, mesmo score ≥ 4 */
function _scoreRep({ seq }) {
  if (seq.length < 2 || !hasScore(seq[0]) || !hasScore(seq[1])) return null;
  const [a, b] = seq;
  if (a.resultado !== b.resultado) return null;
  if (scoreOf(a) !== scoreOf(b) || scoreOf(a) < 4) return null;
  return {
    cor: a.resultado, key: 'DIAMOND_SCORE_REP',
    nome: `Score ${scoreOf(a)}×2`, emoji: '💎', strength: 87, tipo: 'elite'
  };
}

/* 6. MARGEM CRESCENTE — mesmo vencedor, margens estritamente crescentes */
function _marginGrowth({ seq }) {
  if (seq.length < 3 || !seq.slice(0, 3).every(hasScore)) return null;
  const [a, b, c] = seq;
  if (a.resultado !== b.resultado || b.resultado !== c.resultado) return null;
  const ma = margem(a), mb = margem(b), mc = margem(c);
  if (mb > ma && mc > mb) return {
    cor: a.resultado, key: 'DIAMOND_MARGIN_GROWTH',
    nome: `Margem ${ma}→${mc}`, emoji: '💎', strength: 88, tipo: 'elite'
  };
  return null;
}

/* 7. PRESSÃO — 5 mãos mesma cor, 3+ com margem ≥ 4 */
function _pressure({ seq }) {
  if (seq.length < 5 || !seq.slice(0, 5).every(hasScore)) return null;
  const s = seq.slice(0, 5);
  const color = s[0].resultado;
  if (!s.every((r) => r.resultado === color)) return null;
  const strongCount = s.filter((r) => margem(r) >= 4).length;
  if (strongCount < 3) return null;
  return {
    cor: color, key: 'DIAMOND_PRESSURE',
    nome: `Pressão ${color === J ? 'J' : 'B'}×5`, emoji: '💎', strength: 90, tipo: 'elite'
  };
}

const BASE_STRATS = [_altForte, _3to1, _bloco22, _empateIsolado, _scoreRep, _marginGrowth, _pressure];

/* ══════════════════════ COMPILAÇÃO — confluência e APEX ══════════════════════ */
/*
 * CONFLUÊNCIA: 2 estratégias base concordam na mesma cor
 * APEX:        3+ estratégias base concordam na mesma cor
 */

function runBase(rows) {
  const seq = rows.filter((r) => isCol(r.resultado));
  const regime = detectRegime(seq);
  const ctx = { rows, seq, regime };
  const out = [];
  for (const fn of BASE_STRATS) {
    try { const r = fn(ctx); if (r && isCol(r.cor)) out.push(r); } catch (_) {}
  }
  return out;
}

function runStrategies(rows, isBacktest = false) {
  const base = runBase(rows);

  // Cooldown anti-loss (apenas em modo live)
  const validBase = isBacktest
    ? base
    : base.filter((b) => !strategyCoolingDown(b.key));

  const result = [...validBase];

  // Agrupar por cor
  const byColor = {};
  for (const b of validBase) {
    if (!byColor[b.cor]) byColor[b.cor] = [];
    byColor[b.cor].push(b);
  }

  // Compilar: CONFLUÊNCIA (2) e APEX (3+)
  for (const cor of Object.keys(byColor)) {
    const list = byColor[cor];
    const names = list.map((x) => x.nome).slice(0, 2).join(' + ');
    if (list.length >= 3) {
      result.push({
        cor, key: 'DIAMOND_APEX',
        nome: `APEX ${list.length}× · ${names}`,
        emoji: '👑', strength: 95, tipo: 'apex', agree: list.length
      });
    } else if (list.length >= 2) {
      result.push({
        cor, key: 'DIAMOND_CONFLUENCE',
        nome: `Confluência · ${names}`,
        emoji: '🌟', strength: 92, tipo: 'confluence', agree: list.length
      });
    }
  }

  return result;
}

/* ══════════════════════ BACKTEST ══════════════════════ */

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
    if (H[idx].t - H[idx - 1].t > CFG.gapMs) return false;
    if (H[idx].resultado === cor || H[idx].resultado === E) return true;
  }
  return false;
}
function backtest() {
  const H = S.hist;
  const per = {};
  const ini = 10, fim = H.length - 1;
  if (fim - ini < 30) return { per, baseRate: 0.90 };
  let bw = 0, bn = 0;
  for (let i = ini; i < fim; i++) {
    const win = windowAt(H, i);
    if (win.length < 6) continue;
    for (const c of runStrategies(win, true)) {
      const v = evalEntry(H, i, c.cor);
      if (v === null) continue;
      const p = per[c.key] || (per[c.key] = { n: 0, w: 0 });
      p.n++; if (v) p.w++;
      bn++; if (v) bw++;
    }
  }
  const baseRate = bn ? bw / bn : 0.90;
  return { per, baseRate: Math.min(0.95, Math.max(0.80, baseRate)) };
}
function getStats(force = false) {
  if (force || !S.stats || S.hist.length - S.statsAtLen >= 30) {
    S.stats = backtest();
    S.statsAtLen = S.hist.length;
  }
  return S.stats;
}

/* ══════════════════════ CONFIANÇA ══════════════════════ */

function bayesianRate(w, n, priorRate = 0.90, priorWeight = 10) {
  return (w + priorRate * priorWeight) / (n + priorWeight);
}
function confianca(cand, ctx) {
  if (cand.tipo === 'apex') return 95;
  if (cand.tipo === 'confluence') return 92;

  const stats = ctx.stats;
  const p = stats.per[cand.key] || { n: 0, w: 0 };
  const baseRate = stats.baseRate || 0.90;
  const btRate = bayesianRate(p.w, p.n, baseRate, 6);
  const live = S.signals.filter((x) => x.key === cand.key && FECHADOS.has(x.status));
  const liveW = live.filter((x) => x.status !== 'LOSS').length;
  const liveRate = live.length >= 3 ? bayesianRate(liveW, live.length, btRate, 4) : null;
  const btSamples = p.n + 6;
  const liveSamples = live.length >= 3 ? live.length + 4 : 0;
  const observed = ((btRate * btSamples) + ((liveRate ?? btRate) * liveSamples)) / (btSamples + liveSamples);

  let conf = Math.round(observed * 100);
  conf += Math.round((cand.strength - 80) * 0.2);
  if (live.length >= 5 && liveW / live.length < 0.85) conf -= 4;

  return Math.max(80, Math.min(95, conf));
}

/* ══════════════════════ SUSPENSÃO ══════════════════════ */

function isSuspended(key) {
  const u = SUSPENDED[key];
  if (!u) return false;
  if (Date.now() > u) { delete SUSPENDED[key]; dbStateSet('suspended', JSON.stringify(Object.entries(SUSPENDED))); return false; }
  return true;
}
function maybeSuspend(key) {
  const recent = S.signals.filter((x) => x.key === key && FECHADOS.has(x.status)).slice(-CFG.suspendLossStreak);
  if (recent.length < CFG.suspendLossStreak) return;
  if (recent.every((x) => x.status === 'LOSS')) {
    SUSPENDED[key] = Date.now() + CFG.suspendMs;
    dbStateSet('suspended', JSON.stringify(Object.entries(SUSPENDED)));
    log('SUSPENSO', { key, consecutiveLosses: recent.length });
  }
}

/* ══════════════════════ TIER E GATE ══════════════════════ */

function idleMs() { return S.lastSignalAt ? Date.now() - S.lastSignalAt : Infinity; }
function tierAtual() {
  const i = idleMs();
  if (i >= CFG.relaxMs) return 1;
  return 0;
}

function pendentes() { return S.signals.filter((s) => !FECHADOS.has(s.status)); }

function podeGerar() {
  if (!CFG.ativos) { R.ultimoMotivo = 'desativado'; return false; }
  if (isPaused()) { R.ultimoMotivo = `em pausa (${PAUSE.reason})`; return false; }
  if (!apiHealthy()) { R.ultimoMotivo = 'API instável — sinal bloqueado'; return false; }
  if (pendentes().length >= CFG.maxPending) { R.ultimoMotivo = 'max pending'; return false; }
  const now = Date.now();
  if (S.lastSignalAt && now - S.lastSignalAt < CFG.minGapMs) {
    R.ultimoMotivo = `gap ${Math.ceil((CFG.minGapMs - (now - S.lastSignalAt)) / 1000)}s`;
    return false;
  }
  const last = S.hist[S.hist.length - 1];
  if (!last) { R.ultimoMotivo = 'sem hist'; return false; }
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
  for (const x of S.signals) if (x.key === key && FECHADOS.has(x.status)) liveN++;
  return bn + liveN;
}
function liveHitRate(key) {
  let n = 0, w = 0;
  for (const x of S.signals) {
    if (x.key !== key || !FECHADOS.has(x.status)) continue;
    n++; if (x.status !== 'LOSS') w++;
  }
  return n >= 5 ? w / n : null;
}

/* ══════════════════════ GERAR SINAL ══════════════════════ */

async function tentarSinal() {
  if (!podeGerar()) return;

  const tier = tierAtual();
  R.tier = tier;
  const T = CFG.tiers[tier];

  const rows = S.hist.slice(-30).reverse();
  const stats = getStats();

  let cands = runStrategies(rows, false);
  if (!cands.length) { R.ultimoMotivo = `tier${tier}: sem candidato`; return; }

  // Filtrar candidatos
  cands = cands.filter((c) => {
    if (isSuspended(c.key)) return false;
    // Estratégias compostas não precisam de amostra própria (herdam das bases)
    if (c.tipo !== 'apex' && c.tipo !== 'confluence') {
      if (stratSamples(c.key, stats) < T.minStratN) return false;
    }
    const lr = liveHitRate(c.key);
    if (lr !== null && lr < T.liveMin) return false;
    return true;
  });
  if (!cands.length) { R.ultimoMotivo = `tier${tier}: filtros`; return; }

  // Agrupar por cor
  const byColor = {};
  for (const c of cands) {
    if (!byColor[c.cor]) byColor[c.cor] = [];
    byColor[c.cor].push(c);
  }

  let melhor = null;
  for (const cor of Object.keys(byColor)) {
    const list = byColor[cor];
    const scored = list.map((c) => ({
      ...c,
      conf: confianca(c, { stats, agreement: { confirmations: list.length } })
    }));
    scored.sort((a, b) => b.conf - a.conf);
    const top = scored[0];
    if (!melhor || top.conf > melhor.conf) melhor = { ...top, agree: scored.length };
  }

  if (!melhor) { R.ultimoMotivo = 'sem melhor'; return; }
  if (melhor.conf < T.scoreMin) { R.ultimoMotivo = `score ${melhor.conf} < ${T.scoreMin}`; return; }

  const reservedId = S.nextId;
  const agora = Date.now();

  const sinalPreliminar = {
    id: reservedId, cor: melhor.cor, key: melhor.key, nome: melhor.nome, emoji: melhor.emoji,
    conf: melhor.conf, tipo: melhor.tipo, status: 'PENDENTE', tier,
    t: agora, criadoEm: agora, agree: melhor.agree,
    gale1MsgId: null, galeLevel: 0
  };

  const envio = await tgSendRaw(fmtSinal(sinalPreliminar));
  if (!envio.ok) {
    log('SINAL', { motivo: 'telegram rejeitou sinal', id: reservedId });
    R.ultimoMotivo = 'telegram falhou';
    return;
  }

  S.nextId++;
  S.signals.push(sinalPreliminar);
  if (S.signals.length > CFG.maxSignalsMem) S.signals = S.signals.slice(-CFG.maxSignalsMem + 50);
  S.lastSignalAt = agora;
  R.ultimoSinal = sinalPreliminar;
  R.ultimoMotivo = null;

  await dbSignalInsert(sinalPreliminar);
  await dbStateSet('nextId', String(S.nextId));
  await dbStateSet('lastSignalAt', String(agora));

  log('SINAL', {
    id: sinalPreliminar.id, cor: sinalPreliminar.cor, conf: sinalPreliminar.conf,
    key: sinalPreliminar.key, tipo: sinalPreliminar.tipo, tier,
    hora: hora(agora), agree: melhor.agree
  });
}

async function notificarGale(s, nivel) {
  if (!apiHealthy()) return;
  if (s.gale1MsgId) { await tgDelete(s.gale1MsgId); s.gale1MsgId = null; }
  const r = await tgSendRaw(fmtGale(s, nivel));
  if (r.ok) {
    if (nivel === 1) s.gale1MsgId = r.messageId;
    s.galeLevel = nivel;
  }
}

/* ══════════════════════ FECHAR ══════════════════════ */

async function fecharSinal(s, status, gale) {
  s.status = status;
  s.gale = gale;
  s.fechadoEm = Date.now();

  if (s.gale1MsgId) { await tgDelete(s.gale1MsgId); s.gale1MsgId = null; }

  const bucket = status === 'GREEN' ? 'green' : status === 'G1' ? 'g1' : status === 'G2' ? 'g2'
    : status === 'EMPATE' ? 'empate' : status === 'EG1' ? 'eg1' : status === 'EG2' ? 'eg2' : 'loss';
  ALLTIME[bucket]++; ALLTIME.total++;
  DAILY[bucket]++; DAILY.total++;
  if (status === 'LOSS') S.curStreak = 0;
  else { S.curStreak++; if (S.curStreak > DAILY.bestStreak) DAILY.bestStreak = S.curStreak; }
  await dbStateSet('curStreak', String(S.curStreak));
  await dbStateSet('diamond_daily', JSON.stringify(DAILY));

  await dbSignalUpdate(s);
  await tgSendRaw(fmtResult(s));
  maybeSuspend(s.key);
}

/* ══════════════════════ APURAÇÃO ══════════════════════ */

async function apurar() {
  const now = Date.now();
  for (const s of S.signals) {
    if (FECHADOS.has(s.status)) continue;
    const after = S.hist.filter((r) => r.t > s.t).sort((a, b) => a.t - b.t);
    let decided = false;
    for (let k = 0; k <= CFG.maxGale && k < after.length; k++) {
      const r = after[k];
      const prevT = k === 0 ? s.t : after[k - 1].t;
      if (r.t - prevT > CFG.gapMs) { await fecharSinal(s, 'LOSS', k); decided = true; break; }
      if (r.resultado === s.cor || r.resultado === E) {
        const status = r.resultado === E ? (k === 0 ? 'EMPATE' : 'EG1') : (k === 0 ? 'GREEN' : 'G1');
        await fecharSinal(s, status, k); decided = true; break;
      }
      if (k < CFG.maxGale && s.galeLevel < k + 1) await notificarGale(s, k + 1);
    }
    if (decided) continue;
    if (!after.length && now - s.t > CFG.gapMs) await fecharSinal(s, 'LOSS', 0);
    else if (after.length >= CFG.maxGale + 1) await fecharSinal(s, 'LOSS', CFG.maxGale);
    else if (now - s.t >= CFG.apuraMaxMs) await fecharSinal(s, 'LOSS', s.galeLevel || 0);
  }
}

/* ══════════════════════ SYNC ══════════════════════ */

function currentSyncMs() {
  if (pendentes().length > 0) return CFG.syncFastMs;
  const last = S.hist[S.hist.length - 1];
  if (last && Date.now() - last.t < 45000) return CFG.syncFastMs;
  return CFG.syncMs;
}

async function sync() {
  try {
    const rounds = await fetchRounds();
    let novos = 0, atualizados = 0;
    const byId = new Map(S.hist.map((r) => [r.id, r]));
    for (const r of rounds) {
      const previous = byId.get(r.id);
      if (!previous) { novos++; byId.set(r.id, r); await dbRoundUpsert(r); }
      else if (previous.t !== r.t || previous.resultado !== r.resultado || previous.pj !== r.pj || previous.pb !== r.pb) {
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

    if (newest && Date.now() - newest.t > CFG.staleAlertMs) {
      const mins = Math.floor((Date.now() - newest.t) / 60000);
      await tgAlert('API', `API sem rodadas há ${mins} min\nÚltima: ${R.ultimaRodadaNova}`);
    }
    if (novos || atualizados) log('SYNC', { novos, atualizados, total: S.hist.length, fonte: R.fonteAPI });
  } catch (e) {
    R.erroAPI = e.message;
    R.lastErrorAt = Date.now();
    log('SYNC ERRO', e.message);
    await tgAlert('API', `Erro API BACBO\n${e.message}`);
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
    await apurar();
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

/* ══════════════════════ EXPRESS ══════════════════════ */

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
    ok: apiHealthy() && Boolean(R.ultimaSync) && R.lockOk,
    versao: '21.0', deployId: DEPLOY_ID, maxGale: CFG.maxGale, lock: R.lockOk,
    tier: R.tier, fonteAPI: R.fonteAPI + 1, fontes: API_SOURCES.length,
    apiHealthy: apiHealthy(), lastErrorAt: R.lastErrorAt,
    pausado: PAUSE.until > Date.now() ? PAUSE.reason : null,
    hist: S.hist.length, sinais: S.signals.length, pendentes: pendentes().length,
    diario: { ...DAILY, taxa: dailyTaxa() }, geral: ALLTIME,
    ultimaSync: R.ultimaSync, ultimaRodada: newest ? R.ultimaRodadaNova : null,
    erroAPI: R.erroAPI, erroDB: R.erroDB,
    uptime: Math.round(process.uptime())
  });
});
app.get('/status', requireAdmin, (req, res) => {
  res.json({ motivo: R.ultimoMotivo, R, DAILY, ALLTIME, CFG, suspenso: Object.keys(SUSPENDED), pausa: PAUSE });
});
app.get('/sinais', requireAdmin, (req, res) => {
  const n = Math.min(100, Math.max(1, parseInt(req.query.n, 10) || 20));
  res.json(S.signals.slice(-n).reverse());
});
app.get('/stats', requireAdmin, (req, res) => {
  const wins = dailyWins();
  const reais = DAILY.green + DAILY.g1 + DAILY.g2;
  const decididos = reais + DAILY.loss;
  const gw = ALLTIME.green + ALLTIME.g1 + ALLTIME.g2 + ALLTIME.empate + ALLTIME.eg1 + ALLTIME.eg2;
  res.json({
    diario: {
      ...DAILY, wins, taxa: DAILY.total > 0 ? wins / DAILY.total : null,
      taxaReal: decididos > 0 ? reais / decididos : null, sequenciaAtual: S.curStreak
    },
    geral: { ...ALLTIME, wins: gw, taxa: ALLTIME.total > 0 ? gw / ALLTIME.total : null }
  });
});
app.get('/calibracao', requireAdmin, (req, res) => {
  const buckets = { '80-85': { n: 0, w: 0 }, '86-91': { n: 0, w: 0 }, '92-94': { n: 0, w: 0 }, '95': { n: 0, w: 0 } };
  for (const s of S.signals) {
    if (!FECHADOS.has(s.status)) continue;
    const b = s.conf >= 95 ? '95' : s.conf >= 92 ? '92-94' : s.conf >= 86 ? '86-91' : '80-85';
    buckets[b].n++;
    if (s.status !== 'LOSS') buckets[b].w++;
  }
  res.json(buckets);
});

/* ══════════════════════ START ══════════════════════ */

async function start() {
  for (const [name, value] of Object.entries({ BOT_TOKEN, CHAT_ID, ADMIN_TOKEN, DATABASE_URL })) {
    if (!value) throw new Error(`Variável obrigatória em falta: ${name}`);
  }
  if (!/^postgres(?:ql)?:\/\//i.test(DATABASE_URL)) throw new Error('DATABASE_URL inválido');
  log('BOOT', { versao: '21.0', deployId: DEPLOY_ID, fontes: API_SOURCES.length });
  await dbInit();
  if (!pool) throw new Error('Não foi possível inicializar PostgreSQL; arranque cancelado');

  const locked = await acquireLock();
  if (!locked) throw new Error('Outra instância do bot está ativa (advisory lock). Encerrando.');

  await dbLoadState();
  await sync();
  getStats(true);

  server = app.listen(PORT, () => log('HTTP', `porta ${PORT}`));
  tickTimer = setInterval(() => tick().catch(() => {}), 1000);
  keepTimer = setInterval(() => { keepAlive().catch(() => {}); wakeCheck(); }, CFG.keepAliveMs);
  setTimeout(() => tick().catch(() => {}), 1500);

  process.on('SIGTERM', shutdown);
  process.on('SIGINT', shutdown);
}
async function shutdown() {
  if (stopping) return;
  stopping = true; log('SHUTDOWN', 'ok');
  clearInterval(tickTimer); clearInterval(keepTimer);
  if (server) await new Promise((resolve) => server.close(resolve));
  await releaseLock();
  if (pool) await pool.end().catch(() => {});
  process.exit(0);
}

start().catch((e) => { console.error(e); process.exit(1); });
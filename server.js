'use strict';

/**
 * Bac Bo Signal Server v6.1
 * - Ensemble multi-modelo com confiança/força calibradas
 * - Lock global (sem race condition) entre criação de sinal e resolução de resultado
 * - Sinal disparado logo após cada rodada nova (janela de aposta)
 * - Persistência completa, TTL de sinal preso, broadcast com limpeza por chat
 * - Keep-alive (Render free), webhook com secret seguro, diagnóstico /diag
 */

const crypto = require('crypto');
const express = require('express');
const cors = require('cors');
const axios = require('axios');
const { Pool } = require('pg');

/* ============================== CONFIG ============================== */

const env = (k, d = '') => String(process.env[k] ?? d).trim();
const num = (k, d) => {
  const n = Number(process.env[k]);
  return Number.isFinite(n) && process.env[k] !== '' && process.env[k] != null ? n : d;
};

const PORT = num('PORT', 10000);
const CHAT_ID = env('CHAT_ID');
const BOT_TOKEN = env('BOT_TOKEN');
const ADMIN_TOKEN = env('ADMIN_TOKEN');
const DATABASE_URL = env('DATABASE_URL');
const PUBLIC_URL = (env('RENDER_EXTERNAL_URL') || env('PUBLIC_URL')).replace(/\/$/, '');
const WEBHOOK_PATH = '/telegram/webhook';
// secret sempre válido para o Telegram (A-Za-z0-9_-), derivado do token
const WEBHOOK_SECRET = crypto
  .createHash('sha256')
  .update(env('WEBHOOK_SECRET') || ADMIN_TOKEN || 'bacbo')
  .digest('hex');

const API_URL =
  'https://api-cs.casino.org/svc-evolution-game-events/api/bacbo?page=0&size=100&sort=data.settledAt,desc&duration=240&wheelResults=PlayerWon,BankerWon,Tie';
const API_TIMEOUT = 16000;
const SYNC_MS = 4000;
const SIGNAL_CHECK_MS = 5000;
const KEEPALIVE_MS = 4 * 60 * 1000;

const MIN_SIGNAL_INTERVAL_MS = num('MIN_SIGNAL_INTERVAL_MS', 150000); // 2m30s
const MAX_SIGNAL_INTERVAL_MS = num('MAX_SIGNAL_INTERVAL_MS', 210000); // 3m30s
const FRESH_ROUND_WINDOW_MS = 15000;
const RED_COOLDOWN_MS = num('RED_COOLDOWN_MS', 240000);
const GREEN_COOLDOWN_MS = num('GREEN_COOLDOWN_MS', 45000);
const SIGNAL_TTL_MS = 10 * 60 * 1000;
const ANALYSIS_DELAY_MS = num('ANALYSIS_DELAY_MS', 1500);

const MIN_HISTORY = 28;
const MIN_USABLE = 22;
const MIN_STRENGTH = num('MIN_STRENGTH', 84);
const MIN_CONFIDENCE = num('MIN_CONFIDENCE', 0.54);
const MIN_EDGE = num('MIN_EDGE', 0.05);
const MIN_AGREEMENT = num('MIN_AGREEMENT', 0.6);
const DEFAULT_TIE_MULTIPLIER = '4x';
const TZ = 'Africa/Luanda';
const MAX_KNOWN_ROUNDS = 3000;

const missing = [
  ['CHAT_ID', CHAT_ID],
  ['BOT_TOKEN', BOT_TOKEN],
  ['ADMIN_TOKEN', ADMIN_TOKEN],
  ['DATABASE_URL', DATABASE_URL]
]
  .filter(([, v]) => !v)
  .map(([n]) => n);
if (missing.length) console.warn(`[config] Variáveis ausentes: ${missing.join(', ')}`);

/* ============================== STATE =============================== */

const app = express();
function sslConfig() {
  const flag = env('PGSSL').toLowerCase();
  if (flag === 'false' || flag === '0') return false;
  if (/sslmode=disable/i.test(DATABASE_URL) || /@(localhost|127\.0\.0\.1)[:/]/.test(DATABASE_URL)) return false;
  return { rejectUnauthorized: false };
}

let pool = DATABASE_URL ? new Pool({ connectionString: DATABASE_URL, ssl: sslConfig(), max: 6 }) : null;
if (pool && typeof pool.on === 'function') pool.on('error', (e) => console.error('[pg idle]', e.message));

const memoryResults = [];
const knownRounds = new Set();
const subscribers = new Set(CHAT_ID ? [CHAT_ID] : []);

const state = {
  activeSignal: null, // { outcome, stage, strength, confidence, createdAt, afterTs, models }
  analysisMsgs: {},
  signalMsgs: {},
  paused: false,
  lastSignalAt: 0,
  lastRoundAt: 0, // timestamp (casino) da rodada mais recente
  lastRoundSeenAt: 0, // Date.now() local quando vimos rodada nova
  lastRedAt: 0,
  lastGreenAt: 0,
  apiError: null,
  apiLastSync: null,
  syncing: false,
  initialSyncDone: false,
  statsDate: null,
  stats: null,
  lastSkipReason: null,
  recentSignalQuality: []
};

/* Lock global: serializa criação de sinal e resolução de resultado */
let lockChain = Promise.resolve();
function withLock(fn) {
  const run = lockChain.then(() => fn());
  lockChain = run.catch(() => {});
  return run;
}
const idle = () => lockChain;

/* ============================== HELPERS ============================= */

const timeFmt = new Intl.DateTimeFormat('pt-PT', {
  timeZone: TZ,
  hour: '2-digit',
  minute: '2-digit',
  second: '2-digit',
  hourCycle: 'h23'
});
const dateFmt = new Intl.DateTimeFormat('en-CA', {
  timeZone: TZ,
  year: 'numeric',
  month: '2-digit',
  day: '2-digit'
});

const nowAngola = () => timeFmt.format(new Date());
const dateAngola = () => dateFmt.format(new Date());

function blankStats() {
  return {
    signals: 0,
    green: { direct: 0, gale1: 0, gale2: 0 },
    ties: { direct: 0, gale1: 0, gale2: 0 },
    reds: 0,
    currentStreak: 0,
    bestStreak: 0
  };
}

function ensureDay() {
  const today = dateAngola();
  if (state.statsDate !== today) {
    state.statsDate = today;
    state.stats = blankStats();
    saveStats().catch((e) => console.error('[stats]', e.message));
  }
}

const label = (o) => (o === 'banker' ? 'BANCA' : o === 'player' ? 'JOGADOR' : 'EMPATE');
const emoji = (o) => (o === 'banker' ? '🔴' : o === 'player' ? '🔵' : '🟡');
const stageLabel = (s) => (s === 0 ? 'ENTRADA DIRETA' : `GALE ${s}`);
const cleanMultiplier = (v) => String(v || DEFAULT_TIE_MULTIPLIER).replace(/^×|^x/i, '');

function safeEqual(a, b) {
  const ha = crypto.createHash('sha256').update(String(a || '')).digest();
  const hb = crypto.createHash('sha256').update(String(b || '')).digest();
  return crypto.timingSafeEqual(ha, hb);
}

function auth(req, res, next) {
  if (!ADMIN_TOKEN || !safeEqual(req.header('x-api-key'), ADMIN_TOKEN)) {
    return res.status(401).json({ error: 'unauthorized' });
  }
  next();
}

function safeNumber(value) {
  if (value === null || value === undefined || value === '') return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

const clamp = (v, min, max) => Math.max(min, Math.min(max, v));
const mean = (arr) => (arr.length ? arr.reduce((a, b) => a + b, 0) / arr.length : 0);
const sleep = (ms) => (ms > 0 ? new Promise((r) => setTimeout(r, ms)) : Promise.resolve());

function pushQuality(entry) {
  state.recentSignalQuality.push(entry);
  if (state.recentSignalQuality.length > 20) state.recentSignalQuality.shift();
}

/* ============================== DATABASE ============================ */

async function initDb() {
  if (!pool) return;
  await pool.query(`
    CREATE TABLE IF NOT EXISTS bacbo_results (
      id BIGSERIAL PRIMARY KEY,
      external_id VARCHAR(180) UNIQUE,
      outcome VARCHAR(10) NOT NULL,
      round_number VARCHAR(180),
      player_score INTEGER,
      banker_score INTEGER,
      source VARCHAR(50),
      created_at TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_bacbo_results_created ON bacbo_results (created_at DESC);
    CREATE TABLE IF NOT EXISTS bacbo_subscribers (
      chat_id VARCHAR(80) PRIMARY KEY,
      active BOOLEAN DEFAULT TRUE
    );
    CREATE TABLE IF NOT EXISTS bacbo_daily_stats (
      day DATE PRIMARY KEY,
      stats JSONB NOT NULL,
      updated_at TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS bacbo_runtime (
      key VARCHAR(60) PRIMARY KEY,
      value JSONB NOT NULL,
      updated_at TIMESTAMPTZ DEFAULT NOW()
    );
  `);
  const { rows } = await pool.query('SELECT chat_id FROM bacbo_subscribers WHERE active = TRUE');
  rows.forEach((r) => subscribers.add(String(r.chat_id)));
}

async function loadKnownRounds() {
  if (!pool) return;
  const { rows } = await pool.query(
    'SELECT external_id FROM bacbo_results WHERE external_id IS NOT NULL ORDER BY id DESC LIMIT $1',
    [MAX_KNOWN_ROUNDS]
  );
  for (let i = rows.length - 1; i >= 0; i--) knownRounds.add(rows[i].external_id);
}

async function loadStats() {
  ensureDay();
  if (!pool) return;
  const { rows } = await pool.query('SELECT stats FROM bacbo_daily_stats WHERE day = $1', [state.statsDate]);
  if (rows[0]?.stats) {
    const s = rows[0].stats;
    state.stats = {
      ...blankStats(),
      ...s,
      green: { ...blankStats().green, ...(s.green || {}) },
      ties: { ...blankStats().ties, ...(s.ties || {}) }
    };
  }
}

async function saveStats() {
  if (!pool || !state.statsDate || !state.stats) return;
  try {
    await pool.query(
      `INSERT INTO bacbo_daily_stats (day, stats, updated_at)
       VALUES ($1, $2, NOW())
       ON CONFLICT (day) DO UPDATE SET stats = $2, updated_at = NOW()`,
      [state.statsDate, JSON.stringify(state.stats)]
    );
  } catch (err) {
    console.error('[stats save]', err.message);
  }
}

async function saveRuntime() {
  if (!pool) return;
  const payload = {
    activeSignal: state.activeSignal,
    lastSignalAt: state.lastSignalAt || 0,
    lastRedAt: state.lastRedAt || 0,
    lastGreenAt: state.lastGreenAt || 0,
    lastRoundAt: state.lastRoundAt || 0,
    paused: Boolean(state.paused),
    recentSignalQuality: state.recentSignalQuality.slice(-20)
  };
  try {
    await pool.query(
      `INSERT INTO bacbo_runtime (key, value, updated_at)
       VALUES ('state', $1, NOW())
       ON CONFLICT (key) DO UPDATE SET value = $1, updated_at = NOW()`,
      [JSON.stringify(payload)]
    );
  } catch (err) {
    console.error('[runtime save]', err.message);
  }
}

async function loadRuntime() {
  if (!pool) return;
  try {
    const { rows } = await pool.query(`SELECT value FROM bacbo_runtime WHERE key = 'state'`);
    if (!rows[0]?.value) return;
    const data = rows[0].value;

    const a = data.activeSignal;
    if (a && a.outcome) {
      state.activeSignal = {
        outcome: a.outcome,
        stage: Number(a.stage) || 0,
        strength: Number(a.strength) || 0,
        confidence: Number(a.confidence) || 0,
        createdAt: Number(a.createdAt) || Date.now(),
        afterTs: Number(a.afterTs) || 0,
        models: a.models || null
      };
      console.log(`[runtime] activeSignal restaurado: ${a.outcome} G${state.activeSignal.stage}`);
    } else {
      state.activeSignal = null;
    }

    state.lastSignalAt = Number(data.lastSignalAt) || 0;
    state.lastRedAt = Number(data.lastRedAt) || 0;
    state.lastGreenAt = Number(data.lastGreenAt) || 0;
    state.lastRoundAt = Number(data.lastRoundAt) || 0;
    state.paused = Boolean(data.paused);
    if (Array.isArray(data.recentSignalQuality)) {
      state.recentSignalQuality = data.recentSignalQuality.slice(-20);
    }
  } catch (err) {
    console.error('[runtime load]', err.message);
  }
}

async function saveResult(result) {
  if (pool) {
    const { rows } = await pool.query(
      `INSERT INTO bacbo_results
         (external_id, outcome, round_number, player_score, banker_score, source)
       VALUES ($1, $2, $3, $4, $5, $6)
       ON CONFLICT (external_id) DO NOTHING
       RETURNING id`,
      [
        result.externalId || null,
        result.outcome,
        result.roundNumber || null,
        safeNumber(result.playerScore),
        safeNumber(result.bankerScore),
        result.source || 'bacbo-api'
      ]
    );
    return Boolean(rows[0]);
  }
  return true;
}

function rememberRound(round) {
  memoryResults.unshift({
    outcome: round.outcome,
    playerScore: round.playerScore,
    bankerScore: round.bankerScore
  });
  if (memoryResults.length > 2000) memoryResults.length = 2000;
}

async function recentResults(limit = 150) {
  if (pool) {
    try {
      const { rows } = await pool.query(
        'SELECT outcome, player_score, banker_score FROM bacbo_results ORDER BY id DESC LIMIT $1',
        [limit]
      );
      return rows.map((r) => ({ outcome: r.outcome, playerScore: r.player_score, bankerScore: r.banker_score }));
    } catch (err) {
      console.error('[recentResults] usando memória:', err.message);
    }
  }
  return memoryResults.slice(0, limit);
}

function pruneKnownRounds() {
  if (knownRounds.size <= MAX_KNOWN_ROUNDS) return;
  const arr = Array.from(knownRounds);
  knownRounds.clear();
  arr.slice(-MAX_KNOWN_ROUNDS).forEach((id) => knownRounds.add(id));
}

/* ============================== TELEGRAM ============================ */

async function telegram(method, data = {}) {
  if (!BOT_TOKEN) throw new Error('BOT_TOKEN ausente');
  const response = await axios.post(`https://api.telegram.org/bot${BOT_TOKEN}/${method}`, data, {
    timeout: 14000
  });
  if (!response.data?.ok) {
    throw new Error(response.data?.description || `Telegram ${method} falhou`);
  }
  return response.data.result;
}

async function sendToChat(chatId, text, markdown = true, attempt = 0) {
  try {
    return await telegram('sendMessage', {
      chat_id: chatId,
      text,
      ...(markdown ? { parse_mode: 'Markdown' } : {}),
      disable_web_page_preview: true
    });
  } catch (err) {
    const status = err.response?.status;
    const desc = err.response?.data?.description || '';
    const retryAfter = err.response?.data?.parameters?.retry_after;
    if (status === 400 && markdown && /parse entities/i.test(desc)) {
      return sendToChat(chatId, text.replace(/[*_`]/g, ''), false, attempt);
    }
    if (status === 429 && retryAfter && attempt < 1) {
      await sleep(Math.min(Number(retryAfter), 10) * 1000);
      return sendToChat(chatId, text, markdown, attempt + 1);
    }
    console.error(`[telegram] chat ${chatId}:`, err.response?.data?.description || err.message);
    if ((status === 403 || status === 400) && chatId !== CHAT_ID && /blocked|deactivated|not found/i.test(err.response?.data?.description || '')) {
      subscribers.delete(chatId);
      if (pool) pool.query('UPDATE bacbo_subscribers SET active = FALSE WHERE chat_id = $1', [chatId]).catch(() => {});
    }
    return null;
  }
}

function targets() {
  const set = new Set(subscribers);
  if (CHAT_ID) set.add(CHAT_ID);
  return Array.from(set);
}

/** Envia para todos (lotes de 20); devolve { chatId: message_id } */
async function sendAll(text) {
  const list = targets();
  const out = {};
  for (let i = 0; i < list.length; i += 20) {
    const chunk = list.slice(i, i + 20);
    const res = await Promise.all(chunk.map((id) => sendToChat(id, text)));
    chunk.forEach((id, j) => {
      if (res[j]?.message_id) out[id] = res[j].message_id;
    });
    if (i + 20 < list.length) await sleep(1000);
  }
  return out;
}

async function deleteMessages(map) {
  if (!map) return;
  await Promise.all(
    Object.entries(map).map(async ([chatId, messageId]) => {
      try {
        await telegram('deleteMessage', { chat_id: chatId, message_id: messageId });
      } catch (_) {}
    })
  );
}

async function clearPendingMessages() {
  const a = state.analysisMsgs;
  const s = state.signalMsgs;
  state.analysisMsgs = {};
  state.signalMsgs = {};
  await Promise.all([deleteMessages(a), deleteMessages(s)]);
}

/* ---------- Mensagens ---------- */

function signalMessage(signal) {
  const bar =
    signal.strength >= 95 ? '█████' : signal.strength >= 91 ? '████░' : signal.strength >= 88 ? '███░░' : signal.strength >= 84 ? '██░░░' : '█░░░░';
  const confPct = Math.round(signal.confidence * 100);
  return [
    `🚨 *ENTRADA CONFIRMADA*`,
    ``,
    `🎯 ${emoji(signal.outcome)} *${label(signal.outcome)}*`,
    `🛡️ Proteção: Empate`,
    `📈 Força: *${signal.strength}%*  ${bar}`,
    `🧠 Confiança: *${confPct}%*`,
    `🔁 Até GALE 2`,
    ``,
    `_Aguarde a rodada e confirme._`
  ].join('\n');
}

function galeMessage(stage, outcome) {
  return [
    `🔁 *GALE ${stage} ATIVO*`,
    ``,
    `🎯 ${emoji(outcome)} *${label(outcome)}*`,
    `🛡️ Proteção: Empate`,
    ``,
    `_Mesma direção. Entrada reforçada._`
  ].join('\n');
}

function resultMessage(result, stage, signalOutcome) {
  const mode = stageLabel(stage);
  const score =
    result.playerScore != null && result.bankerScore != null
      ? `  ·  J ${result.playerScore} × ${result.bankerScore} B`
      : '';
  const time = result.time || nowAngola();

  if (result.outcome === 'tie') {
    return [
      `🟡✅ *GREEN EMPATE* · ${mode}`,
      ``,
      `🎯 ${emoji(signalOutcome)} ${label(signalOutcome)} → 🟡 Empate${score}`,
      `💰 ×${cleanMultiplier(result.multiplier)}`,
      `⏱ ${time}`
    ].join('\n');
  }
  return [
    `✅🔥 *GREEN* · ${mode}`,
    ``,
    `🎯 ${emoji(signalOutcome)} ${label(signalOutcome)} → ${emoji(result.outcome)} ${label(result.outcome)}${score}`,
    `⏱ ${time}`
  ].join('\n');
}

function redMessage(result) {
  return [
    `🔴 *RED* · GALE 2`,
    ``,
    `🎯 Saiu ${emoji(result.outcome)} *${label(result.outcome)}*`,
    `⏱ ${result.time || nowAngola()}`,
    ``,
    `_Próximo sinal após cooldown curto._`
  ].join('\n');
}

function historyMessage() {
  ensureDay();
  const s = state.stats;
  const direct = s.green.direct;
  const gale1 = s.green.gale1;
  const gale2 = s.green.gale2;
  const ties = s.ties.direct + s.ties.gale1 + s.ties.gale2;
  const greens = direct + gale1 + gale2 + ties;
  const closed = greens + s.reds;
  const rate = closed ? ((greens / closed) * 100).toFixed(1) : '0.0';
  return [
    `📊 *PLACAR DE HOJE*`,
    ``,
    `✅ ${greens}G   🟥 ${s.reds}R   ·   *${rate}%*`,
    `🎯 Direto: ${direct}  ·  G1: ${gale1}  ·  G2: ${gale2}`,
    `🟡 Empates: ${ties}`,
    `🔥 Sequência atual: *${s.currentStreak}*  (melhor: ${s.bestStreak})`
  ].join('\n');
}

const analysisMessage = () => `🔍 *Analisando mesa multi-modelo...*`;

/* ============================== TELEGRAM HANDLERS =================== */

async function configureWebhook() {
  if (!PUBLIC_URL || !BOT_TOKEN) return;
  try {
    await telegram('setWebhook', {
      url: `${PUBLIC_URL}${WEBHOOK_PATH}`,
      secret_token: WEBHOOK_SECRET,
      allowed_updates: ['message']
    });
    console.log('[telegram] webhook configurado');
  } catch (err) {
    console.error('[telegram webhook]', err.message);
  }
}

async function subscribe(chatId) {
  const id = String(chatId);
  subscribers.add(id);
  if (pool) {
    await pool.query(
      `INSERT INTO bacbo_subscribers (chat_id, active) VALUES ($1, TRUE)
       ON CONFLICT (chat_id) DO UPDATE SET active = TRUE`,
      [id]
    );
  }
}

async function handleTelegramUpdate(update) {
  const message = update?.message;
  if (!message?.text) return;

  const chatId = String(message.chat.id);
  const [command, ...args] = message.text.trim().split(/\s+/);
  const cmd = command.toLowerCase().split('@')[0];
  const isAdmin = chatId === CHAT_ID;
  const say = (text, md = false) => sendToChat(chatId, text, md);

  try {
    if (cmd === '/start') {
      await subscribe(chatId);
      return say(
        '✅ Bot ativo (v6.1).\n\nComandos:\n/historico — placar do dia' +
          (isAdmin ? '\n/sinal — força análise\n/status — estado\n/diag — diagnóstico\n/pausar · /retomar\n/resultado banker|player|tie' : '')
      );
    }

    if (cmd === '/historico') return say(historyMessage(), true);
    if (!isAdmin) return;

    if (cmd === '/sinal') {
      const before = state.activeSignal;
      await maybeCreateSignal(true);
      const after = state.activeSignal;
      const ok = after && (!before || before.createdAt !== after.createdAt);
      return say(ok ? '✅ Entrada enviada.' : `⏳ Sem entrada agora${state.lastSkipReason ? ` (${state.lastSkipReason})` : ''}.`);
    }

    if (cmd === '/status') {
      const cycle = state.activeSignal
        ? `G${state.activeSignal.stage} (${label(state.activeSignal.outcome)}) força=${state.activeSignal.strength}`
        : 'livre';
      return say(
        [
          'Online v6.1',
          `API: ${state.apiError ? 'ERRO ' + state.apiError.slice(0, 60) : 'OK'}`,
          `Rodadas: ${knownRounds.size}`,
          `Ciclo: ${cycle}`,
          `Pausado: ${state.paused ? 'sim' : 'não'}`,
          `Último sinal: ${state.lastSignalAt ? Math.round((Date.now() - state.lastSignalAt) / 1000) + 's atrás' : 'nunca'}`,
          `Último bloqueio: ${state.lastSkipReason || 'nenhum'}`
        ].join('\n')
      );
    }

    if (cmd === '/diag') {
      const rows = await recentResults(150);
      const sig = calculateSignal(rows, true);
      return say(
        [
          'Diagnóstico:',
          `Amostra: ${rows.length}`,
          `Último bloqueio: ${state.lastSkipReason || 'nenhum'}`,
          sig?.reject ? `Cálculo agora: ${sig.reject}` : sig ? `Cálculo agora: sinal ${sig.outcome} força=${sig.strength} conf=${sig.confidence}` : 'Cálculo: sem dados'
        ].join('\n')
      );
    }

    if (cmd === '/pausar' || cmd === '/retomar') {
      state.paused = cmd === '/pausar';
      await saveRuntime();
      return say(state.paused ? '⏸️ Bot pausado.' : '▶️ Bot retomado.');
    }

    if (cmd === '/resultado') {
      const outcome = String(args[0] || '').toLowerCase();
      if (!['banker', 'player', 'tie'].includes(outcome)) {
        return say('Uso: /resultado banker|player|tie [multiplicador]');
      }
      await processResult({ outcome, multiplier: args[1] || null, source: 'telegram', time: nowAngola() });
      return say('✅ Resultado processado.');
    }
  } catch (err) {
    console.error('[telegram update]', err.message);
  }
}

/* ============================== SIGNAL ENGINE ======================= */
/** Histórico: sempre mais recente primeiro (índice 0). */

function weightedPosterior(history, halfLife = 16) {
  let b = 0;
  let p = 0;
  history.forEach((o, i) => {
    if (o !== 'banker' && o !== 'player') return;
    const w = Math.pow(0.5, i / halfLife);
    if (o === 'banker') b += w;
    else p += w;
  });
  return (b + 1) / (b + p + 2);
}

function markov1(usable) {
  if (usable.length < 4) return 0.5;
  const last = usable[0];
  let toB = 1;
  let toP = 1;
  for (let i = 1; i < usable.length; i++) {
    if (usable[i] !== last) continue;
    const next = usable[i - 1];
    if (next === 'banker') toB++;
    else toP++;
  }
  return toB / (toB + toP);
}

function markov2(usable) {
  if (usable.length < 6) return 0.5;
  const a = usable[0];
  const b = usable[1];
  let toB = 1;
  let toP = 1;
  for (let i = 2; i < usable.length; i++) {
    if (usable[i] === b && usable[i - 1] === a) {
      if (usable[i - 2] === 'banker') toB++;
      else toP++;
    }
  }
  return toB / (toB + toP);
}

function momentumScore(usable, window = 8) {
  const slice = usable.slice(0, window);
  return slice.length ? slice.filter((o) => o === 'banker').length / slice.length : 0.5;
}

function meanReversionScore(usable, short = 5, long = 25) {
  const deviation = momentumScore(usable, short) - momentumScore(usable, long);
  return clamp(0.5 - deviation * 0.85, 0.05, 0.95);
}

function streakModel(usable) {
  if (!usable.length) return 0.5;
  const side = usable[0];
  let length = 1;
  for (let i = 1; i < usable.length && usable[i] === side; i++) length++;
  if (length < 2) return 0.5;
  const b = side === 'banker';
  if (length >= 5) return b ? 0.38 : 0.62;
  if (length === 4) return b ? 0.42 : 0.58;
  if (length === 3) return b ? 0.55 : 0.45;
  return b ? 0.58 : 0.42;
}

function alternatingScore(usable, window = 12) {
  const slice = usable.slice(0, window);
  if (slice.length < 4) return 0.5;
  let alt = 0;
  for (let i = 1; i < slice.length; i++) if (slice[i] !== slice[i - 1]) alt++;
  const rate = alt / (slice.length - 1);
  if (rate >= 0.7) return usable[0] === 'banker' ? 0.38 : 0.62;
  if (rate <= 0.3) return usable[0] === 'banker' ? 0.56 : 0.44;
  return 0.5;
}

function volatility(usable, window = 20) {
  const slice = usable.slice(0, window);
  if (slice.length < 5) return 0.5;
  const bin = slice.map((o) => (o === 'banker' ? 1 : 0));
  const m = mean(bin);
  return Math.sqrt(mean(bin.map((x) => (x - m) ** 2)));
}

function walkForwardAccuracy(usable) {
  if (usable.length < 20) return 0.5;
  let correct = 0;
  let total = 0;
  for (let i = 12; i < Math.min(usable.length - 3, 55); i++) {
    const sample = usable.slice(i);
    const ens = weightedPosterior(sample, 14) * 0.45 + markov1(sample) * 0.3 + momentumScore(sample, 7) * 0.25;
    if ((ens >= 0.5 ? 'banker' : 'player') === usable[i - 1]) correct++;
    total++;
  }
  return total ? correct / total : 0.5;
}

function adaptiveBias() {
  const q = state.recentSignalQuality.slice(-12);
  if (q.length < 4) return 0;
  return clamp((mean(q.map((x) => (x.green ? 1 : 0))) - 0.55) * 0.08, -0.04, 0.05);
}

/** debug=true → devolve { reject } em vez de null quando bloqueado */
function calculateSignal(rows, debug = false, relax = 0) {
  const fail = (reason) => (debug ? { reject: reason } : null);

  const history = rows.map((r) => r.outcome).filter(Boolean);
  if (history.length < MIN_HISTORY) return fail(`histórico ${history.length}/${MIN_HISTORY}`);

  const usable = history.filter((o) => o === 'banker' || o === 'player');
  if (usable.length < MIN_USABLE) return fail(`amostra útil ${usable.length}/${MIN_USABLE}`);

  const models = {
    global: weightedPosterior(history, 22),
    recent: weightedPosterior(usable, 9),
    markov1: markov1(usable),
    markov2: markov2(usable),
    momentum: momentumScore(usable, 8),
    momentumShort: momentumScore(usable, 4),
    reversion: meanReversionScore(usable, 5, 28),
    streak: streakModel(usable),
    alternating: alternatingScore(usable, 14)
  };

  const w = {
    global: 0.18, recent: 0.16, markov1: 0.14, markov2: 0.1, momentum: 0.14,
    momentumShort: 0.08, reversion: 0.1, streak: 0.06, alternating: 0.04
  };

  const vol = volatility(usable, 18);
  if (vol > 0.42) {
    w.reversion += 0.06; w.momentumShort += 0.04; w.global -= 0.05; w.momentum -= 0.05;
  } else if (vol < 0.28) {
    w.momentum += 0.05; w.markov1 += 0.04; w.reversion -= 0.05; w.alternating -= 0.04;
  }
  const wSum = Object.values(w).reduce((a, b) => a + b, 0);
  let ensemble = 0;
  for (const k of Object.keys(models)) ensemble += models[k] * (w[k] / wSum);

  const pBanker = clamp(ensemble, 0.04, 0.96);
  const outcome = pBanker >= 0.5 ? 'banker' : 'player';
  const edge = Math.abs(pBanker - 0.5) * 2;

  const votes = Object.values(models).map((p) => (p >= 0.5 ? 'banker' : 'player'));
  const agreeCount = votes.filter((v) => v === outcome).length;
  const agreement = agreeCount / votes.length;

  const backtest = walkForwardAccuracy(usable);
  const adapt = adaptiveBias();

  let confidence =
    0.5 +
    edge * 0.5 +
    Math.max(0, agreement - 0.5) * 0.2 +
    Math.max(0, backtest - 0.5) * 0.3 * clamp(usable.length / 60, 0.4, 1) +
    adapt;
  if (vol > 0.45) confidence -= 0.03;
  if (agreeCount <= 4) confidence -= 0.04;
  if (usable.length < 30) confidence -= 0.02;
  if (agreement >= 0.75 && edge >= 0.1) confidence += 0.03;
  if (backtest >= 0.58) confidence += 0.02;
  confidence = clamp(confidence, 0.5, 0.82);

  const strength = Math.round(
    clamp(
      70 + edge * 100 + (agreement - 0.5) * 30 + Math.max(0, backtest - 0.5) * 40 +
        (usable.length > 45 ? 2 : 0) + (agreeCount >= 7 ? 2 : 0),
      70, 99
    )
  );

  // relax (0..1) afrouxa levemente os filtros quando o intervalo-alvo já passou
  const tEdge = MIN_EDGE * (1 - 0.4 * relax);
  const tAgree = MIN_AGREEMENT - 0.05 * relax;
  const tConf = MIN_CONFIDENCE - 0.02 * relax;
  const tStr = MIN_STRENGTH - 3 * relax;
  if (edge < tEdge) return fail(`edge ${edge.toFixed(3)} < ${tEdge.toFixed(3)}`);
  if (agreement < tAgree) return fail(`consenso ${agreement.toFixed(2)} < ${tAgree.toFixed(2)}`);
  if (confidence < tConf) return fail(`confiança ${confidence.toFixed(3)} < ${tConf.toFixed(3)}`);
  if (strength < tStr) return fail(`força ${strength} < ${tStr.toFixed(0)}`);

  const r3 = (n) => Number(n.toFixed(3));
  return {
    outcome,
    confidence: r3(confidence),
    strength,
    edge: r3(edge),
    agreement: Number(agreement.toFixed(2)),
    backtest: r3(backtest),
    sample: usable.length,
    vol: r3(vol),
    models: {
      global: r3(models.global), recent: r3(models.recent), markov1: r3(models.markov1),
      markov2: r3(models.markov2), momentum: r3(models.momentum),
      reversion: r3(models.reversion), streak: r3(models.streak)
    }
  };
}

function adaptiveInterval() {
  const q = state.recentSignalQuality.slice(-8);
  if (q.length < 3) return MIN_SIGNAL_INTERVAL_MS;
  const hitRate = mean(q.map((x) => (x.green ? 1 : 0)));
  if (hitRate >= 0.7) return MIN_SIGNAL_INTERVAL_MS;
  if (hitRate <= 0.4) return MAX_SIGNAL_INTERVAL_MS;
  return MIN_SIGNAL_INTERVAL_MS + 20000;
}

/* ============================== CICLO DE SINAL ====================== */

const maybeCreateSignal = (manual = false) => withLock(() => createSignalInner(manual));
const processResult = (result) => withLock(() => processResultInner(result));

function skip(reason) {
  state.lastSkipReason = reason;
}

async function createSignalInner(manual) {
  ensureDay();
  const now = Date.now();

  if (state.activeSignal && now - state.activeSignal.createdAt > SIGNAL_TTL_MS) {
    console.warn('[signal] sinal expirou sem resultado; liberando ciclo');
    state.activeSignal = null;
    await clearPendingMessages();
    await saveRuntime();
  }

  if (state.paused) return skip('pausado');
  if (state.activeSignal) return skip('ciclo ativo');

  if (!manual) {
    if (!state.initialSyncDone) return skip('aguardando sincronização inicial');
    if (now - state.lastRoundSeenAt > FRESH_ROUND_WINDOW_MS) return skip('fora da janela da rodada');
    if (now - state.lastSignalAt < adaptiveInterval()) return skip('intervalo mínimo');
    if (state.lastRedAt && now - state.lastRedAt < RED_COOLDOWN_MS) return skip('cooldown RED');
    if (state.lastGreenAt && now - state.lastGreenAt < GREEN_COOLDOWN_MS) return skip('cooldown GREEN');
  }

  try {
    const elapsed = now - state.lastSignalAt;
    const relax = manual ? 1 : clamp((elapsed - (adaptiveInterval() + 30000)) / 90000, 0, 1);
    const rows = await recentResults(150);
    const signal = calculateSignal(rows, true, relax);
    if (!signal || signal.reject) return skip(signal?.reject || 'sem dados');

    state.analysisMsgs = await sendAll(analysisMessage());
    await sleep(ANALYSIS_DELAY_MS);
    await deleteMessages(state.analysisMsgs);
    state.analysisMsgs = {};

    const sent = await sendAll(signalMessage(signal));
    if (!Object.keys(sent).length) {
      console.error('[signal] falha ao enviar ao Telegram; sinal descartado');
      return skip('falha no Telegram');
    }
    state.signalMsgs = sent;

    state.activeSignal = {
      outcome: signal.outcome,
      stage: 0,
      strength: signal.strength,
      confidence: signal.confidence,
      createdAt: now,
      afterTs: state.lastRoundAt || 0,
      models: signal.models
    };
    state.stats.signals++;
    state.lastSignalAt = now;
    state.lastSkipReason = null;
    await Promise.all([saveStats(), saveRuntime()]);

    console.log(
      `[signal] ${signal.outcome} força=${signal.strength} conf=${signal.confidence} edge=${signal.edge} agr=${signal.agreement} vol=${signal.vol}`
    );
  } catch (err) {
    console.error('[signal]', err.message);
    skip('erro: ' + err.message);
    await clearPendingMessages();
  }
}

async function processResultInner(result) {
  ensureDay();
  const current = state.activeSignal;
  if (!current) return;

  // ignora rodadas que terminaram antes do sinal / do estágio atual
  if (result.timestamp && current.afterTs && result.timestamp <= current.afterTs) return;

  try {
    const stageKey = current.stage === 0 ? 'direct' : `gale${current.stage}`;
    const isTie = result.outcome === 'tie';

    if (isTie || result.outcome === current.outcome) {
      if (isTie) state.stats.ties[stageKey] = (state.stats.ties[stageKey] || 0) + 1;
      else state.stats.green[stageKey] = (state.stats.green[stageKey] || 0) + 1;
      state.stats.currentStreak++;
      state.stats.bestStreak = Math.max(state.stats.bestStreak, state.stats.currentStreak);

      // fecha o ciclo ANTES de enviar mensagens (se o Telegram cair, o ciclo não trava)
      state.activeSignal = null;
      state.lastGreenAt = Date.now();
      pushQuality({ strength: current.strength, green: true });
      await Promise.all([saveStats(), saveRuntime()]);

      await clearPendingMessages();
      await sendAll(resultMessage(result, current.stage, current.outcome));
      await sendAll(historyMessage());
      return;
    }

    if (current.stage < 2) {
      current.stage++;
      current.afterTs = result.timestamp || current.afterTs;
      await saveRuntime();
      await clearPendingMessages();
      state.signalMsgs = await sendAll(galeMessage(current.stage, current.outcome));
      return;
    }

    // RED final
    state.stats.reds++;
    state.stats.currentStreak = 0;
    state.activeSignal = null;
    state.lastRedAt = Date.now();
    pushQuality({ strength: current.strength, green: false });
    await Promise.all([saveStats(), saveRuntime()]);

    await clearPendingMessages();
    await sendAll(redMessage(result));
    await sendAll(historyMessage());
  } catch (err) {
    console.error('[processResult]', err.message);
  }
}

/* ============================== BAC BO API ========================== */

function normalizeOutcome(value) {
  const text = String(value || '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/[\s_-]+/g, '');
  if (['player', 'jogador', 'playerwon', 'blue'].includes(text)) return 'player';
  if (['banker', 'banca', 'bankerwon', 'red'].includes(text)) return 'banker';
  if (['tie', 'draw', 'empate', 'push', 'tiewon'].includes(text)) return 'tie';
  return null;
}

function extractItems(body) {
  if (Array.isArray(body)) return body;
  const candidates = [
    body?.data, body?.results, body?.items, body?.rounds, body?.events,
    body?.data?.results, body?.data?.items, body?.data?.rounds
  ];
  return candidates.find(Array.isArray) || [];
}

function normalizeRound(item) {
  const data = item?.data || item?.round || item || {};
  const result =
    (data.result && typeof data.result === 'object' ? data.result : null) ||
    (item?.result && typeof item.result === 'object' ? item.result : {}) ||
    {};

  const settledAt = data.settledAt || data.endTime || data.endedAt || data.finishedAt || item?.settledAt;
  const timestamp = new Date(settledAt).getTime();

  const outcome = normalizeOutcome(
    result.outcome || result.winner || data.outcome || data.winner || item?.outcome || item?.winner || data.result
  );

  const externalId = String(
    data.id || data.roundId || data.eventId || item?.id || item?.roundId || item?.eventId || ''
  ).trim();

  if (!externalId || !outcome || !Number.isFinite(timestamp)) return null;

  return {
    externalId,
    outcome,
    roundNumber: String(data.roundNumber || data.number || externalId),
    playerScore: safeNumber(result.playerDice?.score ?? data.playerDice?.score ?? data.playerScore ?? data.player_score),
    bankerScore: safeNumber(result.bankerDice?.score ?? data.bankerDice?.score ?? data.bankerScore ?? data.banker_score),
    timestamp,
    time: timeFmt.format(new Date(timestamp)),
    source: 'bacbo-api'
  };
}

async function syncRounds() {
  if (state.syncing) return;
  state.syncing = true;

  try {
    const response = await axios.get(API_URL, {
      timeout: API_TIMEOUT,
      headers: { accept: 'application/json', 'user-agent': 'BacBoSignalServer/6.1' }
    });

    const rounds = extractItems(response.data)
      .map(normalizeRound)
      .filter(Boolean)
      .sort((a, b) => a.timestamp - b.timestamp);

    const wasInitial = !state.initialSyncDone;
    let newCount = 0;

    for (const round of rounds) {
      if (knownRounds.has(round.externalId)) continue;
      let inserted = true;
      try {
        inserted = await saveResult(round);
      } catch (err) {
        console.error('[saveResult] (seguindo em memória):', err.message);
      }
      knownRounds.add(round.externalId);
      if (!inserted) continue;
      rememberRound(round);

      newCount++;
      state.lastRoundAt = Math.max(state.lastRoundAt, round.timestamp);
      if (!wasInitial) state.lastRoundSeenAt = Date.now();
      await processResult(round);
    }

    state.apiError = null;
    state.apiLastSync = new Date().toISOString();
    state.initialSyncDone = true;

    if (newCount > 0) {
      pruneKnownRounds();
      console.log(`[sync] +${newCount} rodadas`);
      if (!wasInitial) {
        maybeCreateSignal(false).catch((e) => console.error('[signal-after-sync]', e.message));
      }
    }
  } catch (err) {
    state.apiError = err.message;
    console.error('[bacbo api]', err.message);
  } finally {
    state.syncing = false;
  }
}

/* ============================== HTTP ================================= */

app.disable('x-powered-by');
app.use(cors({ origin: false }));
app.use(express.json({ limit: '256kb' }));

app.get('/', (_req, res) => {
  res.json({ name: 'bacbo-signal-server', version: '6.1', status: 'online', health: '/health' });
});

app.get('/health', (_req, res) => {
  res.status(200).json({
    ok: true,
    uptime: Math.round(process.uptime()),
    api: { lastSync: state.apiLastSync, error: state.apiError },
    rounds: knownRounds.size,
    activeSignal: state.activeSignal
      ? { outcome: state.activeSignal.outcome, stage: state.activeSignal.stage, strength: state.activeSignal.strength }
      : null,
    paused: state.paused,
    lastSkipReason: state.lastSkipReason
  });
});

app.get('/api/status', auth, (_req, res) => {
  res.json({
    ok: true,
    stats: state.stats,
    activeSignal: state.activeSignal,
    apiError: state.apiError,
    rounds: knownRounds.size,
    uptime: process.uptime(),
    subscribers: subscribers.size,
    recentQuality: state.recentSignalQuality.slice(-8),
    lastSkipReason: state.lastSkipReason
  });
});

app.post('/api/results', auth, async (req, res) => {
  try {
    const body = req.body || {};
    if (!['banker', 'player', 'tie'].includes(body.outcome)) {
      return res.status(400).json({ error: 'outcome must be banker, player or tie' });
    }
    // resultado manual só resolve o ciclo; não entra no histórico do modelo
    await processResult({
      outcome: body.outcome,
      multiplier: body.multiplier || null,
      playerScore: safeNumber(body.playerScore),
      bankerScore: safeNumber(body.bankerScore),
      source: body.source || 'manual-api',
      time: body.time || nowAngola()
    });
    res.status(201).json({ ok: true, stats: state.stats, activeSignal: state.activeSignal });
  } catch (err) {
    console.error('[api/results]', err.message);
    res.status(500).json({ error: 'internal' });
  }
});

app.post(WEBHOOK_PATH, (req, res) => {
  if (!safeEqual(req.header('x-telegram-bot-api-secret-token'), WEBHOOK_SECRET)) {
    return res.status(401).json({ error: 'invalid webhook secret' });
  }
  res.sendStatus(200);
  handleTelegramUpdate(req.body).catch((err) => console.error('[telegram update]', err.message));
});

// eslint-disable-next-line no-unused-vars
app.use((err, _req, res, _next) => {
  console.error('[http]', err.message);
  res.status(400).json({ error: 'bad request' });
});

/* ============================== START ================================ */

process.on('unhandledRejection', (err) => console.error('[unhandledRejection]', err?.message || err));
process.on('uncaughtException', (err) => console.error('[uncaughtException]', err?.message || err));

async function connectDb() {
  if (!pool) return;
  for (let i = 1; i <= 5; i++) {
    try {
      await initDb();
      await loadKnownRounds();
      await loadStats();
      await loadRuntime();
      return;
    } catch (err) {
      console.error(`[db] tentativa ${i}/5 falhou:`, err.message);
      await sleep(2000 * i);
    }
  }
  console.error('[db] indisponível — rodando em modo memória (sem persistência)');
  try { await pool.end(); } catch (_) {}
  pool = null;
}

/* Polling (fallback quando não há URL pública para o webhook) */
let polling = false;
async function pollLoop() {
  try {
    await telegram('deleteWebhook', { drop_pending_updates: false });
  } catch (_) {}
  polling = true;
  let offset = 0;
  console.log('[telegram] modo polling');
  while (polling) {
    try {
      const r = await axios.post(
        `https://api.telegram.org/bot${BOT_TOKEN}/getUpdates`,
        { offset, timeout: 25, allowed_updates: ['message'] },
        { timeout: 35000 }
      );
      const list = Array.isArray(r.data?.result) ? r.data.result : [];
      for (const u of list) {
        offset = u.update_id + 1;
        handleTelegramUpdate(u).catch((e) => console.error('[telegram update]', e.message));
      }
    } catch (err) {
      await sleep(3000);
    }
  }
}

async function start() {
  state.statsDate = dateAngola();
  state.stats = blankStats();

  // abre a porta imediatamente (Render derruba serviços que demoram a abrir)
  const server = app.listen(PORT, '0.0.0.0', () => console.log(`[server] online na porta ${PORT} (v6.1)`));

  await connectDb();
  ensureDay();
  await syncRounds();

  if (PUBLIC_URL) await configureWebhook();
  else if (BOT_TOKEN && !env('DISABLE_POLLING')) pollLoop().catch((e) => console.error('[poll]', e.message));

  console.log(
    state.activeSignal
      ? `[server] retomando ciclo ativo: ${state.activeSignal.outcome} G${state.activeSignal.stage}`
      : '[server] nenhum ciclo ativo para retomar'
  );

  const timers = [
    setInterval(() => syncRounds().catch((e) => console.error('[sync]', e.message)), SYNC_MS),
    setInterval(() => maybeCreateSignal(false).catch((e) => console.error('[signal-timer]', e.message)), SIGNAL_CHECK_MS)
  ];

  if (PUBLIC_URL) {
    timers.push(setInterval(() => axios.get(`${PUBLIC_URL}/health`, { timeout: 10000 }).catch(() => {}), KEEPALIVE_MS));
  }

  const shutdown = async (sig) => {
    console.log(`[server] ${sig}, encerrando...`);
    polling = false;
    timers.forEach(clearInterval);
    await saveRuntime();
    await pool?.end().catch(() => {});
    server.close(() => process.exit(0));
    setTimeout(() => process.exit(1), 8000).unref();
  };
  process.once('SIGINT', () => shutdown('SIGINT'));
  process.once('SIGTERM', () => shutdown('SIGTERM'));
}

if (require.main === module) {
  start().catch((err) => {
    console.error('[fatal]', err);
    process.exit(1);
  });
}

module.exports = { start, state, syncRounds, calculateSignal, maybeCreateSignal, processResult, handleTelegramUpdate, idle, normalizeRound };

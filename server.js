'use strict';

/**
 * Bac Bo Signal Server v4 — lógica complexa + confiança calibrada
 *
 * - Ensemble multi-modelo (Bayes, Markov, Momentum, Mean-Reversion, Streak, Pattern)
 * - Confiança dinâmica com penalizações e bônus
 * - Frequência alvo ~3 min (máximo), adaptativa
 * - Persistência completa de activeSignal + cooldowns
 * - Race-condition safe, limpeza de memória, broadcast
 */

const express = require('express');
const cors = require('cors');
const axios = require('axios');
const { Pool } = require('pg');

/* ============================== CONFIG ============================== */

const PORT = Number(process.env.PORT || 10000);
const CHAT_ID = String(process.env.CHAT_ID || '').trim();
const BOT_TOKEN = String(process.env.BOT_TOKEN || '').trim();
const ADMIN_TOKEN = String(process.env.ADMIN_TOKEN || '').trim();
const DATABASE_URL = String(process.env.DATABASE_URL || '').trim();
const PUBLIC_URL = String(process.env.RENDER_EXTERNAL_URL || process.env.PUBLIC_URL || '').replace(/\/$/, '');
const WEBHOOK_PATH = '/telegram/webhook';

const API_URL =
  'https://api-cs.casino.org/svc-evolution-game-events/api/bacbo?page=0&size=100&sort=data.settledAt,desc&duration=240&wheelResults=PlayerWon,BankerWon,Tie';
const API_TIMEOUT = 16000;
const SYNC_MS = 4000;
const SIGNAL_CHECK_MS = 25000; // verifica a cada 25s

// Frequência alvo ~3 min
const MIN_SIGNAL_INTERVAL_MS = 2 * 60 * 1000 + 45 * 1000; // 2m45s base
const MAX_SIGNAL_INTERVAL_MS = 3 * 60 * 1000 + 30 * 1000; // 3m30s teto suave
const ROUND_COOLDOWN_MS = 18000; // 18s após última rodada
const RED_COOLDOWN_MS = 4 * 60 * 1000; // 4 min após RED
const GREEN_COOLDOWN_MS = 90 * 1000; // 1m30s após GREEN

const MIN_HISTORY = 28;
const MIN_USABLE = 22;
const MIN_STRENGTH = 86;
const MIN_CONFIDENCE = 0.56;
const MIN_EDGE = 0.09;
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
const pool = DATABASE_URL
  ? new Pool({ connectionString: DATABASE_URL, ssl: { rejectUnauthorized: false }, max: 6 })
  : null;

const memoryResults = [];
const knownRounds = new Set();
const subscribers = new Set(CHAT_ID ? [CHAT_ID] : []);

const state = {
  activeSignal: null, // { outcome, stage, strength, confidence, createdAt, models }
  analysisMessageId: null,
  signalMessageId: null,
  paused: false,
  lastSignalAt: 0,
  lastRoundAt: 0,
  lastRedAt: 0,
  lastGreenAt: 0,
  apiError: null,
  apiLastSync: null,
  syncing: false,
  processing: false,
  statsDate: null,
  stats: null,
  // métricas internas para adaptação
  recentSignalQuality: [] // últimos 20 { strength, green: bool }
};

/* ============================== HELPERS ============================= */

function nowAngola() {
  return new Intl.DateTimeFormat('pt-PT', {
    timeZone: TZ,
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false
  }).format(new Date());
}

function dateAngola() {
  return new Intl.DateTimeFormat('en-CA', {
    timeZone: TZ,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit'
  }).format(new Date());
}

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
    if (state.activeSignal) {
      clearPendingMessages().catch(() => {});
      clearActiveSignal(true).catch(() => {});
    }
    saveStats().catch((e) => console.error('[stats]', e.message));
  }
}

function label(outcome) {
  if (outcome === 'banker') return 'BANCA';
  if (outcome === 'player') return 'JOGADOR';
  return 'EMPATE';
}

function emoji(outcome) {
  if (outcome === 'banker') return '🔴';
  if (outcome === 'player') return '🔵';
  return '🟡';
}

function stageLabel(stage) {
  return stage === 0 ? 'ENTRADA DIRETA' : `GALE ${stage}`;
}

function cleanMultiplier(value) {
  return String(value || DEFAULT_TIE_MULTIPLIER).replace(/^×|^x/i, '');
}

function auth(req, res, next) {
  if (!ADMIN_TOKEN || req.header('x-api-key') !== ADMIN_TOKEN) {
    return res.status(401).json({ error: 'unauthorized' });
  }
  next();
}

function safeNumber(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function clamp(v, min, max) {
  return Math.max(min, Math.min(max, v));
}

function mean(arr) {
  if (!arr.length) return 0;
  return arr.reduce((a, b) => a + b, 0) / arr.length;
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
  rows.forEach((r) => knownRounds.add(r.external_id));
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
  await pool.query(
    `INSERT INTO bacbo_daily_stats (day, stats, updated_at)
     VALUES ($1, $2, NOW())
     ON CONFLICT (day) DO UPDATE SET stats = $2, updated_at = NOW()`,
    [state.statsDate, JSON.stringify(state.stats)]
  );
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
    if (data.activeSignal && data.activeSignal.outcome) {
      state.activeSignal = {
        outcome: data.activeSignal.outcome,
        stage: Number(data.activeSignal.stage) || 0,
        strength: Number(data.activeSignal.strength) || 0,
        confidence: Number(data.activeSignal.confidence) || 0,
        createdAt: Number(data.activeSignal.createdAt) || Date.now(),
        models: data.activeSignal.models || null
      };
      console.log(
        `[runtime] activeSignal restaurado: ${state.activeSignal.outcome} G${state.activeSignal.stage}`
      );
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

async function clearActiveSignal(persist = true) {
  state.activeSignal = null;
  state.analysisMessageId = null;
  state.signalMessageId = null;
  if (persist) await saveRuntime();
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
        result.playerScore,
        result.bankerScore,
        result.source || 'manual'
      ]
    );
    return Boolean(rows[0]);
  }

  if (result.externalId && knownRounds.has(result.externalId)) return false;
  memoryResults.unshift({ ...result, createdAt: new Date().toISOString() });
  if (memoryResults.length > 2000) memoryResults.length = 2000;
  return true;
}

async function recentResults(limit = 150) {
  if (pool) {
    const { rows } = await pool.query(
      'SELECT outcome, player_score, banker_score FROM bacbo_results ORDER BY created_at DESC LIMIT $1',
      [limit]
    );
    return rows.map((r) => ({
      outcome: r.outcome,
      playerScore: r.player_score,
      bankerScore: r.banker_score
    }));
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

async function sendToChat(chatId, text) {
  try {
    return await telegram('sendMessage', {
      chat_id: chatId,
      text,
      parse_mode: 'Markdown',
      disable_web_page_preview: true
    });
  } catch (err) {
    console.error(`[telegram] chat ${chatId}:`, err.message);
    return null;
  }
}

async function sendTelegram(text) {
  if (CHAT_ID) {
    const msg = await sendToChat(CHAT_ID, text);
    for (const chatId of subscribers) {
      if (chatId !== CHAT_ID) await sendToChat(chatId, text);
    }
    return msg;
  }
  // fallback broadcast
  let first = null;
  for (const chatId of subscribers) {
    const m = await sendToChat(chatId, text);
    if (!first && m) first = m;
  }
  return first;
}

async function deleteTelegram(messageId) {
  if (!CHAT_ID || !messageId) return;
  try {
    await telegram('deleteMessage', { chat_id: CHAT_ID, message_id: messageId });
  } catch (_) {}
}

async function clearPendingMessages() {
  await Promise.all([deleteTelegram(state.analysisMessageId), deleteTelegram(state.signalMessageId)]);
  state.analysisMessageId = null;
  state.signalMessageId = null;
}

/* ---------- Mensagens ---------- */

function signalMessage(signal) {
  const bar =
    signal.strength >= 95
      ? '█████'
      : signal.strength >= 92
        ? '████░'
        : signal.strength >= 89
          ? '███░░'
          : signal.strength >= 86
            ? '██░░░'
            : '█░░░░';

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

function resultMessage(result, stage) {
  const mode = stageLabel(stage);
  const score =
    result.playerScore != null && result.bankerScore != null
      ? `  ·  J ${result.playerScore} × ${result.bankerScore} B`
      : '';
  const time = result.time || nowAngola();
  const signalOutcome = state.activeSignal?.outcome;

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

function analysisMessage() {
  return `🔍 *Analisando mesa multi-modelo...*`;
}

/* ============================== TELEGRAM HANDLERS =================== */

async function configureWebhook() {
  if (!PUBLIC_URL || !BOT_TOKEN) return;
  try {
    await telegram('setWebhook', {
      url: `${PUBLIC_URL}${WEBHOOK_PATH}`,
      secret_token: ADMIN_TOKEN,
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
      `INSERT INTO bacbo_subscribers (chat_id, active)
       VALUES ($1, TRUE)
       ON CONFLICT (chat_id) DO UPDATE SET active = TRUE`,
      [id]
    );
  }
}

async function handleTelegramUpdate(update) {
  const message = update?.message;
  if (!message?.text) return;

  const chatId = String(message.chat.id);
  const text = message.text.trim();
  const [command, ...args] = text.split(/\s+/);
  const cmd = command.toLowerCase().split('@')[0];

  try {
    if (cmd === '/start') {
      await subscribe(chatId);
      return telegram('sendMessage', {
        chat_id: chatId,
        text:
          '✅ *Bot ativo (v4 multi-modelo).*\n\nComandos:\n/sinal — força análise\n/historico — placar do dia\n/status — estado (admin)',
        parse_mode: 'Markdown'
      });
    }

    if (cmd === '/historico') {
      return telegram('sendMessage', {
        chat_id: chatId,
        text: historyMessage(),
        parse_mode: 'Markdown'
      });
    }

    if (cmd === '/sinal') {
      const before = state.activeSignal;
      await maybeCreateSignal(true);
      const after = state.activeSignal;
      const reply =
        after && (!before || before.createdAt !== after.createdAt)
          ? '✅ Entrada enviada.'
          : '⏳ Sem entrada de qualidade no momento.';
      return telegram('sendMessage', { chat_id: chatId, text: reply });
    }

    if (chatId !== CHAT_ID) return;

    if (cmd === '/status') {
      const cycle = state.activeSignal
        ? `G${state.activeSignal.stage} (${label(state.activeSignal.outcome)}) str=${state.activeSignal.strength}`
        : 'livre';
      const text = [
        `✅ *Online v4*`,
        `API: ${state.apiError ? '❌ ' + state.apiError.slice(0, 40) : 'OK'}`,
        `Rodadas: ${knownRounds.size}`,
        `Ciclo: ${cycle}`,
        `Pausado: ${state.paused ? 'sim' : 'não'}`,
        `Último sinal: ${
          state.lastSignalAt ? Math.round((Date.now() - state.lastSignalAt) / 1000) + 's atrás' : 'nunca'
        }`
      ].join('\n');
      return telegram('sendMessage', { chat_id: chatId, text, parse_mode: 'Markdown' });
    }

    if (cmd === '/pausar' || cmd === '/retomar') {
      state.paused = cmd === '/pausar';
      await saveRuntime();
      return telegram('sendMessage', {
        chat_id: chatId,
        text: state.paused ? '⏸️ Bot pausado.' : '▶️ Bot retomado.'
      });
    }

    if (cmd === '/resultado') {
      const outcome = String(args[0] || '').toLowerCase();
      if (!['banker', 'player', 'tie'].includes(outcome)) {
        return telegram('sendMessage', {
          chat_id: chatId,
          text: 'Uso: `/resultado banker|player|tie [multiplicador]`',
          parse_mode: 'Markdown'
        });
      }
      await processResult({
        outcome,
        multiplier: args[1] || null,
        source: 'telegram',
        time: nowAngola()
      });
      return telegram('sendMessage', { chat_id: chatId, text: '✅ Resultado processado.' });
    }
  } catch (err) {
    console.error('[telegram update]', err.message);
  }
}

/* ============================== SIGNAL ENGINE (COMPLEX) ============= */

/**
 * History is always newest-first (index 0 = most recent).
 * Usable = only banker/player (ties removed for most models).
 */

function weightedPosterior(history, halfLife = 16) {
  let b = 0;
  let p = 0;
  history.forEach((o, i) => {
    if (o !== 'banker' && o !== 'player') return;
    const w = Math.pow(0.5, i / halfLife);
    if (o === 'banker') b += w;
    else p += w;
  });
  return (b + 1) / (b + p + 2); // Laplace
}

function markov1(usable) {
  // P(next=banker | last)
  if (usable.length < 4) return 0.5;
  const last = usable[0];
  let toB = 1;
  let toP = 1;
  for (let i = 1; i < usable.length; i++) {
    const prev = usable[i];
    const next = usable[i - 1];
    if (prev !== last) continue;
    if (next === 'banker') toB++;
    else if (next === 'player') toP++;
  }
  return toB / (toB + toP);
}

function markov2(usable) {
  // order-2: last two outcomes
  if (usable.length < 6) return 0.5;
  const a = usable[0];
  const b = usable[1];
  let toB = 1;
  let toP = 1;
  for (let i = 2; i < usable.length; i++) {
    if (usable[i] === b && usable[i - 1] === a) {
      const next = usable[i - 2];
      if (next === 'banker') toB++;
      else if (next === 'player') toP++;
    }
  }
  return toB / (toB + toP);
}

function momentumScore(usable, window = 8) {
  const slice = usable.slice(0, window);
  if (!slice.length) return 0.5;
  let b = 0;
  slice.forEach((o) => {
    if (o === 'banker') b++;
  });
  return b / slice.length;
}

function meanReversionScore(usable, short = 5, long = 25) {
  const shortP = momentumScore(usable, short);
  const longP = momentumScore(usable, long);
  // if short is extreme vs long, expect reversion
  const deviation = shortP - longP;
  // revert toward 0.5 from the short extreme
  return clamp(0.5 - deviation * 0.85, 0.05, 0.95);
}

function streakInfo(usable) {
  if (!usable.length) return { side: null, length: 0 };
  const side = usable[0];
  let len = 1;
  for (let i = 1; i < usable.length; i++) {
    if (usable[i] === side) len++;
    else break;
  }
  return { side, length: len };
}

function streakModel(usable) {
  const { side, length } = streakInfo(usable);
  if (!side || length < 2) return 0.5;

  // after long streak, slight mean-reversion bias; short streak = momentum
  if (length >= 5) {
    // strong reversion
    return side === 'banker' ? 0.38 : 0.62;
  }
  if (length === 4) {
    return side === 'banker' ? 0.42 : 0.58;
  }
  if (length === 3) {
    return side === 'banker' ? 0.55 : 0.45; // slight continuation
  }
  // length 2 → continuation
  return side === 'banker' ? 0.58 : 0.42;
}

function alternatingScore(usable, window = 12) {
  const slice = usable.slice(0, window);
  if (slice.length < 4) return 0.5;
  let alternations = 0;
  for (let i = 1; i < slice.length; i++) {
    if (slice[i] !== slice[i - 1]) alternations++;
  }
  const rate = alternations / (slice.length - 1);
  // high alternation → expect continue alternating
  if (rate >= 0.7) {
    return usable[0] === 'banker' ? 0.38 : 0.62; // expect flip
  }
  if (rate <= 0.3) {
    // clustered → slight continuation
    return usable[0] === 'banker' ? 0.56 : 0.44;
  }
  return 0.5;
}

function volatility(usable, window = 20) {
  const slice = usable.slice(0, window);
  if (slice.length < 5) return 0.5;
  const bin = slice.map((o) => (o === 'banker' ? 1 : 0));
  const m = mean(bin);
  const variance = mean(bin.map((x) => (x - m) ** 2));
  return Math.sqrt(variance); // 0..0.5
}

function walkForwardAccuracy(history, usable) {
  if (usable.length < 20) return 0.5;
  let correct = 0;
  let total = 0;

  for (let i = 12; i < Math.min(usable.length - 3, 55); i++) {
    const sample = usable.slice(i);
    const g = weightedPosterior(sample, 14);
    const m = markov1(sample);
    const mom = momentumScore(sample, 7);
    const ensemble = g * 0.45 + m * 0.3 + mom * 0.25;
    const pred = ensemble >= 0.5 ? 'banker' : 'player';
    if (pred === usable[i - 1]) correct++;
    total++;
  }
  return total ? correct / total : 0.5;
}

/**
 * Adaptive weight based on recent live performance of the bot itself.
 */
function adaptiveBias() {
  const q = state.recentSignalQuality.slice(-12);
  if (q.length < 4) return 0;
  const hitRate = mean(q.map((x) => (x.green ? 1 : 0)));
  // if bot is running hot, slight confidence boost; if cold, penalty
  return clamp((hitRate - 0.55) * 0.08, -0.04, 0.05);
}

function calculateSignal(rows) {
  const history = rows.map((r) => r.outcome).filter(Boolean);
  if (history.length < MIN_HISTORY) return null;

  const usable = history.filter((o) => o === 'banker' || o === 'player');
  if (usable.length < MIN_USABLE) return null;

  // ---------- individual models (all return P(banker)) ----------
  const mGlobal = weightedPosterior(history, 22);
  const mRecent = weightedPosterior(usable, 9);
  const mMarkov1 = markov1(usable);
  const mMarkov2 = markov2(usable);
  const mMomentum = momentumScore(usable, 8);
  const mMomentumShort = momentumScore(usable, 4);
  const mReversion = meanReversionScore(usable, 5, 28);
  const mStreak = streakModel(usable);
  const mAlternating = alternatingScore(usable, 14);

  // ---------- dynamic ensemble weights ----------
  // base weights
  let w = {
    global: 0.18,
    recent: 0.16,
    markov1: 0.14,
    markov2: 0.1,
    momentum: 0.14,
    momentumShort: 0.08,
    reversion: 0.1,
    streak: 0.06,
    alternating: 0.04
  };

  // if high volatility → more weight on reversion + shorter windows
  const vol = volatility(usable, 18);
  if (vol > 0.42) {
    w.reversion += 0.06;
    w.momentumShort += 0.04;
    w.global -= 0.05;
    w.momentum -= 0.05;
  } else if (vol < 0.28) {
    // low vol / trending → more momentum + markov
    w.momentum += 0.05;
    w.markov1 += 0.04;
    w.reversion -= 0.05;
    w.alternating -= 0.04;
  }

  // normalize
  const wSum = Object.values(w).reduce((a, b) => a + b, 0);
  Object.keys(w).forEach((k) => (w[k] /= wSum));

  const ensemble =
    mGlobal * w.global +
    mRecent * w.recent +
    mMarkov1 * w.markov1 +
    mMarkov2 * w.markov2 +
    mMomentum * w.momentum +
    mMomentumShort * w.momentumShort +
    mReversion * w.reversion +
    mStreak * w.streak +
    mAlternating * w.alternating;

  const pBanker = clamp(ensemble, 0.04, 0.96);
  const outcome = pBanker >= 0.5 ? 'banker' : 'player';
  const edge = Math.abs(pBanker - 0.5) * 2;

  // agreement: how many models point the same way
  const models = {
    global: mGlobal,
    recent: mRecent,
    markov1: mMarkov1,
    markov2: mMarkov2,
    momentum: mMomentum,
    momentumShort: mMomentumShort,
    reversion: mReversion,
    streak: mStreak,
    alternating: mAlternating
  };

  const votes = Object.values(models).map((p) => (p >= 0.5 ? 'banker' : 'player'));
  const agreeCount = votes.filter((v) => v === outcome).length;
  const agreement = agreeCount / votes.length;

  const backtest = walkForwardAccuracy(history, usable);
  const adapt = adaptiveBias();

  // ---------- confidence ----------
  let confidence =
    0.48 +
    edge * 0.32 +
    Math.max(0, agreement - 0.5) * 0.22 +
    Math.max(0, backtest - 0.5) * 0.18 * clamp(usable.length / 60, 0.4, 1) +
    adapt;

  // penalties
  if (vol > 0.45) confidence -= 0.04; // very noisy
  if (agreeCount <= 4) confidence -= 0.05; // weak consensus
  if (usable.length < 30) confidence -= 0.03;

  // bonus for strong consensus + decent edge
  if (agreement >= 0.75 && edge >= 0.14) confidence += 0.04;
  if (backtest >= 0.58) confidence += 0.03;

  confidence = clamp(confidence, 0.5, 0.82);

  // ---------- strength (display 80-99) ----------
  let strength = Math.round(
    clamp(
      80 +
        edge * 16 +
        (agreement - 0.5) * 14 +
        Math.max(0, backtest - 0.5) * 12 +
        (usable.length > 45 ? 2 : 0) +
        (agreeCount >= 7 ? 2 : 0),
      80,
      99
    )
  );

  // ---------- hard filters ----------
  if (strength < MIN_STRENGTH) return null;
  if (confidence < MIN_CONFIDENCE) return null;
  if (edge < MIN_EDGE) return null;
  if (agreement < 0.55) return null; // at least ~5/9 models

  return {
    outcome,
    confidence: Number(confidence.toFixed(3)),
    strength,
    edge: Number(edge.toFixed(3)),
    agreement: Number(agreement.toFixed(2)),
    backtest: Number(backtest.toFixed(3)),
    sample: usable.length,
    vol: Number(vol.toFixed(3)),
    models: {
      global: Number(mGlobal.toFixed(3)),
      recent: Number(mRecent.toFixed(3)),
      markov1: Number(mMarkov1.toFixed(3)),
      markov2: Number(mMarkov2.toFixed(3)),
      momentum: Number(mMomentum.toFixed(3)),
      reversion: Number(mReversion.toFixed(3)),
      streak: Number(mStreak.toFixed(3))
    }
  };
}

function adaptiveInterval() {
  // se o bot está acertando, pode ser um pouco mais frequente
  // se está errando, espaça mais
  const q = state.recentSignalQuality.slice(-8);
  if (q.length < 3) return MIN_SIGNAL_INTERVAL_MS;

  const hitRate = mean(q.map((x) => (x.green ? 1 : 0)));
  if (hitRate >= 0.7) return MIN_SIGNAL_INTERVAL_MS; // ~2m45s
  if (hitRate <= 0.4) return MAX_SIGNAL_INTERVAL_MS; // ~3m30s
  return MIN_SIGNAL_INTERVAL_MS + 20 * 1000; // meio termo
}

async function maybeCreateSignal(manual = false) {
  ensureDay();
  if (state.paused || state.activeSignal || state.processing) return;

  const now = Date.now();
  const interval = adaptiveInterval();

  if (!manual && now - state.lastSignalAt < interval) return;
  if (!manual && state.lastRedAt && now - state.lastRedAt < RED_COOLDOWN_MS) return;
  if (!manual && state.lastGreenAt && now - state.lastGreenAt < GREEN_COOLDOWN_MS) return;
  if (state.lastRoundAt && now - state.lastRoundAt < ROUND_COOLDOWN_MS) return;

  state.processing = true;
  try {
    if (CHAT_ID) {
      const analysis = await sendToChat(CHAT_ID, analysisMessage());
      state.analysisMessageId = analysis?.message_id || null;
    }

    const rows = await recentResults(150);
    const signal = calculateSignal(rows);

    await deleteTelegram(state.analysisMessageId);
    state.analysisMessageId = null;

    if (!signal) return;

    const sent = await sendTelegram(signalMessage(signal));
    state.signalMessageId = sent?.message_id || null;

    state.activeSignal = {
      outcome: signal.outcome,
      stage: 0,
      strength: signal.strength,
      confidence: signal.confidence,
      createdAt: now,
      models: signal.models
    };
    state.stats.signals++;
    state.lastSignalAt = now;
    await Promise.all([saveStats(), saveRuntime()]);

    console.log(
      `[signal] ${signal.outcome} str=${signal.strength} conf=${signal.confidence} edge=${signal.edge} agr=${signal.agreement} vol=${signal.vol}`
    );
  } catch (err) {
    console.error('[signal]', err.message);
    await clearPendingMessages();
  } finally {
    state.processing = false;
  }
}

async function processResult(result) {
  ensureDay();
  if (!state.activeSignal || state.processing) return;

  state.processing = true;
  try {
    const current = state.activeSignal;
    const stageKey = current.stage === 0 ? 'direct' : `gale${current.stage}`;

    // TIE
    if (result.outcome === 'tie') {
      state.stats.ties[stageKey] = (state.stats.ties[stageKey] || 0) + 1;
      await clearPendingMessages();
      await sendTelegram(resultMessage(result, current.stage));
      await sendTelegram(historyMessage());
      state.recentSignalQuality.push({ strength: current.strength, green: true });
      if (state.recentSignalQuality.length > 20) state.recentSignalQuality.shift();
      state.activeSignal = null;
      state.lastGreenAt = Date.now();
      await Promise.all([saveStats(), saveRuntime()]);
      return;
    }

    // GREEN
    if (result.outcome === current.outcome) {
      state.stats.green[stageKey] = (state.stats.green[stageKey] || 0) + 1;
      state.stats.currentStreak++;
      state.stats.bestStreak = Math.max(state.stats.bestStreak, state.stats.currentStreak);
      await clearPendingMessages();
      await sendTelegram(resultMessage(result, current.stage));
      await sendTelegram(historyMessage());
      state.recentSignalQuality.push({ strength: current.strength, green: true });
      if (state.recentSignalQuality.length > 20) state.recentSignalQuality.shift();
      state.activeSignal = null;
      state.lastGreenAt = Date.now();
      await Promise.all([saveStats(), saveRuntime()]);
      return;
    }

    // sobe gale
    if (current.stage < 2) {
      current.stage++;
      await clearPendingMessages();
      const sent = await sendTelegram(galeMessage(current.stage, current.outcome));
      state.signalMessageId = sent?.message_id || null;
      await saveRuntime();
      return;
    }

    // RED final
    state.stats.reds++;
    state.stats.currentStreak = 0;
    await clearPendingMessages();
    await sendTelegram(redMessage(result));
    await sendTelegram(historyMessage());
    state.recentSignalQuality.push({ strength: current.strength, green: false });
    if (state.recentSignalQuality.length > 20) state.recentSignalQuality.shift();
    state.activeSignal = null;
    state.lastRedAt = Date.now();
    await Promise.all([saveStats(), saveRuntime()]);
  } catch (err) {
    console.error('[processResult]', err.message);
  } finally {
    state.processing = false;
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
  if (['tie', 'draw', 'empate', 'push'].includes(text)) return 'tie';
  return null;
}

function extractItems(body) {
  if (Array.isArray(body)) return body;
  const candidates = [
    body?.data,
    body?.results,
    body?.items,
    body?.rounds,
    body?.events,
    body?.data?.results,
    body?.data?.items,
    body?.data?.rounds
  ];
  return candidates.find(Array.isArray) || [];
}

function normalizeRound(item) {
  const data = item?.data || item?.round || item || {};
  const result =
    (data.result && typeof data.result === 'object' ? data.result : null) ||
    (item?.result && typeof item.result === 'object' ? item.result : {}) ||
    {};

  const settledAt =
    data.settledAt || data.endTime || data.endedAt || data.finishedAt || item?.settledAt;
  const timestamp = new Date(settledAt).getTime();

  const outcome = normalizeOutcome(
    result.outcome ||
      result.winner ||
      data.outcome ||
      data.winner ||
      item?.outcome ||
      item?.winner ||
      data.result
  );

  const externalId = String(
    data.id || data.roundId || data.eventId || item?.id || item?.roundId || item?.eventId || ''
  ).trim();

  if (!externalId || !outcome || !Number.isFinite(timestamp)) return null;

  const playerScore = safeNumber(
    result.playerDice?.score ?? data.playerDice?.score ?? data.playerScore ?? data.player_score
  );
  const bankerScore = safeNumber(
    result.bankerDice?.score ?? data.bankerDice?.score ?? data.bankerScore ?? data.banker_score
  );

  return {
    externalId,
    outcome,
    roundNumber: String(data.roundNumber || data.number || externalId),
    playerScore,
    bankerScore,
    timestamp,
    time: new Intl.DateTimeFormat('pt-PT', {
      timeZone: TZ,
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
      hour12: false
    }).format(new Date(timestamp)),
    source: 'bacbo-api'
  };
}

async function syncRounds() {
  if (state.syncing) return;
  state.syncing = true;

  try {
    const response = await axios.get(API_URL, {
      timeout: API_TIMEOUT,
      headers: {
        accept: 'application/json',
        'user-agent': 'BacBoSignalServer/4.0'
      }
    });

    const rounds = extractItems(response.data)
      .map(normalizeRound)
      .filter(Boolean)
      .sort((a, b) => a.timestamp - b.timestamp);

    let newCount = 0;
    for (const round of rounds) {
      if (knownRounds.has(round.externalId)) continue;

      const inserted = await saveResult(round);
      knownRounds.add(round.externalId);
      if (!inserted) continue;

      newCount++;
      state.lastRoundAt = Math.max(state.lastRoundAt, round.timestamp);
      await processResult(round);
    }

    if (newCount > 0) {
      pruneKnownRounds();
      console.log(`[sync] +${newCount} rodadas`);
    }

    state.apiError = null;
    state.apiLastSync = new Date().toISOString();
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
  res.json({
    name: 'bacbo-signal-server',
    version: '4.0-complex',
    status: 'online',
    health: '/health'
  });
});

app.get('/health', (_req, res) => {
  res.status(200).json({
    ok: true,
    uptime: Math.round(process.uptime()),
    api: { lastSync: state.apiLastSync, error: state.apiError },
    rounds: knownRounds.size,
    activeSignal: state.activeSignal
      ? {
          outcome: state.activeSignal.outcome,
          stage: state.activeSignal.stage,
          strength: state.activeSignal.strength
        }
      : null,
    paused: state.paused
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
    recentQuality: state.recentSignalQuality.slice(-8)
  });
});

app.post('/api/results', auth, async (req, res) => {
  try {
    const body = req.body || {};
    if (!['banker', 'player', 'tie'].includes(body.outcome)) {
      return res.status(400).json({ error: 'outcome must be banker, player or tie' });
    }
    const result = {
      ...body,
      source: body.source || 'manual-api',
      time: body.time || nowAngola()
    };
    await saveResult(result);
    await processResult(result);
    res.status(201).json({
      ok: true,
      stats: state.stats,
      activeSignal: state.activeSignal
    });
  } catch (err) {
    console.error('[api/results]', err.message);
    res.status(500).json({ error: 'internal' });
  }
});

app.post(WEBHOOK_PATH, (req, res) => {
  if (req.header('x-telegram-bot-api-secret-token') !== ADMIN_TOKEN) {
    return res.status(401).json({ error: 'invalid webhook secret' });
  }
  handleTelegramUpdate(req.body)
    .then(() => res.sendStatus(200))
    .catch((err) => {
      console.error('[telegram update]', err.message);
      res.sendStatus(500);
    });
});

/* ============================== START ================================ */

async function start() {
  state.statsDate = dateAngola();
  state.stats = blankStats();

  await initDb();
  await loadKnownRounds();
  await loadStats();
  await loadRuntime();
  ensureDay();
  await syncRounds();
  await configureWebhook();

  if (state.activeSignal) {
    console.log(
      `[server] retomando ciclo ativo: ${state.activeSignal.outcome} G${state.activeSignal.stage}`
    );
  } else {
    console.log('[server] nenhum ciclo ativo para retomar');
  }

  const server = app.listen(PORT, '0.0.0.0', () => {
    console.log(`[server] online na porta ${PORT} (v4 complex)`);
  });

  const syncTimer = setInterval(() => {
    syncRounds().catch((e) => console.error('[sync]', e.message));
  }, SYNC_MS);

  const signalTimer = setInterval(() => {
    maybeCreateSignal(false).catch((e) => console.error('[signal-timer]', e.message));
  }, SIGNAL_CHECK_MS);

  const shutdown = async (sig) => {
    console.log(`[server] ${sig}, encerrando...`);
    clearInterval(syncTimer);
    clearInterval(signalTimer);
    await pool?.end().catch(() => {});
    server.close(() => process.exit(0));
    setTimeout(() => process.exit(1), 8000).unref();
  };

  process.once('SIGINT', () => shutdown('SIGINT'));
  process.once('SIGTERM', () => shutdown('SIGTERM'));
}

start().catch((err) => {
  console.error('[fatal]', err);
  process.exit(1);
});

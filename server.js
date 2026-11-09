'use strict';

const express = require('express');
const cors = require('cors');
const axios = require('axios');
const crypto = require('crypto');
const { Pool } = require('pg');

/* ============================== CONFIG ============================== */

const PORT = Number(process.env.PORT || 10000);
const CHAT_ID = String(process.env.CHAT_ID || '').trim();
const BOT_TOKEN = String(process.env.BOT_TOKEN || '').trim();
const ADMIN_TOKEN = String(process.env.ADMIN_TOKEN || '').trim();
const DATABASE_URL = String(process.env.DATABASE_URL || '').trim();
const PUBLIC_URL = String(process.env.RENDER_EXTERNAL_URL || '').replace(/\/$/, '');
const WEBHOOK_PATH = '/telegram/webhook';
const API_URL = 'https://api-cs.casino.org/svc-evolution-game-events/api/bacbo?page=0&size=50&sort=data.settledAt,desc&duration=120&wheelResults=PlayerWon,BankerWon,Tie';
const API_TIMEOUT = 14000;
const SYNC_MS = 5000;
const SIGNAL_INTERVAL_MS = 5 * 60 * 1000;
const ROUND_DELAY_MS = 35000;
const MIN_HISTORY = 20;
const DEFAULT_TIE_MULTIPLIER = '4x';
const TZ = 'Africa/Luanda';

const missing = [
  ['CHAT_ID', CHAT_ID],
  ['BOT_TOKEN', BOT_TOKEN],
  ['ADMIN_TOKEN', ADMIN_TOKEN],
  ['DATABASE_URL', DATABASE_URL]
].filter(([, value]) => !value).map(([name]) => name);

if (missing.length) console.warn(`[config] Variáveis ausentes: ${missing.join(', ')}`);

/* ============================== STATE =============================== */

const app = express();
const pool = DATABASE_URL ? new Pool({ connectionString: DATABASE_URL, ssl: { rejectUnauthorized: false }, max: 5 }) : null;
const memoryResults = [];
const knownRounds = new Set();
const subscribers = new Set(CHAT_ID ? [CHAT_ID] : []);
const state = {
  activeSignal: null,
  analysisMessageId: null,
  signalMessageId: null,
  paused: false,
  lastSignalAt: 0,
  lastRoundAt: 0,
  apiError: null,
  apiLastSync: null,
  syncing: false,
  statsDate: null,
  stats: null
};

/* ============================== HELPERS ============================= */

function nowAngola() {
  return new Intl.DateTimeFormat('pt-PT', { timeZone: TZ, hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false }).format(new Date());
}

function dateAngola() {
  return new Intl.DateTimeFormat('en-CA', { timeZone: TZ, year: 'numeric', month: '2-digit', day: '2-digit' }).format(new Date());
}

function blankStats() {
  return { signals: 0, green: { direct: 0, gale1: 0, gale2: 0 }, ties: { direct: 0, gale1: 0, gale2: 0 }, reds: 0, currentStreak: 0, bestStreak: 0 };
}

function ensureDay() {
  const today = dateAngola();
  if (state.statsDate !== today) {
    state.statsDate = today;
    state.stats = blankStats();
    state.activeSignal = null;
    saveStats().catch((error) => console.error('[stats]', error.message));
  }
}

function label(outcome) {
  return outcome === 'banker' ? 'BANCA' : outcome === 'player' ? 'JOGADOR' : 'EMPATE';
}

function emoji(outcome) {
  return outcome === 'banker' ? '🔴' : outcome === 'player' ? '🔵' : '🟡';
}

function stageLabel(stage) {
  return stage === 0 ? 'ENTRADA DIRETA' : `GALE ${stage}`;
}

function cleanMultiplier(value) {
  return String(value || DEFAULT_TIE_MULTIPLIER).replace(/^×|^x/i, '');
}

function auth(req, res, next) {
  if (!ADMIN_TOKEN || req.header('x-api-key') !== ADMIN_TOKEN) return res.status(401).json({ error: 'unauthorized' });
  next();
}

function safeNumber(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
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
    CREATE TABLE IF NOT EXISTS bacbo_subscribers (chat_id VARCHAR(80) PRIMARY KEY, active BOOLEAN DEFAULT TRUE);
    CREATE TABLE IF NOT EXISTS bacbo_daily_stats (day DATE PRIMARY KEY, stats JSONB NOT NULL, updated_at TIMESTAMPTZ DEFAULT NOW());
  `);
  const { rows } = await pool.query('SELECT chat_id FROM bacbo_subscribers WHERE active=TRUE');
  rows.forEach((row) => subscribers.add(String(row.chat_id)));
}

async function loadKnownRounds() {
  if (!pool) return;
  const { rows } = await pool.query('SELECT external_id FROM bacbo_results WHERE external_id IS NOT NULL ORDER BY id DESC LIMIT 3000');
  rows.forEach((row) => knownRounds.add(row.external_id));
}

async function loadStats() {
  ensureDay();
  if (!pool) return;
  const { rows } = await pool.query('SELECT stats FROM bacbo_daily_stats WHERE day=$1', [state.statsDate]);
  if (rows[0]?.stats) state.stats = { ...blankStats(), ...rows[0].stats, green: { ...blankStats().green, ...(rows[0].stats.green || {}) }, ties: { ...blankStats().ties, ...(rows[0].stats.ties || {}) } };
}

async function saveStats() {
  if (!pool || !state.statsDate || !state.stats) return;
  await pool.query('INSERT INTO bacbo_daily_stats (day, stats, updated_at) VALUES ($1,$2,NOW()) ON CONFLICT (day) DO UPDATE SET stats=$2, updated_at=NOW()', [state.statsDate, JSON.stringify(state.stats)]);
}

async function saveResult(result) {
  if (pool) {
    const { rows } = await pool.query(`INSERT INTO bacbo_results (external_id, outcome, round_number, player_score, banker_score, source) VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT (external_id) DO NOTHING RETURNING id`, [result.externalId || null, result.outcome, result.roundNumber || null, result.playerScore, result.bankerScore, result.source || 'manual']);
    return Boolean(rows[0]);
  }
  if (result.externalId && knownRounds.has(result.externalId)) return false;
  memoryResults.unshift({ ...result, createdAt: new Date().toISOString() });
  memoryResults.splice(2000);
  return true;
}

async function recentResults(limit = 100) {
  if (pool) {
    const { rows } = await pool.query('SELECT outcome FROM bacbo_results ORDER BY created_at DESC LIMIT $1', [limit]);
    return rows;
  }
  return memoryResults.slice(0, limit);
}

/* ============================== TELEGRAM ============================ */

async function telegram(method, data = {}) {
  if (!BOT_TOKEN) throw new Error('BOT_TOKEN ausente');
  const response = await axios.post(`https://api.telegram.org/bot${BOT_TOKEN}/${method}`, data, { timeout: 12000 });
  if (!response.data?.ok) throw new Error(response.data?.description || `Telegram ${method} falhou`);
  return response.data.result;
}

async function sendTelegram(text) {
  if (!CHAT_ID) return null;
  try { return await telegram('sendMessage', { chat_id: CHAT_ID, text, parse_mode: 'Markdown', disable_web_page_preview: true }); }
  catch (error) { console.error('[telegram send]', error.message); return null; }
}

async function deleteTelegram(messageId) {
  if (!CHAT_ID || !messageId) return;
  try { await telegram('deleteMessage', { chat_id: CHAT_ID, message_id: messageId }); } catch (_) {}
}

async function clearPendingMessages() {
  await Promise.all([deleteTelegram(state.analysisMessageId), deleteTelegram(state.signalMessageId)]);
  state.analysisMessageId = null;
  state.signalMessageId = null;
}

function signalMessage(signal) {
  return `🚨 *ENTRADA CONFIRMADA*\n🎯 ${emoji(signal.outcome)} *${label(signal.outcome)}*\n🛡️ Empate · 📈 ${signal.strength}% · Até G2`;
}

function galeMessage(stage, outcome) {
  return `🔁 *FAZER GALE ${stage}*\n🎯 ${emoji(outcome)} *${label(outcome)}* · 🛡️ Empate`;
}

function resultMessage(result, stage) {
  const mode = stageLabel(stage);
  const score = result.playerScore != null && result.bankerScore != null ? ` (J ${result.playerScore} × ${result.bankerScore} B)` : '';
  const time = result.time || nowAngola();

  if (result.outcome === 'tie') {
    return `🟡✅ *GREEN EMPATE* · ${mode}\n🎯 ${emoji(state.activeSignal?.outcome)} ${label(state.activeSignal?.outcome)} → 🟡 Empate${score}\n💰 ×${cleanMultiplier(result.multiplier)} · ⏱ ${time}`;
  }

  return `✅🔥 *GREEN* · ${mode}\n🎯 ${emoji(state.activeSignal?.outcome)} ${label(state.activeSignal?.outcome)} → ${emoji(result.outcome)} ${label(result.outcome)}${score}\n⏱ ${time}`;
}

function redMessage(result) {
  return `🔴 *RED* · GALE 2\n🎯 Saiu ${emoji(result.outcome)} *${label(result.outcome)}*\n⏱ ${result.time || nowAngola()}`;
}

function historyMessage() {
  ensureDay();
  const direct = state.stats.green.direct;
  const gale1 = state.stats.green.gale1;
  const gale2 = state.stats.green.gale2;
  const ties = state.stats.ties.direct + state.stats.ties.gale1 + state.stats.ties.gale2;
  const greens = direct + gale1 + gale2 + ties;
  const closed = greens + state.stats.reds;
  const rate = closed ? ((greens / closed) * 100).toFixed(1) : '0.0';

  return `📊 *HOJE*\n✅ ${greens}G · 🟥 ${state.stats.reds}R · ${rate}%\n🎯 D:${direct} · G1:${gale1} · G2:${gale2} · 🟡 E:${ties}\n🔥 Sequência: ${state.stats.currentStreak}`;
}

async function configureWebhook() {
  if (!PUBLIC_URL || !BOT_TOKEN) return;
  try {
    await telegram('setWebhook', { url: `${PUBLIC_URL}${WEBHOOK_PATH}`, secret_token: ADMIN_TOKEN, allowed_updates: ['message'] });
    console.log('[telegram] webhook configurado');
  } catch (error) { console.error('[telegram webhook]', error.message); }
}

async function subscribe(chatId) {
  subscribers.add(String(chatId));
  if (pool) await pool.query('INSERT INTO bacbo_subscribers (chat_id, active) VALUES ($1,TRUE) ON CONFLICT (chat_id) DO UPDATE SET active=TRUE', [String(chatId)]);
}

async function handleTelegramUpdate(update) {
  const message = update?.message;
  if (!message?.text) return;

  const chatId = String(message.chat.id);
  const text = message.text.trim();
  const [command, ...args] = text.split(/\s+/);
  const cmd = command.toLowerCase().split('@')[0];

  if (cmd === '/start') {
    await subscribe(chatId);
    return telegram('sendMessage', { chat_id: chatId, text: '✅ Bot ativo. Use /sinal para analisar a mesa ou /historico para ver o balanço.' });
  }

  if (cmd === '/historico') return telegram('sendMessage', { chat_id: chatId, text: historyMessage(), parse_mode: 'Markdown' });
  if (cmd === '/sinal') { await maybeCreateSignal(true); return telegram('sendMessage', { chat_id: chatId, text: state.activeSignal ? '✅ Entrada enviada.' : '⏳ Sem entrada confirmada agora.' }); }

  if (cmd === '/status') {
    if (chatId !== CHAT_ID) return;
    return telegram('sendMessage', { chat_id: chatId, text: `✅ Online\nAPI: ${state.apiError ? 'erro' : 'OK'}\nRodadas: ${knownRounds.size}\nCiclo: ${state.activeSignal ? `G${state.activeSignal.stage}` : 'livre'}` });
  }

  if (cmd === '/pausar' || cmd === '/retomar') {
    if (chatId !== CHAT_ID) return;
    state.paused = cmd === '/pausar';
    return telegram('sendMessage', { chat_id: chatId, text: state.paused ? '⏸️ Pausado.' : '▶️ Retomado.' });
  }

  if (cmd === '/resultado') {
    if (chatId !== CHAT_ID) return;
    const outcome = String(args[0] || '').toLowerCase();
    if (!['banker', 'player', 'tie'].includes(outcome)) return telegram('sendMessage', { chat_id: chatId, text: 'Uso: /resultado banker|player|tie [multiplicador]' });
    await processResult({ outcome, multiplier: args[1] || null, source: 'telegram' });
  }
}

/* ============================== SIGNAL ============================== */

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value));
}

function weightedCounts(history) {
  const counts = { banker: 0, player: 0 };
  history.forEach((outcome, index) => {
    if (outcome !== 'banker' && outcome !== 'player') return;
    counts[outcome] += Math.pow(0.5, index / 30);
  });
  return counts;
}

function posterior(counts) {
  return (counts.banker + 1) / (counts.banker + counts.player + 2);
}

function transitionProbability(history, current) {
  const counts = { banker: 1, player: 1 };
  for (let i = 1; i < history.length; i++) {
    if (history[i] === current && (history[i - 1] === 'banker' || history[i - 1] === 'player')) counts[history[i - 1]]++;
  }
  return counts.banker / (counts.banker + counts.player);
}

function walkForwardAccuracy(history) {
  const usable = history.filter((x) => x === 'banker' || x === 'player');
  if (usable.length < 12) return 0.5;
  let correct = 0;
  let total = 0;
  for (let i = 8; i < usable.length; i++) {
    const sample = usable.slice(i);
    const recent = sample.slice(0, 8);
    const global = posterior(weightedCounts(sample));
    const recentProbability = posterior({ banker: recent.filter((x) => x === 'banker').length, player: recent.filter((x) => x === 'player').length });
    const prediction = global * 0.65 + recentProbability * 0.35 >= 0.5 ? 'banker' : 'player';
    if (prediction === usable[i - 1]) correct++;
    total++;
  }
  return total ? correct / total : 0.5;
}

function calculateSignal(rows) {
  const history = rows.map((row) => row.outcome).filter(Boolean);
  if (history.length < MIN_HISTORY) return null;

  const usable = history.filter((item) => item === 'banker' || item === 'player');
  if (usable.length < Math.max(12, MIN_HISTORY - 2)) return null;

  const globalProbability = posterior(weightedCounts(history));
  const recent = usable.slice(0, 12);
  const recentProbability = posterior({ banker: recent.filter((x) => x === 'banker').length, player: recent.filter((x) => x === 'player').length });
  const transition = transitionProbability(usable, usable[0]);
  const ensemble = clamp(globalProbability * 0.45 + recentProbability * 0.35 + transition * 0.20, 0.05, 0.95);
  const outcome = ensemble >= 0.5 ? 'banker' : 'player';
  const edge = Math.abs(ensemble - 0.5) * 2;
  const agreement = [globalProbability, recentProbability, transition].filter((p) => (p >= 0.5 ? 'banker' : 'player') === outcome).length / 3;
  const backtest = walkForwardAccuracy(history);
  const confidence = clamp(0.5 + edge * 0.22 + Math.max(0, agreement - 0.5) * 0.10 + Math.max(0, backtest - 0.5) * 0.12 * clamp(usable.length / 80, 0, 1), 0.5, 0.72);
  const strength = Math.round(clamp(80 + edge * 12 + (agreement - 0.5) * 8 + Math.max(0, backtest - 0.5) * 10, 80, 100));

  return { outcome, confidence, strength, backtest, sample: usable.length, agreement };
}

async function maybeCreateSignal(manual = false) {
  ensureDay();
  if (state.paused || state.activeSignal) return;
  if (!manual && Date.now() - state.lastSignalAt < SIGNAL_INTERVAL_MS) return;
  if (state.lastRoundAt && Date.now() - state.lastRoundAt < ROUND_DELAY_MS) return;

  const analysis = await sendTelegram('🔍 *ANALISANDO O MERCADO TROPA...*');
  state.analysisMessageId = analysis?.message_id || null;

  const signal = calculateSignal(await recentResults(100));
  if (!signal) {
    await deleteTelegram(state.analysisMessageId);
    state.analysisMessageId = null;
    return;
  }

  await deleteTelegram(state.analysisMessageId);
  state.analysisMessageId = null;

  const sent = await sendTelegram(signalMessage(signal));
  state.signalMessageId = sent?.message_id || null;
  state.activeSignal = { outcome: signal.outcome, stage: 0 };
  state.stats.signals++;
  state.lastSignalAt = Date.now();
  await saveStats();
}

async function processResult(result) {
  ensureDay();
  if (!state.activeSignal) return;

  const current = state.activeSignal;
  const stage = current.stage === 0 ? 'direct' : `gale${current.stage}`;

  if (result.outcome === 'tie') {
    state.stats.ties[stage]++;
    await clearPendingMessages();
    await sendTelegram(resultMessage(result, current.stage));
    await sendTelegram(historyMessage());
    state.activeSignal = null;
    return saveStats();
  }

  if (result.outcome === current.outcome) {
    state.stats.green[stage]++;
    state.stats.currentStreak++;
    state.stats.bestStreak = Math.max(state.stats.bestStreak, state.stats.currentStreak);
    await clearPendingMessages();
    await sendTelegram(resultMessage(result, current.stage));
    await sendTelegram(historyMessage());
    state.activeSignal = null;
    return saveStats();
  }

  if (current.stage < 2) {
    current.stage++;
    await clearPendingMessages();
    const sent = await sendTelegram(galeMessage(current.stage, current.outcome));
    state.signalMessageId = sent?.message_id || null;
    return;
  }

  state.stats.reds++;
  state.stats.currentStreak = 0;
  await clearPendingMessages();
  await sendTelegram(redMessage(result));
  await sendTelegram(historyMessage());
  state.activeSignal = null;
  await saveStats();
}

/* ============================== BAC BO API ========================== */

function normalizeOutcome(value) {
  const text = String(value || '').normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLowerCase().replace(/[\s_-]+/g, '');
  if (['player', 'jogador', 'playerwon', 'blue'].includes(text)) return 'player';
  if (['banker', 'banca', 'bankerwon', 'red'].includes(text)) return 'banker';
  if (['tie', 'draw', 'empate', 'push'].includes(text)) return 'tie';
  return null;
}

function extractItems(body) {
  if (Array.isArray(body)) return body;
  const candidates = [body?.data, body?.results, body?.items, body?.rounds, body?.events, body?.data?.results, body?.data?.items, body?.data?.rounds];
  return candidates.find(Array.isArray) || [];
}

function normalizeRound(item) {
  const data = item?.data || item?.round || item || {};
  const result = data.result && typeof data.result === 'object' ? data.result : item?.result && typeof item.result === 'object' ? item.result : {};
  const settledAt = data.settledAt || data.endTime || data.endedAt || data.finishedAt || item?.settledAt;
  const timestamp = new Date(settledAt).getTime();
  const outcome = normalizeOutcome(result.outcome || result.winner || data.outcome || data.winner || item?.outcome || item?.winner || data.result);
  const externalId = String(data.id || data.roundId || data.eventId || item?.id || item?.roundId || item?.eventId || '').trim();
  if (!externalId || !outcome || !Number.isFinite(timestamp)) return null;

  const playerScore = safeNumber(result.playerDice?.score ?? data.playerDice?.score ?? data.playerScore ?? data.player_score);
  const bankerScore = safeNumber(result.bankerDice?.score ?? data.bankerDice?.score ?? data.bankerScore ?? data.banker_score);

  return {
    externalId,
    outcome,
    roundNumber: String(data.roundNumber || data.number || externalId),
    playerScore,
    bankerScore,
    timestamp,
    time: new Intl.DateTimeFormat('pt-PT', { timeZone: TZ, hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false }).format(new Date(timestamp)),
    source: 'bacbo-api'
  };
}

async function syncRounds() {
  if (state.syncing) return;
  state.syncing = true;

  try {
    const response = await axios.get(API_URL, { timeout: API_TIMEOUT, headers: { accept: 'application/json', 'user-agent': 'BacBoSingleServer/2.0' } });
    const rounds = extractItems(response.data).map(normalizeRound).filter(Boolean).sort((a, b) => a.timestamp - b.timestamp);

    for (const round of rounds) {
      if (knownRounds.has(round.externalId)) continue;
      const inserted = await saveResult(round);
      knownRounds.add(round.externalId);
      if (!inserted) continue;
      state.lastRoundAt = Math.max(state.lastRoundAt, round.timestamp);
      await processResult(round);
    }

    state.apiError = null;
    state.apiLastSync = new Date().toISOString();
  } catch (error) {
    state.apiError = error.message;
    console.error('[bacbo api]', error.message);
  } finally {
    state.syncing = false;
  }
}

/* ============================== HTTP ================================= */

app.disable('x-powered-by');
app.use(cors({ origin: false }));
app.use(express.json({ limit: '256kb' }));

app.get('/', (_req, res) => res.json({ name: 'bacbo-single-server', status: 'online', health: '/health' }));
app.get('/health', (_req, res) => res.status(200).json({ ok: true, uptime: Math.round(process.uptime()), api: { lastSync: state.apiLastSync, error: state.apiError }, rounds: knownRounds.size }));
app.get('/api/status', auth, (_req, res) => res.json({ ok: true, stats: state.stats, activeSignal: state.activeSignal, apiError: state.apiError, rounds: knownRounds.size, uptime: process.uptime() }));
app.post('/api/results', auth, async (req, res) => {
  const body = req.body || {};
  if (!['banker', 'player', 'tie'].includes(body.outcome)) return res.status(400).json({ error: 'outcome must be banker, player or tie' });
  const result = { ...body, source: body.source || 'manual-api' };
  await saveResult(result);
  await processResult(result);
  res.status(201).json({ ok: true, stats: state.stats, activeSignal: state.activeSignal });
});
app.post(WEBHOOK_PATH, (req, res) => {
  if (req.header('x-telegram-bot-api-secret-token') !== ADMIN_TOKEN) return res.status(401).json({ error: 'invalid webhook secret' });
  handleTelegramUpdate(req.body).then(() => res.sendStatus(200)).catch((error) => { console.error('[telegram update]', error.message); res.sendStatus(500); });
});

/* ============================== START ================================= */

async function start() {
  state.statsDate = dateAngola();
  state.stats = blankStats();
  await initDb();
  await loadKnownRounds();
  await loadStats();
  await syncRounds();
  await configureWebhook();

  const server = app.listen(PORT, '0.0.0.0', () => console.log(`[server] online na porta ${PORT}`));
  const syncTimer = setInterval(syncRounds, SYNC_MS);
  const signalTimer = setInterval(() => maybeCreateSignal(false).catch((error) => console.error('[signal]', error.message)), 60000);

  const shutdown = async (signal) => {
    console.log(`[server] ${signal}, encerrando`);
    clearInterval(syncTimer);
    clearInterval(signalTimer);
    await pool?.end().catch(() => {});
    server.close(() => process.exit(0));
    setTimeout(() => process.exit(1), 10000).unref();
  };

  process.once('SIGINT', () => shutdown('SIGINT'));
  process.once('SIGTERM', () => shutdown('SIGTERM'));
}

start().catch((error) => {
  console.error('[fatal]', error);
  process.exit(1);
});

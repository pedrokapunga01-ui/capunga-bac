
'use strict';

const express = require('express');
const cors = require('cors');
const axios = require('axios');
const crypto = require('crypto');
const { Pool } = require('pg');

/* ============================== CONFIG ============================== */

const env = (name, fallback) => {
  const v = process.env[name];
  return v === undefined || v === '' ? fallback : v;
};
const num = (name, fallback) => {
  const n = Number(env(name, fallback));
  return Number.isFinite(n) ? n : fallback;
};

const PORT = num('PORT', 10000);
const CHAT_ID = String(env('CHAT_ID', '')).trim();
const BOT_TOKEN = String(env('BOT_TOKEN', '')).trim();
const ADMIN_TOKEN = String(env('ADMIN_TOKEN', '')).trim();
const DATABASE_URL = String(env('DATABASE_URL', '')).trim();
const PGSSL_DISABLE = String(env('PGSSL', '')).toLowerCase() === 'disable';
const ADMIN_USER_IDS = new Set(String(env('ADMIN_USER_IDS', '')).split(',').map((s) => s.trim()).filter(Boolean));
const PUBLIC_URL = String(env('RENDER_EXTERNAL_URL', '')).replace(/\/$/, '');
const WEBHOOK_PATH = '/telegram/webhook';
const WEBHOOK_SECRET = ADMIN_TOKEN ? crypto.createHmac('sha256', ADMIN_TOKEN).update('tg-webhook').digest('hex') : '';

const API_URL = 'https://api-cs.casino.org/svc-evolution-game-events/api/bacbo?page=0&size=50&sort=data.settledAt,desc&duration=120&wheelResults=PlayerWon,BankerWon,Tie';
const API_TIMEOUT = 14000;
const SYNC_MS = 4000;
const SYNC_MAX_BACKOFF_MS = 60000;
const API_STALE_MS = 60000; // API sem sync com sucesso há mais disto => não emite sinais

// Frequência / gestão de risco (tudo configurável por env)
const SIGNAL_INTERVAL_MS = num('SIGNAL_INTERVAL_MIN', 4) * 60 * 1000; // mínimo entre sinais
const MAX_SIGNALS_DAY = num('MAX_SIGNALS_DAY', 15);
const STOP_LOSS_REDS = num('STOP_LOSS_REDS', 2); // REDs seguidos até pausa automática
const COOLDOWN_MS = num('COOLDOWN_MIN', 30) * 60 * 1000;
const SIGNAL_TTL_MS = num('SIGNAL_TTL_MIN', 8) * 60 * 1000; // sinal sem resultado => anulado
const FRESH_MS = num('FRESH_SECONDS', 45) * 1000; // só emite logo após uma rodada fechar
const ANALYSIS_DELAY_MS = num('ANALYSIS_DELAY_MS', 2500);
const MIN_HISTORY = 30;
const MIN_EDGE = 0.04;
const MIN_ALIGN = 2;
const MAX_KNOWN = 5000;
const RECENT_CYCLES = 50;
const DEFAULT_TIE_MULTIPLIER = '4x';
const TZ = 'Africa/Luanda';

const missing = [['CHAT_ID', CHAT_ID], ['BOT_TOKEN', BOT_TOKEN], ['ADMIN_TOKEN', ADMIN_TOKEN], ['DATABASE_URL', DATABASE_URL]]
  .filter(([, v]) => !v).map(([n]) => n);
if (missing.length) console.warn(`[config] Variáveis ausentes: ${missing.join(', ')}`);

/* ============================== STATE =============================== */

const app = express();
const pool = DATABASE_URL
  ? new Pool({ connectionString: DATABASE_URL, ssl: PGSSL_DISABLE ? false : { rejectUnauthorized: false }, max: 5 })
  : null;
if (pool) pool.on('error', (e) => console.error('[pg pool]', e.message));

const memoryResults = [];
const knownRounds = new Set();
const state = {
  activeSignal: null, // { id, outcome, stage, align, createdAt, since, messageId }
  paused: false,
  pausedUntil: 0,
  consecutiveReds: 0,
  recent: [], // últimos ciclos fechados: 'G' | 'R'
  lastSignalAt: 0,
  lastRoundAt: 0,
  apiError: null,
  apiLastSync: null,
  apiLastSyncMs: 0,
  statsDate: null,
  stats: null
};

// Fila: tudo o que altera estado corre em série (sync, webhook, API manual, sinais)
let chain = Promise.resolve();
function exclusive(fn) {
  const run = chain.then(fn);
  chain = run.catch(() => {});
  return run;
}

/* ============================== HELPERS ============================= */

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function fmtTime(date) {
  return new Intl.DateTimeFormat('pt-PT', { timeZone: TZ, hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false }).format(date);
}
const nowAngola = () => fmtTime(new Date());
const dateAngola = () => new Intl.DateTimeFormat('en-CA', { timeZone: TZ, year: 'numeric', month: '2-digit', day: '2-digit' }).format(new Date());

function blankStats() {
  return { signals: 0, voids: 0, green: { direct: 0, gale1: 0, gale2: 0 }, ties: { direct: 0, gale1: 0, gale2: 0 }, reds: 0, currentStreak: 0, bestStreak: 0, limitNotified: false };
}

function mergeStats(saved) {
  const b = blankStats();
  return { ...b, ...saved, green: { ...b.green, ...(saved?.green || {}) }, ties: { ...b.ties, ...(saved?.ties || {}) } };
}

function ensureDay() {
  const today = dateAngola();
  if (state.statsDate !== today) {
    state.statsDate = today;
    state.stats = blankStats();
    // Um ciclo ativo NÃO é apagado na virada do dia; termina normalmente.
    saveStats().catch((e) => console.error('[stats]', e.message));
  }
}

const label = (o) => (o === 'banker' ? 'BANCA' : o === 'player' ? 'JOGADOR' : 'EMPATE');
const emoji = (o) => (o === 'banker' ? '🔴' : o === 'player' ? '🔵' : '🟡');
const stageLabel = (s) => (s === 0 ? 'ENTRADA DIRETA' : `GALE ${s}`);
const cleanMultiplier = (v) => String(v || DEFAULT_TIE_MULTIPLIER).replace(/^×|^x/i, '');

function safeEqual(a, b) {
  const A = Buffer.from(String(a || ''));
  const B = Buffer.from(String(b || ''));
  return A.length === B.length && crypto.timingSafeEqual(A, B);
}

function auth(req, res, next) {
  if (!ADMIN_TOKEN || !safeEqual(req.header('x-api-key'), ADMIN_TOKEN)) return res.status(401).json({ error: 'unauthorized' });
  next();
}

function safeNumber(v) {
  if (v === null || v === undefined || v === '') return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function rememberRound(id) {
  knownRounds.add(id);
  while (knownRounds.size > MAX_KNOWN) knownRounds.delete(knownRounds.values().next().value);
}

function totals() {
  const { green, ties, reds, voids } = state.stats;
  const tieTotal = ties.direct + ties.gale1 + ties.gale2;
  const greens = green.direct + green.gale1 + green.gale2 + tieTotal;
  const closed = greens + reds;
  const rate = closed ? ((greens / closed) * 100).toFixed(1) : '0.0';
  return { greens, tieTotal, reds, voids, closed, rate };
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
      settled_at TIMESTAMPTZ,
      created_at TIMESTAMPTZ DEFAULT NOW()
    );
    ALTER TABLE bacbo_results ADD COLUMN IF NOT EXISTS settled_at TIMESTAMPTZ;
    CREATE INDEX IF NOT EXISTS bacbo_results_settled_idx ON bacbo_results (settled_at DESC NULLS LAST, id DESC);
    CREATE TABLE IF NOT EXISTS bacbo_daily_stats (day DATE PRIMARY KEY, stats JSONB NOT NULL, updated_at TIMESTAMPTZ DEFAULT NOW());
    CREATE TABLE IF NOT EXISTS bacbo_state (key VARCHAR(40) PRIMARY KEY, value JSONB NOT NULL, updated_at TIMESTAMPTZ DEFAULT NOW());
  `);
}

async function loadKnownRounds() {
  if (!pool) return;
  const { rows } = await pool.query('SELECT external_id FROM bacbo_results WHERE external_id IS NOT NULL ORDER BY id DESC LIMIT 3000');
  rows.forEach((r) => rememberRound(r.external_id));
}

async function loadStats() {
  ensureDay();
  if (!pool) return;
  const { rows } = await pool.query('SELECT stats FROM bacbo_daily_stats WHERE day=$1', [state.statsDate]);
  if (rows[0]?.stats) state.stats = mergeStats(rows[0].stats);
}

async function saveStats() {
  if (!pool || !state.statsDate || !state.stats) return;
  await pool.query(
    'INSERT INTO bacbo_daily_stats (day, stats, updated_at) VALUES ($1,$2,NOW()) ON CONFLICT (day) DO UPDATE SET stats=$2, updated_at=NOW()',
    [state.statsDate, JSON.stringify(state.stats)]
  );
}

async function loadState() {
  if (!pool) return;
  const { rows } = await pool.query("SELECT value FROM bacbo_state WHERE key='main'");
  const v = rows[0]?.value;
  if (!v) return;
  state.activeSignal = v.activeSignal || null;
  state.lastSignalAt = Number(v.lastSignalAt) || 0;
  state.paused = Boolean(v.paused);
  state.pausedUntil = Number(v.pausedUntil) || 0;
  state.consecutiveReds = Number(v.consecutiveReds) || 0;
  state.recent = Array.isArray(v.recent) ? v.recent.slice(-RECENT_CYCLES) : [];
}

async function saveState() {
  if (!pool) return;
  const value = {
    activeSignal: state.activeSignal,
    lastSignalAt: state.lastSignalAt,
    paused: state.paused,
    pausedUntil: state.pausedUntil,
    consecutiveReds: state.consecutiveReds,
    recent: state.recent
  };
  await pool.query(
    "INSERT INTO bacbo_state (key, value, updated_at) VALUES ('main',$1,NOW()) ON CONFLICT (key) DO UPDATE SET value=$1, updated_at=NOW()",
    [JSON.stringify(value)]
  );
}

async function persist() {
  await Promise.all([saveStats(), saveState()]);
}

async function saveResult(r) {
  const ts = r.timestamp || Date.now();
  if (pool) {
    const { rows } = await pool.query(
      `INSERT INTO bacbo_results (external_id, outcome, round_number, player_score, banker_score, source, settled_at)
       VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT (external_id) DO NOTHING RETURNING id`,
      [r.externalId || null, r.outcome, r.roundNumber || null, r.playerScore ?? null, r.bankerScore ?? null, r.source || 'manual', new Date(ts)]
    );
    return Boolean(rows[0]);
  }
  if (r.externalId && memoryResults.some((m) => m.externalId === r.externalId)) return false;
  memoryResults.push({ ...r, timestamp: ts });
  memoryResults.sort((a, b) => b.timestamp - a.timestamp);
  memoryResults.splice(2000);
  return true;
}

// Mais recente primeiro, pela hora REAL da rodada
async function recentResults(limit = 100) {
  if (pool) {
    const { rows } = await pool.query('SELECT outcome FROM bacbo_results ORDER BY settled_at DESC NULLS LAST, id DESC LIMIT $1', [limit]);
    return rows;
  }
  return memoryResults.slice(0, limit);
}

/* ============================== TELEGRAM ============================ */

async function telegram(method, data = {}, retry = true) {
  if (!BOT_TOKEN) throw new Error('BOT_TOKEN ausente');
  try {
    const res = await axios.post(`https://api.telegram.org/bot${BOT_TOKEN}/${method}`, data, { timeout: 12000 });
    if (!res.data?.ok) throw new Error(res.data?.description || `Telegram ${method} falhou`);
    return res.data.result;
  } catch (error) {
    const wait = error.response?.data?.parameters?.retry_after;
    if (retry && error.response?.status === 429 && wait !== undefined && wait <= 30) {
      await sleep((wait + 1) * 1000);
      return telegram(method, data, false);
    }
    throw new Error(error.response?.data?.description || error.message);
  }
}

async function sendTelegram(text, { chatId = CHAT_ID, replyTo = null } = {}) {
  if (!chatId) return null;
  const payload = { chat_id: chatId, text, parse_mode: 'HTML', disable_web_page_preview: true };
  if (replyTo) { payload.reply_to_message_id = replyTo; payload.allow_sending_without_reply = true; }
  try { return await telegram('sendMessage', payload); }
  catch (e) { console.error('[telegram send]', e.message); return null; }
}

async function deleteTelegram(messageId) {
  if (!CHAT_ID || !messageId) return;
  try { await telegram('deleteMessage', { chat_id: CHAT_ID, message_id: messageId }, false); } catch (_) {}
}

/* ---------- Modelos de mensagem ---------- */

const LINE = '━━━━━━━━━━━━━━';

function signalMessage(s) {
  return [
    `🚨 <b>SINAL #${s.id}</b> · ⏱ ${nowAngola()}`,
    LINE,
    `🎯 ${emoji(s.outcome)} <b>${label(s.outcome)}</b>`,
    '🛡️ Cobrir <b>EMPATE</b>',
    '🔁 Até <b>GALE 2</b>',
    `📐 Janelas alinhadas: <b>${s.align}/3</b>`,
    LINE,
    '⚠️ Indicativo, sem garantia de ganho.'
  ].join('\n');
}

function galeMessage(s) {
  return [`🔁 <b>FAZER GALE ${s.stage}</b> · SINAL #${s.id}`, `🎯 ${emoji(s.outcome)} <b>${label(s.outcome)}</b> + 🛡️ Empate`, `⏱ ${nowAngola()}`].join('\n');
}

function summaryLine() {
  const t = totals();
  return `📊 Hoje: ${t.greens}✅ ${t.reds}❌ · ${t.rate}%`;
}

function scoreText(r) {
  return r.playerScore != null && r.bankerScore != null ? ` (J ${r.playerScore} × ${r.bankerScore} B)` : '';
}

function greenMessage(r, s) {
  const time = r.time || nowAngola();
  if (r.outcome === 'tie') {
    return [`🟡✅ <b>GREEN EMPATE</b> · SINAL #${s.id} · ${stageLabel(s.stage)}`, `🎯 ${emoji(s.outcome)} ${label(s.outcome)} → 🟡 Empate${scoreText(r)}`, `💰 ×${cleanMultiplier(r.multiplier)} · ⏱ ${time}`, summaryLine()].join('\n');
  }
  return [`✅🔥 <b>GREEN</b> · SINAL #${s.id} · ${stageLabel(s.stage)}`, `🎯 ${emoji(s.outcome)} ${label(s.outcome)} → ${emoji(r.outcome)} ${label(r.outcome)}${scoreText(r)}`, `⏱ ${time}`, summaryLine()].join('\n');
}

function redMessage(r, s) {
  return [`❌ <b>RED</b> · SINAL #${s.id} · GALE 2`, `🎯 Saiu ${emoji(r.outcome)} <b>${label(r.outcome)}</b>${scoreText(r)}`, `⏱ ${r.time || nowAngola()}`, summaryLine()].join('\n');
}

function voidMessage(s, reason) {
  return `⚪ <b>SINAL #${s.id} ANULADO</b>\n${reason}\nNão conta para o balanço.`;
}

function historyMessage() {
  ensureDay();
  const { green, ties, currentStreak, bestStreak } = state.stats;
  const t = totals();
  const last = state.recent.slice(-20).map((c) => (c === 'G' ? '✅' : '❌')).join('') || '—';
  return [
    '📊 <b>BALANÇO DE HOJE</b>',
    LINE,
    `✅ Green: <b>${t.greens}</b> (D ${green.direct} · G1 ${green.gale1} · G2 ${green.gale2} · 🟡 ${t.tieTotal})`,
    `❌ Red: <b>${t.reds}</b>${t.voids ? ` · ⚪ Anulados: ${t.voids}` : ''}`,
    `🎯 Taxa real: <b>${t.rate}%</b> (${t.closed} ciclos)`,
    `🔥 Sequência: ${currentStreak} · Melhor: ${bestStreak}`,
    `🕒 Últimos 20: ${last}`,
    LINE,
    'ℹ️ Um RED custa várias entradas de GREEN. Taxa alta não é lucro garantido.'
  ].join('\n');
}

/* ---------- Webhook e comandos ---------- */

async function configureWebhook() {
  if (!PUBLIC_URL || !BOT_TOKEN || !WEBHOOK_SECRET) return;
  try {
    await telegram('setWebhook', { url: `${PUBLIC_URL}${WEBHOOK_PATH}`, secret_token: WEBHOOK_SECRET, allowed_updates: ['message'] });
    console.log('[telegram] webhook configurado');
  } catch (e) { console.error('[telegram webhook]', e.message); }
}

function isAdmin(message) {
  return String(message.chat.id) === CHAT_ID || ADMIN_USER_IDS.has(String(message.from?.id));
}

async function handleTelegramUpdate(update) {
  const message = update?.message;
  if (!message?.text) return;

  const chatId = String(message.chat.id);
  const [command, ...args] = message.text.trim().split(/\s+/);
  const cmd = command.toLowerCase().split('@')[0];
  const reply = (text) => sendTelegram(text, { chatId });

  if (cmd === '/start') return reply('✅ Bot ativo. Use /historico para ver o balanço de hoje.');
  if (cmd === '/historico') return reply(historyMessage());

  if (!isAdmin(message)) return;

  if (cmd === '/ajuda') {
    return reply('/historico · /status · /sinal · /cancelar · /pausar · /retomar · /resultado banker|player|tie [mult]');
  }

  if (cmd === '/status') {
    const left = Math.max(0, Math.round((state.pausedUntil - Date.now()) / 60000));
    return reply([
      '✅ Online',
      `API: ${state.apiError ? `erro (${state.apiError})` : 'OK'} · sync ${state.apiLastSync || '-'}`,
      `Rodadas: ${knownRounds.size}`,
      `Pausa manual: ${state.paused ? 'sim' : 'não'} · Pausa de segurança: ${left ? `${left} min` : 'não'}`,
      `Ciclo: ${state.activeSignal ? `#${state.activeSignal.id} G${state.activeSignal.stage}` : 'livre'}`,
      `Sinais hoje: ${state.stats.signals}/${MAX_SIGNALS_DAY}`
    ].join('\n'));
  }

  if (cmd === '/pausar') {
    state.paused = true;
    await exclusive(saveState);
    return reply('⏸️ Pausado.');
  }

  if (cmd === '/retomar') {
    state.paused = false;
    state.pausedUntil = 0;
    state.consecutiveReds = 0;
    await exclusive(saveState);
    return reply('▶️ Retomado.');
  }

  if (cmd === '/cancelar') {
    const ok = await exclusive(() => voidSignal('Cancelado pelo administrador.'));
    return reply(ok ? '⚪ Sinal anulado.' : 'Nenhum sinal ativo.');
  }

  if (cmd === '/sinal') {
    const r = await maybeCreateSignal(true);
    return reply(r.ok ? '✅ Sinal enviado.' : `⏳ ${r.reason}`);
  }

  if (cmd === '/resultado') {
    const outcome = String(args[0] || '').toLowerCase();
    if (!['banker', 'player', 'tie'].includes(outcome)) return reply('Uso: /resultado banker|player|tie [multiplicador]');
    const result = { outcome, multiplier: args[1] || null, source: 'telegram', timestamp: Date.now() };
    return exclusive(async () => {
      await saveResult(result);
      await processResult(result);
    });
  }
}

/* ============================== SIGNAL ============================== */

/*
 * IMPORTANTE: rodadas de Bac Bo são independentes. Nenhum padrão do histórico
 * dá vantagem real. Este seletor segue a tendência recente apenas como CRITÉRIO
 * de escolha; "janelas alinhadas" descreve concordância entre janelas de 8/20/50
 * rodadas, NÃO é probabilidade de ganho. A medida honesta de desempenho é a taxa
 * REAL de ciclos fechados, mostrada em /historico.
 */
function calculateSignal(rows) {
  const usable = rows.map((r) => r.outcome).filter((o) => o === 'banker' || o === 'player'); // mais recente primeiro
  if (usable.length < MIN_HISTORY) return null;

  let banker = 0;
  let player = 0;
  usable.forEach((o, i) => {
    const w = Math.pow(0.5, i / 15);
    if (o === 'banker') banker += w; else player += w;
  });
  const prob = (banker + 1) / (banker + player + 2);
  if (Math.abs(prob - 0.5) < MIN_EDGE) return null; // tendência neutra

  const outcome = prob >= 0.5 ? 'banker' : 'player';
  const lean = (n) => {
    const w = usable.slice(0, n);
    const b = w.filter((x) => x === 'banker').length;
    const p = w.length - b;
    return b > p ? 'banker' : p > b ? 'player' : null;
  };
  const align = [8, 20, 50].filter((n) => lean(n) === outcome).length;
  if (align < MIN_ALIGN) return null;

  return { outcome, align, sample: usable.length };
}

function apiHealthy() {
  return !state.apiError && state.apiLastSyncMs && Date.now() - state.apiLastSyncMs < API_STALE_MS;
}

// Chamar sempre dentro de exclusive()
async function createSignalLocked(manual = false) {
  ensureDay();
  const now = Date.now();
  const no = (reason) => ({ ok: false, reason });

  if (state.activeSignal) return no('Já existe um ciclo ativo.');
  if (state.paused) return no('Bot pausado.');
  if (state.pausedUntil > now) return no('Pausa de segurança ativa.');
  if (!apiHealthy()) return no('API sem dados atualizados.');
  if (!state.lastRoundAt) return no('Sem rodadas ainda.');
  if (!manual) {
    if (state.stats.signals >= MAX_SIGNALS_DAY) return no('Limite diário atingido.');
    if (now - state.lastSignalAt < SIGNAL_INTERVAL_MS) return no('Intervalo mínimo entre sinais.');
    if (now - state.lastRoundAt > FRESH_MS) return no('Rodada antiga; aguardando a próxima.');
  }

  const signal = calculateSignal(await recentResults(100));
  if (!signal) return no('Histórico insuficiente ou tendência neutra.');

  const analysis = await sendTelegram('🔍 <b>ANALISANDO A MESA...</b>');
  if (ANALYSIS_DELAY_MS) await sleep(ANALYSIS_DELAY_MS);
  await deleteTelegram(analysis?.message_id);

  state.stats.signals++;
  const active = { id: state.stats.signals, outcome: signal.outcome, stage: 0, align: signal.align, createdAt: Date.now(), since: Date.now(), messageId: null };
  const sent = await sendTelegram(signalMessage(active));
  active.messageId = sent?.message_id || null;
  state.activeSignal = active;
  state.lastSignalAt = active.createdAt;
  await persist();
  return { ok: true, reason: 'ok' };
}

function maybeCreateSignal(manual = false) {
  return exclusive(() => createSignalLocked(manual));
}

async function voidSignal(reason) {
  const s = state.activeSignal;
  if (!s) return false;
  ensureDay();
  state.stats.voids++;
  await sendTelegram(voidMessage(s, reason), { replyTo: s.messageId });
  state.activeSignal = null;
  await persist();
  return true;
}

async function housekeeping() {
  const s = state.activeSignal;
  if (s && Date.now() - s.createdAt > SIGNAL_TTL_MS) await voidSignal('Sem resultado a tempo.');
}

async function closeCycle(s, kind, text) {
  state.recent.push(kind);
  if (state.recent.length > RECENT_CYCLES) state.recent.splice(0, state.recent.length - RECENT_CYCLES);
  state.activeSignal = null;
  await persist();
  await sendTelegram(text, { replyTo: s.messageId });

  if (kind === 'R' && STOP_LOSS_REDS > 0 && state.consecutiveReds >= STOP_LOSS_REDS) {
    state.pausedUntil = Date.now() + COOLDOWN_MS;
    state.consecutiveReds = 0;
    await sendTelegram(`⏸️ <b>PAUSA DE SEGURANÇA</b>\n${STOP_LOSS_REDS} REDs seguidos. Sem sinais por ${Math.round(COOLDOWN_MS / 60000)} min.`);
  }
  if (state.stats.signals >= MAX_SIGNALS_DAY && !state.stats.limitNotified) {
    state.stats.limitNotified = true;
    await sendTelegram('🏁 <b>Limite diário de sinais atingido.</b> Voltamos amanhã.');
  }
  await persist();
}

// Chamar sempre dentro de exclusive()
async function processResult(result) {
  ensureDay();
  const s = state.activeSignal;
  if (!s) return;
  // Só conta rodadas posteriores ao sinal e em ordem
  if (!result.timestamp || result.timestamp <= s.since) return;
  s.since = result.timestamp;

  const key = s.stage === 0 ? 'direct' : `gale${s.stage}`;

  if (result.outcome === 'tie' || result.outcome === s.outcome) {
    if (result.outcome === 'tie') state.stats.ties[key]++; else state.stats.green[key]++;
    state.stats.currentStreak++;
    state.stats.bestStreak = Math.max(state.stats.bestStreak, state.stats.currentStreak);
    state.consecutiveReds = 0;
    return closeCycle(s, 'G', greenMessage(result, s));
  }

  if (s.stage < 2) {
    s.stage++;
    await sendTelegram(galeMessage(s), { replyTo: s.messageId });
    return persist();
  }

  state.stats.reds++;
  state.stats.currentStreak = 0;
  state.consecutiveReds++;
  return closeCycle(s, 'R', redMessage(result, s));
}

/* ============================== BAC BO API ========================== */

function normalizeOutcome(value) {
  const t = String(value || '').normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLowerCase().replace(/[\s_-]+/g, '');
  if (['player', 'jogador', 'playerwon', 'blue'].includes(t)) return 'player';
  if (['banker', 'banca', 'bankerwon', 'red'].includes(t)) return 'banker';
  if (['tie', 'draw', 'empate', 'push'].includes(t)) return 'tie';
  return null;
}

function extractItems(body) {
  if (Array.isArray(body)) return body;
  const c = [body?.data, body?.results, body?.items, body?.rounds, body?.events, body?.data?.results, body?.data?.items, body?.data?.rounds];
  return c.find(Array.isArray) || [];
}

function normalizeRound(item) {
  const data = item?.data || item?.round || item || {};
  const result = data.result && typeof data.result === 'object' ? data.result : item?.result && typeof item.result === 'object' ? item.result : {};
  const settledAt = data.settledAt || data.endTime || data.endedAt || data.finishedAt || item?.settledAt;
  const timestamp = new Date(settledAt).getTime();
  const outcome = normalizeOutcome(result.outcome || result.winner || data.outcome || data.winner || item?.outcome || item?.winner || data.result);
  const externalId = String(data.id || data.roundId || data.eventId || item?.id || item?.roundId || item?.eventId || '').trim();
  if (!externalId || !outcome || !Number.isFinite(timestamp)) return null;

  return {
    externalId,
    outcome,
    roundNumber: String(data.roundNumber || data.number || externalId),
    playerScore: safeNumber(result.playerDice?.score ?? data.playerDice?.score ?? data.playerScore ?? data.player_score),
    bankerScore: safeNumber(result.bankerDice?.score ?? data.bankerDice?.score ?? data.bankerScore ?? data.banker_score),
    multiplier: result.tieMultiplier ?? result.multiplier ?? data.multiplier ?? null,
    timestamp,
    time: fmtTime(new Date(timestamp)),
    source: 'bacbo-api'
  };
}

async function syncOnce() {
  const response = await axios.get(API_URL, { timeout: API_TIMEOUT, headers: { accept: 'application/json', 'user-agent': 'BacBoServer/3.0' } });
  const rounds = extractItems(response.data).map(normalizeRound).filter(Boolean).sort((a, b) => a.timestamp - b.timestamp);
  state.apiError = null;
  state.apiLastSync = new Date().toISOString();
  state.apiLastSyncMs = Date.now();

  await exclusive(async () => {
    let fresh = 0;
    for (const round of rounds) {
      if (knownRounds.has(round.externalId)) continue;
      const inserted = await saveResult(round);
      rememberRound(round.externalId);
      if (!inserted) continue;
      fresh++;
      state.lastRoundAt = Math.max(state.lastRoundAt, round.timestamp);
      try { await processResult(round); } catch (e) { console.error('[processResult]', e.message); }
    }
    await housekeeping();
    // Evento: logo após uma rodada fechar é o melhor momento para emitir sinal
    if (fresh) {
      try { await createSignalLocked(false); } catch (e) { console.error('[signal]', e.message); }
    }
  });
}

let syncDelay = SYNC_MS;
let stopped = false;
let syncTimer = null;
async function syncLoop() {
  if (stopped) return;
  try {
    await syncOnce();
    syncDelay = SYNC_MS;
  } catch (e) {
    state.apiError = e.message;
    syncDelay = Math.min(syncDelay * 2, SYNC_MAX_BACKOFF_MS);
    console.error('[bacbo api]', e.message);
  }
  syncTimer = setTimeout(syncLoop, syncDelay);
}

/* ============================== HTTP ================================= */

app.disable('x-powered-by');
app.use(cors({ origin: false }));
app.use(express.json({ limit: '256kb' }));

app.get('/', (_req, res) => res.json({ name: 'bacbo-server', status: 'online', health: '/health' }));
app.get('/health', (_req, res) => res.status(200).json({ ok: true, uptime: Math.round(process.uptime()), api: { lastSync: state.apiLastSync, error: state.apiError }, rounds: knownRounds.size }));
app.get('/api/status', auth, (_req, res) => res.json({ ok: true, stats: state.stats, activeSignal: state.activeSignal, paused: state.paused, pausedUntil: state.pausedUntil, recent: state.recent, apiError: state.apiError, rounds: knownRounds.size, uptime: process.uptime() }));

app.post('/api/results', auth, async (req, res, next) => {
  try {
    const body = req.body || {};
    if (!['banker', 'player', 'tie'].includes(body.outcome)) return res.status(400).json({ error: 'outcome must be banker, player or tie' });
    const result = {
      outcome: body.outcome,
      externalId: body.externalId ? String(body.externalId).slice(0, 180) : null,
      roundNumber: body.roundNumber || null,
      playerScore: safeNumber(body.playerScore),
      bankerScore: safeNumber(body.bankerScore),
      multiplier: body.multiplier || null,
      source: 'manual-api',
      timestamp: Date.now()
    };
    await exclusive(async () => {
      const inserted = await saveResult(result);
      if (result.externalId) rememberRound(result.externalId);
      if (inserted) { state.lastRoundAt = result.timestamp; await processResult(result); }
    });
    res.status(201).json({ ok: true, stats: state.stats, activeSignal: state.activeSignal });
  } catch (e) { next(e); }
});

app.post(WEBHOOK_PATH, (req, res) => {
  if (!safeEqual(req.header('x-telegram-bot-api-secret-token'), WEBHOOK_SECRET)) return res.status(401).json({ error: 'invalid webhook secret' });
  res.sendStatus(200); // responde já: evita reenvios do Telegram
  handleTelegramUpdate(req.body).catch((e) => console.error('[telegram update]', e.message));
});

app.use((err, _req, res, _next) => {
  console.error('[http]', err.message);
  res.status(err.status || 500).json({ error: err.status === 400 ? 'bad request' : 'internal error' });
});

/* ============================== START ================================= */

async function start() {
  process.on('unhandledRejection', (e) => console.error('[unhandledRejection]', e?.message || e));
  process.on('uncaughtException', (e) => console.error('[uncaughtException]', e?.message || e));

  state.statsDate = dateAngola();
  state.stats = blankStats();
  await initDb();
  await loadKnownRounds();
  await loadStats();
  await loadState();

  const server = app.listen(PORT, '0.0.0.0', () => console.log(`[server] online na porta ${PORT}`));
  syncLoop();
  configureWebhook();
  const housekeepingTimer = setInterval(() => exclusive(housekeeping).catch((e) => console.error('[housekeeping]', e.message)), 15000);

  const shutdown = async (sig) => {
    console.log(`[server] ${sig}, encerrando`);
    stopped = true;
    clearTimeout(syncTimer);
    clearInterval(housekeepingTimer);
    setTimeout(() => process.exit(1), 10000).unref();
    try { await chain; await persist(); } catch (_) {}
    await pool?.end().catch(() => {});
    server.close(() => process.exit(0));
  };
  process.once('SIGINT', () => shutdown('SIGINT'));
  process.once('SIGTERM', () => shutdown('SIGTERM'));
}

if (require.main === module) {
  start().catch((e) => {
    console.error('[fatal]', e);
    process.exit(1);
  });
}

module.exports = {
  state, calculateSignal, normalizeRound, normalizeOutcome, syncOnce, saveResult, processResult,
  maybeCreateSignal, handleTelegramUpdate, historyMessage, housekeeping, exclusive, ensureDay, blankStats,
  app, constants: { MIN_HISTORY, SIGNAL_TTL_MS, MAX_SIGNALS_DAY, STOP_LOSS_REDS }
};

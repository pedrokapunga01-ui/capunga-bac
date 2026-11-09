import 'dotenv/config';
import express from 'express';
import cors from 'cors';
import crypto from 'node:crypto';
import { Telegraf } from 'telegraf';
import { Store } from './store.js';
import { calculateSignal, formatSignal } from './signal-engine.js';
import { fetchBacboRounds, DEFAULT_API_URL } from './bacbo-api.js';

const required = ['BOT_TOKEN', 'CHAT_ID', 'ADMIN_TOKEN', 'DATABASE_URL'];
const missing = required.filter((key) => !process.env[key]);
if (missing.length) console.warn(`[config] Variáveis ausentes: ${missing.join(', ')}`);

const PORT = Number(process.env.PORT || 10000);
const PUBLIC_URL = String(process.env.PUBLIC_URL || process.env.RENDER_EXTERNAL_URL || '').replace(/\/$/, '');
const WEBHOOK_PATH = '/telegram/webhook';
const WEBHOOK_SECRET = process.env.TELEGRAM_WEBHOOK_SECRET || process.env.ADMIN_TOKEN || crypto.randomBytes(32).toString('hex');
const ADMIN_KEY = process.env.ADMIN_TOKEN || process.env.ADMIN_API_KEY || '';
const DEFAULT_CHAT_ID = Number(process.env.CHAT_ID || 0);
const ADMIN_IDS = new Set((process.env.ADMIN_TELEGRAM_IDS || process.env.CHAT_ID || '').split(',').map((x) => x.trim()).filter(Boolean).map(Number));
const MIN_HISTORY = Number(process.env.MIN_HISTORY || 20);
const INTERVAL_MINUTES = Number(process.env.SIGNAL_INTERVAL_MINUTES || 5);
const MIN_CONFIDENCE = Number(process.env.SIGNAL_MIN_CONFIDENCE || 0.62);
const ANGOLA_TIMEZONE = process.env.ANGOLA_TIMEZONE || 'Africa/Luanda';
const DEFAULT_TIE_MULTIPLIER = process.env.TIE_MULTIPLIER || '4x';
const BACBO_API_ENABLED = process.env.BACBO_API_ENABLED !== 'false';
const BACBO_API_URL = process.env.BACBO_API_URL || DEFAULT_API_URL;
const SYNC_MS = Math.max(2000, Number(process.env.SYNC_MS || 5000));
const ROUND_DELAY_MS = Math.max(5000, Number(process.env.ATRASO_RODADA_MS || 35000));
const store = new Store();
const bot = new Telegraf(process.env.BOT_TOKEN || 'invalid-token');
let paused = false;
let lastSignalAt = 0;
let activeSignal = null;
let analysisMessageId = null;
let signalMessageId = null;
let stats = blankStats();
let statsDate = angolaDate();
let knownExternalIds = new Set();
let lastRoundAt = 0;
let syncBusy = false;
let apiError = null;
let apiLastSync = null;

function blankStats() { return { signals: 0, green: { first: 0, gale1: 0, gale2: 0 }, ties: { first: 0, gale1: 0, gale2: 0 }, loss: 0, currentWinStreak: 0, bestWinStreak: 0, lastGreen: null }; }
function normalizeStats(value) { const base = blankStats(); return { ...base, ...(value || {}), green: { ...base.green, ...(value?.green || {}) }, ties: { ...base.ties, ...(value?.ties || {}) } }; }
function angolaDate() { return new Intl.DateTimeFormat('en-CA', { timeZone: ANGOLA_TIMEZONE, year: 'numeric', month: '2-digit', day: '2-digit' }).format(new Date()); }
function persistStats() { return store.saveDailyStats(statsDate, stats).catch((error) => console.error('[stats]', error.message)); }
function resetIfNewDay() { const today = angolaDate(); if (today !== statsDate) { stats = blankStats(); statsDate = today; activeSignal = null; persistStats(); } }
function isAdmin(ctx) { return ADMIN_IDS.has(Number(ctx.from?.id)); }
function requireAdmin(ctx) { if (!isAdmin(ctx)) { ctx.reply('⛔ Sem permissão para este comando.'); return false; } return true; }
function idsToNotify() { return [...new Set([DEFAULT_CHAT_ID, ...[]].filter(Boolean))]; }
function colour(outcome) { return outcome === 'banker' ? '🔴' : outcome === 'player' ? '🔵' : '🟢'; }
function label(outcome) { return outcome === 'banker' ? 'BANKER' : outcome === 'player' ? 'PLAYER' : 'EMPATE'; }
function stageName(stage) { return stage === 0 ? 'primeiro lance' : `gale ${stage}`; }
function deleteMessageSafe(messageId) { if (!messageId || !DEFAULT_CHAT_ID) return Promise.resolve(); return bot.telegram.deleteMessage(DEFAULT_CHAT_ID, messageId).catch(() => undefined); }
async function clearSignalMessages() { await Promise.all([deleteMessageSafe(analysisMessageId), deleteMessageSafe(signalMessageId)]); analysisMessageId = null; signalMessageId = null; }
function signalText(signal) { const pct = Math.round(signal.confidence * 100); return `🚨 *ENTRADA CONFIRMADA*\n🎯 ${colour(signal.outcome)} *${label(signal.outcome)}*\n🛡️ Empate  ·  📈 ${pct}%  ·  Até G2`; }
function galeText(stage, outcome) { return `🔁 *FAZER GALE ${stage}*\n🎯 ${colour(outcome)} *${label(outcome)}*  ·  🛡️ Empate`; }
function multiplierText(value) { return String(value || DEFAULT_TIE_MULTIPLIER).replace(/^x/i, '').replace(/^×/, ''); }
function clockText(value) { return value || new Intl.DateTimeFormat('pt-PT', { timeZone: ANGOLA_TIMEZONE, hour: '2-digit', minute: '2-digit', second: '2-digit' }).format(new Date()); }
function resultText(outcome, stage, result = {}) {
  const mode = stage === 0 ? 'ENTRADA DIRETA' : `GALE ${stage}`;
  const round = result.roundNumber || result.round || '—';
  const player = result.playerScore ?? '?';
  const banker = result.bankerScore ?? '?';
  const score = `(J ${player} × ${banker} B)`;
  const chosen = result.predictedOutcome || activeSignal?.outcome || outcome;
  const numeric = result.averagePoints || result.numericAverage || '—';
  const confidence = result.confidence ?? result.numericConfidence ?? '—';
  if (outcome === 'tie') return `🟡✅ *GREEN EMPATE* · ${mode}\n🎯 ${colour(chosen)} ${label(chosen)} → 🟡 Empate\n💰 ×${multiplierText(result.multiplier)}  ·  ⏱ ${clockText(result.time)}`;
  return `✅🔥 *GREEN* · ${mode}\n🎯 ${colour(chosen)} ${label(chosen)} → ${colour(outcome)} ${label(outcome)}\n⏱ ${clockText(result.time)}`;
}
function lossText(outcome, result = {}) { return `🔴 *RED* · G2\n🎯 Saiu ${colour(outcome)} *${label(outcome)}*\n⏱ ${clockText(result.time)}`; }
function historyText() { resetIfNewDay(); const direct = stats.green.first; const gale1 = stats.green.gale1; const gale2 = stats.green.gale2; const tieTotal = stats.ties.first + stats.ties.gale1 + stats.ties.gale2; const totalGreens = direct + gale1 + gale2 + tieTotal; const totalClosed = totalGreens + stats.loss; const accuracy = totalClosed ? ((totalGreens / totalClosed) * 100).toFixed(1) : '0.0'; return `📊 *HOJE*\n✅ ${totalGreens}G  🟥 ${stats.loss}R  ·  ${accuracy}%\n🎯 D:${direct}  G1:${gale1}  G2:${gale2}  🟡 E:${tieTotal}\n🔥 Sequência: ${stats.currentWinStreak}`; }
async function sendToDefault(text, options = {}) { if (!DEFAULT_CHAT_ID) return null; return bot.telegram.sendMessage(DEFAULT_CHAT_ID, text, { parse_mode: 'Markdown', ...options }); }
async function sendAnalysisAndSignal() {
  resetIfNewDay();
  if (paused || activeSignal || Date.now() - lastSignalAt < INTERVAL_MINUTES * 60_000 || (lastRoundAt && Date.now() - lastRoundAt < ROUND_DELAY_MS)) return;
  const analysis = await sendToDefault('🔍✨ *ANALISANDO O MERCADO TROPA...* ✨🔍\n📊 A verificar a mesa e os últimos resultados...');
  analysisMessageId = analysis?.message_id || null;
  const signal = calculateSignal(await store.recentResults(100), MIN_HISTORY);
  if (!signal.ready || signal.confidence < MIN_CONFIDENCE) { await deleteMessageSafe(analysisMessageId); analysisMessageId = null; return; }
  await deleteMessageSafe(analysisMessageId); analysisMessageId = null;
  const message = await sendToDefault(signalText(signal));
  signalMessageId = message?.message_id || null;
  activeSignal = { outcome: signal.outcome, stage: 0, multiplier: null };
  stats.signals++;
  await persistStats();
  lastSignalAt = Date.now();
}
async function processResult(result) {
  resetIfNewDay();
  if (!activeSignal) return;
  const current = activeSignal;
  if (result.outcome === 'tie') {
    stats.ties[['first', 'gale1', 'gale2'][current.stage]]++;
    await clearSignalMessages();
    await sendToDefault(resultText('tie', current.stage, result));
    await sendToDefault(historyText());
    await persistStats();
    activeSignal = null;
    return;
  }
  if (result.outcome === current.outcome) {
    const greenStage = ['first', 'gale1', 'gale2'][current.stage];
    stats.green[greenStage]++;
    stats.currentWinStreak++;
    stats.bestWinStreak = Math.max(stats.bestWinStreak, stats.currentWinStreak);
    stats.lastGreen = current.stage === 0 ? 'GREEN NO PRIMEIRO LANCE' : `GREEN NO GALE ${current.stage}`;
    await clearSignalMessages();
    await sendToDefault(resultText(result.outcome, current.stage, result));
    await sendToDefault(historyText());
    await persistStats();
    activeSignal = null;
    return;
  }
  if (current.stage < 2) {
    current.stage++;
    await clearSignalMessages();
    const message = await sendToDefault(galeText(current.stage, current.outcome));
    signalMessageId = message?.message_id || null;
    return;
  }
  stats.loss++;
  stats.currentWinStreak = 0;
  await clearSignalMessages();
  await sendToDefault(lossText(result.outcome, result));
  await sendToDefault(historyText());
  await persistStats();
  activeSignal = null;
}

async function syncExternalResults() {
  if (!BACBO_API_ENABLED || syncBusy) return;
  syncBusy = true;
  try {
    const rounds = await fetchBacboRounds(BACBO_API_URL);
    let imported = 0;
    for (const round of rounds) {
      if (knownExternalIds.has(round.externalId)) continue;
      const result = { externalId: round.externalId, outcome: round.outcome, round: round.externalId, roundNumber: round.roundNumber, playerScore: round.playerScore, bankerScore: round.bankerScore, time: round.time, source: 'bacbo-api' };
      const saved = await store.addResult(result);
      knownExternalIds.add(round.externalId);
      if (!saved) continue;
      imported++;
      lastRoundAt = Math.max(lastRoundAt, round.timestamp);
      await processResult(result);
    }
    apiError = null;
    apiLastSync = new Date().toISOString();
    if (imported) console.log(`[bacbo-api] ${imported} rodada(s) nova(s); histórico conhecido: ${knownExternalIds.size}`);
  } catch (error) {
    apiError = error.message;
    console.error(`[bacbo-api] ${error.message}`);
  } finally { syncBusy = false; }
}

bot.start(async (ctx) => { await store.subscribe(ctx.chat); await ctx.reply('✅ Bot ativo, tropa!\n\n🔍 /sinal — analisar a mesa\n📚 /historico — histórico do dia\n❓ /help — comandos'); });
bot.help((ctx) => ctx.reply('🔍 /sinal — gerar entrada\n📚 /historico — histórico diário\n🧾 /resultado banker|player|tie — administrador\n⏸️ /pausar e ▶️ /retomar — administrador\n📡 /status — administrador\n\n⚠️ Sinais não garantem lucro.'));
bot.command('sinal', async (ctx) => { await store.subscribe(ctx.chat); await sendAnalysisAndSignal(); await ctx.reply(activeSignal ? '✅ Entrada enviada no canal principal.' : '⏳ Sem entrada confirmada neste momento.'); });
bot.command('historico', async (ctx) => ctx.reply(historyText(), { parse_mode: 'Markdown' }));
bot.command('resultado', async (ctx) => { if (!requireAdmin(ctx)) return; const parts = String(ctx.message.text).split(/\s+/); const outcome = parts[1]?.toLowerCase(); const multiplier = parts[2] || null; const roundNumber = parts[3] || null; if (!['banker', 'player', 'tie'].includes(outcome)) return ctx.reply('Uso: /resultado banker|player|tie [multiplicador] [rodada]'); const result = { outcome, multiplier, roundNumber }; await store.addResult({ ...result, source: 'telegram-admin' }); await processResult(result); await ctx.reply('✅ Resultado guardado e ciclo atualizado.'); });
bot.command('pausar', async (ctx) => { if (!requireAdmin(ctx)) return; paused = true; await ctx.reply('⏸️ Sinais automáticos pausados.'); });
bot.command('retomar', async (ctx) => { if (!requireAdmin(ctx)) return; paused = false; await ctx.reply('▶️ Sinais automáticos retomados.'); });
bot.command('status', async (ctx) => { if (!requireAdmin(ctx)) return; const info = await bot.telegram.getWebhookInfo(); await ctx.reply(`✅ Serviço online\nWebhook: ${info.url ? 'ativo' : 'inativo'}\nPendentes: ${info.pending_update_count}\nCiclo: ${activeSignal ? `ativo — ${stageName(activeSignal.stage)}` : 'sem entrada'}`); });
bot.command('broadcast', async (ctx) => { if (!requireAdmin(ctx)) return; const text = ctx.message.text.replace(/^\/broadcast\s*/i, '').trim(); if (!text) return ctx.reply('Uso: /broadcast texto'); const ids = await store.subscribers(); let sent = 0; for (const id of ids) { try { await bot.telegram.sendMessage(id, text); sent++; } catch (error) { console.error('[broadcast]', error.message); } } await ctx.reply(`📣 Enviado para ${sent}/${ids.length}.`); });

const app = express();
app.disable('x-powered-by');
app.use(cors({ origin: false }));
app.use(express.json({ limit: '256kb' }));
app.get('/', (_req, res) => res.json({ name: 'bacbo-signal-bot', status: 'online', health: '/health' }));
app.get('/health', (_req, res) => res.status(200).json({ ok: true, uptime: Math.round(process.uptime()), time: new Date().toISOString(), bacboApi: { enabled: BACBO_API_ENABLED, lastSync: apiLastSync, error: apiError } }));
const apiAuth = (req, res, next) => { if (!ADMIN_KEY || req.header('x-api-key') !== ADMIN_KEY) return res.status(401).json({ error: 'unauthorized' }); next(); };
app.get('/api/status', apiAuth, async (_req, res) => res.json({ ok: true, paused, activeSignal, stats, uptime: process.uptime(), bacboApi: { enabled: BACBO_API_ENABLED, url: BACBO_API_URL, lastSync: apiLastSync, error: apiError, knownRounds: knownExternalIds.size, lastRoundAt } }));
app.post('/api/results', apiAuth, async (req, res) => { const { outcome, shoe = null, round = null, roundNumber = null, source = 'api', multiplier = null, playerScore = null, bankerScore = null, averagePoints = null, numericAverage = null, confidence = null, numericConfidence = null, time = null, predictedOutcome = null } = req.body || {}; if (!['banker', 'player', 'tie'].includes(outcome)) return res.status(400).json({ error: 'outcome must be banker, player or tie' }); const result = { outcome, shoe, round, roundNumber, source, multiplier, playerScore, bankerScore, averagePoints, numericAverage, confidence, numericConfidence, time, predictedOutcome }; const saved = await store.addResult(result); await processResult(result); res.status(201).json({ ok: true, result: saved, activeSignal, stats }); });
app.post(WEBHOOK_PATH, (req, res, next) => { if (req.header('x-telegram-bot-api-secret-token') !== WEBHOOK_SECRET) return res.status(401).json({ error: 'invalid webhook secret' }); next(); }, async (req, res) => { try { await bot.handleUpdate(req.body, res); } catch (error) { console.error('[telegram-update]', error); if (!res.headersSent) res.sendStatus(500); } });

async function main() {
  await store.init();
  knownExternalIds = await store.knownExternalIds();
  stats = normalizeStats(await store.loadDailyStats(statsDate));
  const server = app.listen(PORT, '0.0.0.0', async () => {
    console.log(`[server] listening on ${PORT}`);
    if (PUBLIC_URL && process.env.BOT_TOKEN) { await bot.telegram.setWebhook(`${PUBLIC_URL}${WEBHOOK_PATH}`, { secret_token: WEBHOOK_SECRET, allowed_updates: ['message'] }); console.log(`[telegram] webhook configured at ${PUBLIC_URL}${WEBHOOK_PATH}`); }
  });
  await syncExternalResults();
  const timer = setInterval(sendAnalysisAndSignal, 60_000);
  const syncTimer = setInterval(syncExternalResults, SYNC_MS);
  const shutdown = async (signal) => { console.log(`[server] ${signal}, shutting down`); clearInterval(timer); clearInterval(syncTimer); await bot.stop(signal); await store.close(); server.close(() => process.exit(0)); setTimeout(() => process.exit(1), 10_000).unref(); };
  process.once('SIGINT', shutdown); process.once('SIGTERM', shutdown);
}
main().catch((error) => { console.error('[fatal]', error); process.exit(1); });

'use strict';

/**
 * CAPUNGA DADOS — BAC BO · v17.5
 * Sinal imediato: poll 400ms + trigger event-driven + atrasoRodada 100ms.
 * Env: BOT_TOKEN, CHAT_ID, ADMIN_TOKEN, DATABASE_URL, PORT
 */

const fs = require('fs');
const crypto = require('crypto');
const express = require('express');
const cors = require('cors');
const axios = require('axios');
const { Pool } = require('pg');
const http = require('http');
const https = require('https');

const PORT = Number(process.env.PORT || 10000);
const BOT_TOKEN = process.env.BOT_TOKEN || '';
const CHAT_ID = process.env.CHAT_ID || '';
const ADMIN_TOKEN = process.env.ADMIN_TOKEN || '';
const DATABASE_URL = process.env.DATABASE_URL || '';

const CFG = {
  ativos: true,
  maxGale: 2,
  maxPending: 3,
  minAmostra: 40,
  sinalInicial: 1,

  // 🔥 Poll rápido
  syncIntervalPendingMs: 400,    // 400ms com sinal aberto
  syncIntervalIdleMs: 800,       // 800ms idle

  // Timing
  minGapMs: 20000,
  atrasoRodadaMs: 100,           // 🔥 100ms (era 3000) — praticamente imediato
  apuraMaxMs: 240000,
  maxHist: 4000,
  gapMs: 7 * 60 * 1000,
  maxSignalsMem: 500,
  signalsOnBoot: 500,
  suspendMs: 2 * 3600 * 1000,
  suspendLossStreak: 4,
  suspendLookback: 5,
  keepAliveMs: 60000,

  maxPorHora: 30,
  minPorHora: 25,
  softUrgencyMs: 180000,
  ultraUrgencyMs: 240000,
  maxSemSinalMs: 270000,
  forceClosePendingMs: 255000,
  scoreNormal: 82,
  scoreUrgent: 72,
  scoreUltra: 62,
  scoreForced: 50,

  fallbackCheckMs: 3000
};

const J = 'Jogador';
const B = 'Banca';
const E = 'Empate';
const isCol = (x) => x === J || x === B;
const opp = (c) => (c === J ? B : c === B ? J : null);
const FECHADOS = new Set(['GREEN', 'G1', 'G2', 'EMPATE', 'EG1', 'EG2', 'LOSS']);

const API_BACBO =
  'https://api-cs.casino.org/svc-evolution-game-events/api/bacbo?page=0&size=50&sort=data.settledAt,desc&duration=120&wheelResults=PlayerWon,BankerWon,Tie';

/* ============================ HTTP AGENT KEEP-ALIVE ============================ */

const httpAgent = new http.Agent({ keepAlive: true, maxSockets: 8, keepAliveMsecs: 30000 });
const httpsAgent = new https.Agent({ keepAlive: true, maxSockets: 8, keepAliveMsecs: 30000 });

const apiClient = axios.create({
  timeout: 3000,
  httpAgent,
  httpsAgent,
  headers: { Accept: 'application/json', 'User-Agent': 'CapungaDados/17.5' }
});

/* ============================ DEPLOY ID ============================ */

const DEPLOY_ID = (() => {
  if (process.env.DEPLOY_ID) return String(process.env.DEPLOY_ID).trim();
  try { return crypto.createHash('sha256').update(fs.readFileSync(__filename)).digest('hex').slice(0, 16); }
  catch { return 'unknown'; }
})();

/* ============================ STATE ============================ */

const S = {
  hist: [], seen: new Set(), signals: [],
  nextId: CFG.sinalInicial, lastSignalAt: 0,
  stats: null, statsAtLen: -1
};

const STATS = { total: 0, green: 0, g1: 0, g2: 0, empate: 0, eg1: 0, eg2: 0, loss: 0 };
const SUSPENDED = {};
const ANALYSIS = { msgId: null, shownAt: 0, enviando: false };

const R = {
  iniciadoEm: new Date().toISOString(), deployId: DEPLOY_ID,
  ultimaSync: null, ultimaRodadaNova: null,
  erroAPI: null, erroTelegram: null, erroDB: null,
  ultimoMotivo: null, ultimoSinal: null,
  wakes: 0, ultimoWake: null,
  ultimoThreshold: null, ultimoTimingMs: null,
  ultimoDetectMs: null,       // tempo do fetch após rodada settle
  ultimoDelayRodadaMs: null,  // delay real entre settle e envio do sinal
  syncBusy: false, apurarBusy: false, signalBusy: false,
  apiBugs: 0
};

let pool = null, server = null, stopping = false;
let tickSyncTimer = null, tickFallbackTimer = null, keepTimer = null, analisandoTimer = null;

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

/* ============================ FREQUÊNCIA ============================ */

function sinaisUltimos60Min() {
  const agora = Date.now();
  return S.signals.filter((s) => s.t && agora - s.t < 3600000).length;
}

function calcularPace() {
  const agora = Date.now();
  const inicioHora = agora - (agora % 3600000);
  const minutosPassados = (agora - inicioHora) / 60000;
  if (minutosPassados < 10) return null;
  const sinaisNestaHora = S.signals.filter((s) => s.t && s.t >= inicioHora).length;
  const esperado = (minutosPassados / 60) * CFG.minPorHora;
  return {
    sinaisNestaHora, minutosPassados,
    deficit: esperado - sinaisNestaHora,
    pace: sinaisNestaHora / (minutosPassados / 60)
  };
}

function calcularThreshold() {
  const agora = Date.now();
  const desdeUltimo = S.lastSignalAt ? agora - S.lastSignalAt : Infinity;
  const sinaisHora = sinaisUltimos60Min();

  if (sinaisHora >= CFG.maxPorHora) {
    return { threshold: 999, forcar: false, motivo: `cap ${CFG.maxPorHora}/h` };
  }

  let threshold, forcar = false;
  if (desdeUltimo >= CFG.maxSemSinalMs) { threshold = CFG.scoreForced; forcar = true; }
  else if (desdeUltimo >= CFG.ultraUrgencyMs) { threshold = CFG.scoreUltra; }
  else if (desdeUltimo >= CFG.softUrgencyMs) { threshold = CFG.scoreUrgent; }
  else if (desdeUltimo >= 90000) { threshold = 78; }
  else { threshold = CFG.scoreNormal; }

  const pace = calcularPace();
  if (pace && sinaisHora < 27) {
    if (pace.deficit > 3) threshold -= 10;
    else if (pace.deficit > 1) threshold -= 5;
  } else if (sinaisHora >= 28) {
    threshold += 5;
  }

  threshold = Math.max(45, Math.min(95, threshold));
  return { threshold, forcar, motivo: `score≥${threshold} (t=${Math.round(desdeUltimo / 1000)}s)` };
}

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
    await pool.query(`INSERT INTO capunga_signals (id,cor,key,nome,tipo,conf,status,gale,agree,created_at,closed_at)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) ON CONFLICT (id) DO NOTHING`,
      [s.id, s.cor, s.key, s.nome, s.tipo, s.conf, s.status, s.gale ?? null, s.agree ?? 1,
       new Date(s.t), s.fechadoEm ? new Date(s.fechadoEm) : null]);
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
    S.signals = r.rows.reverse().map((row) => ({
      id: row.id, cor: row.cor, key: row.key, nome: row.nome, tipo: row.tipo,
      conf: row.conf, status: row.status, gale: row.gale, agree: row.agree,
      t: new Date(row.created_at).getTime(), criadoEm: new Date(row.created_at).getTime(),
      fechadoEm: row.closed_at ? new Date(row.closed_at).getTime() : null,
      gale1MsgId: null, gale2MsgId: null, galeLevel: 0,
      lastRoundId: null
    }));
    rebuildStats();
    log('DB', `estado carregado: nextId=${S.nextId}, sinais=${S.signals.length}`);
  } catch (e) { R.erroDB = e.message; log('DB ERRO load', e.message); }
}
function rebuildStats() {
  STATS.total = 0; STATS.green = 0; STATS.g1 = 0; STATS.g2 = 0;
  STATS.empate = 0; STATS.eg1 = 0; STATS.eg2 = 0; STATS.loss = 0;
  for (const s of S.signals) {
    if (!FECHADOS.has(s.status)) continue;
    STATS.total++;
    if (s.status === 'GREEN') STATS.green++;
    else if (s.status === 'G1') STATS.g1++;
    else if (s.status === 'G2') STATS.g2++;
    else if (s.status === 'EMPATE') STATS.empate++;
    else if (s.status === 'EG1') STATS.eg1++;
    else if (s.status === 'EG2') STATS.eg2++;
    else if (s.status === 'LOSS') STATS.loss++;
  }
}

/* ============================ TELEGRAM ============================ */

async function tgSendRaw(texto) {
  if (!BOT_TOKEN || !CHAT_ID) { R.erroTelegram = 'TOKEN/CHAT em falta'; return { ok: false }; }
  for (let t = 1; t <= 2; t++) {
    try {
      const r = await axios.post(`https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`,
        { chat_id: CHAT_ID, text: texto, parse_mode: 'HTML', disable_web_page_preview: true },
        { timeout: 5000, httpAgent, httpsAgent });
      if (r.data?.ok) { R.erroTelegram = null; return { ok: true, messageId: r.data.result.message_id }; }
      R.erroTelegram = JSON.stringify(r.data);
    } catch (e) {
      R.erroTelegram = e.response?.data ? JSON.stringify(e.response.data) : e.message;
      const retry = Number(e.response?.data?.parameters?.retry_after);
      if (retry > 0 && retry <= 20) await sleep(retry * 1000 + 200);
    }
    if (t < 2) await sleep(400);
  }
  return { ok: false };
}

function tgFAF(texto) {
  tgSendRaw(texto).catch((e) => log('TG FAF', e.message));
}

async function tgDelete(messageId) {
  if (!messageId || !BOT_TOKEN || !CHAT_ID) return;
  try {
    await axios.post(`https://api.telegram.org/bot${BOT_TOKEN}/deleteMessage`,
      { chat_id: CHAT_ID, message_id: messageId }, { timeout: 4000, httpAgent, httpsAgent });
  } catch (_) {}
}

/* ============================ ANALISANDO ============================ */

async function ensureAnalisando() {
  if (ANALYSIS.msgId || ANALYSIS.enviando) return;
  ANALYSIS.enviando = true;
  try {
    const r = await tgSendRaw(fmtAnalisando());
    if (r.ok) {
      ANALYSIS.msgId = r.messageId;
      ANALYSIS.shownAt = Date.now();
    }
  } finally { ANALYSIS.enviando = false; }
}

function hideAnalisandoAsync() {
  const id = ANALYSIS.msgId;
  ANALYSIS.msgId = null;
  if (id) tgDelete(id).catch(() => {});
}

function manageAnalisando() {
  if (stopping) return;
  const podeEscanear = CFG.ativos && pendentes().length < CFG.maxPending;
  const desdeUltimo = S.lastSignalAt ? Date.now() - S.lastSignalAt : Infinity;
  const precisaMostrar = podeEscanear && desdeUltimo >= CFG.minGapMs;

  if (precisaMostrar) {
    if (!ANALYSIS.msgId && !ANALYSIS.enviando) ensureAnalisando().catch(() => {});
  } else {
    if (ANALYSIS.msgId) hideAnalisandoAsync();
  }
}

/* ============================ TEMPLATES ============================ */

function fmtAnalisando() {
  return ['🔍 <b>Analisando o mercado tropa…</b>', '', `⏱ ${hora()}`].join('\n');
}

function fmtSinal(s) {
  const confEmoji = s.conf >= 94 ? '💎' : s.conf >= 86 ? '🔥' : '⚡';
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
    `Sequência de greens: ${streak >= 3 ? streak + '🔥' : streak}`,
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
  let resultado = normalizeOutcome(raw);
  const id = String(d.id ?? d.roundId ?? d.round_id ?? d.eventId ?? item?.id ?? item?.roundId ?? item?.round_id ?? item?.eventId ?? '').trim();
  if (!id || !resultado || !t) return null;

  const pj = Number(r.playerDice?.score ?? d.playerDice?.score ?? d.playerScore ?? d.player_score ?? d.pontos_jogador);
  const pb = Number(r.bankerDice?.score ?? d.bankerDice?.score ?? d.bankerScore ?? d.banker_score ?? d.pontos_banca);
  const pjOk = Number.isFinite(pj) ? pj : null;
  const pbOk = Number.isFinite(pb) ? pb : null;

  if (pjOk !== null && pbOk !== null && pjOk === pbOk && resultado !== E) {
    R.apiBugs++;
    log('API BUG FIX', { id, reported: resultado, pj: pjOk, pb: pbOk, fixed: 'Empate' });
    resultado = E;
  }

  return { id, resultado, t, pj: pjOk, pb: pbOk };
}

async function fetchRounds() {
  let lastErr = null;
  for (let i = 1; i <= 2; i++) {
    try {
      const resp = await apiClient.get(API_BACBO);
      const map = new Map();
      for (const it of extractItems(resp.data)) {
        const rd = normalizeRound(it);
        if (rd) map.set(rd.id, rd);
      }
      R.erroAPI = null;
      return [...map.values()].sort((a, b) => a.t - b.t || (a.id < b.id ? -1 : 1));
    } catch (e) {
      lastErr = e; R.erroAPI = e.message;
      if (i < 2) await sleep(200);
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
  ({ seq }) => { const n = streak(seq); if (n < 3 || n > 7) return null; return { cor: seq[0].resultado, key: `SEQ_REP_${n}`, nome: `Sequência x${n}`, emoji: '🔁', strength: [0,0,0,72,78,84,86,86][n] || 86, tipo: 'seq' }; },
  ({ seq }) => { const n = streak(seq); if (n < 4 || n > 8) return null; return { cor: opp(seq[0].resultado), key: `SEQ_QUEBRA_${n}`, nome: `Quebra x${n}`, emoji: '💥', strength: [0,0,0,0,74,80,84,85,85][n] || 85, tipo: 'seq' }; },
  ({ seq }) => { const L = alternation(seq); if (L < 4) return null; return { cor: opp(seq[0].resultado), key: `SEQ_SURF_${L}`, nome: `Surf ${L}`, emoji: '🌊', strength: L >= 6 ? 84 : L >= 5 ? 78 : 72, tipo: 'seq' }; },
  ({ seq }) => { if (seq.length < 4) return null; const [a,b,c,d] = seq.map(x => x.resultado); return (a === b && c === d && a !== c) ? { cor: c, key: 'SEQ_2x2', nome: 'Bloco 2+2', emoji: '♟️', strength: 76, tipo: 'seq' } : null; },
  ({ seq }) => { if (seq.length < 4) return null; const [a,b,c,d] = seq.map(x => x.resultado); return (a !== b && b === c && c === d) ? { cor: a, key: 'SEQ_3_1', nome: '3→1 confirmado', emoji: '🧩', strength: 78, tipo: 'seq' } : null; },
  ({ seq }) => { if (seq.length < 3 || !seq.slice(0,3).every(hasScore)) return null; const [a,b,c] = seq; if (a.resultado !== b.resultado || b.resultado !== c.resultado) return null; const sa = scoreOf(a), sb = scoreOf(b), sc = scoreOf(c); return ((sb-sa===1 && sc-sb===1) || (sa-sb===1 && sb-sc===1)) ? { cor: a.resultado, key: 'NUM_ESCADA', nome: `Escada ${sa}→${sb}→${sc}`, emoji: '🪜', strength: 88, tipo: 'num' } : null; },
  ({ seq }) => { if (seq.length < 2 || !hasScore(seq[0]) || !hasScore(seq[1])) return null; const [a,b] = seq; return (a.resultado === b.resultado && scoreOf(a) === scoreOf(b)) ? { cor: a.resultado, key: 'NUM_REPETE', nome: `Score repetido ${scoreOf(a)}`, emoji: '🔂', strength: 84, tipo: 'num' } : null; },
  ({ seq }) => { if (!seq.length || !hasScore(seq[0])) return null; const s = scoreOf(seq[0]); return s === 0 ? { cor: opp(seq[0].resultado), key: 'NUM_GATILHO_0', nome: 'Gatilho 0', emoji: '🎯', strength: 86, tipo: 'num' } : null; },
  ({ seq }) => { if (!seq.length || !hasScore(seq[0])) return null; const s = scoreOf(seq[0]); return s === 1 ? { cor: opp(seq[0].resultado), key: 'NUM_GATILHO_1', nome: 'Gatilho 1', emoji: '🎯', strength: 84, tipo: 'num' } : null; },
  ({ seq }) => { if (!seq.length || !hasScore(seq[0])) return null; const s = scoreOf(seq[0]); return s === 9 ? { cor: opp(seq[0].resultado), key: 'NUM_GATILHO_9', nome: 'Gatilho 9', emoji: '🎯', strength: 88, tipo: 'num' } : null; },
  ({ seq }) => { if (!seq.length || !hasScore(seq[0])) return null; const s = scoreOf(seq[0]); return s === 8 ? { cor: opp(seq[0].resultado), key: 'NUM_GATILHO_8', nome: 'Gatilho 8', emoji: '🎯', strength: 78, tipo: 'num' } : null; },
  ({ rows }) => { const r = rows[0]; if (!r || !isCol(r.resultado) || !hasScore(r) || margem(r) < 5) return null; return { cor: r.resultado, key: 'NUM_MARGEM5', nome: `Margem alta ${r.pj}×${r.pb}`, emoji: '📊', strength: 82, tipo: 'num' }; },
  ({ seq }) => { if (seq.length < 3) return null; const t = seq.slice(0,3); if (!t.every(r => hasScore(r) && margem(r) >= 4 && r.resultado === t[0].resultado)) return null; return { cor: t[0].resultado, key: 'NUM_DIFF4x3', nome: 'Diff ≥4 ×3', emoji: '🔢', strength: 85, tipo: 'num' }; },
  ({ seq }) => { if (seq.length < 2) return null; const [a,b] = seq; if (!hasScore(a) || !hasScore(b) || a.resultado !== b.resultado || scoreOf(a) > 4 || scoreOf(b) > 4) return null; return { cor: opp(a.resultado), key: 'NUM_VIRADA_BAIXA', nome: 'Virada de baixos', emoji: '🔄', strength: 80, tipo: 'num' }; },
  ({ rows }) => { const r = rows[0]; if (!r || !hasScore(r) || r.pj !== r.pb) return null; return { cor: r.resultado, key: 'NUM_ESPELHO', nome: `Espelho ${r.pj}×${r.pb}`, emoji: '🪞', strength: 83, tipo: 'num' }; },
  ({ rows }) => { const s = rows.slice(0,8).filter(hasScore); if (s.length < 6) return null; const mj = s.reduce((a,r) => a+r.pj,0)/s.length; const mb = s.reduce((a,r) => a+r.pb,0)/s.length; const d = mj-mb; return Math.abs(d) >= 1.5 ? { cor: d > 0 ? J : B, key: 'NUM_TENDENCIA', nome: `Tendência ${mj.toFixed(1)}×${mb.toFixed(1)}`, emoji: '📈', strength: Math.abs(d) >= 2.2 ? 84 : 76, tipo: 'num' } : null; },
  ({ seq }) => { if (seq.length < 2 || !hasScore(seq[0]) || !hasScore(seq[1])) return null; const [a,b] = seq; const ra = scoreRange(scoreOf(a)), rb = scoreRange(scoreOf(b)); if (!ra || !rb) return null; return ((ra==='MB'&&rb==='MA') || (ra==='MA'&&rb==='MB')) ? { cor: opp(a.resultado), key: 'NUM_SALTO_RANGE', nome: `Salto ${scoreOf(b)}→${scoreOf(a)}`, emoji: '📐', strength: 81, tipo: 'num' } : null; },
  ({ rows, seq }) => { if (!rows[0] || rows[0].resultado !== E || !seq.length) return null; return { cor: seq[0].resultado, key: 'NUM_POS_EMPATE', nome: 'Pós-empate', emoji: '🎯', strength: 74, tipo: 'num' }; },
  ({ seq }) => { const L = alternation(seq); if (L < 4 || L > 10) return null; return { cor: opp(seq[0].resultado), key: 'SEQ_PINGPONG', nome: `Ping-pong ${L}`, emoji: '🏓', strength: 79, tipo: 'seq' }; },
  ({ seq }) => { if (seq.length < 5) return null; const [a,b,c,d,e] = seq.map(x => x.resultado); return (a===b && b===c && c!==d && d===e) ? { cor: a, key: 'SEQ_EXTEND', nome: 'Streak extend', emoji: '⏭️', strength: 80, tipo: 'seq' } : null; },
  ({ seq }) => { const n = streak(seq); if (n < 4 || n > 6) return null; if (!seq.slice(0,n).every(r => hasScore(r) && margem(r) >= 3)) return null; return { cor: seq[0].resultado, key: 'SEQ_STRONG_STREAK', nome: `Dragon ${n}`, emoji: '🐉', strength: 82, tipo: 'seq' }; },
  ({ rows }) => { const r = rows[0]; if (!r || !hasScore(r)) return null; return (r.pj >= 8 && r.pb >= 8) ? { cor: r.resultado, key: 'NUM_NATURAL', nome: `Natural ${r.pj}×${r.pb}`, emoji: '🎴', strength: 86, tipo: 'num' } : null; },
  ({ seq }) => { if (!seq.length || !hasScore(seq[0])) return null; const s = scoreOf(seq[0]); return (s === 5 || s === 8) ? { cor: seq[0].resultado, key: 'NUM_FIBONACCI', nome: `Fibonacci ${s}`, emoji: '🌀', strength: 80, tipo: 'num' } : null; },
  ({ seq }) => { if (seq.length < 3 || !seq.slice(0,3).every(hasScore)) return null; const [a,b,c] = seq; if (a.resultado !== b.resultado || b.resultado !== c.resultado) return null; const sa = scoreOf(a), sb = scoreOf(b), sc = scoreOf(c); return (sa < sb && sb < sc) ? { cor: a.resultado, key: 'NUM_SCORE_TREND_3', nome: `Escala ${sa}→${sc}`, emoji: '📊', strength: 82, tipo: 'num' } : null; },
  ({ seq }) => { if (seq.length < 2 || !seq.slice(0,2).every(hasScore)) return null; const [a,b] = seq; if (a.resultado !== b.resultado || margem(a) < 5 || margem(b) < 5) return null; return { cor: a.resultado, key: 'NUM_DOUBLE_HIGH', nome: 'Dupla margem alta', emoji: '💪', strength: 84, tipo: 'num' }; },
  ({ rows }) => { const ties = rows.slice(0,3).filter(r => r.resultado === E).length; if (ties < 2) return null; const pre = rows.find(r => isCol(r.resultado)); return pre ? { cor: pre.resultado, key: 'TIE_CASCADE', nome: 'Cascata empates', emoji: '🟡', strength: 76, tipo: 'num' } : null; },
  ({ rows, seq }) => { if (!rows[0] || rows[0].resultado !== E || !rows[1] || rows[1].resultado === E || !seq.length) return null; return { cor: opp(seq[0].resultado), key: 'TIE_SINGLE', nome: 'Empate isolado', emoji: '🔀', strength: 77, tipo: 'num' }; },
  ({ seq }) => { const s = seq.slice(0,20); if (s.length < 15) return null; const b = s.filter(r => r.resultado === B).length; const j = s.filter(r => r.resultado === J).length; if (b >= 12) return { cor: B, key: 'BANKER_BIAS_20', nome: 'Viés Banca', emoji: '📉', strength: 80, tipo: 'seq' }; if (j >= 12) return { cor: J, key: 'BANKER_BIAS_20', nome: 'Viés Jogador', emoji: '📈', strength: 80, tipo: 'seq' }; return null; },
  ({ seq }) => { if (seq.length < 4 || !seq.slice(0,4).every(hasScore)) return null; const [a,b,c,d] = seq; const sa = scoreOf(a), sb = scoreOf(b), sc = scoreOf(c), sd = scoreOf(d); return (sa-sb < 0 && sb-sc < 0 && sc-sd < 0) ? { cor: opp(a.resultado), key: 'MOMENTUM_SHIFT', nome: 'Mudança momentum', emoji: '🔀', strength: 78, tipo: 'num' } : null; },
  ({ seq }) => { const L = alternation(seq); if (L < 3) return null; if (!seq.slice(0,L).every(r => hasScore(r) && margem(r) >= 3)) return null; return { cor: opp(seq[0].resultado), key: 'SEQ_ALT_STRONG', nome: `Alt. forte ${L}`, emoji: '⚔️', strength: 78, tipo: 'seq' }; }
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
  const H = S.hist;
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
function getStats(force = false) {
  if (force || !S.stats || S.hist.length - S.statsAtLen >= 30) {
    S.stats = backtest();
    S.statsAtLen = S.hist.length;
  }
  return S.stats;
}

/* ============================ CONFIANÇA ============================ */

function bayesianRate(w, n, priorRate = 0.75, priorWeight = 6) {
  return (w + priorRate * priorWeight) / (n + priorWeight);
}
function mapObservedToScore(observed) {
  if (observed <= 0.75) return 80;
  if (observed >= 0.93) return 95;
  return 80 + ((observed - 0.75) / 0.18) * 15;
}
function confianca(cand, ctx) {
  const stats = ctx.stats;
  const p = stats.per[cand.key] || { n: 0, w: 0 };
  const btRate = bayesianRate(p.w, p.n, 0.75, 6);
  const live = S.signals.filter((x) => x.key === cand.key && FECHADOS.has(x.status));
  const liveW = live.filter((x) => x.status !== 'LOSS').length;
  const liveRate = live.length >= 3 ? bayesianRate(liveW, live.length, btRate, 3) : null;
  const btSamples = p.n + 6;
  const liveSamples = live.length >= 3 ? live.length + 3 : 0;
  const observed = (btRate * btSamples + (liveRate ?? btRate) * liveSamples) / (btSamples + liveSamples);

  let score = mapObservedToScore(observed);
  const agree = ctx.agreement?.confirmations || 0;
  if (agree >= 3) score += 2;
  else if (agree >= 2) score += 1;
  score += (cand.strength - 80) * 0.4;
  if (cand.tipo === 'seq' && ctx.regime) {
    const isRev = cand.key.includes('QUEBRA') || cand.key.includes('SURF') || cand.key.includes('PINGPONG');
    const fit = isRev ? ctx.regime.chop : ctx.regime.trend;
    score += (fit - 0.5) * 6;
  }
  const recentLoss = S.signals.find((x) =>
    x.key === cand.key && x.status === 'LOSS' && Date.now() - (x.fechadoEm || 0) < 5 * 60 * 1000);
  if (recentLoss) score -= 8;

  return Math.max(78, Math.min(96, Math.round(score)));
}

/* ============================ SUSPENSÃO / FALLBACK / PENDENTES ============================ */

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
function fallbackCor(rows) {
  const seq = rows.filter((r) => isCol(r.resultado)).slice(0, 12);
  if (!seq.length) return J;
  const b = seq.filter((r) => r.resultado === B).length;
  const j = seq.filter((r) => r.resultado === J).length;
  if (b > j) return B;
  if (j > b) return J;
  return seq[0].resultado;
}
function fallbackCandidate(rows) {
  return { cor: fallbackCor(rows), key: 'FALLBACK_TREND', nome: 'Tendência curta', emoji: '🛟', strength: 55, tipo: 'seq' };
}
function pendentes() {
  return S.signals.filter((s) => !FECHADOS.has(s.status));
}

/* ============================ SYNC (event-driven, rápido) ============================ */

async function syncLoop() {
  if (stopping) return;
  if (R.syncBusy) { scheduleSync(); return; }
  R.syncBusy = true;
  const t0 = Date.now();

  try {
    const rounds = await fetchRounds();
    let newRound = null;

    for (const r of rounds) {
      if (S.seen.has(r.id)) continue;
      S.seen.add(r.id);
      S.hist.push(r);
      if (!newRound || r.t > newRound.t) newRound = r;
    }

    if (newRound) {
      // Ordena + corta
      S.hist.sort((a, b) => a.t - b.t || (a.id < b.id ? -1 : 1));
      if (S.hist.length > CFG.maxHist) {
        const ex = S.hist.length - CFG.maxHist;
        for (let i = 0; i < ex; i++) S.seen.delete(S.hist[i].id);
        S.hist = S.hist.slice(ex);
      }

      R.ultimaRodadaNova = new Date(newRound.t).toISOString();
      R.ultimaSync = new Date().toISOString();
      R.ultimoDetectMs = Date.now() - t0;
      R.ultimoDelayRodadaMs = Date.now() - newRound.t;

      log('SYNC', {
        newId: newRound.id,
        cor: newRound.resultado,
        detectMs: R.ultimoDetectMs,
        delayRodadaMs: R.ultimoDelayRodadaMs
      });

      // 🔥 Em paralelo, sem bloquear:
      // 1. apurar (fecha sinais — síncrono + telegram FAF)
      // 2. tentar sinal imediatamente
      apurarOnce().catch(e => log('APURAR ERR', e.message));
      tentarSinal().catch(e => log('SIGNAL ERR', e.message));
    }
  } catch (e) {
    R.erroAPI = e.message;
    log('SYNC ERRO', e.message);
  } finally {
    R.syncBusy = false;
    scheduleSync();
  }
}

function scheduleSync() {
  const delay = pendentes().length > 0 ? CFG.syncIntervalPendingMs : CFG.syncIntervalIdleMs;
  tickSyncTimer = setTimeout(syncLoop, delay);
}

/* ============================ APURAR (síncrono no estado, telegram FAF) ============================ */

function roundsAfterSignal(s) {
  if (s.lastRoundId) {
    const idx = S.hist.findIndex((r) => r.id === s.lastRoundId);
    if (idx >= 0) return S.hist.slice(idx + 1);
  }
  return S.hist.filter((r) => r.t > s.t).sort((a, b) => a.t - b.t);
}

function markClosed(s, status, gale) {
  s.status = status;
  s.gale = gale;
  s.fechadoEm = Date.now();

  if (status === 'GREEN') STATS.green++;
  else if (status === 'G1') STATS.g1++;
  else if (status === 'G2') STATS.g2++;
  else if (status === 'EMPATE') STATS.empate++;
  else if (status === 'EG1') STATS.eg1++;
  else if (status === 'EG2') STATS.eg2++;
  else if (status === 'LOSS') STATS.loss++;
  STATS.total++;

  dbSignalUpdate(s).catch(() => {});
  maybeSuspend(s.key);
  log('FECHO', { id: s.id, status, gale, demorou: Math.round((Date.now() - s.t) / 1000) });
}

async function sendCloseTelegram(s) {
  if (s.gale1MsgId) { await tgDelete(s.gale1MsgId); s.gale1MsgId = null; }
  if (s.gale2MsgId) { await tgDelete(s.gale2MsgId); s.gale2MsgId = null; }
  await tgSendRaw(fmtResult(s));
  tgFAF(fmtHistorico());
}

async function notificarGale(s, nivel) {
  if (nivel === 2 && s.gale1MsgId) { await tgDelete(s.gale1MsgId); s.gale1MsgId = null; }
  const r = await tgSendRaw(fmtGale(s, nivel));
  if (r.ok) {
    if (nivel === 1) s.gale1MsgId = r.messageId;
    else s.gale2MsgId = r.messageId;
    s.galeLevel = nivel;
  }
}

async function apurarOnce() {
  if (R.apurarBusy) return;
  R.apurarBusy = true;
  const now = Date.now();
  const snapshot = [...S.signals];
  const telegramPromises = [];

  try {
    for (const s of snapshot) {
      if (FECHADOS.has(s.status)) continue;
      const after = roundsAfterSignal(s);

      if (after.length === 0) {
        if (now - s.t >= CFG.apuraMaxMs) {
          markClosed(s, 'LOSS', 0);
          telegramPromises.push(sendCloseTelegram(s));
        }
        continue;
      }

      let decided = false;
      let lastR = null;

      for (let k = 0; k <= CFG.maxGale; k++) {
        if (k >= after.length) break;
        const r = after[k];
        const prevT = k === 0 ? s.t : after[k - 1].t;
        if (r.t - prevT > CFG.gapMs) break;
        lastR = r;

        if (r.resultado === s.cor) {
          const status = k === 0 ? 'GREEN' : k === 1 ? 'G1' : 'G2';
          markClosed(s, status, k);
          telegramPromises.push(sendCloseTelegram(s));
          decided = true;
          break;
        }
        if (r.resultado === E) {
          const status = k === 0 ? 'EMPATE' : k === 1 ? 'EG1' : 'EG2';
          markClosed(s, status, k);
          telegramPromises.push(sendCloseTelegram(s));
          decided = true;
          break;
        }

        if (k < CFG.maxGale && s.galeLevel < k + 1) {
          telegramPromises.push(notificarGale(s, k + 1));
        }
      }

      if (!decided) {
        const precisaMaisRodadas = (lastR && s.galeLevel < CFG.maxGale);
        if (!precisaMaisRodadas && after.length > CFG.maxGale) {
          markClosed(s, 'LOSS', CFG.maxGale);
          telegramPromises.push(sendCloseTelegram(s));
        } else if (now - s.t >= CFG.apuraMaxMs) {
          markClosed(s, 'LOSS', s.galeLevel || 0);
          telegramPromises.push(sendCloseTelegram(s));
        }
      }
    }
  } finally {
    R.apurarBusy = false;
  }

  // 🔥 Telegram em paralelo — não bloqueia nada
  Promise.allSettled(telegramPromises).catch(() => {});
}

/* ============================ GERAR SINAL (rápido) ============================ */

async function tentarSinal() {
  if (stopping) return;
  if (!CFG.ativos) { R.ultimoMotivo = 'desativado'; return; }
  if (R.signalBusy) return;

  const t0 = Date.now();
  const thresholdInfo = calcularThreshold();
  R.ultimoThreshold = thresholdInfo.threshold;
  if (thresholdInfo.threshold >= 999) { R.ultimoMotivo = thresholdInfo.motivo; return; }

  const agora = Date.now();
  const desdeUltimo = S.lastSignalAt ? agora - S.lastSignalAt : Infinity;

  if (desdeUltimo < CFG.minGapMs) {
    R.ultimoMotivo = `gap ${Math.ceil((CFG.minGapMs - desdeUltimo) / 1000)}s`;
    return;
  }

  const last = S.hist[S.hist.length - 1];
  if (!last) { R.ultimoMotivo = 'sem hist'; return; }
  if (agora - last.t < CFG.atrasoRodadaMs) {
    R.ultimoMotivo = `atraso ${Math.ceil((CFG.atrasoRodadaMs - (agora - last.t)) / 1000)}s`;
    return;
  }
  if (S.hist.length < CFG.minAmostra) { R.ultimoMotivo = `amostra ${S.hist.length}`; return; }

  let pend = pendentes();
  if (pend.length >= CFG.maxPending && desdeUltimo >= CFG.ultraUrgencyMs) {
    const oldest = pend.sort((a, b) => a.t - b.t)[0];
    if (oldest && (agora - oldest.t) > CFG.forceClosePendingMs) {
      markClosed(oldest, 'LOSS', oldest.galeLevel || 0);
      sendCloseTelegram(oldest).catch(() => {});
      pend = pendentes();
    }
  }
  if (pend.length >= CFG.maxPending) {
    R.ultimoMotivo = `pendentes ${pend.length}/${CFG.maxPending}`;
    return;
  }

  R.signalBusy = true;
  try {
    const rows = S.hist.slice(-30).reverse();
    const cands = runStrategies(rows);
    const stats = getStats();
    const seq = rows.filter((r) => isCol(r.resultado));
    const regime = detectRegime(seq);

    const scored = [];
    const byColor = {};
    for (const c of cands) {
      if (isSuspended(c.key)) continue;
      if (!byColor[c.cor]) byColor[c.cor] = [];
      byColor[c.cor].push(c);
    }
    for (const cor of Object.keys(byColor)) {
      const list = byColor[cor];
      for (const c of list) {
        const conf = confianca(c, { stats, regime, agreement: { confirmations: list.length } });
        scored.push({ ...c, conf, agree: list.length });
      }
    }
    scored.sort((a, b) => b.conf - a.conf);

    let melhor = scored.find((c) => c.conf >= thresholdInfo.threshold);
    if (!melhor && thresholdInfo.forcar) {
      melhor = scored[0] || { ...fallbackCandidate(rows), conf: CFG.scoreForced, agree: 1 };
      melhor.isForced = true;
    }
    if (!melhor) { R.ultimoMotivo = `nenhum ≥${thresholdInfo.threshold}`; return; }

    const analisandoId = ANALYSIS.msgId;
    ANALYSIS.msgId = null;

    const reservedId = S.nextId;
    const now = Date.now();
    const lastRound = S.hist[S.hist.length - 1];

    const sinal = {
      id: reservedId, cor: melhor.cor, key: melhor.key, nome: melhor.nome, emoji: melhor.emoji,
      conf: melhor.conf, tipo: melhor.tipo, status: 'PENDENTE',
      t: now, criadoEm: now, agree: melhor.agree,
      gale1MsgId: null, gale2MsgId: null, galeLevel: 0,
      lastRoundId: lastRound?.id || null
    };

    // 🔥 ÚNICA operação awaited: envio do sinal
    const envio = await tgSendRaw(fmtSinal(sinal)).catch(() => null);
    if (!envio?.ok) {
      ANALYSIS.msgId = analisandoId;
      R.ultimoMotivo = 'telegram falhou';
      return;
    }

    // Sucesso: restantes operações em paralelo/async
    if (analisandoId) tgDelete(analisandoId).catch(() => {});

    S.nextId++;
    S.signals.push(sinal);
    if (S.signals.length > CFG.maxSignalsMem) S.signals = S.signals.slice(-CFG.maxSignalsMem + 50);
    S.lastSignalAt = now;
    R.ultimoSinal = sinal;
    R.ultimoMotivo = null;
    R.ultimoTimingMs = Date.now() - t0;

    dbSignalInsert(sinal).catch(() => {});
    dbStateSet('nextId', String(S.nextId)).catch(() => {});
    dbStateSet('lastSignalAt', String(now)).catch(() => {});

    log('SINAL', {
      id: sinal.id, cor: sinal.cor, conf: sinal.conf, key: sinal.key,
      lastRoundId: sinal.lastRoundId,
      threshold: thresholdInfo.threshold, forced: !!melhor.isForced,
      timingMs: R.ultimoTimingMs,
      delayRodadaMs: lastRound ? now - lastRound.t : null,
      sinaisHora: sinaisUltimos60Min()
    });
  } finally {
    R.signalBusy = false;
  }
}

/* ============================ FALLBACK (só para 4:30) ============================ */

function fallbackLoop() {
  if (stopping) return;
  const desdeUltimo = S.lastSignalAt ? Date.now() - S.lastSignalAt : Infinity;
  if (desdeUltimo >= CFG.maxSemSinalMs && !R.signalBusy && pendentes().length < CFG.maxPending) {
    tentarSinal().catch(e => log('SIGNAL FALLBACK', e.message));
  }
  tickFallbackTimer = setTimeout(fallbackLoop, CFG.fallbackCheckMs);
}

/* ============================ KEEP-ALIVE ============================ */

async function keepAlive() {
  const self = process.env.SELF_URL || process.env.RENDER_EXTERNAL_URL || '';
  if (self) { try { await axios.get(self.replace(/\/$/, '') + '/health', { timeout: 7000 }); } catch (_) {} }
}

/* ============================ EXPRESS ============================ */

const app = express();
app.disable('x-powered-by');
app.use(cors());
app.use(express.json({ limit: '200kb' }));

app.get('/health', (req, res) => {
  const pace = calcularPace();
  const desdeUltimo = S.lastSignalAt ? Date.now() - S.lastSignalAt : null;
  const desdeRodada = S.hist.length ? Date.now() - S.hist[S.hist.length - 1].t : null;
  res.json({
    ok: true, versao: '17.5', deployId: DEPLOY_ID, maxGale: CFG.maxGale,
    hist: S.hist.length, sinais: S.signals.length, pendentes: pendentes().length,
    sinaisUltimaHora: sinaisUltimos60Min(),
    pace: pace ? { sinaisNestaHora: pace.sinaisNestaHora, minutosPassados: Math.round(pace.minutosPassados), deficit: Math.round(pace.deficit * 10) / 10 } : null,
    desdeUltimoSinalSeg: desdeUltimo ? Math.round(desdeUltimo / 1000) : null,
    desdeUltimaRodadaSeg: desdeRodada ? Math.round(desdeRodada / 1000) : null,
    ultimoThreshold: R.ultimoThreshold,
    ultimoTimingMs: R.ultimoTimingMs,
    ultimoDetectMs: R.ultimoDetectMs,
    ultimoDelayRodadaMs: R.ultimoDelayRodadaMs,
    analisandoVisivel: !!ANALYSIS.msgId,
    apiBugs: R.apiBugs,
    loops: { syncBusy: R.syncBusy, apurarBusy: R.apurarBusy, signalBusy: R.signalBusy },
    stats: { ...STATS, greenStreak: getGreenStreak() },
    ultimaSync: R.ultimaSync, erroAPI: R.erroAPI, erroDB: R.erroDB,
    uptime: Math.round(process.uptime())
  });
});
app.get('/status', (req, res) => {
  if (ADMIN_TOKEN && req.query.token !== ADMIN_TOKEN) return res.status(401).json({ erro: 'token' });
  res.json({ motivo: R.ultimoMotivo, R, STATS, CFG, pace: calcularPace() });
});
app.get('/sinais', (req, res) => {
  if (ADMIN_TOKEN && req.query.token !== ADMIN_TOKEN) return res.status(401).json({ erro: 'token' });
  const n = Math.min(100, Math.max(1, parseInt(req.query.n, 10) || 20));
  res.json(S.signals.slice(-n).reverse());
});
app.get('/stats', (req, res) => {
  if (ADMIN_TOKEN && req.query.token !== ADMIN_TOKEN) return res.status(401).json({ erro: 'token' });
  const wins = STATS.green + STATS.g1 + STATS.g2 + STATS.empate + STATS.eg1 + STATS.eg2;
  const winsReais = STATS.green + STATS.g1 + STATS.g2;
  const decididos = winsReais + STATS.loss;
  res.json({
    ...STATS, wins,
    taxaComEmpate: STATS.total > 0 ? wins / STATS.total : null,
    taxaReal: decididos > 0 ? winsReais / decididos : null,
    greenStreak: getGreenStreak()
  });
});
app.get('/calibracao', (req, res) => {
  if (ADMIN_TOKEN && req.query.token !== ADMIN_TOKEN) return res.status(401).json({ erro: 'token' });
  const buckets = { '78-82': { n: 0, w: 0 }, '83-86': { n: 0, w: 0 }, '87-90': { n: 0, w: 0 }, '91-93': { n: 0, w: 0 }, '94-96': { n: 0, w: 0 } };
  for (const s of S.signals) {
    if (!FECHADOS.has(s.status)) continue;
    let b;
    if (s.conf <= 82) b = '78-82';
    else if (s.conf <= 86) b = '83-86';
    else if (s.conf <= 90) b = '87-90';
    else if (s.conf <= 93) b = '91-93';
    else b = '94-96';
    buckets[b].n++;
    if (s.status !== 'LOSS') buckets[b].w++;
  }
  res.json(buckets);
});

/* ============================ START ============================ */

async function start() {
  log('BOOT', {
    versao: '17.5', deployId: DEPLOY_ID, maxGale: CFG.maxGale,
    syncPendingMs: CFG.syncIntervalPendingMs,
    syncIdleMs: CFG.syncIntervalIdleMs,
    atrasoRodadaMs: CFG.atrasoRodadaMs
  });

  await dbInit();
  if (pool) await dbLoadState();
  await syncLoopFirst();
  getStats(true);

  if (pool) {
    for (const s of S.signals) {
      if (s.status !== 'PENDENTE') continue;
      const after = roundsAfterSignal(s);
      let consecutivasFalhas = 0;
      for (const r of after) {
        if (r.resultado === s.cor || r.resultado === E) break;
        consecutivasFalhas++;
        if (consecutivasFalhas > CFG.maxGale) break;
      }
      s.galeLevel = Math.min(consecutivasFalhas, CFG.maxGale);
    }
  }

  server = app.listen(PORT, () => log('HTTP', `porta ${PORT}`));

  // Sync loop (principal)
  scheduleSync();
  // Fallback (4:30 sem sinal)
  fallbackLoop();
  // Gestor "analisando"
  analisandoTimer = setInterval(() => { manageAnalisando(); }, 1500);
  // Keep-alive
  keepTimer = setInterval(() => keepAlive().catch(() => {}), CFG.keepAliveMs);

  process.on('SIGTERM', shutdown);
  process.on('SIGINT', shutdown);
}

async function syncLoopFirst() {
  try {
    const rounds = await fetchRounds();
    for (const r of rounds) {
      if (S.seen.has(r.id)) continue;
      S.seen.add(r.id);
      S.hist.push(r);
    }
    S.hist.sort((a, b) => a.t - b.t || (a.id < b.id ? -1 : 1));
    log('SYNC BOOT', { total: S.hist.length });
  } catch (e) {
    log('SYNC BOOT ERRO', e.message);
  }
}

async function shutdown() {
  if (stopping) return;
  stopping = true; log('SHUTDOWN', 'ok');
  clearTimeout(tickSyncTimer);
  clearTimeout(tickFallbackTimer);
  clearInterval(analisandoTimer);
  clearInterval(keepTimer);
  if (server) server.close();
  if (pool) await pool.end().catch(() => {});
  process.exit(0);
}

start().catch((e) => { console.error(e); process.exit(1); });'use strict';

/**
 * CAPUNGA DADOS — BAC BO · v17.5
 * Sinal imediato: poll 400ms + trigger event-driven + atrasoRodada 100ms.
 * Env: BOT_TOKEN, CHAT_ID, ADMIN_TOKEN, DATABASE_URL, PORT
 */

const fs = require('fs');
const crypto = require('crypto');
const express = require('express');
const cors = require('cors');
const axios = require('axios');
const { Pool } = require('pg');
const http = require('http');
const https = require('https');

const PORT = Number(process.env.PORT || 10000);
const BOT_TOKEN = process.env.BOT_TOKEN || '';
const CHAT_ID = process.env.CHAT_ID || '';
const ADMIN_TOKEN = process.env.ADMIN_TOKEN || '';
const DATABASE_URL = process.env.DATABASE_URL || '';

const CFG = {
  ativos: true,
  maxGale: 2,
  maxPending: 3,
  minAmostra: 40,
  sinalInicial: 1,

  // 🔥 Poll rápido
  syncIntervalPendingMs: 400,    // 400ms com sinal aberto
  syncIntervalIdleMs: 800,       // 800ms idle

  // Timing
  minGapMs: 20000,
  atrasoRodadaMs: 100,           // 🔥 100ms (era 3000) — praticamente imediato
  apuraMaxMs: 240000,
  maxHist: 4000,
  gapMs: 7 * 60 * 1000,
  maxSignalsMem: 500,
  signalsOnBoot: 500,
  suspendMs: 2 * 3600 * 1000,
  suspendLossStreak: 4,
  suspendLookback: 5,
  keepAliveMs: 60000,

  maxPorHora: 30,
  minPorHora: 25,
  softUrgencyMs: 180000,
  ultraUrgencyMs: 240000,
  maxSemSinalMs: 270000,
  forceClosePendingMs: 255000,
  scoreNormal: 82,
  scoreUrgent: 72,
  scoreUltra: 62,
  scoreForced: 50,

  fallbackCheckMs: 3000
};

const J = 'Jogador';
const B = 'Banca';
const E = 'Empate';
const isCol = (x) => x === J || x === B;
const opp = (c) => (c === J ? B : c === B ? J : null);
const FECHADOS = new Set(['GREEN', 'G1', 'G2', 'EMPATE', 'EG1', 'EG2', 'LOSS']);

const API_BACBO =
  'https://api-cs.casino.org/svc-evolution-game-events/api/bacbo?page=0&size=50&sort=data.settledAt,desc&duration=120&wheelResults=PlayerWon,BankerWon,Tie';

/* ============================ HTTP AGENT KEEP-ALIVE ============================ */

const httpAgent = new http.Agent({ keepAlive: true, maxSockets: 8, keepAliveMsecs: 30000 });
const httpsAgent = new https.Agent({ keepAlive: true, maxSockets: 8, keepAliveMsecs: 30000 });

const apiClient = axios.create({
  timeout: 3000,
  httpAgent,
  httpsAgent,
  headers: { Accept: 'application/json', 'User-Agent': 'CapungaDados/17.5' }
});

/* ============================ DEPLOY ID ============================ */

const DEPLOY_ID = (() => {
  if (process.env.DEPLOY_ID) return String(process.env.DEPLOY_ID).trim();
  try { return crypto.createHash('sha256').update(fs.readFileSync(__filename)).digest('hex').slice(0, 16); }
  catch { return 'unknown'; }
})();

/* ============================ STATE ============================ */

const S = {
  hist: [], seen: new Set(), signals: [],
  nextId: CFG.sinalInicial, lastSignalAt: 0,
  stats: null, statsAtLen: -1
};

const STATS = { total: 0, green: 0, g1: 0, g2: 0, empate: 0, eg1: 0, eg2: 0, loss: 0 };
const SUSPENDED = {};
const ANALYSIS = { msgId: null, shownAt: 0, enviando: false };

const R = {
  iniciadoEm: new Date().toISOString(), deployId: DEPLOY_ID,
  ultimaSync: null, ultimaRodadaNova: null,
  erroAPI: null, erroTelegram: null, erroDB: null,
  ultimoMotivo: null, ultimoSinal: null,
  wakes: 0, ultimoWake: null,
  ultimoThreshold: null, ultimoTimingMs: null,
  ultimoDetectMs: null,       // tempo do fetch após rodada settle
  ultimoDelayRodadaMs: null,  // delay real entre settle e envio do sinal
  syncBusy: false, apurarBusy: false, signalBusy: false,
  apiBugs: 0
};

let pool = null, server = null, stopping = false;
let tickSyncTimer = null, tickFallbackTimer = null, keepTimer = null, analisandoTimer = null;

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

/* ============================ FREQUÊNCIA ============================ */

function sinaisUltimos60Min() {
  const agora = Date.now();
  return S.signals.filter((s) => s.t && agora - s.t < 3600000).length;
}

function calcularPace() {
  const agora = Date.now();
  const inicioHora = agora - (agora % 3600000);
  const minutosPassados = (agora - inicioHora) / 60000;
  if (minutosPassados < 10) return null;
  const sinaisNestaHora = S.signals.filter((s) => s.t && s.t >= inicioHora).length;
  const esperado = (minutosPassados / 60) * CFG.minPorHora;
  return {
    sinaisNestaHora, minutosPassados,
    deficit: esperado - sinaisNestaHora,
    pace: sinaisNestaHora / (minutosPassados / 60)
  };
}

function calcularThreshold() {
  const agora = Date.now();
  const desdeUltimo = S.lastSignalAt ? agora - S.lastSignalAt : Infinity;
  const sinaisHora = sinaisUltimos60Min();

  if (sinaisHora >= CFG.maxPorHora) {
    return { threshold: 999, forcar: false, motivo: `cap ${CFG.maxPorHora}/h` };
  }

  let threshold, forcar = false;
  if (desdeUltimo >= CFG.maxSemSinalMs) { threshold = CFG.scoreForced; forcar = true; }
  else if (desdeUltimo >= CFG.ultraUrgencyMs) { threshold = CFG.scoreUltra; }
  else if (desdeUltimo >= CFG.softUrgencyMs) { threshold = CFG.scoreUrgent; }
  else if (desdeUltimo >= 90000) { threshold = 78; }
  else { threshold = CFG.scoreNormal; }

  const pace = calcularPace();
  if (pace && sinaisHora < 27) {
    if (pace.deficit > 3) threshold -= 10;
    else if (pace.deficit > 1) threshold -= 5;
  } else if (sinaisHora >= 28) {
    threshold += 5;
  }

  threshold = Math.max(45, Math.min(95, threshold));
  return { threshold, forcar, motivo: `score≥${threshold} (t=${Math.round(desdeUltimo / 1000)}s)` };
}

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
    await pool.query(`INSERT INTO capunga_signals (id,cor,key,nome,tipo,conf,status,gale,agree,created_at,closed_at)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) ON CONFLICT (id) DO NOTHING`,
      [s.id, s.cor, s.key, s.nome, s.tipo, s.conf, s.status, s.gale ?? null, s.agree ?? 1,
       new Date(s.t), s.fechadoEm ? new Date(s.fechadoEm) : null]);
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
    S.signals = r.rows.reverse().map((row) => ({
      id: row.id, cor: row.cor, key: row.key, nome: row.nome, tipo: row.tipo,
      conf: row.conf, status: row.status, gale: row.gale, agree: row.agree,
      t: new Date(row.created_at).getTime(), criadoEm: new Date(row.created_at).getTime(),
      fechadoEm: row.closed_at ? new Date(row.closed_at).getTime() : null,
      gale1MsgId: null, gale2MsgId: null, galeLevel: 0,
      lastRoundId: null
    }));
    rebuildStats();
    log('DB', `estado carregado: nextId=${S.nextId}, sinais=${S.signals.length}`);
  } catch (e) { R.erroDB = e.message; log('DB ERRO load', e.message); }
}
function rebuildStats() {
  STATS.total = 0; STATS.green = 0; STATS.g1 = 0; STATS.g2 = 0;
  STATS.empate = 0; STATS.eg1 = 0; STATS.eg2 = 0; STATS.loss = 0;
  for (const s of S.signals) {
    if (!FECHADOS.has(s.status)) continue;
    STATS.total++;
    if (s.status === 'GREEN') STATS.green++;
    else if (s.status === 'G1') STATS.g1++;
    else if (s.status === 'G2') STATS.g2++;
    else if (s.status === 'EMPATE') STATS.empate++;
    else if (s.status === 'EG1') STATS.eg1++;
    else if (s.status === 'EG2') STATS.eg2++;
    else if (s.status === 'LOSS') STATS.loss++;
  }
}

/* ============================ TELEGRAM ============================ */

async function tgSendRaw(texto) {
  if (!BOT_TOKEN || !CHAT_ID) { R.erroTelegram = 'TOKEN/CHAT em falta'; return { ok: false }; }
  for (let t = 1; t <= 2; t++) {
    try {
      const r = await axios.post(`https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`,
        { chat_id: CHAT_ID, text: texto, parse_mode: 'HTML', disable_web_page_preview: true },
        { timeout: 5000, httpAgent, httpsAgent });
      if (r.data?.ok) { R.erroTelegram = null; return { ok: true, messageId: r.data.result.message_id }; }
      R.erroTelegram = JSON.stringify(r.data);
    } catch (e) {
      R.erroTelegram = e.response?.data ? JSON.stringify(e.response.data) : e.message;
      const retry = Number(e.response?.data?.parameters?.retry_after);
      if (retry > 0 && retry <= 20) await sleep(retry * 1000 + 200);
    }
    if (t < 2) await sleep(400);
  }
  return { ok: false };
}

function tgFAF(texto) {
  tgSendRaw(texto).catch((e) => log('TG FAF', e.message));
}

async function tgDelete(messageId) {
  if (!messageId || !BOT_TOKEN || !CHAT_ID) return;
  try {
    await axios.post(`https://api.telegram.org/bot${BOT_TOKEN}/deleteMessage`,
      { chat_id: CHAT_ID, message_id: messageId }, { timeout: 4000, httpAgent, httpsAgent });
  } catch (_) {}
}

/* ============================ ANALISANDO ============================ */

async function ensureAnalisando() {
  if (ANALYSIS.msgId || ANALYSIS.enviando) return;
  ANALYSIS.enviando = true;
  try {
    const r = await tgSendRaw(fmtAnalisando());
    if (r.ok) {
      ANALYSIS.msgId = r.messageId;
      ANALYSIS.shownAt = Date.now();
    }
  } finally { ANALYSIS.enviando = false; }
}

function hideAnalisandoAsync() {
  const id = ANALYSIS.msgId;
  ANALYSIS.msgId = null;
  if (id) tgDelete(id).catch(() => {});
}

function manageAnalisando() {
  if (stopping) return;
  const podeEscanear = CFG.ativos && pendentes().length < CFG.maxPending;
  const desdeUltimo = S.lastSignalAt ? Date.now() - S.lastSignalAt : Infinity;
  const precisaMostrar = podeEscanear && desdeUltimo >= CFG.minGapMs;

  if (precisaMostrar) {
    if (!ANALYSIS.msgId && !ANALYSIS.enviando) ensureAnalisando().catch(() => {});
  } else {
    if (ANALYSIS.msgId) hideAnalisandoAsync();
  }
}

/* ============================ TEMPLATES ============================ */

function fmtAnalisando() {
  return ['🔍 <b>Analisando o mercado tropa…</b>', '', `⏱ ${hora()}`].join('\n');
}

function fmtSinal(s) {
  const confEmoji = s.conf >= 94 ? '💎' : s.conf >= 86 ? '🔥' : '⚡';
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
    `Sequência de greens: ${streak >= 3 ? streak + '🔥' : streak}`,
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
  let resultado = normalizeOutcome(raw);
  const id = String(d.id ?? d.roundId ?? d.round_id ?? d.eventId ?? item?.id ?? item?.roundId ?? item?.round_id ?? item?.eventId ?? '').trim();
  if (!id || !resultado || !t) return null;

  const pj = Number(r.playerDice?.score ?? d.playerDice?.score ?? d.playerScore ?? d.player_score ?? d.pontos_jogador);
  const pb = Number(r.bankerDice?.score ?? d.bankerDice?.score ?? d.bankerScore ?? d.banker_score ?? d.pontos_banca);
  const pjOk = Number.isFinite(pj) ? pj : null;
  const pbOk = Number.isFinite(pb) ? pb : null;

  if (pjOk !== null && pbOk !== null && pjOk === pbOk && resultado !== E) {
    R.apiBugs++;
    log('API BUG FIX', { id, reported: resultado, pj: pjOk, pb: pbOk, fixed: 'Empate' });
    resultado = E;
  }

  return { id, resultado, t, pj: pjOk, pb: pbOk };
}

async function fetchRounds() {
  let lastErr = null;
  for (let i = 1; i <= 2; i++) {
    try {
      const resp = await apiClient.get(API_BACBO);
      const map = new Map();
      for (const it of extractItems(resp.data)) {
        const rd = normalizeRound(it);
        if (rd) map.set(rd.id, rd);
      }
      R.erroAPI = null;
      return [...map.values()].sort((a, b) => a.t - b.t || (a.id < b.id ? -1 : 1));
    } catch (e) {
      lastErr = e; R.erroAPI = e.message;
      if (i < 2) await sleep(200);
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
  ({ seq }) => { const n = streak(seq); if (n < 3 || n > 7) return null; return { cor: seq[0].resultado, key: `SEQ_REP_${n}`, nome: `Sequência x${n}`, emoji: '🔁', strength: [0,0,0,72,78,84,86,86][n] || 86, tipo: 'seq' }; },
  ({ seq }) => { const n = streak(seq); if (n < 4 || n > 8) return null; return { cor: opp(seq[0].resultado), key: `SEQ_QUEBRA_${n}`, nome: `Quebra x${n}`, emoji: '💥', strength: [0,0,0,0,74,80,84,85,85][n] || 85, tipo: 'seq' }; },
  ({ seq }) => { const L = alternation(seq); if (L < 4) return null; return { cor: opp(seq[0].resultado), key: `SEQ_SURF_${L}`, nome: `Surf ${L}`, emoji: '🌊', strength: L >= 6 ? 84 : L >= 5 ? 78 : 72, tipo: 'seq' }; },
  ({ seq }) => { if (seq.length < 4) return null; const [a,b,c,d] = seq.map(x => x.resultado); return (a === b && c === d && a !== c) ? { cor: c, key: 'SEQ_2x2', nome: 'Bloco 2+2', emoji: '♟️', strength: 76, tipo: 'seq' } : null; },
  ({ seq }) => { if (seq.length < 4) return null; const [a,b,c,d] = seq.map(x => x.resultado); return (a !== b && b === c && c === d) ? { cor: a, key: 'SEQ_3_1', nome: '3→1 confirmado', emoji: '🧩', strength: 78, tipo: 'seq' } : null; },
  ({ seq }) => { if (seq.length < 3 || !seq.slice(0,3).every(hasScore)) return null; const [a,b,c] = seq; if (a.resultado !== b.resultado || b.resultado !== c.resultado) return null; const sa = scoreOf(a), sb = scoreOf(b), sc = scoreOf(c); return ((sb-sa===1 && sc-sb===1) || (sa-sb===1 && sb-sc===1)) ? { cor: a.resultado, key: 'NUM_ESCADA', nome: `Escada ${sa}→${sb}→${sc}`, emoji: '🪜', strength: 88, tipo: 'num' } : null; },
  ({ seq }) => { if (seq.length < 2 || !hasScore(seq[0]) || !hasScore(seq[1])) return null; const [a,b] = seq; return (a.resultado === b.resultado && scoreOf(a) === scoreOf(b)) ? { cor: a.resultado, key: 'NUM_REPETE', nome: `Score repetido ${scoreOf(a)}`, emoji: '🔂', strength: 84, tipo: 'num' } : null; },
  ({ seq }) => { if (!seq.length || !hasScore(seq[0])) return null; const s = scoreOf(seq[0]); return s === 0 ? { cor: opp(seq[0].resultado), key: 'NUM_GATILHO_0', nome: 'Gatilho 0', emoji: '🎯', strength: 86, tipo: 'num' } : null; },
  ({ seq }) => { if (!seq.length || !hasScore(seq[0])) return null; const s = scoreOf(seq[0]); return s === 1 ? { cor: opp(seq[0].resultado), key: 'NUM_GATILHO_1', nome: 'Gatilho 1', emoji: '🎯', strength: 84, tipo: 'num' } : null; },
  ({ seq }) => { if (!seq.length || !hasScore(seq[0])) return null; const s = scoreOf(seq[0]); return s === 9 ? { cor: opp(seq[0].resultado), key: 'NUM_GATILHO_9', nome: 'Gatilho 9', emoji: '🎯', strength: 88, tipo: 'num' } : null; },
  ({ seq }) => { if (!seq.length || !hasScore(seq[0])) return null; const s = scoreOf(seq[0]); return s === 8 ? { cor: opp(seq[0].resultado), key: 'NUM_GATILHO_8', nome: 'Gatilho 8', emoji: '🎯', strength: 78, tipo: 'num' } : null; },
  ({ rows }) => { const r = rows[0]; if (!r || !isCol(r.resultado) || !hasScore(r) || margem(r) < 5) return null; return { cor: r.resultado, key: 'NUM_MARGEM5', nome: `Margem alta ${r.pj}×${r.pb}`, emoji: '📊', strength: 82, tipo: 'num' }; },
  ({ seq }) => { if (seq.length < 3) return null; const t = seq.slice(0,3); if (!t.every(r => hasScore(r) && margem(r) >= 4 && r.resultado === t[0].resultado)) return null; return { cor: t[0].resultado, key: 'NUM_DIFF4x3', nome: 'Diff ≥4 ×3', emoji: '🔢', strength: 85, tipo: 'num' }; },
  ({ seq }) => { if (seq.length < 2) return null; const [a,b] = seq; if (!hasScore(a) || !hasScore(b) || a.resultado !== b.resultado || scoreOf(a) > 4 || scoreOf(b) > 4) return null; return { cor: opp(a.resultado), key: 'NUM_VIRADA_BAIXA', nome: 'Virada de baixos', emoji: '🔄', strength: 80, tipo: 'num' }; },
  ({ rows }) => { const r = rows[0]; if (!r || !hasScore(r) || r.pj !== r.pb) return null; return { cor: r.resultado, key: 'NUM_ESPELHO', nome: `Espelho ${r.pj}×${r.pb}`, emoji: '🪞', strength: 83, tipo: 'num' }; },
  ({ rows }) => { const s = rows.slice(0,8).filter(hasScore); if (s.length < 6) return null; const mj = s.reduce((a,r) => a+r.pj,0)/s.length; const mb = s.reduce((a,r) => a+r.pb,0)/s.length; const d = mj-mb; return Math.abs(d) >= 1.5 ? { cor: d > 0 ? J : B, key: 'NUM_TENDENCIA', nome: `Tendência ${mj.toFixed(1)}×${mb.toFixed(1)}`, emoji: '📈', strength: Math.abs(d) >= 2.2 ? 84 : 76, tipo: 'num' } : null; },
  ({ seq }) => { if (seq.length < 2 || !hasScore(seq[0]) || !hasScore(seq[1])) return null; const [a,b] = seq; const ra = scoreRange(scoreOf(a)), rb = scoreRange(scoreOf(b)); if (!ra || !rb) return null; return ((ra==='MB'&&rb==='MA') || (ra==='MA'&&rb==='MB')) ? { cor: opp(a.resultado), key: 'NUM_SALTO_RANGE', nome: `Salto ${scoreOf(b)}→${scoreOf(a)}`, emoji: '📐', strength: 81, tipo: 'num' } : null; },
  ({ rows, seq }) => { if (!rows[0] || rows[0].resultado !== E || !seq.length) return null; return { cor: seq[0].resultado, key: 'NUM_POS_EMPATE', nome: 'Pós-empate', emoji: '🎯', strength: 74, tipo: 'num' }; },
  ({ seq }) => { const L = alternation(seq); if (L < 4 || L > 10) return null; return { cor: opp(seq[0].resultado), key: 'SEQ_PINGPONG', nome: `Ping-pong ${L}`, emoji: '🏓', strength: 79, tipo: 'seq' }; },
  ({ seq }) => { if (seq.length < 5) return null; const [a,b,c,d,e] = seq.map(x => x.resultado); return (a===b && b===c && c!==d && d===e) ? { cor: a, key: 'SEQ_EXTEND', nome: 'Streak extend', emoji: '⏭️', strength: 80, tipo: 'seq' } : null; },
  ({ seq }) => { const n = streak(seq); if (n < 4 || n > 6) return null; if (!seq.slice(0,n).every(r => hasScore(r) && margem(r) >= 3)) return null; return { cor: seq[0].resultado, key: 'SEQ_STRONG_STREAK', nome: `Dragon ${n}`, emoji: '🐉', strength: 82, tipo: 'seq' }; },
  ({ rows }) => { const r = rows[0]; if (!r || !hasScore(r)) return null; return (r.pj >= 8 && r.pb >= 8) ? { cor: r.resultado, key: 'NUM_NATURAL', nome: `Natural ${r.pj}×${r.pb}`, emoji: '🎴', strength: 86, tipo: 'num' } : null; },
  ({ seq }) => { if (!seq.length || !hasScore(seq[0])) return null; const s = scoreOf(seq[0]); return (s === 5 || s === 8) ? { cor: seq[0].resultado, key: 'NUM_FIBONACCI', nome: `Fibonacci ${s}`, emoji: '🌀', strength: 80, tipo: 'num' } : null; },
  ({ seq }) => { if (seq.length < 3 || !seq.slice(0,3).every(hasScore)) return null; const [a,b,c] = seq; if (a.resultado !== b.resultado || b.resultado !== c.resultado) return null; const sa = scoreOf(a), sb = scoreOf(b), sc = scoreOf(c); return (sa < sb && sb < sc) ? { cor: a.resultado, key: 'NUM_SCORE_TREND_3', nome: `Escala ${sa}→${sc}`, emoji: '📊', strength: 82, tipo: 'num' } : null; },
  ({ seq }) => { if (seq.length < 2 || !seq.slice(0,2).every(hasScore)) return null; const [a,b] = seq; if (a.resultado !== b.resultado || margem(a) < 5 || margem(b) < 5) return null; return { cor: a.resultado, key: 'NUM_DOUBLE_HIGH', nome: 'Dupla margem alta', emoji: '💪', strength: 84, tipo: 'num' }; },
  ({ rows }) => { const ties = rows.slice(0,3).filter(r => r.resultado === E).length; if (ties < 2) return null; const pre = rows.find(r => isCol(r.resultado)); return pre ? { cor: pre.resultado, key: 'TIE_CASCADE', nome: 'Cascata empates', emoji: '🟡', strength: 76, tipo: 'num' } : null; },
  ({ rows, seq }) => { if (!rows[0] || rows[0].resultado !== E || !rows[1] || rows[1].resultado === E || !seq.length) return null; return { cor: opp(seq[0].resultado), key: 'TIE_SINGLE', nome: 'Empate isolado', emoji: '🔀', strength: 77, tipo: 'num' }; },
  ({ seq }) => { const s = seq.slice(0,20); if (s.length < 15) return null; const b = s.filter(r => r.resultado === B).length; const j = s.filter(r => r.resultado === J).length; if (b >= 12) return { cor: B, key: 'BANKER_BIAS_20', nome: 'Viés Banca', emoji: '📉', strength: 80, tipo: 'seq' }; if (j >= 12) return { cor: J, key: 'BANKER_BIAS_20', nome: 'Viés Jogador', emoji: '📈', strength: 80, tipo: 'seq' }; return null; },
  ({ seq }) => { if (seq.length < 4 || !seq.slice(0,4).every(hasScore)) return null; const [a,b,c,d] = seq; const sa = scoreOf(a), sb = scoreOf(b), sc = scoreOf(c), sd = scoreOf(d); return (sa-sb < 0 && sb-sc < 0 && sc-sd < 0) ? { cor: opp(a.resultado), key: 'MOMENTUM_SHIFT', nome: 'Mudança momentum', emoji: '🔀', strength: 78, tipo: 'num' } : null; },
  ({ seq }) => { const L = alternation(seq); if (L < 3) return null; if (!seq.slice(0,L).every(r => hasScore(r) && margem(r) >= 3)) return null; return { cor: opp(seq[0].resultado), key: 'SEQ_ALT_STRONG', nome: `Alt. forte ${L}`, emoji: '⚔️', strength: 78, tipo: 'seq' }; }
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
  const H = S.hist;
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
function getStats(force = false) {
  if (force || !S.stats || S.hist.length - S.statsAtLen >= 30) {
    S.stats = backtest();
    S.statsAtLen = S.hist.length;
  }
  return S.stats;
}

/* ============================ CONFIANÇA ============================ */

function bayesianRate(w, n, priorRate = 0.75, priorWeight = 6) {
  return (w + priorRate * priorWeight) / (n + priorWeight);
}
function mapObservedToScore(observed) {
  if (observed <= 0.75) return 80;
  if (observed >= 0.93) return 95;
  return 80 + ((observed - 0.75) / 0.18) * 15;
}
function confianca(cand, ctx) {
  const stats = ctx.stats;
  const p = stats.per[cand.key] || { n: 0, w: 0 };
  const btRate = bayesianRate(p.w, p.n, 0.75, 6);
  const live = S.signals.filter((x) => x.key === cand.key && FECHADOS.has(x.status));
  const liveW = live.filter((x) => x.status !== 'LOSS').length;
  const liveRate = live.length >= 3 ? bayesianRate(liveW, live.length, btRate, 3) : null;
  const btSamples = p.n + 6;
  const liveSamples = live.length >= 3 ? live.length + 3 : 0;
  const observed = (btRate * btSamples + (liveRate ?? btRate) * liveSamples) / (btSamples + liveSamples);

  let score = mapObservedToScore(observed);
  const agree = ctx.agreement?.confirmations || 0;
  if (agree >= 3) score += 2;
  else if (agree >= 2) score += 1;
  score += (cand.strength - 80) * 0.4;
  if (cand.tipo === 'seq' && ctx.regime) {
    const isRev = cand.key.includes('QUEBRA') || cand.key.includes('SURF') || cand.key.includes('PINGPONG');
    const fit = isRev ? ctx.regime.chop : ctx.regime.trend;
    score += (fit - 0.5) * 6;
  }
  const recentLoss = S.signals.find((x) =>
    x.key === cand.key && x.status === 'LOSS' && Date.now() - (x.fechadoEm || 0) < 5 * 60 * 1000);
  if (recentLoss) score -= 8;

  return Math.max(78, Math.min(96, Math.round(score)));
}

/* ============================ SUSPENSÃO / FALLBACK / PENDENTES ============================ */

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
function fallbackCor(rows) {
  const seq = rows.filter((r) => isCol(r.resultado)).slice(0, 12);
  if (!seq.length) return J;
  const b = seq.filter((r) => r.resultado === B).length;
  const j = seq.filter((r) => r.resultado === J).length;
  if (b > j) return B;
  if (j > b) return J;
  return seq[0].resultado;
}
function fallbackCandidate(rows) {
  return { cor: fallbackCor(rows), key: 'FALLBACK_TREND', nome: 'Tendência curta', emoji: '🛟', strength: 55, tipo: 'seq' };
}
function pendentes() {
  return S.signals.filter((s) => !FECHADOS.has(s.status));
}

/* ============================ SYNC (event-driven, rápido) ============================ */

async function syncLoop() {
  if (stopping) return;
  if (R.syncBusy) { scheduleSync(); return; }
  R.syncBusy = true;
  const t0 = Date.now();

  try {
    const rounds = await fetchRounds();
    let newRound = null;

    for (const r of rounds) {
      if (S.seen.has(r.id)) continue;
      S.seen.add(r.id);
      S.hist.push(r);
      if (!newRound || r.t > newRound.t) newRound = r;
    }

    if (newRound) {
      // Ordena + corta
      S.hist.sort((a, b) => a.t - b.t || (a.id < b.id ? -1 : 1));
      if (S.hist.length > CFG.maxHist) {
        const ex = S.hist.length - CFG.maxHist;
        for (let i = 0; i < ex; i++) S.seen.delete(S.hist[i].id);
        S.hist = S.hist.slice(ex);
      }

      R.ultimaRodadaNova = new Date(newRound.t).toISOString();
      R.ultimaSync = new Date().toISOString();
      R.ultimoDetectMs = Date.now() - t0;
      R.ultimoDelayRodadaMs = Date.now() - newRound.t;

      log('SYNC', {
        newId: newRound.id,
        cor: newRound.resultado,
        detectMs: R.ultimoDetectMs,
        delayRodadaMs: R.ultimoDelayRodadaMs
      });

      // 🔥 Em paralelo, sem bloquear:
      // 1. apurar (fecha sinais — síncrono + telegram FAF)
      // 2. tentar sinal imediatamente
      apurarOnce().catch(e => log('APURAR ERR', e.message));
      tentarSinal().catch(e => log('SIGNAL ERR', e.message));
    }
  } catch (e) {
    R.erroAPI = e.message;
    log('SYNC ERRO', e.message);
  } finally {
    R.syncBusy = false;
    scheduleSync();
  }
}

function scheduleSync() {
  const delay = pendentes().length > 0 ? CFG.syncIntervalPendingMs : CFG.syncIntervalIdleMs;
  tickSyncTimer = setTimeout(syncLoop, delay);
}

/* ============================ APURAR (síncrono no estado, telegram FAF) ============================ */

function roundsAfterSignal(s) {
  if (s.lastRoundId) {
    const idx = S.hist.findIndex((r) => r.id === s.lastRoundId);
    if (idx >= 0) return S.hist.slice(idx + 1);
  }
  return S.hist.filter((r) => r.t > s.t).sort((a, b) => a.t - b.t);
}

function markClosed(s, status, gale) {
  s.status = status;
  s.gale = gale;
  s.fechadoEm = Date.now();

  if (status === 'GREEN') STATS.green++;
  else if (status === 'G1') STATS.g1++;
  else if (status === 'G2') STATS.g2++;
  else if (status === 'EMPATE') STATS.empate++;
  else if (status === 'EG1') STATS.eg1++;
  else if (status === 'EG2') STATS.eg2++;
  else if (status === 'LOSS') STATS.loss++;
  STATS.total++;

  dbSignalUpdate(s).catch(() => {});
  maybeSuspend(s.key);
  log('FECHO', { id: s.id, status, gale, demorou: Math.round((Date.now() - s.t) / 1000) });
}

async function sendCloseTelegram(s) {
  if (s.gale1MsgId) { await tgDelete(s.gale1MsgId); s.gale1MsgId = null; }
  if (s.gale2MsgId) { await tgDelete(s.gale2MsgId); s.gale2MsgId = null; }
  await tgSendRaw(fmtResult(s));
  tgFAF(fmtHistorico());
}

async function notificarGale(s, nivel) {
  if (nivel === 2 && s.gale1MsgId) { await tgDelete(s.gale1MsgId); s.gale1MsgId = null; }
  const r = await tgSendRaw(fmtGale(s, nivel));
  if (r.ok) {
    if (nivel === 1) s.gale1MsgId = r.messageId;
    else s.gale2MsgId = r.messageId;
    s.galeLevel = nivel;
  }
}

async function apurarOnce() {
  if (R.apurarBusy) return;
  R.apurarBusy = true;
  const now = Date.now();
  const snapshot = [...S.signals];
  const telegramPromises = [];

  try {
    for (const s of snapshot) {
      if (FECHADOS.has(s.status)) continue;
      const after = roundsAfterSignal(s);

      if (after.length === 0) {
        if (now - s.t >= CFG.apuraMaxMs) {
          markClosed(s, 'LOSS', 0);
          telegramPromises.push(sendCloseTelegram(s));
        }
        continue;
      }

      let decided = false;
      let lastR = null;

      for (let k = 0; k <= CFG.maxGale; k++) {
        if (k >= after.length) break;
        const r = after[k];
        const prevT = k === 0 ? s.t : after[k - 1].t;
        if (r.t - prevT > CFG.gapMs) break;
        lastR = r;

        if (r.resultado === s.cor) {
          const status = k === 0 ? 'GREEN' : k === 1 ? 'G1' : 'G2';
          markClosed(s, status, k);
          telegramPromises.push(sendCloseTelegram(s));
          decided = true;
          break;
        }
        if (r.resultado === E) {
          const status = k === 0 ? 'EMPATE' : k === 1 ? 'EG1' : 'EG2';
          markClosed(s, status, k);
          telegramPromises.push(sendCloseTelegram(s));
          decided = true;
          break;
        }

        if (k < CFG.maxGale && s.galeLevel < k + 1) {
          telegramPromises.push(notificarGale(s, k + 1));
        }
      }

      if (!decided) {
        const precisaMaisRodadas = (lastR && s.galeLevel < CFG.maxGale);
        if (!precisaMaisRodadas && after.length > CFG.maxGale) {
          markClosed(s, 'LOSS', CFG.maxGale);
          telegramPromises.push(sendCloseTelegram(s));
        } else if (now - s.t >= CFG.apuraMaxMs) {
          markClosed(s, 'LOSS', s.galeLevel || 0);
          telegramPromises.push(sendCloseTelegram(s));
        }
      }
    }
  } finally {
    R.apurarBusy = false;
  }

  // 🔥 Telegram em paralelo — não bloqueia nada
  Promise.allSettled(telegramPromises).catch(() => {});
}

/* ============================ GERAR SINAL (rápido) ============================ */

async function tentarSinal() {
  if (stopping) return;
  if (!CFG.ativos) { R.ultimoMotivo = 'desativado'; return; }
  if (R.signalBusy) return;

  const t0 = Date.now();
  const thresholdInfo = calcularThreshold();
  R.ultimoThreshold = thresholdInfo.threshold;
  if (thresholdInfo.threshold >= 999) { R.ultimoMotivo = thresholdInfo.motivo; return; }

  const agora = Date.now();
  const desdeUltimo = S.lastSignalAt ? agora - S.lastSignalAt : Infinity;

  if (desdeUltimo < CFG.minGapMs) {
    R.ultimoMotivo = `gap ${Math.ceil((CFG.minGapMs - desdeUltimo) / 1000)}s`;
    return;
  }

  const last = S.hist[S.hist.length - 1];
  if (!last) { R.ultimoMotivo = 'sem hist'; return; }
  if (agora - last.t < CFG.atrasoRodadaMs) {
    R.ultimoMotivo = `atraso ${Math.ceil((CFG.atrasoRodadaMs - (agora - last.t)) / 1000)}s`;
    return;
  }
  if (S.hist.length < CFG.minAmostra) { R.ultimoMotivo = `amostra ${S.hist.length}`; return; }

  let pend = pendentes();
  if (pend.length >= CFG.maxPending && desdeUltimo >= CFG.ultraUrgencyMs) {
    const oldest = pend.sort((a, b) => a.t - b.t)[0];
    if (oldest && (agora - oldest.t) > CFG.forceClosePendingMs) {
      markClosed(oldest, 'LOSS', oldest.galeLevel || 0);
      sendCloseTelegram(oldest).catch(() => {});
      pend = pendentes();
    }
  }
  if (pend.length >= CFG.maxPending) {
    R.ultimoMotivo = `pendentes ${pend.length}/${CFG.maxPending}`;
    return;
  }

  R.signalBusy = true;
  try {
    const rows = S.hist.slice(-30).reverse();
    const cands = runStrategies(rows);
    const stats = getStats();
    const seq = rows.filter((r) => isCol(r.resultado));
    const regime = detectRegime(seq);

    const scored = [];
    const byColor = {};
    for (const c of cands) {
      if (isSuspended(c.key)) continue;
      if (!byColor[c.cor]) byColor[c.cor] = [];
      byColor[c.cor].push(c);
    }
    for (const cor of Object.keys(byColor)) {
      const list = byColor[cor];
      for (const c of list) {
        const conf = confianca(c, { stats, regime, agreement: { confirmations: list.length } });
        scored.push({ ...c, conf, agree: list.length });
      }
    }
    scored.sort((a, b) => b.conf - a.conf);

    let melhor = scored.find((c) => c.conf >= thresholdInfo.threshold);
    if (!melhor && thresholdInfo.forcar) {
      melhor = scored[0] || { ...fallbackCandidate(rows), conf: CFG.scoreForced, agree: 1 };
      melhor.isForced = true;
    }
    if (!melhor) { R.ultimoMotivo = `nenhum ≥${thresholdInfo.threshold}`; return; }

    const analisandoId = ANALYSIS.msgId;
    ANALYSIS.msgId = null;

    const reservedId = S.nextId;
    const now = Date.now();
    const lastRound = S.hist[S.hist.length - 1];

    const sinal = {
      id: reservedId, cor: melhor.cor, key: melhor.key, nome: melhor.nome, emoji: melhor.emoji,
      conf: melhor.conf, tipo: melhor.tipo, status: 'PENDENTE',
      t: now, criadoEm: now, agree: melhor.agree,
      gale1MsgId: null, gale2MsgId: null, galeLevel: 0,
      lastRoundId: lastRound?.id || null
    };

    // 🔥 ÚNICA operação awaited: envio do sinal
    const envio = await tgSendRaw(fmtSinal(sinal)).catch(() => null);
    if (!envio?.ok) {
      ANALYSIS.msgId = analisandoId;
      R.ultimoMotivo = 'telegram falhou';
      return;
    }

    // Sucesso: restantes operações em paralelo/async
    if (analisandoId) tgDelete(analisandoId).catch(() => {});

    S.nextId++;
    S.signals.push(sinal);
    if (S.signals.length > CFG.maxSignalsMem) S.signals = S.signals.slice(-CFG.maxSignalsMem + 50);
    S.lastSignalAt = now;
    R.ultimoSinal = sinal;
    R.ultimoMotivo = null;
    R.ultimoTimingMs = Date.now() - t0;

    dbSignalInsert(sinal).catch(() => {});
    dbStateSet('nextId', String(S.nextId)).catch(() => {});
    dbStateSet('lastSignalAt', String(now)).catch(() => {});

    log('SINAL', {
      id: sinal.id, cor: sinal.cor, conf: sinal.conf, key: sinal.key,
      lastRoundId: sinal.lastRoundId,
      threshold: thresholdInfo.threshold, forced: !!melhor.isForced,
      timingMs: R.ultimoTimingMs,
      delayRodadaMs: lastRound ? now - lastRound.t : null,
      sinaisHora: sinaisUltimos60Min()
    });
  } finally {
    R.signalBusy = false;
  }
}

/* ============================ FALLBACK (só para 4:30) ============================ */

function fallbackLoop() {
  if (stopping) return;
  const desdeUltimo = S.lastSignalAt ? Date.now() - S.lastSignalAt : Infinity;
  if (desdeUltimo >= CFG.maxSemSinalMs && !R.signalBusy && pendentes().length < CFG.maxPending) {
    tentarSinal().catch(e => log('SIGNAL FALLBACK', e.message));
  }
  tickFallbackTimer = setTimeout(fallbackLoop, CFG.fallbackCheckMs);
}

/* ============================ KEEP-ALIVE ============================ */

async function keepAlive() {
  const self = process.env.SELF_URL || process.env.RENDER_EXTERNAL_URL || '';
  if (self) { try { await axios.get(self.replace(/\/$/, '') + '/health', { timeout: 7000 }); } catch (_) {} }
}

/* ============================ EXPRESS ============================ */

const app = express();
app.disable('x-powered-by');
app.use(cors());
app.use(express.json({ limit: '200kb' }));

app.get('/health', (req, res) => {
  const pace = calcularPace();
  const desdeUltimo = S.lastSignalAt ? Date.now() - S.lastSignalAt : null;
  const desdeRodada = S.hist.length ? Date.now() - S.hist[S.hist.length - 1].t : null;
  res.json({
    ok: true, versao: '17.5', deployId: DEPLOY_ID, maxGale: CFG.maxGale,
    hist: S.hist.length, sinais: S.signals.length, pendentes: pendentes().length,
    sinaisUltimaHora: sinaisUltimos60Min(),
    pace: pace ? { sinaisNestaHora: pace.sinaisNestaHora, minutosPassados: Math.round(pace.minutosPassados), deficit: Math.round(pace.deficit * 10) / 10 } : null,
    desdeUltimoSinalSeg: desdeUltimo ? Math.round(desdeUltimo / 1000) : null,
    desdeUltimaRodadaSeg: desdeRodada ? Math.round(desdeRodada / 1000) : null,
    ultimoThreshold: R.ultimoThreshold,
    ultimoTimingMs: R.ultimoTimingMs,
    ultimoDetectMs: R.ultimoDetectMs,
    ultimoDelayRodadaMs: R.ultimoDelayRodadaMs,
    analisandoVisivel: !!ANALYSIS.msgId,
    apiBugs: R.apiBugs,
    loops: { syncBusy: R.syncBusy, apurarBusy: R.apurarBusy, signalBusy: R.signalBusy },
    stats: { ...STATS, greenStreak: getGreenStreak() },
    ultimaSync: R.ultimaSync, erroAPI: R.erroAPI, erroDB: R.erroDB,
    uptime: Math.round(process.uptime())
  });
});
app.get('/status', (req, res) => {
  if (ADMIN_TOKEN && req.query.token !== ADMIN_TOKEN) return res.status(401).json({ erro: 'token' });
  res.json({ motivo: R.ultimoMotivo, R, STATS, CFG, pace: calcularPace() });
});
app.get('/sinais', (req, res) => {
  if (ADMIN_TOKEN && req.query.token !== ADMIN_TOKEN) return res.status(401).json({ erro: 'token' });
  const n = Math.min(100, Math.max(1, parseInt(req.query.n, 10) || 20));
  res.json(S.signals.slice(-n).reverse());
});
app.get('/stats', (req, res) => {
  if (ADMIN_TOKEN && req.query.token !== ADMIN_TOKEN) return res.status(401).json({ erro: 'token' });
  const wins = STATS.green + STATS.g1 + STATS.g2 + STATS.empate + STATS.eg1 + STATS.eg2;
  const winsReais = STATS.green + STATS.g1 + STATS.g2;
  const decididos = winsReais + STATS.loss;
  res.json({
    ...STATS, wins,
    taxaComEmpate: STATS.total > 0 ? wins / STATS.total : null,
    taxaReal: decididos > 0 ? winsReais / decididos : null,
    greenStreak: getGreenStreak()
  });
});
app.get('/calibracao', (req, res) => {
  if (ADMIN_TOKEN && req.query.token !== ADMIN_TOKEN) return res.status(401).json({ erro: 'token' });
  const buckets = { '78-82': { n: 0, w: 0 }, '83-86': { n: 0, w: 0 }, '87-90': { n: 0, w: 0 }, '91-93': { n: 0, w: 0 }, '94-96': { n: 0, w: 0 } };
  for (const s of S.signals) {
    if (!FECHADOS.has(s.status)) continue;
    let b;
    if (s.conf <= 82) b = '78-82';
    else if (s.conf <= 86) b = '83-86';
    else if (s.conf <= 90) b = '87-90';
    else if (s.conf <= 93) b = '91-93';
    else b = '94-96';
    buckets[b].n++;
    if (s.status !== 'LOSS') buckets[b].w++;
  }
  res.json(buckets);
});

/* ============================ START ============================ */

async function start() {
  log('BOOT', {
    versao: '17.5', deployId: DEPLOY_ID, maxGale: CFG.maxGale,
    syncPendingMs: CFG.syncIntervalPendingMs,
    syncIdleMs: CFG.syncIntervalIdleMs,
    atrasoRodadaMs: CFG.atrasoRodadaMs
  });

  await dbInit();
  if (pool) await dbLoadState();
  await syncLoopFirst();
  getStats(true);

  if (pool) {
    for (const s of S.signals) {
      if (s.status !== 'PENDENTE') continue;
      const after = roundsAfterSignal(s);
      let consecutivasFalhas = 0;
      for (const r of after) {
        if (r.resultado === s.cor || r.resultado === E) break;
        consecutivasFalhas++;
        if (consecutivasFalhas > CFG.maxGale) break;
      }
      s.galeLevel = Math.min(consecutivasFalhas, CFG.maxGale);
    }
  }

  server = app.listen(PORT, () => log('HTTP', `porta ${PORT}`));

  // Sync loop (principal)
  scheduleSync();
  // Fallback (4:30 sem sinal)
  fallbackLoop();
  // Gestor "analisando"
  analisandoTimer = setInterval(() => { manageAnalisando(); }, 1500);
  // Keep-alive
  keepTimer = setInterval(() => keepAlive().catch(() => {}), CFG.keepAliveMs);

  process.on('SIGTERM', shutdown);
  process.on('SIGINT', shutdown);
}

async function syncLoopFirst() {
  try {
    const rounds = await fetchRounds();
    for (const r of rounds) {
      if (S.seen.has(r.id)) continue;
      S.seen.add(r.id);
      S.hist.push(r);
    }
    S.hist.sort((a, b) => a.t - b.t || (a.id < b.id ? -1 : 1));
    log('SYNC BOOT', { total: S.hist.length });
  } catch (e) {
    log('SYNC BOOT ERRO', e.message);
  }
}

async function shutdown() {
  if (stopping) return;
  stopping = true; log('SHUTDOWN', 'ok');
  clearTimeout(tickSyncTimer);
  clearTimeout(tickFallbackTimer);
  clearInterval(analisandoTimer);
  clearInterval(keepTimer);
  if (server) server.close();
  if (pool) await pool.end().catch(() => {});
  process.exit(0);
}

start().catch((e) => { console.error(e); process.exit(1); });
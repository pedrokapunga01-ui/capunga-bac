'use strict';

/**
 * CAPUNGA DADOS — BAC BO · v18 (Gale 2 · Anti-freeze · lastHistId)
 * Ficheiro: server.js
 * Env: BOT_TOKEN, CHAT_ID, ADMIN_TOKEN, DATABASE_URL, PORT
 *
 * Correções v18:
 *  - Apuração usa lastHistId (não timestamp) → elimina fuso de relógio API/servidor
 *  - Notificações Telegram (resultado/gale/analisando) fire-and-forget → sem freeze
 *  - galeLevel atualizado SEMPRE (mesmo se TG falhar) → nunca fica pendurado
 *  - Atraso reduzido (3s) e sync 3s → sinais chegam antes da rodada seguinte
 *  - SMS "Analisando" só depois de resultado; apaga quando o próximo sinal sai;
 *    envio/remoção não bloqueiam nem atrasam o sinal
 *  - Força LOSS quando todos os gales esgotam (sem ficar à espera do timeout)
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
  maxGale: 2,
  maxPending: 1,
  scoreMin: 80,
  minAmostra: 60,
  sinalInicial: 1,
  syncMs: 3000,                // poll mais rápido
  minGapMs: 30000,
  atrasoRodadaMs: 3000,        // reduzido: sinal chega antes da rodada seguinte
  apuraMaxMs: 900000,          // fallback LOSS se API parar (10–15 min)
  maxHist: 4000,
  gapMs: 7 * 60 * 1000,
  maxSignalsMem: 500,
  signalsOnBoot: 500,
  suspendMs: 4 * 3600 * 1000,
  suspendLossStreak: 3,
  suspendLookback: 5,
  keepAliveMs: 60000,
  analisandoTtlMs: 10 * 60 * 1000
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
  nextId: CFG.sinalInicial, lastSignalAt: 0,
  stats: null, statsAtLen: -1
};

const STATS = { total: 0, green: 0, g1: 0, g2: 0, empate: 0, eg1: 0, eg2: 0, loss: 0 };
const SUSPENDED = {};

const R = {
  iniciadoEm: new Date().toISOString(), deployId: DEPLOY_ID,
  ultimaSync: null, ultimaRodadaNova: null,
  erroAPI: null, erroTelegram: null, erroDB: null,
  ultimoMotivo: null, ultimoSinal: null, wakes: 0, ultimoWake: null,
  analisandoMsgId: null, analisandoEm: 0
};

let pool = null, server = null, tickTimer = null, keepTimer = null;
let busy = false, stopping = false, lastTickMs = Date.now();

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
        gale INT, agree INT, prev_hist_id TEXT, created_at TIMESTAMPTZ NOT NULL, closed_at TIMESTAMPTZ
      );
      CREATE INDEX IF NOT EXISTS idx_cap_sig_status ON capunga_signals(status);
      CREATE INDEX IF NOT EXISTS idx_cap_sig_created ON capunga_signals(created_at DESC);
    `);
    await pool.query(`ALTER TABLE capunga_signals ADD COLUMN IF NOT EXISTS prev_hist_id TEXT`).catch(() => {});
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
    await pool.query(`INSERT INTO capunga_signals (id,cor,key,nome,tipo,conf,status,gale,agree,prev_hist_id,created_at,closed_at)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) ON CONFLICT (id) DO NOTHING`,
      [s.id, s.cor, s.key, s.nome, s.tipo, s.conf, s.status, s.gale ?? null, s.agree ?? 1,
       s.prevHistId || null, new Date(s.t), s.fechadoEm ? new Date(s.fechadoEm) : null]);
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
      prevHistId: row.prev_hist_id || null,
      t: new Date(row.created_at).getTime(), criadoEm: new Date(row.created_at).getTime(),
      fechadoEm: row.closed_at ? new Date(row.closed_at).getTime() : null,
      gale1MsgId: null, gale2MsgId: null, galeLevel: row.gale ?? 0
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
  for (let t = 1; t <= 3; t++) {
    try {
      const r = await axios.post(`https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`,
        { chat_id: CHAT_ID, text: texto, parse_mode: 'HTML', disable_web_page_preview: true },
        { timeout: 8000 });
      if (r.data?.ok) { R.erroTelegram = null; return { ok: true, messageId: r.data.result.message_id }; }
      R.erroTelegram = JSON.stringify(r.data);
    } catch (e) {
      R.erroTelegram = e.response?.data ? JSON.stringify(e.response.data) : e.message;
      const retry = Number(e.response?.data?.parameters?.retry_after);
      if (retry > 0 && retry <= 15) await sleep(retry * 1000 + 200);
    }
    if (t < 3) await sleep(800 * t);
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

// Fire-and-forget helpers (nunca bloqueiam o loop principal)
function fireSend(texto, tag = 'SEND') {
  tgSendRaw(texto).catch((e) => log(`TG ${tag}`, e.message));
}
function fireDelete(msgId) {
  if (!msgId) return;
  tgDelete(msgId).catch(() => {});
}

/* ============================ TEMPLATES ============================ */

function fmtAnalisando() {
  return ['🔍 <b>Analisando o mercado tropa…</b>', '', `⏱ ${hora()}`].join('\n');
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
  for (let i = 1; i <= 3; i++) {
    try {
      const resp = await axios.get(API_BACBO, {
        timeout: 12000, headers: { Accept: 'application/json', 'User-Agent': 'CapungaDados/18.0' }
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
      if (i < 3) await sleep(400 * i);
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
  if (now - last.t < CFG.atrasoRodadaMs) {
    R.ultimoMotivo = `atraso ${Math.ceil((CFG.atrasoRodadaMs - (now - last.t)) / 1000)}s`;
    return false;
  }
  if (S.hist.length < CFG.minAmostra) { R.ultimoMotivo = `amostra ${S.hist.length}`; return false; }
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
    if (!melhor || top.conf > melhor.conf) {
      melhor = { ...top, agree: scored.length };
    }
  }

  if (!melhor || melhor.conf < CFG.scoreMin) { R.ultimoMotivo = `score < ${CFG.scoreMin}`; return; }

  const lastHist = S.hist[S.hist.length - 1];
  const reservedId = S.nextId;
  const now = Date.now();
  const sinalPreliminar = {
    id: reservedId, cor: melhor.cor, key: melhor.key, nome: melhor.nome, emoji: melhor.emoji,
    conf: melhor.conf, tipo: melhor.tipo, status: 'PENDENTE',
    t: now, criadoEm: now, agree: melhor.agree,
    prevHistId: lastHist ? lastHist.id : null,
    gale1MsgId: null, gale2MsgId: null, galeLevel: 0
  };

  // 1º: enviar o SINAL (prioritário — não bloqueia no "Analisando")
  const envio = await tgSendRaw(fmtSinal(sinalPreliminar));
  if (!envio.ok) {
    log('SINAL', { motivo: 'telegram rejeitou sinal', id: reservedId });
    R.ultimoMotivo = 'telegram falhou';
    return;
  }

  // 2º: apagar SMS "Analisando" anterior (fire-and-forget — não atrasa nada)
  if (R.analisandoMsgId) {
    const idAntigo = R.analisandoMsgId;
    R.analisandoMsgId = null;
    fireDelete(idAntigo);
  }

  S.nextId++;
  S.signals.push(sinalPreliminar);
  if (S.signals.length > CFG.maxSignalsMem) S.signals = S.signals.slice(-CFG.maxSignalsMem + 50);
  S.lastSignalAt = now;
  R.ultimoSinal = sinalPreliminar;
  R.ultimoMotivo = null;

  await dbSignalInsert(sinalPreliminar);
  await dbStateSet('nextId', String(S.nextId));
  await dbStateSet('lastSignalAt', String(now));

  log('SINAL', { id: sinalPreliminar.id, cor: sinalPreliminar.cor, conf: sinalPreliminar.conf, key: sinalPreliminar.key, prevHistId: sinalPreliminar.prevHistId });
}

/* ============================ GALE ============================ */

function dispararGale(s, nivel) {
  if (s.galeLevel >= nivel) return;
  s.galeLevel = nivel; // atualiza IMEDIATAMENTE — a lógica nunca fica pendurada

  // Fire-and-forget: envio da SMS de gale (não bloqueia apuração)
  (async () => {
    try {
      if (nivel === 2 && s.gale1MsgId) {
        const id = s.gale1MsgId;
        s.gale1MsgId = null;
        await tgDelete(id);
      }
      const r = await tgSendRaw(fmtGale(s, nivel));
      if (r.ok) {
        if (nivel === 1) s.gale1MsgId = r.messageId;
        else s.gale2MsgId = r.messageId;
      }
    } catch (e) {
      log('GALE TG ERR', e.message);
    }
  })();
}

/* ============================ FECHAR ============================ */

async function fecharSinal(s, status, gale) {
  s.status = status;
  s.gale = gale;
  s.fechadoEm = Date.now();

  // Atualiza contadores já (síncrono)
  if (status === 'GREEN') STATS.green++;
  else if (status === 'G1') STATS.g1++;
  else if (status === 'G2') STATS.g2++;
  else if (status === 'EMPATE') STATS.empate++;
  else if (status === 'EG1') STATS.eg1++;
  else if (status === 'EG2') STATS.eg2++;
  else if (status === 'LOSS') STATS.loss++;
  STATS.total++;

  // Apaga msgs de gale pendentes (fire-and-forget)
  if (s.gale1MsgId) { const id = s.gale1MsgId; s.gale1MsgId = null; fireDelete(id); }
  if (s.gale2MsgId) { const id = s.gale2MsgId; s.gale2MsgId = null; fireDelete(id); }

  // Persiste imediatamente (rápido)
  await dbSignalUpdate(s);
  maybeSuspend(s.key);

  // Notificações em background — NUNCA bloqueiam o loop
  (async () => {
    try {
      // 1. Envia resultado
      const res = await tgSendRaw(fmtResult(s));
      if (res.ok) {
        // 2. Envia histórico
        await tgSendRaw(fmtHistorico());
        // 3. Apaga "Analisando" anterior (se ainda existir) e envia novo
        if (R.analisandoMsgId) {
          const idAntigo = R.analisandoMsgId;
          R.analisandoMsgId = null;
          await tgDelete(idAntigo).catch(() => {});
        }
        // 4. Envia SMS "Analisando" (depois de green/loss)
        const r = await tgSendRaw(fmtAnalisando());
        if (r.ok) {
          R.analisandoMsgId = r.messageId;
          R.analisandoEm = Date.now();
        }
      }
    } catch (e) {
      log('FECHAR TG ERR', e.message);
    }
  })();

  log('FECHADO', { id: s.id, status, gale, prevHistId: s.prevHistId });
}

/* ============================ APURAÇÃO ============================ */

// Retorna as rodadas posteriores ao prevHistId do sinal.
// Usa POSIÇÃO no histórico (não timestamp) → imune a fuso API/servidor.
function roundsAfterSignal(s) {
  if (!S.hist.length) return [];
  if (s.prevHistId) {
    const idx = S.hist.findIndex((r) => r.id === s.prevHistId);
    if (idx >= 0) return S.hist.slice(idx + 1);
  }
  // fallback (raro): por timestamp
  return S.hist.filter((r) => r.t > s.t);
}

async function apurar() {
  const now = Date.now();

  for (const s of S.signals) {
    if (FECHADOS.has(s.status)) continue;

    const after = roundsAfterSignal(s);

    if (after.length === 0) {
      // Sem rodadas posteriores — só fecha LOSS por timeout extremo
      if (now - s.t >= CFG.apuraMaxMs) {
        await fecharSinal(s, 'LOSS', s.galeLevel || 0);
      }
      continue;
    }

    let decidido = false;

    for (let k = 0; k <= CFG.maxGale; k++) {
      if (k >= after.length) break;
      const r = after[k];
      const prevT = k === 0 ? (S.hist.find(x => x.id === s.prevHistId)?.t ?? s.t) : after[k - 1].t;
      // gap de segurança: sessão interrompida
      if (r.t - prevT > CFG.gapMs) break;

      if (r.resultado === s.cor) {
        const st = k === 0 ? 'GREEN' : k === 1 ? 'G1' : 'G2';
        await fecharSinal(s, st, k);
        decidido = true;
        break;
      }
      if (r.resultado === E) {
        const st = k === 0 ? 'EMPATE' : k === 1 ? 'EG1' : 'EG2';
        await fecharSinal(s, st, k);
        decidido = true;
        break;
      }

      // Não bateu → dispara gale (k+1), sem bloquear o loop
      if (k < CFG.maxGale) {
        dispararGale(s, k + 1);
      }
    }

    if (decidido) continue;

    // Se já processámos maxGale+1 rodadas e nenhuma bateu → LOSS forçado
    if (after.length > CFG.maxGale && s.galeLevel >= CFG.maxGale) {
      await fecharSinal(s, 'LOSS', CFG.maxGale);
      continue;
    }

    // Fallback de segurança: passou tempo extremo
    if (now - s.t >= CFG.apuraMaxMs) {
      await fecharSinal(s, 'LOSS', s.galeLevel || 0);
    }
  }

  // Limpa "Analisando" órfão (caso não haja sinal seguinte)
  if (R.analisandoMsgId && now - R.analisandoEm > CFG.analisandoTtlMs) {
    const id = R.analisandoMsgId;
    R.analisandoMsgId = null;
    fireDelete(id);
  }
}

/* ============================ SYNC ============================ */

async function sync() {
  try {
    const rounds = await fetchRounds();
    let novos = 0;
    for (const r of rounds) {
      if (S.seen.has(r.id)) continue;
      S.seen.add(r.id); S.hist.push(r); novos++;
      R.ultimaRodadaNova = new Date(r.t).toISOString();
    }
    if (S.hist.length > CFG.maxHist) {
      const ex = S.hist.length - CFG.maxHist;
      for (let i = 0; i < ex; i++) S.seen.delete(S.hist[i].id);
      S.hist = S.hist.slice(ex);
    }
    S.hist.sort((a, b) => a.t - b.t || (a.id < b.id ? -1 : 1));
    R.ultimaSync = new Date().toISOString();
    if (novos) log('SYNC', { novos, total: S.hist.length });
  } catch (e) { R.erroAPI = e.message; log('SYNC ERRO', e.message); }
}

/* ============================ LOOP ============================ */

async function tick() {
  if (busy || stopping) return;
  busy = true; lastTickMs = Date.now();
  try {
    await sync();
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
  if (Date.now() - lastTickMs > CFG.syncMs * 5) {
    R.wakes++; R.ultimoWake = new Date().toISOString();
    log('WAKE', 'atraso');
    tick().catch(() => {});
  }
}

/* ============================ EXPRESS ============================ */

const app = express();
app.disable('x-powered-by');
app.use(cors());
app.use(express.json({ limit: '200kb' }));

app.get('/health', (req, res) => {
  res.json({
    ok: true, versao: '18.0', deployId: DEPLOY_ID, maxGale: CFG.maxGale,
    hist: S.hist.length, sinais: S.signals.length, pendentes: pendentes().length,
    stats: { ...STATS, greenStreak: getGreenStreak() },
    ultimaSync: R.ultimaSync, erroAPI: R.erroAPI, erroDB: R.erroDB,
    uptime: Math.round(process.uptime())
  });
});
app.get('/status', (req, res) => {
  if (ADMIN_TOKEN && req.query.token !== ADMIN_TOKEN) return res.status(401).json({ erro: 'token' });
  res.json({ motivo: R.ultimoMotivo, R, STATS, CFG });
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
    ...STATS,
    wins,
    taxaComEmpate: STATS.total > 0 ? wins / STATS.total : null,
    taxaReal: decididos > 0 ? winsReais / decididos : null,
    greenStreak: getGreenStreak()
  });
});
app.get('/calibracao', (req, res) => {
  if (ADMIN_TOKEN && req.query.token !== ADMIN_TOKEN) return res.status(401).json({ erro: 'token' });
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

  // Recalcula galeLevel de sinais pendentes após restart
  if (pool) {
    for (const s of S.signals) {
      if (s.status !== 'PENDENTE') continue;
      const after = roundsAfterSignal(s);
      let falhas = 0;
      for (const r of after) {
        if (r.resultado === s.cor || r.resultado === E) break;
        falhas++;
        if (falhas > CFG.maxGale) break;
      }
      s.galeLevel = Math.min(falhas, CFG.maxGale);
    }
  }

  server = app.listen(PORT, () => log('HTTP', `porta ${PORT}`));
  tickTimer = setInterval(() => tick().catch(() => {}), CFG.syncMs);
  keepTimer = setInterval(() => { keepAlive().catch(() => {}); wakeCheck(); }, CFG.keepAliveMs);
  setTimeout(() => tick().catch(() => {}), 1500);

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
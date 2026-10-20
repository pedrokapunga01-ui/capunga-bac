'use strict';

/**
 * CAPUNGA DADOS — BAC BO · GALE 2 · SLOTS 3MIN · FINAL 100%
 *
 * - Sinais em slots fixos de 3 em 3 minutos (minuto 00, 03, 06, 09… de cada hora)
 * - Gale máximo: 2
 * - Análise multi-camada + consenso + bónus de mesa
 * - Empate = GREEN e fecha
 * - Mensagens curtas + placar minimalista
 * - Apaga mensagem de Gale quando sai resultado
 * - Timing 100% baseado em relógio absoluto (Africa/Luanda)
 *   → Resolve o problema do Render free sleep/wake
 * - Keep-alive agressivo + endpoints leves para pinger externo
 *
 * Env obrigatórios:
 *   DATABASE_URL, BOT_TOKEN, CHAT_ID, ADMIN_TOKEN
 *
 * Env opcionais:
 *   API_BACBO
 *   INTERVALO_SYNC_MS=6000
 *   KEEP_ALIVE_MS=90000
 *   KEEP_ALIVE_URL / RENDER_EXTERNAL_URL
 *   MIN_PRIORIDADE=72
 *   CONSENSO_MIN=1
 *   SINAL_EXPIRA_MIN=6
 *   SLOT_JANELA_MS=110000   (janela de abertura do slot)
 *   SINAIS_ATIVOS=true
 *   EMPATE_PROTEGIDO=true
 *
 * Aviso: Bac Bo é jogo de azar. Nenhum código garante 100% de acerto.
 * Gestão de banca e disciplina são obrigatórias.
 */

const express = require('express');
const cors = require('cors');
const axios = require('axios');
const { Pool } = require('pg');

const app = express();
app.disable('x-powered-by');
app.use(cors());
app.use(express.json({ limit: '1mb' }));

const PORT = Number(process.env.PORT || 10000);
const DATABASE_URL = process.env.DATABASE_URL || '';
const BOT_TOKEN = process.env.BOT_TOKEN || '';
const CHAT_ID = process.env.CHAT_ID || '';
const ADMIN_TOKEN = process.env.ADMIN_TOKEN || '';
const API_BACBO =
  process.env.API_BACBO ||
  'https://api-cs.casino.org/svc-evolution-game-events/api/bacbo';

function boolEnv(v, fb = false) {
  if (v === undefined || v === '') return fb;
  return ['true', '1', 'yes', 'sim', 'on'].includes(String(v).trim().toLowerCase());
}
function intEnv(name, fb, min, max) {
  const n = Number.parseInt(process.env[name], 10);
  if (!Number.isFinite(n)) return fb;
  return Math.max(min, Math.min(max, n));
}

const CONFIG = {
  sinaisAtivos: boolEnv(process.env.SINAIS_ATIVOS, true),
  maxGale: 2,
  empateProtegido: boolEnv(process.env.EMPATE_PROTEGIDO, true),
  sinalExpiraMin: intEnv('SINAL_EXPIRA_MIN', 6, 3, 20),
  intervaloSyncMs: intEnv('INTERVALO_SYNC_MS', 6000, 4000, 30000),
  keepAliveMs: intEnv('KEEP_ALIVE_MS', 90000, 30000, 300000),
  consensoMin: intEnv('CONSENSO_MIN', 1, 1, 5),
  minPrioridade: intEnv('MIN_PRIORIDADE', 72, 0, 100),
  // Janela em que o slot ainda aceita sinal (ex: 110s após o minuto do slot)
  slotJanelaMs: intEnv('SLOT_JANELA_MS', 110000, 45000, 170000),
  keepAliveUrl: process.env.KEEP_ALIVE_URL || process.env.RENDER_EXTERNAL_URL || ''
};

/* ===================== TEMPO ABSOLUTO (Luanda) ===================== */

const TZ = 'Africa/Luanda';

function agoraLuanda() {
  return new Date(
    new Date().toLocaleString('en-US', { timeZone: TZ })
  );
}

function horaAngola() {
  return new Intl.DateTimeFormat('pt-PT', {
    timeZone: TZ,
    dateStyle: 'short',
    timeStyle: 'medium',
    hour12: false
  }).format(new Date());
}

/** Devolve a chave do slot atual ou null se fora de janela.
 *  Slots: minuto 0,3,6,9…57 de cada hora.
 *  Janela: desde o início do minuto do slot até slotJanelaMs.
 */
function obterSlotAtual() {
  const now = agoraLuanda();
  const min = now.getMinutes();
  const slotMin = Math.floor(min / 3) * 3;
  const slotStart = new Date(now);
  slotStart.setMinutes(slotMin, 0, 0);
  const elapsed = now.getTime() - slotStart.getTime();

  if (elapsed < 0 || elapsed > CONFIG.slotJanelaMs) return null;

  const y = slotStart.getFullYear();
  const m = String(slotStart.getMonth() + 1).padStart(2, '0');
  const d = String(slotStart.getDate()).padStart(2, '0');
  const h = String(slotStart.getHours()).padStart(2, '0');
  const mi = String(slotMin).padStart(2, '0');
  return `${y}-${m}-${d} ${h}:${mi}`;
}

function slotKeyFromDate(iso) {
  if (!iso) return null;
  const d = new Date(
    new Date(iso).toLocaleString('en-US', { timeZone: TZ })
  );
  const min = d.getMinutes();
  const slotMin = Math.floor(min / 3) * 3;
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, '0');
  const day = String(d.getDate()).padStart(2, '0');
  const h = String(d.getHours()).padStart(2, '0');
  const mi = String(slotMin).padStart(2, '0');
  return `${y}-${m}-${day} ${h}:${mi}`;
}

/* ===================== ANÁLISE AVANÇADA ===================== */

const OPOSTO = { Banca: 'Jogador', Jogador: 'Banca' };

function decisivos(hist) {
  return hist.filter((r) => r === 'Banca' || r === 'Jogador');
}

function sequencia(dec) {
  if (!dec.length) return { cor: null, tamanho: 0 };
  let n = 1;
  while (n < dec.length && dec[n] === dec[0]) n++;
  return { cor: dec[0], tamanho: n };
}

function contar(dec, n) {
  const s = dec.slice(0, n);
  const banca = s.filter((x) => x === 'Banca').length;
  return {
    n: s.length,
    banca,
    jogador: s.length - banca,
    pctBanca: s.length ? banca / s.length : 0.5
  };
}

function zProp(k, n, p0 = 0.5) {
  return n ? (k / n - p0) / Math.sqrt((p0 * (1 - p0)) / n) : 0;
}

function taxaAlt(dec, n) {
  const s = dec.slice(0, n);
  if (s.length < 2) return 0.5;
  let t = 0;
  for (let i = 1; i < s.length; i++) if (s[i] !== s[i - 1]) t++;
  return t / (s.length - 1);
}

function analisarMesa(hist) {
  const dec = decisivos(hist);
  const seq = sequencia(dec);
  const c8 = contar(dec, 8);
  const c12 = contar(dec, 12);
  const c20 = contar(dec, 20);
  const c50 = contar(dec, 50);
  const c100 = contar(dec, 100);
  const alt12 = taxaAlt(dec, 12);
  const alt20 = taxaAlt(dec, 20);

  let viesMedio = null;
  if (c20.n >= 16) {
    if (c20.pctBanca >= 0.6) viesMedio = 'Banca';
    else if (c20.pctBanca <= 0.4) viesMedio = 'Jogador';
  }

  let viesLongo = null;
  if (c50.n >= 40) {
    const z = zProp(c50.banca, c50.n);
    if (Math.abs(z) >= 1.45) viesLongo = z > 0 ? 'Banca' : 'Jogador';
  }
  if (!viesLongo && c100.n >= 70) {
    const z = zProp(c100.banca, c100.n);
    if (Math.abs(z) >= 1.35) viesLongo = z > 0 ? 'Banca' : 'Jogador';
  }

  const modo =
    alt20 >= 0.58 ? 'alternante' : alt20 <= 0.36 ? 'streak' : 'misto';

  return {
    dec,
    seq,
    c8,
    c12,
    c20,
    c50,
    c100,
    alt12,
    alt20,
    viesMedio,
    viesLongo,
    modo,
    amostra: dec.length
  };
}

function bonusMesa(cor, mesa) {
  let b = 0;
  if (mesa.viesMedio === cor) b += 11;
  if (mesa.viesLongo === cor) b += 13;
  if (mesa.viesMedio && mesa.viesMedio !== cor) b -= 9;
  if (mesa.viesLongo && mesa.viesLongo !== cor) b -= 6;
  if (mesa.modo === 'streak' && mesa.seq.cor === cor && mesa.seq.tamanho >= 3) b += 9;
  if (mesa.modo === 'alternante' && mesa.seq.cor && OPOSTO[mesa.seq.cor] === cor) b += 9;
  if (mesa.c12.n >= 10) {
    const dom = mesa.c12.banca > mesa.c12.jogador ? 'Banca' : 'Jogador';
    if (dom === cor && Math.abs(mesa.c12.banca - mesa.c12.jogador) >= 4) b += 8;
  }
  if (mesa.alt12 <= 0.28 && mesa.seq.cor === cor) b += 6;
  return b;
}

/** Lógicas selectivas — pensadas para GREEN / G1 / G2 */
const LOGICAS = [
  {
    id: 'SURF4',
    p: 93,
    avaliar(dec) {
      const s = sequencia(dec);
      if (s.tamanho === 4 && dec.length > 4 && dec[4] !== s.cor) return { cor: s.cor };
      return null;
    }
  },
  {
    id: 'PAD22',
    p: 95,
    avaliar(dec) {
      if (dec.length < 4) return null;
      const [a, b, c, d] = dec;
      if (a === b && c === d && a !== c) return { cor: a };
      return null;
    }
  },
  {
    id: 'DUPLO22',
    p: 97,
    avaliar(dec) {
      if (dec.length < 6) return null;
      const [a, b, c, d, e, f] = dec;
      if (a === b && e === f && a === e && c === d && a !== c) return { cor: a };
      return null;
    }
  },
  {
    id: 'ALT5',
    p: 91,
    avaliar(dec) {
      const s = dec.slice(0, 5);
      if (s.length < 5) return null;
      for (let i = 1; i < 5; i++) if (s[i] === s[i - 1]) return null;
      return { cor: OPOSTO[s[0]] };
    }
  },
  {
    id: 'ALT6',
    p: 94,
    avaliar(dec) {
      const s = dec.slice(0, 6);
      if (s.length < 6) return null;
      for (let i = 1; i < 6; i++) if (s[i] === s[i - 1]) return null;
      return { cor: OPOSTO[s[0]] };
    }
  },
  {
    id: 'STREAK4',
    p: 92,
    avaliar(dec) {
      const s = sequencia(dec);
      if (s.tamanho === 4 || s.tamanho === 5) return { cor: s.cor };
      return null;
    }
  },
  {
    id: 'STREAK6',
    p: 88,
    avaliar(dec) {
      const s = sequencia(dec);
      if (s.tamanho >= 6 && s.tamanho <= 8) return { cor: s.cor };
      return null;
    }
  },
  {
    id: 'QUEBRA8',
    p: 74,
    avaliar(dec) {
      const s = sequencia(dec);
      if (s.tamanho >= 8) return { cor: OPOSTO[s.cor] };
      return null;
    }
  },
  {
    id: 'MAIORIA12',
    p: 84,
    avaliar(dec) {
      const c = contar(dec, 12);
      if (c.n < 12 || Math.abs(c.banca - c.jogador) < 5) return null;
      const cor = c.banca > c.jogador ? 'Banca' : 'Jogador';
      const s = sequencia(dec);
      if (s.cor && s.cor !== cor && s.tamanho >= 2) return null;
      return { cor };
    }
  },
  {
    id: 'PRESSAO5',
    p: 86,
    avaliar(dec) {
      if (dec.length < 5) return null;
      const s = dec.slice(0, 5);
      const b = s.filter((x) => x === 'Banca').length;
      if (b >= 4) return { cor: 'Banca' };
      if (b <= 1) return { cor: 'Jogador' };
      return null;
    }
  },
  {
    id: 'FLUXO50',
    p: 82,
    avaliar(dec, mesa) {
      if (!mesa.viesLongo || mesa.c50.n < 45) return null;
      const s = sequencia(dec);
      if (s.cor && s.cor !== mesa.viesLongo && s.tamanho >= 3) return null;
      return { cor: mesa.viesLongo };
    }
  },
  {
    id: 'MOMENTUM8',
    p: 80,
    avaliar(dec) {
      const c = contar(dec, 8);
      if (c.n < 8) return null;
      if (c.banca >= 6) return { cor: 'Banca' };
      if (c.jogador >= 6) return { cor: 'Jogador' };
      return null;
    }
  },
  // Fallback de slot (sempre disponível quando forçamos)
  {
    id: 'SLOT_STREAK',
    p: 70,
    slotOnly: true,
    avaliar(dec) {
      const s = sequencia(dec);
      if (s.cor && s.tamanho >= 2) return { cor: s.cor };
      return null;
    }
  },
  {
    id: 'SLOT_MAIORIA',
    p: 68,
    slotOnly: true,
    avaliar(dec) {
      const c = contar(dec, 6);
      if (c.n < 4) return null;
      return { cor: c.banca >= c.jogador ? 'Banca' : 'Jogador' };
    }
  }
];

function decidir(hist, opcoes = {}) {
  const minP = Number(opcoes.minPrioridade) || CONFIG.minPrioridade;
  const consensoMin = Math.max(1, Number(opcoes.consensoMin) || CONFIG.consensoMin);
  const forcarSlot = Boolean(opcoes.forcarSlot);

  const mesa = analisarMesa(hist);
  const dec = mesa.dec;
  if (dec.length < 3) return null;

  const limiar = forcarSlot ? 55 : minP;
  const cons = forcarSlot ? 1 : consensoMin;

  const candidatos = [];
  for (const L of LOGICAS) {
    if (L.slotOnly && !forcarSlot) continue;
    const r = L.avaliar(dec, mesa);
    if (!r || !r.cor) continue;
    const bonus = bonusMesa(r.cor, mesa);
    const score = L.p + bonus;
    if (score < limiar) continue;
    candidatos.push({ id: L.id, cor: r.cor, score, bonus });
  }
  if (!candidatos.length) return null;

  const peso = { Banca: 0, Jogador: 0 };
  const votos = { Banca: 0, Jogador: 0 };
  for (const c of candidatos) {
    peso[c.cor] += c.score;
    votos[c.cor]++;
  }

  let cor = null;
  if (peso.Banca > peso.Jogador) cor = 'Banca';
  else if (peso.Jogador > peso.Banca) cor = 'Jogador';
  else return null;

  if (votos[cor] < cons) return null;

  const apoio = candidatos
    .filter((c) => c.cor === cor)
    .sort((a, b) => b.score - a.score);
  const top = apoio[0];

  if (!forcarSlot && top.bonus < 3 && top.score < minP + 5) return null;

  return {
    cor,
    confianca: Math.min(99, Math.round(top.score)),
    forcarSlot,
    votos: votos[cor],
    _ids: apoio.map((c) => c.id).join('+'),
    mesa: {
      modo: mesa.modo,
      viesMedio: mesa.viesMedio,
      viesLongo: mesa.viesLongo,
      amostra: mesa.amostra
    }
  };
}

/** Último recurso dentro do slot: sempre devolve uma cor. */
function forcarEntradaSlot(hist) {
  const dec = decisivos(hist);
  if (dec.length < 1) return null;

  const s = sequencia(dec);
  if (s.cor && s.tamanho >= 2) {
    return {
      cor: s.cor,
      confianca: 58,
      forcarSlot: true,
      votos: 1,
      _ids: 'FORCA_STREAK',
      mesa: { modo: 'forcar', amostra: dec.length }
    };
  }
  const c = contar(dec, 5);
  if (c.n >= 3) {
    const cor = c.banca >= c.jogador ? 'Banca' : 'Jogador';
    return {
      cor,
      confianca: 55,
      forcarSlot: true,
      votos: 1,
      _ids: 'FORCA_MAIORIA',
      mesa: { modo: 'forcar', amostra: dec.length }
    };
  }
  return {
    cor: dec[0],
    confianca: 52,
    forcarSlot: true,
    votos: 1,
    _ids: 'FORCA_ULTIMA',
    mesa: { modo: 'forcar', amostra: dec.length }
  };
}

/* ===================== APP / RUNTIME ===================== */

const pool = DATABASE_URL
  ? new Pool({
      connectionString: DATABASE_URL,
      ssl: /localhost|127\.0\.0\.1/i.test(DATABASE_URL)
        ? false
        : { rejectUnauthorized: false },
      max: 5,
      idleTimeoutMillis: 30000,
      connectionTimeoutMillis: 15000
    })
  : null;

const runtime = {
  iniciadoEm: new Date().toISOString(),
  sincronizacaoEmCurso: false,
  ultimaSincronizacao: null,
  ultimaRodadaNova: null,
  ultimoIdNovo: null,
  ultimaHoraDaAPI: null,
  quantidadeAPI: 0,
  itensRecebidosAPI: 0,
  duplicadosUltimoCiclo: 0,
  novosUltimoCiclo: 0,
  erroSincronizacao: null,
  ultimoSinal: null,
  ultimoSlotUsado: null,
  ultimaMensagemTelegram: null,
  ultimoErroTelegram: null,
  ultimoHTTPAPI: null,
  ultimoErroAPI: null,
  ciclosSemNovos: 0,
  motivoSemSinal: null,
  ultimaAnalise: null,
  rodadasDesdeSinal: 0,
  publicUrl: CONFIG.keepAliveUrl || null,
  ultimoKeepAlive: null,
  ultimoSinalEm: null,
  // message_ids das mensagens de Gale para apagar
  ultimoGaleMsgIds: []
};

let schemaPromise = null;
let server = null;
let syncTimer = null;
let keepAliveTimer = null;

function log(tag, data) {
  console.log(`[${tag}]`, typeof data === 'string' ? data : JSON.stringify(data));
}

function normalizeOutcome(value) {
  if (value && typeof value === 'object') {
    value = value.outcome ?? value.resultado ?? value.winner ?? value.name;
  }
  const s = String(value ?? '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .trim()
    .toLowerCase()
    .replace(/[\s_-]+/g, '');
  if (['player', 'jogador', 'playerwin', 'playerwins', 'playerwon', 'playerwinner'].includes(s))
    return 'Jogador';
  if (['banker', 'banca', 'bankerwin', 'bankerwins', 'bankerwon', 'bankerwinner', 'red', 'vermelho'].includes(s))
    return 'Banca';
  if (['tie', 'draw', 'empate', 'equal', 'push', 'tied'].includes(s)) return 'Empate';
  return null;
}

function validDate(value) {
  if (!value) return null;
  const d = new Date(value);
  return Number.isNaN(d.getTime()) ? null : d.toISOString();
}

function extractItems(body) {
  if (Array.isArray(body)) return body;
  if (!body || typeof body !== 'object') return [];
  const candidates = [
    body.data, body.results, body.items, body.resultados, body.rounds, body.events,
    body.data?.results, body.data?.items, body.data?.resultados, body.data?.rounds,
    body.data?.events, body.data?.data
  ];
  return candidates.find(Array.isArray) || [];
}

function normalizeRound(item) {
  const d = item?.data || item?.round || item || {};
  const r =
    d.result && typeof d.result === 'object' ? d.result
      : item?.result && typeof item.result === 'object' ? item.result : {};
  const status = String(d.status ?? d.estado ?? item?.status ?? item?.estado ?? '').toLowerCase();
  const inicio = validDate(d.startedAt ?? d.startTime ?? d.started_at ?? d.inicio ?? item?.startedAt ?? item?.inicio);
  const fim = validDate(d.settledAt ?? d.endTime ?? d.endedAt ?? d.finishedAt ?? d.fim ?? item?.settledAt ?? item?.fim);
  const rawOutcome =
    r.outcome ?? r.resultado ?? r.winner ?? d.outcome ?? d.resultado ?? d.winner ??
    item?.outcome ?? item?.resultado ?? item?.winner ??
    (typeof d.result === 'string' ? d.result : null) ??
    (typeof item?.result === 'string' ? item.result : null);
  const resultado = normalizeOutcome(rawOutcome);
  const resolvida =
    ['resolved', 'settled', 'finished', 'complete', 'completed', 'resolvido', 'concluido', 'finalizado']
      .some((w) => status.includes(w)) || Boolean(fim);
  const id = String(
    d.id ?? d.roundId ?? d.round_id ?? d.eventId ??
    item?.id ?? item?.roundId ?? item?.round_id ?? item?.eventId ?? ''
  ).trim();
  if (!id || !resultado || !resolvida || !fim) return null;
  return {
    id,
    resultado,
    estado: 'Resolved',
    inicio: inicio || fim,
    fim,
    pontos_jogador: null,
    pontos_banca: null,
    multiplicador: null
  };
}

/* ===================== TELEGRAM ===================== */

async function telegramSend(texto) {
  if (!BOT_TOKEN || !CHAT_ID) {
    runtime.ultimoErroTelegram = 'BOT_TOKEN ou CHAT_ID em falta';
    return null;
  }
  try {
    const response = await axios.post(
      `https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`,
      { chat_id: CHAT_ID, text: texto, disable_web_page_preview: true },
      { timeout: 15000 }
    );
    if (response.data?.ok === true) {
      runtime.ultimaMensagemTelegram = new Date().toISOString();
      runtime.ultimoErroTelegram = null;
      return response.data.result?.message_id ?? true;
    }
    runtime.ultimoErroTelegram = JSON.stringify(response.data);
    return null;
  } catch (error) {
    runtime.ultimoErroTelegram = error.response?.data
      ? JSON.stringify(error.response.data)
      : error.message;
    console.error('[TELEGRAM]', runtime.ultimoErroTelegram);
    return null;
  }
}

async function telegramDelete(messageId) {
  if (!BOT_TOKEN || !CHAT_ID || !messageId || messageId === true) return false;
  try {
    await axios.post(
      `https://api.telegram.org/bot${BOT_TOKEN}/deleteMessage`,
      { chat_id: CHAT_ID, message_id: messageId },
      { timeout: 10000 }
    );
    return true;
  } catch (_) {
    return false;
  }
}

async function limparGaleMsgs() {
  const ids = [...runtime.ultimoGaleMsgIds];
  runtime.ultimoGaleMsgIds = [];
  for (const id of ids) {
    await telegramDelete(id);
  }
}

/* ===================== DATABASE ===================== */

async function ensureSchema() {
  if (!pool) throw new Error('DATABASE_URL não configurada');
  if (schemaPromise) return schemaPromise;
  schemaPromise = (async () => {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS bacbo_resultados (
        id TEXT PRIMARY KEY,
        resultado TEXT NOT NULL,
        estado TEXT NOT NULL DEFAULT 'Resolved',
        inicio TIMESTAMPTZ,
        fim TIMESTAMPTZ,
        pontos_jogador INTEGER,
        pontos_banca INTEGER,
        multiplicador TEXT,
        guardado_em TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        seq BIGSERIAL
      )
    `);
    await pool.query(`ALTER TABLE bacbo_resultados ADD COLUMN IF NOT EXISTS seq BIGSERIAL`);
    await pool.query(`ALTER TABLE bacbo_resultados ADD COLUMN IF NOT EXISTS guardado_em TIMESTAMPTZ NOT NULL DEFAULT NOW()`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_bacbo_fim ON bacbo_resultados (fim DESC)`);

    await pool.query(`
      CREATE TABLE IF NOT EXISTS sinais (
        id BIGSERIAL PRIMARY KEY,
        criado_em TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        cor TEXT NOT NULL,
        estado TEXT NOT NULL DEFAULT 'PENDENTE',
        gale INTEGER NOT NULL DEFAULT 0,
        resultado_id TEXT,
        resultado_final TEXT,
        empates INTEGER NOT NULL DEFAULT 0,
        estrategia TEXT,
        slot_key TEXT
      )
    `);
    await pool.query(`ALTER TABLE sinais ADD COLUMN IF NOT EXISTS estrategia TEXT`);
    await pool.query(`ALTER TABLE sinais ADD COLUMN IF NOT EXISTS slot_key TEXT`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_sinais_estado ON sinais (e
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_sinais_estado ON sinais (estado)`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_sinais_criado ON sinais (criado_em DESC)`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_sinais_slot ON sinais (slot_key)`);

    // Tabela de controlo de slots
    await pool.query(`
      CREATE TABLE IF NOT EXISTS slots_usados (
        slot_key TEXT PRIMARY KEY,
        sinal_id BIGINT,
        usado_em TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `);

    log('DB', { ok: true });
  })();

  try {
    await schemaPromise;
  } catch (e) {
    schemaPromise = null;
    throw e;
  }
      }
      

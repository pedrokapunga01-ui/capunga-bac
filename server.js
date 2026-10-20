'use strict';

/**
 * CAPUNGA DADOS — BAC BO · GALE 1 · FINAL
 *
 * - Gale máximo: 1
 * - Filtro selectivo (várias camadas + consenso)
 * - Média alvo ~15 sinais/hora (intervalo ~2–3 min)
 * - Anti-silêncio: se passar 3 min sem sinal, aceita setup médio
 * - Empate = GREEN e fecha
 * - Mensagens curtas + placar minimalista
 * - Tenta apagar a mensagem de Gale 1 quando sai o resultado
 *
 * Env: DATABASE_URL, BOT_TOKEN, CHAT_ID, ADMIN_TOKEN
 * Opcional: INTERVALO_MINIMO_MS=75000 SILENCIO_MAX_MS=120000
 *   MIN_PRIORIDADE=68 CONSENSO_MIN=1 INTERVALO_SYNC_MS=8000
 *   KEEP_ALIVE_MS KEEP_ALIVE_URL
 *
 * Filtro alto + frequência estável (~12–18 sinais/h quando a mesa confirma).
 * Bac Bo é jogo de azar: nenhum código garante 99–100%. Gestão de banca obrigatória.
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
  maxGale: 1,
  empateProtegido: boolEnv(process.env.EMPATE_PROTEGIDO, true),
  // ~70s entre sinais — ritmo estável sem spam
  intervaloMinimoMs: intEnv('INTERVALO_MINIMO_MS', 70000, 45000, 600000),
  // TETO: no máximo 3 min sem sinal (flex aos 2 min, força aos 3 min)
  silencioMaxMs: intEnv('SILENCIO_MAX_MS', 180000, 90000, 600000),
  // Aos 2 min já baixa o filtro (ainda selectivo)
  silencioFlexMs: intEnv('SILENCIO_FLEX_MS', 120000, 60000, 180000),
  sinalExpiraMin: intEnv('SINAL_EXPIRA_MIN', 5, 3, 20),
  intervaloSyncMs: intEnv('INTERVALO_SYNC_MS', 8000, 5000, 60000),
  keepAliveMs: intEnv('KEEP_ALIVE_MS', 240000, 60000, 900000),
  // Modo normal: muito selectivo
  consensoMin: intEnv('CONSENSO_MIN', 2, 1, 5),
  minPrioridade: intEnv('MIN_PRIORIDADE', 80, 0, 100),
  keepAliveUrl: process.env.KEEP_ALIVE_URL || process.env.RENDER_EXTERNAL_URL || ''
};

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
    if (Math.abs(z) >= 1.5) viesLongo = z > 0 ? 'Banca' : 'Jogador';
  }
  if (!viesLongo && c100.n >= 70) {
    const z = zProp(c100.banca, c100.n);
    if (Math.abs(z) >= 1.4) viesLongo = z > 0 ? 'Banca' : 'Jogador';
  }

  const modo =
    alt20 >= 0.6 ? 'alternante' : alt20 <= 0.35 ? 'streak' : 'misto';

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
  if (mesa.viesMedio === cor) b += 10;
  if (mesa.viesLongo === cor) b += 12;
  if (mesa.viesMedio && mesa.viesMedio !== cor) b -= 8;
  if (mesa.viesLongo && mesa.viesLongo !== cor) b -= 5;
  if (mesa.modo === 'streak' && mesa.seq.cor === cor && mesa.seq.tamanho >= 3) b += 8;
  if (mesa.modo === 'alternante' && mesa.seq.cor && OPOSTO[mesa.seq.cor] === cor) b += 8;
  if (mesa.c12.n >= 10) {
    const dom = mesa.c12.banca > mesa.c12.jogador ? 'Banca' : 'Jogador';
    if (dom === cor && Math.abs(mesa.c12.banca - mesa.c12.jogador) >= 4) b += 7;
  }
  // Estabilidade: pouca alternância recente a favor de streak
  if (mesa.alt12 <= 0.3 && mesa.seq.cor === cor) b += 5;
  return b;
}

/** Lógicas selectivas — pensadas para GREEN ou G1 */
const LOGICAS = [
  {
    id: 'SURF4',
    p: 92,
    avaliar(dec) {
      const s = sequencia(dec);
      if (s.tamanho === 4 && dec.length > 4 && dec[4] !== s.cor) return { cor: s.cor };
      return null;
    }
  },
  {
    id: 'PAD22',
    p: 94,
    avaliar(dec) {
      if (dec.length < 4) return null;
      const [a, b, c, d] = dec;
      if (a === b && c === d && a !== c) return { cor: a };
      return null;
    }
  },
  {
    id: 'DUPLO22',
    p: 96,
    avaliar(dec) {
      if (dec.length < 6) return null;
      const [a, b, c, d, e, f] = dec;
      if (a === b && e === f && a === e && c === d && a !== c) return { cor: a };
      return null;
    }
  },
  {
    id: 'ALT5',
    p: 90,
    avaliar(dec) {
      const s = dec.slice(0, 5);
      if (s.length < 5) return null;
      for (let i = 1; i < 5; i++) if (s[i] === s[i - 1]) return null;
      return { cor: OPOSTO[s[0]] };
    }
  },
  {
    id: 'ALT6',
    p: 93,
    avaliar(dec) {
      const s = dec.slice(0, 6);
      if (s.length < 6) return null;
      for (let i = 1; i < 6; i++) if (s[i] === s[i - 1]) return null;
      return { cor: OPOSTO[s[0]] };
    }
  },
  {
    id: 'STREAK4',
    p: 91,
    avaliar(dec) {
      const s = sequencia(dec);
      // Só 4 ou 5 (não 3) — mais rigor
      if (s.tamanho === 4 || s.tamanho === 5) return { cor: s.cor };
      return null;
    }
  },
  {
    id: 'QUEBRA8',
    p: 72,
    avaliar(dec) {
      // Só quebras muito longas (menos entradas de quebra = menos LOSS lentos)
      const s = sequencia(dec);
      if (s.tamanho >= 8) return { cor: OPOSTO[s.cor] };
      return null;
    }
  },
  {
    id: 'MAIORIA12',
    p: 82,
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
    p: 84,
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
    p: 80,
    avaliar(dec, mesa) {
      if (!mesa.viesLongo || mesa.c50.n < 45) return null;
      const s = sequencia(dec);
      if (s.cor && s.cor !== mesa.viesLongo && s.tamanho >= 3) return null;
      return { cor: mesa.viesLongo };
    }
  },
  // Só no modo flexível (anti-silêncio 3 min)
  {
    id: 'FLEX_STREAK4',
    p: 78,
    flexOnly: true,
    avaliar(dec) {
      const s = sequencia(dec);
      if (s.tamanho >= 4) return { cor: s.cor };
      return null;
    }
  },
  {
    id: 'FLEX_PAD22',
    p: 80,
    flexOnly: true,
    avaliar(dec) {
      if (dec.length < 4) return null;
      const [a, b, c, d] = dec;
      if (a === b && c === d && a !== c) return { cor: a };
      return null;
    }
  }
];

function decidir(hist, opcoes = {}) {
  const minP = Number(opcoes.minPrioridade) || 80;
  const consensoMin = Math.max(1, Number(opcoes.consensoMin) || 2);
  const flexivel = Boolean(opcoes.flexivel);
  const forcar = Boolean(opcoes.forcar); // após 3 min — obriga entrada
  const limiar = forcar ? 50 : flexivel ? Math.max(65, minP - 12) : minP;
  const cons = forcar || flexivel ? 1 : consensoMin;

  const mesa = analisarMesa(hist);
  const dec = mesa.dec;
  if (dec.length < 4) return null;
  if (dec.length < 8 && !flexivel && !forcar) return null;

  const candidatos = [];
  for (const L of LOGICAS) {
    if (L.flexOnly && !flexivel) continue;
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

  const apoio = candidatos.filter((c) => c.cor === cor).sort((a, b) => b.score - a.score);
  const top = apoio[0];

  // Modo normal: exige alinhamento com a mesa
  if (!flexivel && !forcar && top.bonus < 4) return null;
  if (!flexivel && !forcar && top.score < minP) return null;

  return {
    cor,
    confianca: Math.min(99, Math.round(top.score)),
    flexivel,
    forcar,
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

/** Último recurso aos 3 min: melhor cor disponível (streak / maioria recente). */
function forcarEntrada(hist) {
  const dec = decisivos(hist);
  if (dec.length < 2) return null;
  const s = sequencia(dec);
  if (s.cor && s.tamanho >= 2) {
    return {
      cor: s.cor,
      confianca: 55,
      flexivel: true,
      forcar: true,
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
      confianca: 52,
      flexivel: true,
      forcar: true,
      votos: 1,
      _ids: 'FORCA_MAIORIA',
      mesa: { modo: 'forcar', amostra: dec.length }
    };
  }
  // Última cor decisiva
  return {
    cor: dec[0],
    confianca: 50,
    flexivel: true,
    forcar: true,
    votos: 1,
    _ids: 'FORCA_ULTIMA',
    mesa: { modo: 'forcar', amostra: dec.length }
  };
}

/* ===================== APP ===================== */

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
  // message_id do último "Fazer Gale 1" para apagar no resultado
  ultimoGaleMsgId: null
};

let schemaPromise = null;
let server = null;
let syncTimer = null;
let keepAliveTimer = null;

function log(tag, data) {
  console.log(`[${tag}]`, typeof data === 'string' ? data : JSON.stringify(data));
}

function horaAngola() {
  return new Intl.DateTimeFormat('pt-PT', {
    timeZone: 'Africa/Luanda',
    dateStyle: 'short',
    timeStyle: 'medium',
    hour12: false
  }).format(new Date());
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
        estrategia TEXT
      )
    `);
    await pool.query(`ALTER TABLE sinais ADD COLUMN IF NOT EXISTS estrategia TEXT`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_sinais_estado ON sinais (estado)`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_sinais_criado ON sinais (criado_em DESC)`);
    log('DB', { ok: true });
  })();
  try {
    await schemaPromise;
  } catch (e) {
    schemaPromise = null;
    throw e;
  }
}

async function fetchRounds() {
  try {
    const response = await axios.get(API_BACBO, {
      timeout: 20000,
      headers: { Accept: 'application/json', 'User-Agent': 'CapungaG1/1.0' }
    });
    runtime.ultimoHTTPAPI = response.status;
    runtime.ultimoErroAPI = null;
    const items = extractItems(response.data);
    const unique = new Map();
    for (const item of items) {
      const round = normalizeRound(item);
      if (round) unique.set(round.id, round);
    }
    const rounds = [...unique.values()].sort((a, b) => new Date(a.fim) - new Date(b.fim));
    runtime.itensRecebidosAPI = items.length;
    runtime.quantidadeAPI = rounds.length;
    runtime.ultimaHoraDaAPI = rounds.at(-1)?.fim || null;
    return rounds;
  } catch (error) {
    runtime.ultimoErroAPI = error.message || String(error);
    runtime.ultimoHTTPAPI = error.response?.status || null;
    throw error;
  }
}

async function insertRound(round) {
  const result = await pool.query(
    `INSERT INTO bacbo_resultados (id, resultado, estado, inicio, fim)
     VALUES ($1,$2,$3,$4,$5) ON CONFLICT (id) DO NOTHING RETURNING id`,
    [round.id, round.resultado, round.estado, round.inicio, round.fim]
  );
  return result.rows.length > 0;
}

async function getRecentCores(limit = 150) {
  const result = await pool.query(
    `SELECT resultado FROM bacbo_resultados
     ORDER BY fim DESC NULLS LAST, seq DESC LIMIT $1`,
    [limit]
  );
  return result.rows.map((r) => r.resultado);
}

async function getPlacarDia() {
  const result = await pool.query(`
    SELECT
      COUNT(*) FILTER (WHERE estado = 'GREEN')::int AS gs,
      COUNT(*) FILTER (WHERE estado = 'G1')::int AS g1,
      COUNT(*) FILTER (WHERE estado = 'LOSS')::int AS loss,
      COALESCE(SUM(empates), 0)::int AS empates
    FROM sinais
    WHERE criado_em >= date_trunc('day', NOW() AT TIME ZONE 'Africa/Luanda')
      AND estado IN ('GREEN', 'G1', 'LOSS')
  `);
  const s = result.rows[0] || { gs: 0, g1: 0, loss: 0, empates: 0 };
  const greensTotal = (s.gs || 0) + (s.g1 || 0);
  const finalizados = greensTotal + (s.loss || 0);
  const percent = finalizados > 0 ? Number(((greensTotal / finalizados) * 100).toFixed(1)) : 100;
  return {
    greensTotal,
    gs: s.gs || 0,
    g1: s.g1 || 0,
    empates: s.empates || 0,
    loss: s.loss || 0,
    percent
  };
}

function formatPlacar(p) {
  return [
    '🎲 Capunga Dados',
    '📊 Placar do dia',
    `✅ Greens total: ${p.greensTotal}`,
    `🟩 Gs: ${p.gs}`,
    `🟨 G1: ${p.g1}`,
    `🟡 Empates: ${p.empates}`,
    `🟥 Loss: ${p.loss}`,
    `📈 Percentual: ${p.percent}%`
  ].join('\n');
}

async function getPendingSignal() {
  const result = await pool.query(`
    SELECT * FROM sinais WHERE estado = 'PENDENTE'
    ORDER BY criado_em ASC, id ASC LIMIT 1
  `);
  return result.rows[0] || null;
}

async function expirePendingSignal(signal) {
  if (!signal) return false;
  const ageMs = Date.now() - new Date(signal.criado_em).getTime();
  const limite = CONFIG.sinalExpiraMin * 60000;
  if (ageMs < limite) return false;
  if (runtime.rodadasDesdeSinal < 2 && ageMs < limite * 1.3) return false;

  await pool.query(
    `UPDATE sinais SET estado = 'CANCELADO' WHERE id = $1 AND estado = 'PENDENTE'`,
    [signal.id]
  );
  if (runtime.ultimoGaleMsgId) {
    await telegramDelete(runtime.ultimoGaleMsgId);
    runtime.ultimoGaleMsgId = null;
  }
  await telegramSend(`⌛ Sinal #${signal.id} expirado`);
  log('EXPIRADO', { id: signal.id });
  runtime.rodadasDesdeSinal = 0;
  return true;
}

function msgNovoSinal(cor) {
  if (cor === 'Jogador') {
    return ['🔵 Entrada confirmada 🔵', '🟡 Proteção Empate 🟡'].join('\n');
  }
  return ['🔴 Entrada confirmada 🔴', '🟡 Proteção Empate 🟡'].join('\n');
}

function msgResultado(state, porEmpate) {
  if (porEmpate) return '🟡✅ GREEN (Empate)';
  if (state === 'GREEN') return '🟩✅ GREEN';
  if (state === 'G1') return '🟨✅ GREEN G1';
  return '🟥❌ LOSS';
}

async function maybeCreateSignal(round) {
  if (!CONFIG.sinaisAtivos) {
    runtime.motivoSemSinal = 'sinais_desativados';
    return;
  }
  if (!['Jogador', 'Banca'].includes(round.resultado)) {
    runtime.motivoSemSinal = 'rodada_empate';
    return;
  }

  let pending = await getPendingSignal();
  if (pending) {
    if (await expirePendingSignal(pending)) pending = null;
    else {
      runtime.motivoSemSinal = `pendente_#${pending.id}`;
      return;
    }
  }

  const lastResult = await pool.query(
    `SELECT criado_em FROM sinais ORDER BY criado_em DESC, id DESC LIMIT 1`
  );
  const last = lastResult.rows[0];
  if (
    last?.criado_em &&
    Date.now() - new Date(last.criado_em).getTime() < CONFIG.intervaloMinimoMs
  ) {
    runtime.motivoSemSinal = 'intervalo_minimo';
    return;
  }

  const ref = runtime.ultimoSinalEm
    ? new Date(runtime.ultimoSinalEm).getTime()
    : last?.criado_em
      ? new Date(last.criado_em).getTime()
      : 0;
  const silencioMs = ref ? Date.now() - ref : CONFIG.silencioMaxMs + 1;
  const flexMs = CONFIG.silencioFlexMs || 120000;
  const forcar = silencioMs >= CONFIG.silencioMaxMs; // 3 min
  const flexivel = !forcar && silencioMs >= flexMs; // 2 min

  const hist = await getRecentCores(150);
  let entry = decidir(hist, {
    minPrioridade: CONFIG.minPrioridade,
    consensoMin: CONFIG.consensoMin,
    flexivel,
    forcar
  });

  // Garantia: aos 3 min SEMPRE gera sinal
  if (!entry && forcar) {
    entry = forcarEntrada(hist);
  }

  if (!entry) {
    runtime.motivoSemSinal = forcar
      ? `sem_setup_forcar[n=${hist.length}]`
      : flexivel
        ? `sem_setup_flex[n=${hist.length}]`
        : `sem_setup[n=${hist.length}]`;
    return;
  }

  const jaUsada = await pool.query(
    `SELECT 1 FROM sinais WHERE resultado_id = $1 LIMIT 1`,
    [round.id]
  );
  if (jaUsada.rows.length > 0) {
    runtime.motivoSemSinal = 'rodada_ja_usada';
    return;
  }

  const result = await pool.query(
    `INSERT INTO sinais (cor, estado, gale, resultado_id, empates, estrategia)
     VALUES ($1, 'PENDENTE', 0, $2, 0, $3) RETURNING *`,
    [entry.cor, round.id, entry._ids || null]
  );
  const signal = result.rows[0];

  runtime.ultimoSinal = { id: signal.id, cor: signal.cor, criado_em: signal.criado_em };
  runtime.ultimoSinalEm = new Date().toISOString();
  runtime.ultimaAnalise = {
    confianca: entry.confianca,
    flexivel: entry.flexivel,
    votos: entry.votos,
    mesa: entry.mesa,
    ids: entry._ids
  };
  runtime.motivoSemSinal = null;
  runtime.rodadasDesdeSinal = 0;
  runtime.ultimoGaleMsgId = null;

  await telegramSend(msgNovoSinal(entry.cor));
  log('SINAL', { id: signal.id, cor: entry.cor, flexivel: entry.flexivel, forcar: entry.forcar, conf: entry.confianca });
}

async function resolvePending(round) {
  const signal = await getPendingSignal();
  if (!signal) return;
  if (String(signal.resultado_id) === String(round.id)) return;

  runtime.rodadasDesdeSinal = (runtime.rodadasDesdeSinal || 0) + 1;

  const limparGale = async () => {
    if (runtime.ultimoGaleMsgId) {
      await telegramDelete(runtime.ultimoGaleMsgId);
      runtime.ultimoGaleMsgId = null;
    }
  };

  // Empate = GREEN e fecha
  if (round.resultado === 'Empate') {
    if (!CONFIG.empateProtegido) return;
    const state = Number(signal.gale || 0) >= 1 ? 'G1' : 'GREEN';
    await pool.query(
      `UPDATE sinais SET estado = $1, resultado_final = 'Empate',
        empates = COALESCE(empates, 0) + 1
       WHERE id = $2 AND estado = 'PENDENTE'`,
      [state, signal.id]
    );
    await limparGale();
    const placar = await getPlacarDia();
    await telegramSend(msgResultado(state, true) + '\n\n' + formatPlacar(placar));
    log('GREEN EMPATE', { id: signal.id });
    runtime.rodadasDesdeSinal = 0;
    return;
  }

  if (round.resultado === signal.cor) {
    const state = Number(signal.gale || 0) >= 1 ? 'G1' : 'GREEN';
    await pool.query(
      `UPDATE sinais SET estado = $1, resultado_final = $2
       WHERE id = $3 AND estado = 'PENDENTE'`,
      [state, round.resultado, signal.id]
    );
    await limparGale();
    const placar = await getPlacarDia();
    await telegramSend(msgResultado(state, false) + '\n\n' + formatPlacar(placar));
    log('GREEN', { id: signal.id, estado: state });
    runtime.rodadasDesdeSinal = 0;
    return;
  }

  // Errou → Gale 1 ou LOSS
  const nextGale = Number(signal.gale || 0) + 1;
  if (nextGale <= CONFIG.maxGale) {
    await pool.query(
      `UPDATE sinais SET gale = $1 WHERE id = $2 AND estado = 'PENDENTE'`,
      [nextGale, signal.id]
    );
    const mid = await telegramSend('🐔 Fazer o Gale 1 🐔');
    if (mid && mid !== true) runtime.ultimoGaleMsgId = mid;
    log('GALE1', { id: signal.id });
    return;
  }

  await pool.query(
    `UPDATE sinais SET estado = 'LOSS', resultado_final = $1
     WHERE id = $2 AND estado = 'PENDENTE'`,
    [round.resultado, signal.id]
  );
  await limparGale();
  const placar = await getPlacarDia();
  await telegramSend(msgResultado('LOSS', false) + '\n\n' + formatPlacar(placar));
  log('LOSS', { id: signal.id });
  runtime.rodadasDesdeSinal = 0;
}

async function syncOnce() {
  if (!pool) throw new Error('DATABASE_URL não configurada');
  if (runtime.sincronizacaoEmCurso) return { sucesso: false, ignorada: true };
  runtime.sincronizacaoEmCurso = true;

  try {
    await ensureSchema();
    const count = await pool.query('SELECT COUNT(*)::int AS total FROM bacbo_resultados');
    const hadHistory = count.rows[0].total > 0;
    const rounds = await fetchRounds();

    let inserted = 0;
    let duplicates = 0;
    let evaluated = 0;
    let lastNew = null;

    for (const round of rounds) {
      const saved = await insertRound(round);
      if (!saved) {
        duplicates++;
        continue;
      }
      inserted++;
      lastNew = round;
      if (hadHistory) {
        await resolvePending(round);
        await maybeCreateSignal(round);
        evaluated++;
      }
    }

    if (inserted === 0 && hadHistory) {
      const pending = await getPendingSignal();
      if (pending) await expirePendingSignal(pending);
    }

    runtime.ultimaSincronizacao = new Date().toISOString();
    runtime.duplicadosUltimoCiclo = duplicates;
    runtime.novosUltimoCiclo = inserted;
    runtime.erroSincronizacao = null;

    if (lastNew) {
      runtime.ultimaRodadaNova = new Date().toISOString();
      runtime.ultimoIdNovo = lastNew.id;
      runtime.ciclosSemNovos = 0;
    } else {
      runtime.ciclosSemNovos = (runtime.ciclosSemNovos || 0) + 1;
    }

    const summary = {
      novos: inserted,
      duplicados: duplicates,
      avaliados: evaluated,
      motivoSemSinal: runtime.motivoSemSinal,
      horaAngola: horaAngola()
    };
    if (inserted > 0 || runtime.ciclosSemNovos % 8 === 0) log('SYNC', summary);
    return { sucesso: true, ...summary };
  } catch (error) {
    runtime.erroSincronizacao = error.message || String(error);
    console.error('[ERRO SYNC]', error.message);
    return { sucesso: false, erro: runtime.erroSincronizacao };
  } finally {
    runtime.sincronizacaoEmCurso = false;
  }
}

async function keepAliveTick() {
  try {
    let url = runtime.publicUrl || CONFIG.keepAliveUrl || process.env.RENDER_EXTERNAL_URL || '';
    if (!url) return;
    const base = url.replace(/\/$/, '');
    const target = base.includes('/health') ? base : `${base}/health`;
    await axios.get(target, { timeout: 10000 });
    runtime.ultimoKeepAlive = new Date().toISOString();
    runtime.publicUrl = base;
  } catch (_) {}
}

function requireAdmin(req, res, next) {
  if (!ADMIN_TOKEN) {
    return res.status(503).json({ sucesso: false, erro: 'ADMIN_TOKEN não configurado' });
  }
  if (req.get('x-admin-token') !== ADMIN_TOKEN) {
    return res.status(401).json({ sucesso: false, erro: 'Token inválido' });
  }
  next();
}

app.get('/', (req, res) => {
  res.json({
    nome: 'Capunga Dados G1',
    versao: 'g1-final',
    maxGale: 1,
    estado: 'online',
    horaAngola: horaAngola()
  });
});

app.get('/health', (req, res) => {
  res.json({
    sucesso: true,
    estado: 'online',
    horaAngola: horaAngola(),
    ultimaSincronizacao: runtime.ultimaSincronizacao,
    motivoSemSinal: runtime.motivoSemSinal
  });
});

app.get('/diagnostico', async (req, res) => {
  let totalResultados = 0;
  let totalSinais = 0;
  let pendentes = 0;
  let recentes = [];
  let mesa = null;
  try {
    if (pool) {
      await ensureSchema();
      totalResultados = (await pool.query('SELECT COUNT(*)::int AS t FROM bacbo_resultados')).rows[0].t;
      totalSinais = (await pool.query('SELECT COUNT(*)::int AS t FROM sinais')).rows[0].t;
      pendentes = (await pool.query(`SELECT COUNT(*)::int AS t FROM sinais WHERE estado = 'PENDENTE'`)).rows[0].t;
      recentes = (
        await pool.query(
          `SELECT resultado FROM bacbo_resultados ORDER BY fim DESC NULLS LAST, seq DESC LIMIT 15`
        )
      ).rows.map((r) => r.resultado);
      const hist = await getRecentCores(150);
      const m = analisarMesa(hist);
      mesa = {
        amostra: m.amostra,
        modo: m.modo,
        viesMedio: m.viesMedio,
        viesLongo: m.viesLongo,
        seq: m.seq
      };
    }
  } catch (_) {}

  res.json({
    sucesso: true,
    versao: 'g1-final',
    maxGale: 1,
    horaAngola: horaAngola(),
    telegramConfigurado: Boolean(BOT_TOKEN && CHAT_ID),
    httpAPI: runtime.ultimoHTTPAPI,
    erroAPI: runtime.ultimoErroAPI,
    ultimaSincronizacao: runtime.ultimaSincronizacao,
    novosUltimoCiclo: runtime.novosUltimoCiclo,
    ciclosSemNovos: runtime.ciclosSemNovos,
    motivoSemSinal: runtime.motivoSemSinal,
    ultimoSinal: runtime.ultimoSinal,
    ultimaAnalise: runtime.ultimaAnalise,
    ultimosResultados: recentes,
    analiseMesa: mesa,
    baseDados: { totalResultados, totalSinais, pendentes },
    configuracao: CONFIG
  });
});

app.get('/resultados', async (req, res) => {
  try {
    await ensureSchema();
    const limit = Math.min(Math.max(Number.parseInt(req.query.limit, 10) || 40, 1), 300);
    const result = await pool.query(
      `SELECT id, resultado, fim FROM bacbo_resultados
       ORDER BY fim DESC NULLS LAST, seq DESC LIMIT $1`,
      [limit]
    );
    res.json({ sucesso: true, total: result.rows.length, resultados: result.rows });
  } catch (error) {
    res.status(500).json({ sucesso: false, erro: error.message });
  }
});

app.get('/sinais', async (req, res) => {
  try {
    await ensureSchema();
    const limit = Math.min(Math.max(Number.parseInt(req.query.limit, 10) || 40, 1), 300);
    const result = await pool.query(
      `SELECT id, criado_em, cor, estado, gale, resultado_final, empates
       FROM sinais ORDER BY criado_em DESC, id DESC LIMIT $1`,
      [limit]
    );
    res.json({ sucesso: true, total: result.rows.length, sinais: result.rows });
  } catch (error) {
    res.status(500).json({ sucesso: false, erro: error.message });
  }
});

app.get('/estatisticas', async (req, res) => {
  try {
    await ensureSchema();
    const placarDia = await getPlacarDia();
    res.json({ sucesso: true, placarDia, horaAngola: horaAngola() });
  } catch (error) {
    res.status(500).json({ sucesso: false, erro: error.message });
  }
});

app.post('/db/sincronizar', requireAdmin, async (req, res) => {
  try {
    const result = await syncOnce();
    res.status(result.sucesso ? 200 : 500).json(result);
  } catch (error) {
    res.status(500).json({ sucesso: false, erro: error.message });
  }
});

app.post('/telegram/teste', requireAdmin, async (req, res) => {
  const mid = await telegramSend(
    ['🔵 Entrada confirmada 🔵', '🟡 Proteção Empate 🟡', '(teste G1)'].join('\n')
  );
  res.status(mid ? 200 : 502).json({ sucesso: Boolean(mid), erro: runtime.ultimoErroTelegram });
});

app.use((req, res) => {
  res.status(404).json({ sucesso: false, erro: 'Rota não encontrada' });
});

async function start() {
  if (!DATABASE_URL) throw new Error('DATABASE_URL não configurada');
  await ensureSchema();

  server = app.listen(PORT, '0.0.0.0', () => {
    console.log('======================================');
    console.log('CAPUNGA DADOS — GALE 1 · FINAL');
    console.log(`Porta ${PORT} | ${horaAngola()}`);
    console.log(`Intervalo ${CONFIG.intervaloMinimoMs}ms | Silêncio max ${CONFIG.silencioMaxMs}ms`);
    console.log(`Consenso ${CONFIG.consensoMin} | Min score ${CONFIG.minPrioridade}`);
    console.log('======================================');
  });

  const initial = await syncOnce();
  if (!initial.sucesso) console.error('[PRIMEIRA SYNC]', initial.erro);
  else log('PRIMEIRA SYNC', initial);

  syncTimer = setInterval(async () => {
    if (runtime.sincronizacaoEmCurso) return;
    try {
      await syncOnce();
    } catch (err) {
      console.error('[SYNC]', err.message);
    }
  }, CONFIG.intervaloSyncMs);
  if (typeof syncTimer.unref === 'function') syncTimer.unref();

  keepAliveTimer = setInterval(() => keepAliveTick().catch(() => {}), CONFIG.keepAliveMs);
  if (typeof keepAliveTimer.unref === 'function') keepAliveTimer.unref();
  setTimeout(() => keepAliveTick().catch(() => {}), 30000);
}

process.on('unhandledRejection', (r) => console.error('[UNHANDLED]', r));
process.on('uncaughtException', (e) => console.error('[UNCAUGHT]', e));

async function shutdown() {
  if (syncTimer) clearInterval(syncTimer);
  if (keepAliveTimer) clearInterval(keepAliveTimer);
  if (server) await new Promise((r) => server.close(r));
  if (pool) await pool.end();
  process.exit(0);
}
process.on('SIGTERM', () => shutdown().catch(() => process.exit(1)));
process.on('SIGINT', () => shutdown().catch(() => process.exit(1)));

start().catch((e) => {
  console.error('[FATAL]', e);
  process.exit(1);
});

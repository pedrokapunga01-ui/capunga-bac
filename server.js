'use strict';

const express = require('express');
const cors = require('cors');
const axios = require('axios');
const { Pool } = require('pg');
const crypto = require('crypto');

const app = express();

app.disable('x-powered-by');
app.use(express.json({ limit: '32kb' }));

const PORT = Number(process.env.PORT || 10000);
const API_BACBO = process.env.API_BACBO ||
  'https://api-cs.casino.org/svc-evolution-game-events/api/bacbo';

const INTERVALO_SYNC_MS = Math.min(
  60000,
  Math.max(
    5000,
    Number.parseInt(process.env.INTERVALO_SYNC_MS || '8000', 10) || 8000
  )
);

const BOT_TOKEN = process.env.BOT_TOKEN || '';
const CHAT_ID = process.env.CHAT_ID || '';
const ADMIN_TOKEN = process.env.ADMIN_TOKEN || '';
const TELEGRAM_NOTIFY_NEW_RESULT =
  process.env.TELEGRAM_NOTIFY_NEW_RESULT === 'true';

if (!process.env.DATABASE_URL) {
  console.error('[FATAL] DATABASE_URL não configurada.');
  process.exit(1);
}

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: /localhost|127\.0\.0\.1/.test(process.env.DATABASE_URL)
    ? false
    : { rejectUnauthorized: false },
  max: 5,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 10000,
  statement_timeout: 15000
});

pool.on('error', err => {
  console.error('[DB_POOL]', err.message);
});

const runtime = {
  iniciadoEm: new Date().toISOString(),
  ultimaSincronizacao: null,
  ultimaSincronizacaoBemSucedida: null,
  ultimoResultadoNovoEm: null,
  ultimoId: null,
  apiHttp: null,
  apiLatenciaMs: null,
  erroAPI: null,
  erroDB: null,
  ultimaMensagemTelegram: null,
  erroTelegram: null,
  itensBrutos: 0,
  itensNormalizados: 0,
  itensDescartados: 0,
  novos: 0,
  atualizados: 0,
  inalterados: 0,
  ciclosSemNovos: 0,
  duracaoMs: null,
  sincronizacaoEmCurso: false,
  formatoResposta: null,
  falhasConsecutivas: 0
};

let server;
let timer;
let stopping = false;

function nowAngola() {
  return new Date().toLocaleString('pt-PT', {
    timeZone: 'Africa/Luanda'
  });
}

function safeError(err) {
  return {
    mensagem: String(err?.message || err).slice(0, 400),
    codigo: err?.code || null,
    http: err?.response?.status || null
  };
}

function dateValue(value) {
  if (value === null || value === undefined || value === '') {
    return null;
  }

  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? null : date;
}

function firstDefined(...values) {
  return values.find(
    value => value !== undefined &&
      value !== null &&
      value !== ''
  );
}

function normalizeOutcome(value) {
  const text = String(value || '').trim().toLowerCase();

  if ([
    'jogador', 'player', 'blue', 'playerwin', 'player_win'
  ].includes(text)) return 'Jogador';

  if ([
    'banca', 'banker', 'red', 'bankerwin', 'banker_win'
  ].includes(text)) return 'Banca';

  if ([
    'empate', 'tie', 'draw', 'egal'
  ].includes(text)) return 'Empate';

  return null;
}

function numeric(value) {
  if (value === null || value === undefined || value === '') {
    return null;
  }

  const result = Number(value);
  return Number.isFinite(result) ? result : null;
}

function extractItems(body) {
  if (Array.isArray(body)) return body;
  if (!body || typeof body !== 'object') return [];

  const candidates = [
    body.data,
    body.results,
    body.items,
    body.resultados,
    body.rounds,
    body.events,
    body.records,
    body.data?.results,
    body.data?.items,
    body.data?.rounds,
    body.data?.events,
    body.data?.resultados
  ];

  return candidates.find(Array.isArray) || [];
}

function normalizeRound(item) {
  if (!item || typeof item !== 'object') return null;

  const source = {
    ...(item.data && typeof item.data === 'object' ? item.data : {}),
    ...(item.round && typeof item.round === 'object' ? item.round : {}),
    ...item
  };

  const id = firstDefined(
    source.id,
    source._id,
    source.roundId,
    source.round_id,
    source.gameId,
    source.game_id,
    source.eventId,
    source.event_id,
    source.uuid
  );

  const outcome = normalizeOutcome(firstDefined(
    source.result,
    source.outcome,
    source.winner,
    source.winningSide,
    source.winning_side,
    source.winnerName,
    source.statusResult
  ));

  const end = dateValue(firstDefined(
    source.finishedAt,
    source.finished_at,
    source.endTime,
    source.end_time,
    source.endedAt,
    source.ended_at,
    source.createdAt,
    source.created_at,
    source.timestamp,
    source.date,
    source.time
  ));

  if (id === undefined || !outcome || !end) {
    return null;
  }

  const status = String(
    firstDefined(source.status, source.state, 'resolved')
  ).slice(0, 50);

  return {
    id: String(id),
    resultado: outcome,
    estado: status,
    inicio: dateValue(firstDefined(
      source.startedAt,
      source.started_at,
      source.startTime,
      source.start_time
    )),
    fim: end,
    pontos_jogador: numeric(firstDefined(
      source.playerScore,
      source.player_score,
      source.playerPoints,
      source.player_points,
      source.blueScore,
      source.blue_score
    )),
    pontos_banca: numeric(firstDefined(
      source.bankerScore,
      source.banker_score,
      source.bankerPoints,
      source.banker_points,
      source.redScore,
      source.red_score
    )),
    multiplicador: firstDefined(
      source.multiplier,
      source.payoutMultiplier,
      source.payout_multiplier
    ) == null
      ? null
      : String(firstDefined(
          source.multiplier,
          source.payoutMultiplier,
          source.payout_multiplier
        )).slice(0, 40)
  };
}

async function ensureSchema() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS bacbo_resultados (
      id TEXT PRIMARY KEY,
      resultado TEXT NOT NULL,
      estado TEXT,
      inicio TIMESTAMPTZ,
      fim TIMESTAMPTZ NOT NULL,
      pontos_jogador INTEGER,
      pontos_banca INTEGER,
      multiplicador TEXT,
      guardado_em TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      seq BIGSERIAL
    )
  `);

  const columns = [
    ['estado', 'TEXT'],
    ['inicio', 'TIMESTAMPTZ'],
    ['fim', 'TIMESTAMPTZ'],
    ['pontos_jogador', 'INTEGER'],
    ['pontos_banca', 'INTEGER'],
    ['multiplicador', 'TEXT'],
    ['guardado_em', 'TIMESTAMPTZ DEFAULT NOW()'],
    ['seq', 'BIGSERIAL']
  ];

  for (const [name, type] of columns) {
    await pool.query(
      `ALTER TABLE bacbo_resultados
       ADD COLUMN IF NOT EXISTS ${name} ${type}`
    );
  }

  await pool.query(`
    CREATE INDEX IF NOT EXISTS bacbo_resultados_fim_idx
    ON bacbo_resultados (fim DESC)
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS bacbo_resultados_seq_idx
    ON bacbo_resultados (seq DESC)
  `);

  console.log('[DB] Estrutura verificada; histórico preservado.');
}

async function telegramSend(message) {
  if (!BOT_TOKEN || !CHAT_ID) {
    runtime.erroTelegram =
      'Configure BOT_TOKEN e CHAT_ID nas variáveis do Render.';
    return false;
  }

  try {
    const response = await axios.post(
      `https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`,
      {
        chat_id: CHAT_ID,
        text: message,
        disable_web_page_preview: true
      },
      { timeout: 10000 }
    );

    if (!response.data?.ok) {
      throw new Error('Telegram não confirmou a mensagem.');
    }

    runtime.ultimaMensagemTelegram = new Date().toISOString();
    runtime.erroTelegram = null;
    return true;
  } catch (err) {
    runtime.erroTelegram = safeError(err);
    console.error('[TELEGRAM]', JSON.stringify(runtime.erroTelegram));
    return false;
  }
}

async function fetchRounds() {
  const started = Date.now();

  try {
    const response = await axios.get(API_BACBO, {
      timeout: 20000,
      headers: {
        Accept: 'application/json',
        'User-Agent': 'DadosAO-Monitor/2.0'
      },
      validateStatus: status => status >= 200 && status < 300
    });

    runtime.apiHttp = response.status;
    runtime.apiLatenciaMs = Date.now() - started;
    runtime.erroAPI = null;

    const body = response.data;

    runtime.formatoResposta = Array.isArray(body)
      ? 'array'
      : body && typeof body === 'object'
        ? Object.keys(body).slice(0, 25)
        : typeof body;

    const items = extractItems(body);
    runtime.itensBrutos = items.length;

    const unique = new Map();
    let discarded = 0;

    for (const item of items) {
      const round = normalizeRound(item);

      if (!round) {
        discarded++;
        continue;
      }

      unique.set(round.id, round);
    }

    const rounds = [...unique.values()]
      .sort((a, b) => a.fim - b.fim);

    runtime.itensNormalizados = rounds.length;
    runtime.itensDescartados = discarded;

    if (items.length > 0 && rounds.length === 0) {
      runtime.erroAPI = {
        mensagem: 'A API devolveu dados, mas nenhum resultado foi reconhecido.'
      };
    }

    return rounds;
  } catch (err) {
    runtime.apiLatenciaMs = Date.now() - started;
    runtime.erroAPI = safeError(err);

    if (err.response) {
      runtime.apiHttp = err.response.status;
    }

    throw err;
  }
}

async function saveRound(round) {
  const inserted = await pool.query(`
    INSERT INTO bacbo_resultados (
      id, resultado, estado, inicio, fim,
      pontos_jogador, pontos_banca, multiplicador, guardado_em
    )
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,NOW())
    ON CONFLICT (id) DO NOTHING
    RETURNING id
  `, [
    round.id,
    round.resultado,
    round.estado,
    round.inicio,
    round.fim,
    round.pontos_jogador,
    round.pontos_banca,
    round.multiplicador
  ]);

  if (inserted.rowCount > 0) {
    return 'novo';
  }

  const updated = await pool.query(`
    UPDATE bacbo_resultados SET
      resultado=$2,
      estado=$3,
      inicio=$4,
      fim=$5,
      pontos_jogador=$6,
      pontos_banca=$7,
      multiplicador=$8
    WHERE id=$1 AND (
      resultado IS DISTINCT FROM $2 OR
      estado IS DISTINCT FROM $3 OR
      inicio IS DISTINCT FROM $4 OR
      fim IS DISTINCT FROM $5 OR
      pontos_jogador IS DISTINCT FROM $6 OR
      pontos_banca IS DISTINCT FROM $7 OR
      multiplicador IS DISTINCT FROM $8
    )
    RETURNING id
  `, [
    round.id,
    round.resultado,
    round.estado,
    round.inicio,
    round.fim,
    round.pontos_jogador,
    round.pontos_banca,
    round.multiplicador
  ]);

  return updated.rowCount > 0 ? 'atualizado' : 'inalterado';
}

async function syncOnce() {
  if (runtime.sincronizacaoEmCurso || stopping) {
    return { ignorado: true };
  }

  runtime.sincronizacaoEmCurso = true;
  const started = Date.now();

  let novos = 0;
  let atualizados = 0;
  let inalterados = 0;

  try {
    const rounds = await fetchRounds();

    for (const round of rounds) {
      const result = await saveRound(round);

      if (result === 'novo') {
        novos++;
        runtime.ultimoResultadoNovoEm = new Date().toISOString();
        runtime.ultimoId = round.id;

        if (TELEGRAM_NOTIFY_NEW_RESULT) {
          await telegramSend(
            'Dados AO — novo resultado registado\n' +
            `Resultado: ${round.resultado}\n` +
            `Hora: ${round.fim.toISOString()}\n` +
            'Mensagem informativa; não é recomendação de aposta.'
          );
        }
      } else if (result === 'atualizado') {
        atualizados++;
      } else {
        inalterados++;
      }
    }

    runtime.ultimaSincronizacao = new Date().toISOString();
    runtime.ultimaSincronizacaoBemSucedida =
      runtime.ultimaSincronizacao;

    runtime.novos = novos;
    runtime.atualizados = atualizados;
    runtime.inalterados = inalterados;
    runtime.ciclosSemNovos = novos
      ? 0
      : runtime.ciclosSemNovos + 1;

    runtime.erroDB = null;
    runtime.falhasConsecutivas = 0;

    if (novos || atualizados) {
      console.log('[SYNC]', JSON.stringify({
        sucesso: true,
        recebidos: runtime.itensBrutos,
        normalizados: rounds.length,
        novos,
        atualizados,
        inalterados
      }));
    }

    return {
      sucesso: true,
      recebidos: runtime.itensBrutos,
      normalizados: rounds.length,
      novos,
      atualizados,
      inalterados
    };
  } catch (err) {
    runtime.falhasConsecutivas++;
    runtime.erroDB = safeError(err);

    console.error('[SYNC]', JSON.stringify({
      erro: safeError(err),
      falhasConsecutivas: runtime.falhasConsecutivas
    }));

    return { sucesso: false, erro: safeError(err) };
  } finally {
    runtime.duracaoMs = Date.now() - started;
    runtime.sincronizacaoEmCurso = false;
  }
}

function requireAdmin(req, res, next) {
  if (!ADMIN_TOKEN) {
    return res.status(503).json({
      sucesso: false,
      erro: 'ADMIN_TOKEN não configurado.'
    });
  }

  const supplied = req.get('x-admin-token') || '';
  const a = Buffer.from(supplied);
  const b = Buffer.from(ADMIN_TOKEN);

  if (a.length !== b.length || !crypto.timingSafeEqual(a, b)) {
    return res.status(401).json({
      sucesso: false,
      erro: 'Não autorizado.'
    });
  }

  next();
}

function diagnostics() {
  return {
    iniciadoEm: runtime.iniciadoEm,
    ultimaSincronizacao: runtime.ultimaSincronizacao,
    ultimaSincronizacaoBemSucedida:
      runtime.ultimaSincronizacaoBemSucedida,
    ultimoResultadoNovoEm: runtime.ultimoResultadoNovoEm,
    api: {
      http: runtime.apiHttp,
      latenciaMs: runtime.apiLatenciaMs,
      erro: runtime.erroAPI,
      formatoResposta: runtime.formatoResposta
    },
    recolha: {
      itensBrutos: runtime.itensBrutos,
      itensNormalizados: runtime.itensNormalizados,
      itensDescartados: runtime.itensDescartados,
      novos: runtime.novos,
      atualizados: runtime.atualizados,
      inalterados: runtime.inalterados,
      ciclosSemNovos: runtime.ciclosSemNovos,
      duracaoMs: runtime.duracaoMs
    },
    baseDados: { erro: runtime.erroDB },
    telegram: {
      configurado: Boolean(BOT_TOKEN && CHAT_ID),
      ultimaMensagem: runtime.ultimaMensagemTelegram,
      erro: runtime.erroTelegram
    },
    falhasConsecutivas: runtime.falhasConsecutivas,
    sincronizacaoEmCurso: runtime.sincronizacaoEmCurso
  };
}

app.use(cors({
  origin: process.env.CORS_ORIGIN
    ? process.env.CORS_ORIGIN.split(',').map(v => v.trim())
    : false
}));

app.get('/', (req, res) => {
  res.json({
    nome: 'Capunga Dados — BAC BO Monitor',
    estado: 'online',
    horaAngola: nowAngola(),
    rotas: [
      '/health',
      '/diagnostico',
      '/resultados',
      '/estatisticas'
    ]
  });
});

app.get('/health', async (req, res) => {
  let dbOk = false;

  try {
    await pool.query('SELECT 1');
    dbOk = true;
  } catch (_) {}

  const last = runtime.ultimaSincronizacaoBemSucedida
    ? Date.now() -
      new Date(runtime.ultimaSincronizacaoBemSucedida).getTime()
    : null;

  const stale = last === null ||
    last > Math.max(120000, INTERVALO_SYNC_MS * 5);

  const healthy = dbOk && !stale;

  res.status(healthy ? 200 : 503).json({
    estado: healthy ? 'saudavel' : 'degradado',
    servidor: 'online',
    baseDados: dbOk ? 'online' : 'indisponivel',
    sincronizacaoDesatualizada: stale,
    idadeUltimaSincronizacaoMs: last,
    horaAngola: nowAngola()
  });
});

app.get('/diagnostico', async (req, res) => {
  try {
    const total = await pool.query(`
      SELECT COUNT(*)::bigint AS total
      FROM bacbo_resultados
    `);

    const latest = await pool.query(`
      SELECT id, resultado, estado, inicio, fim,
        pontos_jogador, pontos_banca, multiplicador, guardado_em
      FROM bacbo_resultados
      ORDER BY fim DESC NULLS LAST, seq DESC
      LIMIT 1
    `);

    const recent = await pool.query(`
      SELECT id, resultado, estado, fim,
        pontos_jogador, pontos_banca, multiplicador
      FROM bacbo_resultados
      ORDER BY fim DESC NULLS LAST, seq DESC
      LIMIT 10
    `);

    res.json({
      sucesso: !runtime.erroDB,
      nome: 'Capunga Dados — BAC BO Monitor',
      horaAngola: nowAngola(),
      totalResultados: Number(total.rows[0].total),
      ultimoResultado: latest.rows[0] || null,
      recentes: recent.rows,
      sincronizacao: diagnostics()
    });
  } catch (err) {
    res.status(500).json({
      sucesso: false,
      erro: safeError(err),
      sincronizacao: diagnostics()
    });
  }
});

app.get('/resultados', async (req, res) => {
  const parsed = Number.parseInt(req.query.limit || '50', 10);
  const limit = Math.min(500, Math.max(1, parsed || 50));
  const before = req.query.before
    ? dateValue(req.query.before)
    : null;

  if (req.query.before && !before) {
    return res.status(400).json({
      sucesso: false,
      erro: 'O parâmetro before deve ser uma data válida.'
    });
  }

  try {
    const result = before
      ? await pool.query(`
          SELECT id, resultado, estado, inicio, fim,
            pontos_jogador, pontos_banca, multiplicador, guardado_em
          FROM bacbo_resultados
          WHERE fim < $1
          ORDER BY fim DESC NULLS LAST, seq DESC
          LIMIT $2
        `, [before, limit])
      : await pool.query(`
          SELECT id, resultado, estado, inicio, fim,
            pontos_jogador, pontos_banca, multiplicador, guardado_em
          FROM bacbo_resultados
          ORDER BY fim DESC NULLS LAST, seq DESC
          LIMIT $1
        `, [limit]);

    res.json({
      sucesso: true,
      quantidade: result.rowCount,
      limite: limit,
      proximaPaginaAntesDe: result.rows.length
        ? result.rows[result.rows.length - 1].fim
        : null,
      resultados: result.rows
    });
  } catch (err) {
    res.status(500).json({
      sucesso: false,
      erro: safeError(err)
    });
  }
});

app.get('/estatisticas', async (req, res) => {
  try {
    const counts = await pool.query(`
      SELECT resultado, COUNT(*)::bigint AS quantidade
      FROM bacbo_resultados
      GROUP BY resultado
      ORDER BY resultado
    `);

    const quality = await pool.query(`
      SELECT
        COUNT(*)::bigint AS total,
        COUNT(*) FILTER (
          WHERE pontos_jogador IS NULL OR pontos_banca IS NULL
        )::bigint AS sem_pontuacao,
        MIN(fim) AS primeiro_resultado,
        MAX(fim) AS ultimo_resultado
      FROM bacbo_resultados
    `);

    res.json({
      sucesso: true,
      distribuicaoHistorica: counts.rows.map(row => ({
        resultado: row.resultado,
        quantidade: Number(row.quantidade)
      })),
      qualidadeHistorico: {
        total: Number(quality.rows[0].total),
        semPontuacao: Number(quality.rows[0].sem_pontuacao),
        primeiroResultado: quality.rows[0].primeiro_resultado,
        ultimoResultado: quality.rows[0].ultimo_resultado
      },
      nota: 'Estatísticas descritivas do histórico; não preveem resultados futuros.'
    });
  } catch (err) {
    res.status(500).json({
      sucesso: false,
      erro: safeError(err)
    });
  }
});

app.post('/db/preparar', requireAdmin, async (req, res) => {
  try {
    await ensureSchema();
    res.json({
      sucesso: true,
      mensagem: 'Estrutura verificada; histórico preservado.'
    });
  } catch (err) {
    res.status(500).json({
      sucesso: false,
      erro: safeError(err)
    });
  }
});

app.post('/db/sincronizar', requireAdmin, async (req, res) => {
  const result = await syncOnce();
  res.status(result.sucesso === false ? 502 : 200).json(result);
});

app.post('/telegram/teste', requireAdmin, async (req, res) => {
  const ok = await telegramSend(
    'Dados AO — teste de comunicação do monitor. Mensagem informativa.'
  );

  res.status(ok ? 200 : 502).json({ sucesso: ok });
});

app.use((req, res) => {
  res.status(404).json({
    sucesso: false,
    erro: 'Rota não encontrada.'
  });
});

app.use((err, req, res, next) => {
  console.error('[HTTP]', safeError(err));

  if (res.headersSent) return next(err);

  res.status(500).json({
    sucesso: false,
    erro: 'Erro interno do servidor.'
  });
});

async function start() {
  await ensureSchema();

  server = app.listen(PORT, '0.0.0.0', () => {
    console.log('[START]', JSON.stringify({
      nome: 'CAPUNGA DADOS — BAC BO MONITOR',
      porta: PORT,
      intervaloSyncMs: INTERVALO_SYNC_MS,
      horaAngola: nowAngola()
    }));
  });

  await syncOnce();

  timer = setInterval(() => {
    if (!runtime.sincronizacaoEmCurso && !stopping) {
      syncOnce();
    }
  }, INTERVALO_SYNC_MS);
}

async function shutdown(signal) {
  if (stopping) return;
  stopping = true;

  console.log('[SHUTDOWN]', signal);

  if (timer) clearInterval(timer);

  if (server) {
    await new Promise(resolve => server.close(resolve));
  }

  try {
    await pool.end();
  } catch (err) {
    console.error('[DB_CLOSE]', safeError(err));
  }

  process.exit(0);
}

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));

process.on('unhandledRejection', err => {
  console.error('[UNHANDLED_REJECTION]', safeError(err));
});

start().catch(err => {
  console.error('[FATAL_STARTUP]', safeError(err));
  process.exit(1);
});

'use strict';

/**
 * CAPUNGA DADOS — BAC BO
 * Telegram + PostgreSQL + API + diagnóstico
 *
 * Variáveis obrigatórias no Render:
 * BOT_TOKEN
 * CHAT_ID
 * DATABASE_URL
 * ADMIN_TOKEN
 */

const express = require('express');
const cors = require('cors');
const axios = require('axios');
const { Pool } = require('pg');

const app = express();

app.use(cors());
app.use(express.json({ limit: '1mb' }));

const PORT = Number(process.env.PORT) || 3000;

const BOT_TOKEN = process.env.BOT_TOKEN;
const CHAT_ID = process.env.CHAT_ID;
const DATABASE_URL = process.env.DATABASE_URL;
const ADMIN_TOKEN = process.env.ADMIN_TOKEN;

const API_BACBO =
  process.env.API_BACBO ||
  'https://api-cs.casino.org/svc-evolution-game-events/api/bacbo';

const INTERVALO_SYNC_MS = Math.max(
  5000,
  Number(process.env.INTERVALO_SYNC_MS) || 8000
);

const SINAIS_ATIVOS =
  String(process.env.SINAIS_ATIVOS || 'true').toLowerCase() !== 'false';

const EMPATE_PROTEGIDO =
  String(process.env.EMPATE_PROTEGIDO || 'true').toLowerCase() !== 'false';

const SINAL_EXPIRA_MIN = Math.max(
  1,
  Number(process.env.SINAL_EXPIRA_MIN) || 3
);

const MIN_PRIORIDADE = Math.max(
  1,
  Math.min(100, Number(process.env.MIN_PRIORIDADE) || 78)
);

const CONSENSO_MIN = Math.max(
  1,
  Number(process.env.CONSENSO_MIN) || 2
);

if (!DATABASE_URL || !BOT_TOKEN || !CHAT_ID || !ADMIN_TOKEN) {
  console.error(
    '[FATAL] Configura DATABASE_URL, BOT_TOKEN, CHAT_ID e ADMIN_TOKEN no Render.'
  );
  process.exit(1);
}

const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: process.env.PGSSLMODE === 'disable'
    ? false
    : { rejectUnauthorized: false },
  connectionTimeoutMillis: 15000,
  idleTimeoutMillis: 30000,
  max: 5
});

const estado = {
  iniciado: false,
  sincronizando: false,
  ultimaSincronizacao: null,
  ultimoResultado: null,
  ultimaMensagemTelegram: null,
  erroAPI: null,
  httpAPI: null,
  quantidadeAPI: 0,
  itensRecebidosAPI: 0,
  motivoSemSinal: 'A aguardar resultados.',
  totalResultados: 0,
  totalSinais: 0
};

let syncTimer = null;
let keepAliveTimer = null;
let encerrando = false;

// --------------------------------------------------
// BASE DE DADOS
// --------------------------------------------------

async function ensureSchema() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS bacbo_resultados (
      id TEXT PRIMARY KEY,
      resultado TEXT NOT NULL,
      hora TIMESTAMPTZ NOT NULL,
      dados JSONB DEFAULT '{}'::jsonb,
      criado_em TIMESTAMPTZ DEFAULT NOW()
    )
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS sinais (
      id BIGSERIAL PRIMARY KEY,
      rodada_id TEXT UNIQUE NOT NULL,
      entrada TEXT NOT NULL,
      prioridade INTEGER NOT NULL DEFAULT 0,
      status TEXT NOT NULL DEFAULT 'PENDENTE',
      resultado_real TEXT,
      gale INTEGER NOT NULL DEFAULT 0,
      criado_em TIMESTAMPTZ DEFAULT NOW(),
      atualizado_em TIMESTAMPTZ DEFAULT NOW()
    )
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_bacbo_hora
    ON bacbo_resultados (hora DESC)
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_sinais_criado
    ON sinais (criado_em DESC)
  `);

  console.log('[DB] Tabelas verificadas.');
}

// --------------------------------------------------
// TELEGRAM
// --------------------------------------------------

async function telegram(texto) {
  const url = `https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`;

  const response = await axios.post(
    url,
    {
      chat_id: CHAT_ID,
      text: texto,
      disable_web_page_preview: true
    },
    { timeout: 15000 }
  );

  if (!response.data || response.data.ok !== true) {
    throw new Error('Telegram não confirmou o envio.');
  }

  estado.ultimaMensagemTelegram = new Date().toISOString();
  return response.data;
}

function verificarAdmin(req, res, next) {
  const token = req.headers['x-admin-token'];

  if (!token || token !== ADMIN_TOKEN) {
    return res.status(401).json({
      ok: false,
      erro: 'Não autorizado.'
    });
  }

  next();
}

// --------------------------------------------------
// NORMALIZAÇÃO DOS RESULTADOS DA API
// --------------------------------------------------

function procurarLista(obj) {
  if (Array.isArray(obj)) return obj;
  if (!obj || typeof obj !== 'object') return [];

  const chaves = [
    'data',
    'items',
    'results',
    'result',
    'records',
    'rounds',
    'events',
    'history',
    'content'
  ];

  for (const chave of chaves) {
    if (obj[chave] !== undefined) {
      const lista = procurarLista(obj[chave]);
      if (lista.length) return lista;
    }
  }

  return [];
}

function normalizarResultado(valor) {
  if (valor === null || valor === undefined) return null;

  const s = String(valor).trim().toLowerCase();

  if (
    ['player', 'jogador', 'blue', 'azul', 'playerwin'].includes(s)
  ) return 'JOGADOR';

  if (
    ['banker', 'banca', 'red', 'vermelho', 'bankerwin'].includes(s)
  ) return 'BANCA';

  if (
    ['tie', 'draw', 'empate', 'egal'].includes(s)
  ) return 'EMPATE';

  return null;
}

function normalizarRodada(item) {
  if (!item || typeof item !== 'object') return null;

  const id =
    item.id ??
    item.roundId ??
    item.round_id ??
    item.gameId ??
    item.game_id ??
    item.eventId ??
    item.event_id ??
    item.issueNumber ??
    item.issue_number;

  const estadoRodada = String(
    item.status ?? item.state ?? item.gameStatus ?? ''
  ).toLowerCase();

  if (
    estadoRodada &&
    !['resolved', 'finished', 'complete', 'completed', 'settled', 'ended', 'closed'].includes(estadoRodada)
  ) {
    return null;
  }

  let resultado = null;

  const camposResultado = [
    item.winner,
    item.outcome,
    item.result,
    item.win,
    item.winningSide,
    item.winning_side,
    item.resultName,
    item.result_name
  ];

  for (const campo of camposResultado) {
    resultado = normalizarResultado(
      typeof campo === 'object' && campo !== null
        ? campo.name ?? campo.value ?? campo.winner
        : campo
    );

    if (resultado) break;
  }

  if (!resultado) return null;

  const tempoBruto =
    item.timestamp ??
    item.time ??
    item.createdAt ??
    item.created_at ??
    item.endTime ??
    item.end_time ??
    item.settledAt ??
    item.settled_at;

  let hora = tempoBruto ? new Date(tempoBruto) : new Date();

  if (Number.isNaN(hora.getTime())) {
    hora = new Date();
  }

  if (id === undefined || id === null) return null;

  return {
    id: String(id),
    resultado,
    hora: hora.toISOString(),
    dados: item
  };
}

// --------------------------------------------------
// CONSULTA À API
// --------------------------------------------------

async function obterResultadosAPI() {
  const response = await axios.get(API_BACBO, {
    timeout: 15000,
    headers: {
      Accept: 'application/json',
      'User-Agent': 'CapungaDadosBot/1.0'
    }
  });

  estado.httpAPI = response.status;

  const lista = procurarLista(response.data);
  estado.itensRecebidosAPI = lista.length;

  const normalizados = lista
    .map(normalizarRodada)
    .filter(Boolean);

  const unicos = new Map();

  for (const rodada of normalizados) {
    unicos.set(rodada.id, rodada);
  }

  const resultados = [...unicos.values()]
    .sort((a, b) => new Date(a.hora) - new Date(b.hora));

  estado.quantidadeAPI = resultados.length;
  estado.erroAPI = null;

  return resultados;
}

// --------------------------------------------------
// HISTÓRICO E ESTATÍSTICAS
// --------------------------------------------------

async function buscarHistorico(limite = 30) {
  const r = await pool.query(
    `SELECT id, resultado, hora
     FROM bacbo_resultados
     ORDER BY hora DESC
     LIMIT $1`,
    [limite]
  );

  return r.rows;
}

async function atualizarContadores() {
  const resultados = await pool.query(
    'SELECT COUNT(*)::int AS total FROM bacbo_resultados'
  );

  const sinais = await pool.query(
    'SELECT COUNT(*)::int AS total FROM sinais'
  );

  estado.totalResultados = resultados.rows[0].total;
  estado.totalSinais = sinais.rows[0].total;
}

// --------------------------------------------------
// ESTRATÉGIA DE SINAL
// --------------------------------------------------

function decidir(historico) {
  /*
   * Filtro inicial conservador:
   * - exige histórico suficiente;
   * - utiliza os últimos resultados;
   * - exclui empates da contagem de cores;
   * - exige prioridade mínima.
   *
   * Isto é apenas uma regra experimental, não uma
   * previsão garantida do próximo resultado.
   */

  if (historico.length < 5) {
    return {
      entrada: null,
      prioridade: 0,
      motivo: 'Histórico insuficiente.'
    };
  }

  const ultimos = historico
    .slice(0, 5)
    .map(r => r.resultado)
    .filter(r => r === 'BANCA' || r === 'JOGADOR');

  if (ultimos.length < 4) {
    return {
      entrada: null,
      prioridade: 0,
      motivo: 'Poucos resultados válidos para avaliar.'
    };
  }

  const banca = ultimos.filter(r => r === 'BANCA').length;
  const jogador = ultimos.filter(r => r === 'JOGADOR').length;

  const diferenca = Math.abs(banca - jogador);

  if (diferenca < CONSENSO_MIN) {
    return {
      entrada: null,
      prioridade: 0,
      motivo: 'Sem consenso suficiente no histórico.'
    };
  }

  const entrada = banca > jogador ? 'BANCA' : 'JOGADOR';
  const prioridade = Math.min(
    95,
    70 + diferenca * 7
  );

  if (prioridade < MIN_PRIORIDADE) {
    return {
      entrada: null,
      prioridade,
      motivo: 'Prioridade abaixo do mínimo configurado.'
    };
  }

  return {
    entrada,
    prioridade,
    motivo: 'Critérios experimentais atingidos.'
  };
}

async function criarSinal(rodadaId, decisao) {
  const existente = await pool.query(
    'SELECT id FROM sinais WHERE rodada_id = $1',
    [rodadaId]
  );

  if (existente.rowCount > 0) return false;

  const inserido = await pool.query(
    `INSERT INTO sinais
       (rodada_id, entrada, prioridade, status)
     VALUES ($1, $2, $3, 'PENDENTE')
     ON CONFLICT (rodada_id) DO NOTHING
     RETURNING id`,
    [rodadaId, decisao.entrada, decisao.prioridade]
  );

  if (!inserido.rowCount) return false;

  const texto =
    '🎯 CAPUNGA DADOS — BAC BO\n\n' +
    `📍 Entrada: ${decisao.entrada}\n` +
    `📊 Prioridade do filtro: ${decisao.prioridade}%\n` +
    '🛡️ Proteção: Gale 1\n' +
    (EMPATE_PROTEGIDO
      ? '🤝 Empate: protegido na avaliação\n'
      : '') +
    `⏳ Validade indicativa: ${SINAL_EXPIRA_MIN} min\n\n` +
    '⚠️ Análise experimental. Não garante vitória.';

  try {
    await telegram(texto);
    console.log('[SINAL ENVIADO]', decisao.entrada, rodadaId);
  } catch (e) {
    console.error('[TELEGRAM SINAL]', e.message);
  }

  return true;
}

// --------------------------------------------------
// AVALIAÇÃO DOS RESULTADOS NOVOS
// --------------------------------------------------

async function avaliarResultado(rodada) {
  const anterior = await pool.query(
    'SELECT id FROM bacbo_resultados WHERE id = $1',
    [rodada.id]
  );

  if (anterior.rowCount > 0) return;

  await pool.query(
    `INSERT INTO bacbo_resultados (id, resultado, hora, dados)
     VALUES ($1, $2, $3, $4::jsonb)
     ON CONFLICT (id) DO NOTHING`,
    [
      rodada.id,
      rodada.resultado,
      rodada.hora,
      JSON.stringify(rodada.dados)
    ]
  );

  estado.ultimoResultado = {
    id: rodada.id,
    resultado: rodada.resultado,
    hora: rodada.hora
  };

  // Atualiza o sinal pendente mais recente.
  const pendente = await pool.query(
    `SELECT id, entrada, gale
     FROM sinais
     WHERE status = 'PENDENTE'
     ORDER BY criado_em DESC
     LIMIT 1`
  );

  if (pendente.rowCount > 0) {
    const sinal = pendente.rows[0];

    if (rodada.resultado === 'EMPATE' && EMPATE_PROTEGIDO) {
      // Mantém o sinal aberto para avaliação posterior.
    } else if (rodada.resultado === sinal.entrada) {
      await pool.query(
        `UPDATE sinais
         SET status = $1, resultado_real = $2,
             atualizado_em = NOW()
         WHERE id = $3`,
        [
          sinal.gale === 0 ? 'GREEN' : 'G1',
          rodada.resultado,
          sinal.id
        ]
      );
    } else if (sinal.gale < 1) {
      await pool.query(
        `UPDATE sinais
         SET gale = 1, atualizado_em = NOW()
         WHERE id = $1`,
        [sinal.id]
      );
    } else {
      await pool.query(
        `UPDATE sinais
         SET status = 'LOSS', resultado_real = $1,
             atualizado_em = NOW()
         WHERE id = $2`,
        [rodada.resultado, sinal.id]
      );
    }
  }

  const historico = await buscarHistorico(10);
  const decisao = decidir(historico);

  if (SINAIS_ATIVOS && decisao.entrada) {
    await criarSinal(rodada.id, decisao);
    estado.motivoSemSinal = 'Critérios avaliados.';
  } else {
    estado.motivoSemSinal = decisao.motivo;
  }
}

// --------------------------------------------------
// SINCRONIZAÇÃO
// --------------------------------------------------

async function syncOnce() {
  if (estado.sincronizando || encerrando) return;

  estado.sincronizando = true;

  try {
    const resultados = await obterResultadosAPI();

    if (!resultados.length) {
      estado.motivoSemSinal =
        'A API não devolveu resultados reconhecidos. Verificar o formato dos dados.';
      console.warn('[SYNC] Nenhum resultado normalizado.');
      return;
    }

    // Na primeira sincronização, importa o histórico sem
    // criar sinais retroativos.
    const total = await pool.query(
      'SELECT COUNT(*)::int AS total FROM bacbo_resultados'
    );

    if (total.rows[0].total === 0) {
      for (const rodada of resultados) {
        await pool.query(
          `INSERT INTO bacbo_resultados (id, resultado, hora, dados)
           VALUES ($1, $2, $3, $4::jsonb)
           ON CONFLICT (id) DO NOTHING`,
          [
            rodada.id,
            rodada.resultado,
            rodada.hora,
            JSON.stringify(rodada.dados)
          ]
        );
      }

      console.log(`[PRIMEIRA SYNC] Importados ${resultados.length} resultados.`);
    } else {
      for (const rodada of resultados) {
        await avaliarResultado(rodada);
      }
    }

    estado.ultimaSincronizacao = new Date().toISOString();
    estado.erroAPI = null;

    await atualizarContadores();
  } catch (e) {
    estado.erroAPI = e.response
      ? `HTTP ${e.response.status}: ${e.message}`
      : e.message;

    estado.motivoSemSinal = 'Erro durante a sincronização.';

    console.error('[SYNC ERRO]', estado.erroAPI);
  } finally {
    estado.sincronizando = false;
  }
}

// --------------------------------------------------
// ROTAS PÚBLICAS
// --------------------------------------------------

app.get('/', (req, res) => {
  res.json({
    nome: 'Capunga Dados Bac bo',
    estado: estado.iniciado ? 'online' : 'a iniciar',
    rotas: [
      '/health',
      '/diagnostico',
      '/resultados',
      '/sinais',
      '/estatisticas'
    ]
  });
});

app.get('/health', (req, res) => {
  res.json({
    ok: true,
    iniciado: estado.iniciado,
    hora: new Date().toISOString()
  });
});

app.get('/diagnostico', async (req, res) => {
  await atualizarContadores().catch(() => {});

  res.json({
    nome: 'Capunga Dados Bac bo',
    iniciado: estado.iniciado,
    sincronizando: estado.sincronizando,
    ultimaSincronizacao: estado.ultimaSincronizacao,
    ultimoResultado: estado.ultimoResultado,
    ultimaMensagemTelegram: estado.ultimaMensagemTelegram,
    httpAPI: estado.httpAPI,
    quantidadeAPI: estado.quantidadeAPI,
    itensRecebidosAPI: estado.itensRecebidosAPI,
    erroAPI: estado.erroAPI,
    motivoSemSinal: estado.motivoSemSinal,
    baseDados: {
      totalResultados: estado.totalResultados,
      totalSinais: estado.totalSinais
    },
    sinaisAtivos: SINAIS_ATIVOS,
    empateProtegido: EMPATE_PROTEGIDO
  });
});

app.get('/resultados', async (req, res) => {
  try {
    const limite = Math.max(
      1,
      Math.min(200, Number(req.query.limite) || 50)
    );

    const r = await pool.query(
      `SELECT id, resultado, hora
       FROM bacbo_resultados
       ORDER BY hora DESC
       LIMIT $1`,
      [limite]
    );

    res.json({ ok: true, total: r.rowCount, resultados: r.rows });
  } catch (e) {
    res.status(500).json({ ok: false, erro: e.message });
  }
});

app.get('/sinais', async (req, res) => {
  try {
    const limite = Math.max(
      1,
      Math.min(200, Number(req.query.limite) || 50)
    );

    const r = await pool.query(
      `SELECT id, rodada_id, entrada, prioridade, status,
              resultado_real, gale, criado_em, atualizado_em
       FROM sinais
       ORDER BY criado_em DESC
       LIMIT $1`,
      [limite]
    );

    res.json({ ok: true, total: r.rowCount, sinais: r.rows });
  } catch (e) {
    res.status(500).json({ ok: false, erro: e.message });
  }
});

app.get('/estatisticas', async (req, res) => {
  try {
    const r = await pool.query(`
      SELECT status, COUNT(*)::int AS total
      FROM sinais
      GROUP BY status
      ORDER BY status
    `);

    res.json({
      ok: true,
      estatisticas: r.rows
    });
  } catch (e) {
    res.status(500).json({ ok: false, erro: e.message });
  }
});

// --------------------------------------------------
// ROTAS ADMINISTRATIVAS
// --------------------------------------------------

app.post('/telegram/teste', verificarAdmin, async (req, res) => {
  try {
    await telegram('✅ Teste do Capunga Dados Bac bo: Telegram ligado.');
    res.json({ ok: true, mensagem: 'Teste enviado.' });
  } catch (e) {
    res.status(500).json({ ok: false, erro: e.message });
  }
});

app.post('/db/sincronizar', verificarAdmin, async (req, res) => {
  await syncOnce();

  res.json({
    ok: !estado.erroAPI,
    ultimaSincronizacao: estado.ultimaSincronizacao,
    erroAPI: estado.erroAPI
  });
});

// --------------------------------------------------
// ARRANQUE E ENCERRAMENTO
// --------------------------------------------------

async function start() {
  await ensureSchema();
  await atualizarContadores();

  const server = app.listen(PORT, '0.0.0.0', () => {
    estado.iniciado = true;

    console.log(`[HTTP] Servidor ativo na porta ${PORT}`);
    console.log('[BOT] Capunga Dados Bac bo iniciado.');
  });

  await syncOnce();

  syncTimer = setInterval(() => {
    syncOnce().catch(e => console.error('[SYNC TIMER]', e.message));
  }, INTERVALO_SYNC_MS);

  // Mantém a ligação à base de dados ativa.
  // Não garante que o Render gratuito fique sempre acordado.
  keepAliveTimer = setInterval(async () => {
    try {
      await pool.query('SELECT 1');
    } catch (e) {
      console.error('[KEEPALIVE DB]', e.message);
    }
  }, 4 * 60 * 1000);

  async function shutdown() {
    if (encerrando) return;
    encerrando = true;

    if (syncTimer) clearInterval(syncTimer);
    if (keepAliveTimer) clearInterval(keepAliveTimer);

    server.close();

    await pool.end();
    process.exit(0);
  }

  process.on('SIGTERM', () => shutdown().catch(() => process.exit(1)));
  process.on('SIGINT', () => shutdown().catch(() => process.exit(1)));
}

start().catch(e => {
  console.error('[FATAL]', e);
  process.exit(1);
});

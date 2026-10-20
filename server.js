'use strict';

/**
 * CAPUNGA DADOS — BAC BO
 * Telegram + PostgreSQL + sincronização de resultados
 *
 * - Gale máximo: 2
 * - Empate protegido: fecha o sinal como GREEN
 * - Intervalo mínimo entre sinais: 120 segundos
 * - Não utiliza slots fixos de 3 em 3 minutos
 * - Só processa resultados reconhecidos e finalizados
 * - Regista sinais e resultados na base de dados
 * - Endpoints de saúde e diagnóstico
 *
 * Variáveis obrigatórias:
 * DATABASE_URL
 * BOT_TOKEN
 * CHAT_ID
 *
 * Opcional:
 * API_BACBO
 * PORT
 * INTERVALO_SYNC_MS
 * MIN_RODADAS_ANALISE
 * INTERVALO_MIN_SINAL_MS
 * SINAIS_ATIVOS
 * EMPATE_PROTEGIDO
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

const API_BACBO =
  process.env.API_BACBO ||
  'https://api-cs.casino.org/svc-evolution-game-events/api/bacbo';

const INTERVALO_SYNC_MS = limitar(
  process.env.INTERVALO_SYNC_MS,
  6000,
  4000,
  30000
);

const INTERVALO_MIN_SINAL_MS = limitar(
  process.env.INTERVALO_MIN_SINAL_MS,
  120000,
  120000,
  180000
);

const MIN_RODADAS_ANALISE = limitar(
  process.env.MIN_RODADAS_ANALISE,
  12,
  8,
  100
);

const SINAIS_ATIVOS = boolEnv(
  process.env.SINAIS_ATIVOS,
  true
);

const EMPATE_PROTEGIDO = boolEnv(
  process.env.EMPATE_PROTEGIDO,
  true
);

function limitar(valor, padrao, minimo, maximo) {
  const n = Number.parseInt(valor, 10);

  if (!Number.isFinite(n)) {
    return padrao;
  }

  return Math.max(minimo, Math.min(maximo, n));
}

function boolEnv(valor, padrao) {
  if (valor === undefined || valor === '') {
    return padrao;
  }

  return [
    'true',
    '1',
    'yes',
    'sim',
    'on'
  ].includes(String(valor).trim().toLowerCase());
}

const pool = DATABASE_URL
  ? new Pool({
      connectionString: DATABASE_URL,
      ssl: /localhost|127\.0\.0\.1/i.test(DATABASE_URL)
        ? false
        : { rejectUnauthorized: false },
      max: 5,
      connectionTimeoutMillis: 15000,
      idleTimeoutMillis: 30000
    })
  : null;

const runtime = {
  iniciadoEm: new Date().toISOString(),
  ultimaSincronizacao: null,
  ultimoResultado: null,
  ultimoSinal: null,
  ultimoErroAPI: null,
  ultimoErroDB: null,
  ultimoErroTelegram: null,
  resultadosRecebidos: 0,
  resultadosNovos: 0,
  sinaisGerados: 0,
  sincronizacaoEmCurso: false
};

let server = null;
let syncTimer = null;
let syncEmCurso = false;
let encerrando = false;

/* =====================================================
   RESULTADOS: NORMALIZAÇÃO
===================================================== */

function normalizarResultado(valor) {
  if (valor && typeof valor === 'object') {
    valor =
      valor.outcome ??
      valor.winner ??
      valor.resultado ??
      valor.result ??
      valor.name;
  }

  const s = String(valor ?? '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .trim()
    .toLowerCase()
    .replace(/[\s_-]+/g, '');

  if ([
    'banca',
    'banker',
    'bankerwin',
    'bankerwins',
    'bankerwon',
    'red',
    'vermelho'
  ].includes(s)) {
    return 'Banca';
  }

  if ([
    'jogador',
    'player',
    'playerwin',
    'playerwins',
    'playerwon',
    'blue',
    'azul'
  ].includes(s)) {
    return 'Jogador';
  }

  if ([
    'empate',
    'tie',
    'draw',
    'equal',
    'push',
    'tied'
  ].includes(s)) {
    return 'Empate';
  }

  return null;
}

function dataISO(valor) {
  if (!valor) {
    return null;
  }

  const data = new Date(valor);

  if (Number.isNaN(data.getTime())) {
    return null;
  }

  return data.toISOString();
}

function extrairItens(body) {
  if (Array.isArray(body)) {
    return body;
  }

  if (!body || typeof body !== 'object') {
    return [];
  }

  const candidatos = [
    body.data,
    body.results,
    body.items,
    body.rounds,
    body.events,
    body.resultados,
    body.data?.results,
    body.data?.items,
    body.data?.rounds,
    body.data?.events,
    body.data?.resultados
  ];

  return candidatos.find(Array.isArray) || [];
}

function normalizarRodada(item) {
  if (!item || typeof item !== 'object') {
    return null;
  }

  const d = item.data || item.round || item;

  const resultadoObjeto =
    d.result && typeof d.result === 'object'
      ? d.result
      : {};

  const resultadoRaw =
    resultadoObjeto.outcome ??
    resultadoObjeto.winner ??
    resultadoObjeto.resultado ??
    d.outcome ??
    d.winner ??
    d.resultado ??
    (typeof d.result === 'string' ? d.result : null) ??
    item.outcome ??
    item.winner ??
    item.resultado ??
    (typeof item.result === 'string' ? item.result : null);

  const resultado = normalizarResultado(resultadoRaw);

  const idRaw =
    d.id ??
    d.roundId ??
    d.round_id ??
    d.eventId ??
    item.id ??
    item.roundId ??
    item.round_id ??
    item.eventId;

  const fimRaw =
    d.settledAt ??
    d.endTime ??
    d.endedAt ??
    d.finishedAt ??
    d.fim ??
    item.settledAt ??
    item.endTime ??
    item.finishedAt ??
    item.fim;

  const inicioRaw =
    d.startedAt ??
    d.startTime ??
    d.started_at ??
    d.inicio ??
    item.startedAt ??
    item.startTime ??
    item.inicio;

  const status = String(
    d.status ??
    d.estado ??
    item.status ??
    item.estado ??
    ''
  ).toLowerCase();

  const fim = dataISO(fimRaw);
  const inicio = dataISO(inicioRaw);

  const finalizada =
    /resolved|settled|finished|complete|completed|resolvido|concluido|finalizado/
      .test(status) || Boolean(fim);

  if (!idRaw || !resultado || !fim || !finalizada) {
    return null;
  }

  return {
    id: String(idRaw),
    resultado,
    inicio: inicio || fim,
    fim
  };
}

/* =====================================================
   TELEGRAM
===================================================== */

async function enviarTelegram(texto) {
  if (!BOT_TOKEN || !CHAT_ID) {
    runtime.ultimoErroTelegram =
      'BOT_TOKEN ou CHAT_ID não configurado';

    return null;
  }

  try {
    const resposta = await axios.post(
      `https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`,
      {
        chat_id: CHAT_ID,
        text: texto,
        disable_web_page_preview: true
      },
      { timeout: 15000 }
    );

    if (resposta.data?.ok !== true) {
      throw new Error(
        JSON.stringify(resposta.data)
      );
    }

    runtime.ultimoErroTelegram = null;

    return resposta.data.result?.message_id ?? true;
  } catch (erro) {
    runtime.ultimoErroTelegram =
      erro.response?.data
        ? JSON.stringify(erro.response.data)
        : erro.message;

    console.error(
      '[TELEGRAM]',
      runtime.ultimoErroTelegram
    );

    return null;
  }
}

/* =====================================================
   BASE DE DADOS
===================================================== */

async function garantirTabelas() {
  if (!pool) {
    throw new Error('DATABASE_URL não configurada');
  }

  await pool.query(`
    CREATE TABLE IF NOT EXISTS bacbo_resultados (
      id TEXT PRIMARY KEY,
      resultado TEXT NOT NULL,
      inicio TIMESTAMPTZ,
      fim TIMESTAMPTZ NOT NULL,
      guardado_em TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);

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

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_sinais_estado
    ON sinais (estado)
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_sinais_criado
    ON sinais (criado_em DESC)
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_resultados_fim
    ON bacbo_resultados (fim DESC)
  `);

  console.log('[DB] Tabelas verificadas');
}

async function guardarRodadas(rodadas) {
  const novas = [];

  for (const rodada of rodadas) {
    const resultado = await pool.query(
      `
      INSERT INTO bacbo_resultados
        (id, resultado, inicio, fim)
      VALUES ($1, $2, $3, $4)
      ON CONFLICT (id) DO NOTHING
      RETURNING id, resultado, inicio, fim
      `,
      [
        rodada.id,
        rodada.resultado,
        rodada.inicio,
        rodada.fim
      ]
    );

    if (resultado.rowCount > 0) {
      novas.push(resultado.rows[0]);
    }
  }

  return novas;
}

async function obterHistorico(limite = 100) {
  const resultado = await pool.query(
    `
    SELECT id, resultado, inicio, fim
    FROM bacbo_resultados
    ORDER BY fim DESC
    LIMIT $1
    `,
    [limite]
  );

  return resultado.rows;
}

/* =====================================================
   ANÁLISE
===================================================== */

/*
 * Esta análise é apenas um filtro heurístico.
 * A pontuação não representa probabilidade real.
 * Não existe garantia de vitória no Bac Bo.
 */

function analisarHistorico(historico) {
  const decisivos = historico
    .map((r) => r.resultado)
    .filter((r) => r === 'Banca' || r === 'Jogador');

  if (decisivos.length < MIN_RODADAS_ANALISE) {
    return null;
  }

  const recentes = decisivos.slice(0, 12);

  const banca = recentes.filter(
    (r) => r === 'Banca'
  ).length;

  const jogador = recentes.filter(
    (r) => r === 'Jogador'
  ).length;

  let sequenciaCor = recentes[0];
  let sequenciaTamanho = 1;

  for (let i = 1; i < decisivos.length; i++) {
    if (decisivos[i] !== sequenciaCor) {
      break;
    }

    sequenciaTamanho++;
  }

  let cor = null;
  let estrategia = '';

  /*
   * O filtro só aceita um sinal quando existe
   * uma condição definida. Não força entrada
   * apenas para preencher o intervalo.
   */

  if (sequenciaTamanho >= 3 && sequenciaTamanho <= 5) {
    cor = sequenciaCor;
    estrategia = 'SEQUENCIA';
  } else if (banca >= 9) {
    cor = 'Banca';
    estrategia = 'MAIORIA_RECENTE';
  } else if (jogador >= 9) {
    cor = 'Jogador';
    estrategia = 'MAIORIA_RECENTE';
  }

  if (!cor) {
    return null;
  }

  return {
    cor,
    estrategia,
    amostra: decisivos.length
  };
}

/* =====================================================
   SINAIS E GALES
===================================================== */

async function obterSinalPendente() {
  const resultado = await pool.query(`
    SELECT *
    FROM sinais
    WHERE estado = 'PENDENTE'
    ORDER BY id DESC
    LIMIT 1
  `);

  return resultado.rows[0] || null;
}

async function atualizarSinal(sinal, rodada) {
  if (!sinal || !rodada) {
    return;
  }

  if (rodada.id === sinal.resultado_id) {
    return;
  }

  if (rodada.resultado === 'Empate') {
    if (EMPATE_PROTEGIDO) {
      await pool.query(
        `
        UPDATE sinais
        SET estado = 'GREEN',
            resultado_id = $1,
            resultado_final = 'Empate protegido',
            empates = empates + 1
        WHERE id = $2
          AND estado = 'PENDENTE'
        `,
        [rodada.id, sinal.id]
      );

      await enviarTelegram(
        '🟢 GREEN — EMPATE PROTEGIDO\n' +
        'O empate encerrou o sinal.'
      );

      runtime.ultimoResultado = 'Empate';
      return;
    }

    await pool.query(
      `
      UPDATE sinais
      SET resultado_id = $1,
          empates = empates + 1
      WHERE id = $2
        AND estado = 'PENDENTE'
      `,
      [rodada.id, sinal.id]
    );

    return;
  }

  if (rodada.resultado === sinal.cor) {
    const estadoGreen = sinal.gale === 0
      ? 'GREEN'
      : `G${sinal.gale}`;

    await pool.query(
      `
      UPDATE sinais
      SET estado = $1,
          resultado_id = $2,
          resultado_final = $3
      WHERE id = $4
        AND estado = 'PENDENTE'
      `,
      [
        estadoGreen,
        rodada.id,
        rodada.resultado,
        sinal.id
      ]
    );

    await enviarTelegram(
      `🟢 ${estadoGreen}\n` +
      `Entrada: ${sinal.cor}\n` +
      `Resultado: ${rodada.resultado}`
    );

    runtime.ultimoResultado = rodada.resultado;
    return;
  }

  if (sinal.gale < 2) {
    const novoGale = sinal.gale + 1;

    await pool.query(
      `
      UPDATE sinais
      SET gale = $1,
          resultado_id = $2
      WHERE id = $3
        AND estado = 'PENDENTE'
      `,
      [novoGale, rodada.id, sinal.id]
    );

    const mensagemId = await enviarTelegram(
      `🔁 GALE ${novoGale}\n` +
      `Entrada original: ${sinal.cor}\n` +
      `Aguardando o próximo resultado.`
    );

    runtime.ultimoResultado = rodada.resultado;

    return mensagemId;
  }

  await pool.query(
    `
    UPDATE sinais
    SET estado = 'LOSS',
        resultado_id = $1,
        resultado_final = $2
    WHERE id = $3
      AND estado = 'PENDENTE'
    `,
    [rodada.id, rodada.resultado, sinal.id]
  );

  await enviarTelegram(
    '🔴 LOSS\n' +
    'O sinal terminou após o Gale 2.'
  );

  runtime.ultimoResultado = rodada.resultado;
}

async function criarSinalSeElegivel() {
  if (!SINAIS_ATIVOS) {
    return;
  }

  const pendente = await obterSinalPendente();

  if (pendente) {
    return;
  }

  const ultimo = await pool.query(`
    SELECT criado_em
    FROM sinais
    ORDER BY id DESC
    LIMIT 1
  `);

  if (ultimo.rows.length > 0) {
    const instante = new Date(
      ultimo.rows[0].criado_em
    ).getTime();

    if (
      Date.now() - instante <
      INTERVALO_MIN_SINAL_MS
    ) {
      return;
    }
  }

  const historico = await obterHistorico(100);
  const decisao = analisarHistorico(historico);

  if (!decisao) {
    return;
  }

  const inserido = await pool.query(
    `
    INSERT INTO sinais
      (cor, estado, gale, estrategia)
    VALUES ($1, 'PENDENTE', 0, $2)
    RETURNING id, criado_em
    `,
    [decisao.cor, decisao.estrategia]
  );

  const sinal = inserido.rows[0];

  runtime.ultimoSinal = {
    id: sinal.id,
    cor: decisao.cor,
    estrategia: decisao.estrategia,
    criadoEm: sinal.criado_em
  };

  runtime.sinaisGerados++;

  await enviarTelegram(
    '🎲 CAPUNGA DADOS — BAC BO\n\n' +
    `🎯 Entrada: ${decisao.cor}\n` +
    '🛡️ Proteção: Empate\n' +
    '🔁 Gale máximo: 2\n' +
    `📊 Filtro: ${decisao.estrategia}\n\n` +
    '⚠️ Sinal estatístico, sem garantia de vitória.'
  );

  console.log('[SINAL]', runtime.ultimoSinal);
}

/* =====================================================
   SINCRONIZAÇÃO DA API
===================================================== */

async function sincronizar() {
  if (syncEmCurso || encerrando) {
    return;
  }

  syncEmCurso = true;
  runtime.sincronizacaoEmCurso = true;

  try {
    if (!pool) {
      throw new Error('DATABASE_URL não configurada');
    }

    const resposta = await axios.get(API_BACBO, {
      timeout: 15000,
      headers: {
        Accept: 'application/json',
        'User-Agent': 'CapungaDadosBacBo/1.0'
      }
    });

    runtime.ultimoErroAPI = null;

    const itens = extrairItens(resposta.data);

    runtime.resultadosRecebidos = itens.length;

    if (itens.length === 0) {
      throw new Error(
        'A API não devolveu uma lista de rodadas reconhecida. ' +
        'É necessário confirmar o formato real da resposta.'
      );
    }

    const rodadas = itens
      .map(normalizarRodada)
      .filter(Boolean)
      .sort(
        (a, b) =>
          new Date(a.fim).getTime() -
          new Date(b.fim).getTime()
      );

    if (rodadas.length === 0) {
      throw new Error(
        'Nenhuma rodada finalizada foi reconhecida. ' +
        'Nenhum sinal foi criado neste ciclo.'
      );
    }

    const novas = await guardarRodadas(rodadas);

    runtime.resultadosNovos = novas.length;
    runtime.ultimaSincronizacao = new Date().toISOString();

    /*
     * Processa as rodadas novas em ordem cronológica.
     * Assim não confunde a rodada mais recente com
     * uma rodada antiga já guardada na base de dados.
     */

    for (const rodada of novas) {
      const sinal = await obterSinalPendente();

      if (sinal) {
        await atualizarSinal(sinal, rodada);
      }

      runtime.ultimoResultado = rodada.resultado;
    }

    /*
     * Só tenta criar um sinal depois de sincronizar
     * resultados reconhecidos e gravados.
     */

    await criarSinalSeElegivel();

    runtime.ultimoErroDB = null;
  } catch (erro) {
    const mensagem =
      erro.response?.status
        ? `API HTTP ${erro.response.status}: ${erro.message}`
        : erro.message;

    runtime.ultimoErroAPI = mensagem;

    console.error('[SYNC]', mensagem);
  } finally {
    syncEmCurso = false;
    runtime.sincronizacaoEmCurso = false;
  }
}

/* =====================================================
   ROTAS HTTP
===================================================== */

app.get('/', (_req, res) => {
  res.json({
    nome: 'CAPUNGA DADOS — BAC BO',
    estado: 'online',
    hora: new Date().toISOString()
  });
});

app.get('/health', (_req, res) => {
  res.json({
    estado: encerrando ? 'encerrando' : 'online',
    baseConfigurada: Boolean(DATABASE_URL),
    telegramConfigurado: Boolean(BOT_TOKEN && CHAT_ID),
    apiConfigurada: Boolean(API_BACBO),
    sincronizacaoEmCurso: runtime.sincronizacaoEmCurso,
    ultimaSincronizacao: runtime.ultimaSincronizacao,
    ultimoErroAPI: runtime.ultimoErroAPI
  });
});

app.get('/status', async (_req, res) => {
  let dbOk = false;
  let totais = null;

  try {
    if (pool) {
      await pool.query('SELECT 1');

      const resultado = await pool.query(`
        SELECT estado, COUNT(*)::int AS quantidade
        FROM sinais
        GROUP BY estado
      `);

      dbOk = true;
      totais = resultado.rows;
    }
  } catch (erro) {
    runtime.ultimoErroDB = erro.message;
  }

  res.json({
    nome: 'CAPUNGA DADOS — BAC BO',
    estado: 'online',
    iniciadoEm: runtime.iniciadoEm,
    ultimaSincronizacao: runtime.ultimaSincronizacao,
    ultimoResultado: runtime.ultimoResultado,
    ultimoSinal: runtime.ultimoSinal,
    resultadosRecebidos: runtime.resultadosRecebidos,
    resultadosNovos: runtime.resultadosNovos,
    sinaisGerados: runtime.sinaisGerados,
    baseDadosOK: dbOk,
    totaisSinais: totais,
    ultimoErroAPI: runtime.ultimoErroAPI,
    ultimoErroDB: runtime.ultimoErroDB,
    ultimoErroTelegram: runtime.ultimoErroTelegram
  });
});

/* =====================================================
   ARRANQUE E ENCERRAMENTO
===================================================== */

async function iniciar() {
  if (!DATABASE_URL) {
    throw new Error('Configura DATABASE_URL no servidor.');
  }

  if (!BOT_TOKEN) {
    throw new Error('Configura BOT_TOKEN no servidor.');
  }

  if (!CHAT_ID) {
    throw new Error('Configura CHAT_ID no servidor.');
  }

  await garantirTabelas();

  server = app.listen(PORT, () => {
    console.log(
      `[HTTP] Servidor iniciado na porta ${PORT}`
    );
  });

  /*
   * Executa uma sincronização inicial e depois
   * continua a verificar periodicamente.
   */

  await sincronizar();

  syncTimer = setInterval(() => {
    sincronizar().catch((erro) => {
      console.error('[SYNC TIMER]', erro.message);
    });
  }, INTERVALO_SYNC_MS);

  if (typeof syncTimer.unref === 'function') {
    syncTimer.unref();
  }

  console.log('[BOT] CAPUNGA DADOS iniciado');
}

async function encerrar() {
  if (encerrando) {
    return;
  }

  encerrando = true;

  if (syncTimer) {
    clearInterval(syncTimer);
    syncTimer = null;
  }

  if (server) {
    await new Promise((resolve) => {
      server.close(resolve);
    });
  }

  if (pool) {
    await pool.end();
  }

  console.log('[BOT] Servidor encerrado');
}

process.on('SIGTERM', () => {
  encerrar().catch((erro) => {
    console.error('[SHUTDOWN SIGTERM]', erro);
    process.exitCode = 1;
  });
});

process.on('SIGINT', () => {
  encerrar().catch((erro) => {
    console.error('[SHUTDOWN SIGINT]', erro);
    process.exitCode = 1;
  });
});

iniciar().catch((erro) => {
  console.error('[FATAL]', erro.message);
  process.exitCode = 1;
});

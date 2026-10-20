'use strict';

/**
 * CAPUNGA DADOS — BAC BO — VERSÃO FINAL
 * Telegram + PostgreSQL + sincronização de resultados
 *
 * Princípios:
 * - Contagem honesta: GREEN (sem gale), G1, G2, EMPATE, LOSS ficam separados.
 * - Cada sinal só avalia rodadas POSTERIORES à sua criação (ref_fim).
 * - Sinal pendente antigo (ex.: após reinício) é anulado, nunca fechado
 *   com resultados antigos.
 * - Estatísticas reais em /stats e nas mensagens de fecho.
 *
 * AVISO: o Bac Bo é um jogo de azar com rodadas independentes.
 * Nenhum filtro garante vitória. Este bot não promete assertividade.
 *
 * Obrigatórias: DATABASE_URL, BOT_TOKEN, CHAT_ID
 * Opcionais: API_BACBO, PORT, INTERVALO_SYNC_MS, MIN_RODADAS_ANALISE,
 *            INTERVALO_MIN_SINAL_MS, SINAIS_ATIVOS, EMPATE_PROTEGIDO,
 *            TZ_STATS (default Europe/Lisbon), VALIDADE_SINAL_MS
 */

const express = require('express');
const cors = require('cors');
const axios = require('axios');
const { Pool } = require('pg');

const app = express();
app.disable('x-powered-by');
app.use(cors());
app.use(express.json({ limit: '1mb' }));

/* ---------- CONFIG ---------- */

function limitar(valor, padrao, minimo, maximo) {
  const n = Number.parseInt(valor, 10);
  if (!Number.isFinite(n)) return padrao;
  return Math.max(minimo, Math.min(maximo, n));
}

function boolEnv(valor, padrao) {
  if (valor === undefined || valor === '') return padrao;
  return ['true', '1', 'yes', 'sim', 'on'].includes(
    String(valor).trim().toLowerCase()
  );
}

const PORT = Number(process.env.PORT || 10000);
const DATABASE_URL = process.env.DATABASE_URL || '';
const BOT_TOKEN = process.env.BOT_TOKEN || '';
const CHAT_ID = process.env.CHAT_ID || '';
const API_BACBO =
  process.env.API_BACBO ||
  'https://api-cs.casino.org/svc-evolution-game-events/api/bacbo';
const TZ_STATS = process.env.TZ_STATS || 'Europe/Lisbon';

const INTERVALO_SYNC_MS = limitar(process.env.INTERVALO_SYNC_MS, 6000, 4000, 30000);
const INTERVALO_MIN_SINAL_MS = limitar(process.env.INTERVALO_MIN_SINAL_MS, 120000, 120000, 600000);
const MIN_RODADAS_ANALISE = limitar(process.env.MIN_RODADAS_ANALISE, 12, 8, 100);
const VALIDADE_SINAL_MS = limitar(process.env.VALIDADE_SINAL_MS, 900000, 300000, 3600000);
const SINAIS_ATIVOS = boolEnv(process.env.SINAIS_ATIVOS, true);
const EMPATE_PROTEGIDO = boolEnv(process.env.EMPATE_PROTEGIDO, true);
const GALE_MAX = 2;

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
  sinaisGerados: 0
};

let server = null;
let syncTimer = null;
let syncEmCurso = false;
let encerrando = false;

/* ---------- NORMALIZAÇÃO ---------- */

function normalizarResultado(valor) {
  if (valor && typeof valor === 'object') {
    valor = valor.outcome ?? valor.winner ?? valor.resultado ?? valor.result ?? valor.name;
  }

  const s = String(valor ?? '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .trim()
    .toLowerCase()
    .replace(/[\s_-]+/g, '');

  if (['banca', 'banker', 'bankerwin', 'bankerwins', 'bankerwon', 'red', 'vermelho'].includes(s)) return 'Banca';
  if (['jogador', 'player', 'playerwin', 'playerwins', 'playerwon', 'blue', 'azul'].includes(s)) return 'Jogador';
  if (['empate', 'tie', 'draw', 'equal', 'push', 'tied'].includes(s)) return 'Empate';
  return null;
}

function dataISO(valor) {
  if (!valor) return null;
  const d = new Date(valor);
  return Number.isNaN(d.getTime()) ? null : d.toISOString();
}

function extrairItens(body) {
  if (Array.isArray(body)) return body;
  if (!body || typeof body !== 'object') return [];

  const candidatos = [
    body.data, body.results, body.items, body.rounds, body.events, body.resultados,
    body.data?.results, body.data?.items, body.data?.rounds,
    body.data?.events, body.data?.resultados
  ];

  return candidatos.find(Array.isArray) || [];
}

function normalizarRodada(item) {
  if (!item || typeof item !== 'object') return null;

  const d = item.data || item.round || item;
  const ro = d.result && typeof d.result === 'object' ? d.result : {};

  const resultadoRaw =
    ro.outcome ?? ro.winner ?? ro.resultado ??
    d.outcome ?? d.winner ?? d.resultado ??
    (typeof d.result === 'string' ? d.result : null) ??
    item.outcome ?? item.winner ?? item.resultado ??
    (typeof item.result === 'string' ? item.result : null);

  const resultado = normalizarResultado(resultadoRaw);

  const idRaw =
    d.id ?? d.roundId ?? d.round_id ?? d.eventId ??
    item.id ?? item.roundId ?? item.round_id ?? item.eventId;

  const fim = dataISO(
    d.settledAt ?? d.endTime ?? d.endedAt ?? d.finishedAt ?? d.fim ??
    item.settledAt ?? item.endTime ?? item.finishedAt ?? item.fim
  );

  const inicio = dataISO(
    d.startedAt ?? d.startTime ?? d.started_at ?? d.inicio ??
    item.startedAt ?? item.startTime ?? item.inicio
  );

  const status = String(d.status ?? d.estado ?? item.status ?? item.estado ?? '').toLowerCase();

  const finalizada =
    /resolved|settled|finished|complete|completed|resolvido|concluido|finalizado/.test(status) ||
    Boolean(fim);

  if (!idRaw || !resultado || !fim || !finalizada) return null;

  return { id: String(idRaw), resultado, inicio: inicio || fim, fim };
}

/* ---------- TELEGRAM ---------- */

async function enviarTelegram(texto) {
  if (!BOT_TOKEN || !CHAT_ID) {
    runtime.ultimoErroTelegram = 'BOT_TOKEN ou CHAT_ID não configurado';
    return null;
  }

  try {
    const r = await axios.post(
      `https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`,
      { chat_id: CHAT_ID, text: texto, disable_web_page_preview: true },
      { timeout: 15000 }
    );

    if (r.data?.ok !== true) throw new Error(JSON.stringify(r.data));

    runtime.ultimoErroTelegram = null;
    return r.data.result?.message_id ?? true;
  } catch (erro) {
    runtime.ultimoErroTelegram = erro.response?.data
      ? JSON.stringify(erro.response.data)
      : erro.message;
    console.error('[TELEGRAM]', runtime.ultimoErroTelegram);
    return null;
  }
}

/* ---------- BASE DE DADOS ---------- */

async function garantirTabelas() {
  if (!pool) throw new Error('DATABASE_URL não configurada');

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

  /* Migração segura: instante da última rodada já considerada */
  await pool.query(`ALTER TABLE sinais ADD COLUMN IF NOT EXISTS ref_fim TIMESTAMPTZ`);
  await pool.query(`ALTER TABLE sinais ADD COLUMN IF NOT EXISTS fechado_em TIMESTAMPTZ`);

  /* Sinais pendentes de versões antigas não têm ref_fim: anular */
  await pool.query(`
    UPDATE sinais SET estado = 'ANULADO', fechado_em = NOW()
    WHERE estado = 'PENDENTE' AND ref_fim IS NULL
  `);

  await pool.query(`CREATE INDEX IF NOT EXISTS idx_sinais_estado ON sinais (estado)`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_sinais_criado ON sinais (criado_em DESC)`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_resultados_fim ON bacbo_resultados (fim DESC)`);

  console.log('[DB] Tabelas verificadas');
}

async function guardarRodadas(rodadas) {
  const novas = [];

  for (const r of rodadas) {
    const res = await pool.query(
      `INSERT INTO bacbo_resultados (id, resultado, inicio, fim)
       VALUES ($1, $2, $3, $4)
       ON CONFLICT (id) DO NOTHING
       RETURNING id, resultado, inicio, fim`,
      [r.id, r.resultado, r.inicio, r.fim]
    );
    if (res.rowCount > 0) novas.push(res.rows[0]);
  }

  return novas;
}

async function obterHistorico(limite = 100) {
  const r = await pool.query(
    `SELECT id, resultado, inicio, fim
     FROM bacbo_resultados
     ORDER BY fim DESC
     LIMIT $1`,
    [limite]
  );
  return r.rows;
}

/* ---------- ESTATÍSTICAS REAIS ---------- */

async function estatisticas(periodo) {
  const filtro = periodo === 'dia'
    ? `AND (fechado_em AT TIME ZONE $1)::date = (NOW() AT TIME ZONE $1)::date`
    : '';

  const r = await pool.query(
    `SELECT estado, COUNT(*)::int AS n
     FROM sinais
     WHERE estado <> 'PENDENTE' AND estado <> 'ANULADO' ${filtro}
     GROUP BY estado`,
    periodo === 'dia' ? [TZ_STATS] : []
  );

  const c = { GREEN: 0, G1: 0, G2: 0, EMPATE: 0, LOSS: 0 };
  for (const row of r.rows) if (row.estado in c) c[row.estado] = row.n;

  const total = c.GREEN + c.G1 + c.G2 + c.EMPATE + c.LOSS;
  const pct = (n) => (total ? ((n / total) * 100).toFixed(1) : '0.0');

  return {
    ...c,
    total,
    semGalePct: pct(c.GREEN),
    comGalePct: pct(c.GREEN + c.G1 + c.G2),
    totalPositivoPct: pct(c.GREEN + c.G1 + c.G2 + c.EMPATE),
    lossPct: pct(c.LOSS)
  };
}

function linhaStats(s) {
  return (
    `📊 Hoje (${s.total} sinais fechados)\n` +
    `✅ Sem gale: ${s.GREEN} (${s.semGalePct}%)\n` +
    `🔁 G1: ${s.G1} | G2: ${s.G2}\n` +
    `🤝 Empate: ${s.EMPATE}\n` +
    `🔴 LOSS: ${s.LOSS} (${s.lossPct}%)`
  );
}

/* ---------- ANÁLISE (HEURÍSTICA, SEM VANTAGEM COMPROVADA) ---------- */

function analisarHistorico(historico) {
  const decisivos = historico
    .map((r) => r.resultado)
    .filter((r) => r === 'Banca' || r === 'Jogador');

  if (decisivos.length < MIN_RODADAS_ANALISE) return null;

  const recentes = decisivos.slice(0, 12);
  const banca = recentes.filter((r) => r === 'Banca').length;
  const jogador = recentes.length - banca;

  let seq = 1;
  for (let i = 1; i < decisivos.length; i++) {
    if (decisivos[i] !== decisivos[0]) break;
    seq++;
  }

  if (seq >= 3 && seq <= 5) return { cor: decisivos[0], estrategia: 'SEQUENCIA' };
  if (banca >= 9) return { cor: 'Banca', estrategia: 'MAIORIA_RECENTE' };
  if (jogador >= 9) return { cor: 'Jogador', estrategia: 'MAIORIA_RECENTE' };

  return null;
}

/* ---------- SINAIS E GALES ---------- */

async function obterSinalPendente() {
  const r = await pool.query(
    `SELECT * FROM sinais WHERE estado = 'PENDENTE' ORDER BY id DESC LIMIT 1`
  );
  return r.rows[0] || null;
}

async function anularSinaisExpirados() {
  const r = await pool.query(
    `UPDATE sinais
     SET estado = 'ANULADO', fechado_em = NOW()
     WHERE estado = 'PENDENTE'
       AND criado_em < NOW() - ($1 || ' milliseconds')::interval
     RETURNING id`,
    [String(VALIDADE_SINAL_MS)]
  );

  if (r.rowCount > 0) {
    await enviarTelegram('⚪ Sinal anulado por falta de resultados atualizados. Não conta nas estatísticas.');
  }
}

async function fecharSinal(sinal, estado, rodada, textoTitulo) {
  const r = await pool.query(
    `UPDATE sinais
     SET estado = $1, resultado_id = $2, resultado_final = $3,
         ref_fim = $4, fechado_em = NOW()
     WHERE id = $5 AND estado = 'PENDENTE'`,
    [estado, rodada.id, rodada.resultado, rodada.fim, sinal.id]
  );

  if (r.rowCount === 0) return;

  const s = await estatisticas('dia');

  await enviarTelegram(
    `${textoTitulo}\n` +
    `Entrada: ${sinal.cor}\n` +
    `Resultado: ${rodada.resultado}\n\n` +
    linhaStats(s)
  );
}

async function atualizarSinal(sinal, rodada) {
  /* Só conta rodadas estritamente posteriores à última já considerada */
  if (sinal.ref_fim && new Date(rodada.fim) <= new Date(sinal.ref_fim)) return;

  runtime.ultimoResultado = rodada.resultado;

  /* EMPATE */
  if (rodada.resultado === 'Empate') {
    if (EMPATE_PROTEGIDO) {
      await fecharSinal(
        sinal,
        'EMPATE',
        rodada,
        `🤝 EMPATE PROTEGIDO${sinal.gale > 0 ? ` (no Gale ${sinal.gale})` : ''}`
      );
    } else {
      await pool.query(
        `UPDATE sinais SET empates = empates + 1, ref_fim = $1, resultado_id = $2
         WHERE id = $3 AND estado = 'PENDENTE'`,
        [rodada.fim, rodada.id, sinal.id]
      );
    }
    return;
  }

  /* ACERTO */
  if (rodada.resultado === sinal.cor) {
    if (sinal.gale === 0) {
      await fecharSinal(sinal, 'GREEN', rodada, '🟢 GREEN SEM GALE ✅');
    } else {
      await fecharSinal(sinal, `G${sinal.gale}`, rodada, `🟡 GREEN NO GALE ${sinal.gale}`);
    }
    return;
  }

  /* ERRO → próximo gale */
  if (sinal.gale < GALE_MAX) {
    const novoGale = sinal.gale + 1;

    const r = await pool.query(
      `UPDATE sinais SET gale = $1, resultado_id = $2, ref_fim = $3
       WHERE id = $4 AND estado = 'PENDENTE'`,
      [novoGale, rodada.id, rodada.fim, sinal.id]
    );

    if (r.rowCount > 0) {
      await enviarTelegram(
        `🔁 GALE ${novoGale}\n` +
        `Entrada: ${sinal.cor}\n` +
        'Aguardar o próximo resultado.'
      );
    }
    return;
  }

  /* LOSS */
  await fecharSinal(sinal, 'LOSS', rodada, '🔴 LOSS (após Gale 2)');
}

async function criarSinalSeElegivel() {
  if (!SINAIS_ATIVOS) return;
  if (await obterSinalPendente()) return;

  const ultimo = await pool.query(`SELECT criado_em FROM sinais ORDER BY id DESC LIMIT 1`);

  if (ultimo.rows.length > 0) {
    const t = new Date(ultimo.rows[0].criado_em).getTime();
    if (Date.now() - t < INTERVALO_MIN_SINAL_MS) return;
  }

  const historico = await obterHistorico(100);
  const decisao = analisarHistorico(historico);
  if (!decisao) return;

  /* Âncora: só rodadas depois da mais recente conhecida contam */
  const refFim = historico[0].fim;

  const ins = await pool.query(
    `INSERT INTO sinais (cor, estado, gale, estrategia, ref_fim)
     VALUES ($1, 'PENDENTE', 0, $2, $3)
     RETURNING id, criado_em`,
    [decisao.cor, decisao.estrategia, refFim]
  );

  const sinal = ins.rows[0];

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
    `🛡️ Proteção: Empate${EMPATE_PROTEGIDO ? '' : ' (desativada)'}\n` +
    `🔁 Gale máximo: ${GALE_MAX}\n` +
    `📊 Filtro: ${decisao.estrategia}\n\n` +
    '⚠️ Sinal estatístico. Não há garantia de vitória. Jogue com responsabilidade.'
  );

  console.log('[SINAL]', runtime.ultimoSinal);
}

/* ---------- SINCRONIZAÇÃO ---------- */

async function sincronizar() {
  if (syncEmCurso || encerrando) return;
  syncEmCurso = true;

  try {
    if (!pool) throw new Error('DATABASE_URL não configurada');

    const resposta = await axios.get(API_BACBO, {
      timeout: 15000,
      headers: { Accept: 'application/json', 'User-Agent': 'CapungaDadosBacBo/2.0' }
    });

    const itens = extrairItens(resposta.data);
    runtime.resultadosRecebidos = itens.length;

    if (itens.length === 0) {
      throw new Error('A API não devolveu uma lista de rodadas reconhecida.');
    }

    const rodadas = itens
      .map(normalizarRodada)
      .filter(Boolean)
      .sort((a, b) => new Date(a.fim) - new Date(b.fim));

    if (rodadas.length === 0) {
      throw new Error('Nenhuma rodada finalizada reconhecida. Nenhum sinal criado.');
    }

    runtime.ultimoErroAPI = null;

    const novas = await guardarRodadas(rodadas);
    novas.sort((a, b) => new Date(a.fim) - new Date(b.fim));

    runtime.resultadosNovos = novas.length;
    runtime.ultimaSincronizacao = new Date().toISOString();

    await anularSinaisExpirados();

    for (const rodada of novas) {
      const sinal = await obterSinalPendente();
      if (sinal) await atualizarSinal(sinal, rodada);
      else runtime.ultimoResultado = rodada.resultado;
    }

    await criarSinalSeElegivel();

    runtime.ultimoErroDB = null;
  } catch (erro) {
    const msg = erro.response?.status
      ? `API HTTP ${erro.response.status}: ${erro.message}`
      : erro.message;

    runtime.ultimoErroAPI = msg;
    console.error('[SYNC]', msg);
  } finally {
    syncEmCurso = false;
  }
}

/* ---------- ROTAS ---------- */

app.get('/', (_req, res) => {
  res.json({ nome: 'CAPUNGA DADOS — BAC BO', estado: 'online', hora: new Date().toISOString() });
});

app.get('/health', (_req, res) => {
  res.json({
    estado: encerrando ? 'encerrando' : 'online',
    baseConfigurada: Boolean(DATABASE_URL),
    telegramConfigurado: Boolean(BOT_TOKEN && CHAT_ID),
    sincronizacaoEmCurso: syncEmCurso,
    ultimaSincronizacao: runtime.ultimaSincronizacao,
    ultimoErroAPI: runtime.ultimoErroAPI
  });
});

app.get('/stats', async (_req, res) => {
  try {
    res.json({
      aviso: 'Estatísticas reais. Sinais anulados não são contados.',
      hoje: await estatisticas('dia'),
      geral: await estatisticas('geral')
    });
  } catch (erro) {
    runtime.ultimoErroDB = erro.message;
    res.status(500).json({ erro: erro.message });
  }
});

app.get('/status', async (_req, res) => {
  let dbOk = false;

  try {
    if (pool) {
      await pool.query('SELECT 1');
      dbOk = true;
    }
  } catch (erro) {
    runtime.ultimoErroDB = erro.message;
  }

  res.json({
    nome: 'CAPUNGA DADOS — BAC BO',
    iniciadoEm: runtime.iniciadoEm,
    ultimaSincronizacao: runtime.ultimaSincronizacao,
    ultimoResultado: runtime.ultimoResultado,
    ultimoSinal: runtime.ultimoSinal,
    resultadosRecebidos: runtime.resultadosRecebidos,
    resultadosNovos: runtime.resultadosNovos,
    sinaisGerados: runtime.sinaisGerados,
    baseDadosOK: dbOk,
    ultimoErroAPI: runtime.ultimoErroAPI,
    ultimoErroDB: runtime.ultimoErroDB,
    ultimoErroTelegram: runtime.ultimoErroTelegram
  });
});

/* ---------- ARRANQUE E ENCERRAMENTO ---------- */

async function iniciar() {
  if (!DATABASE_URL) throw new Error('Configura DATABASE_URL no servidor.');
  if (!BOT_TOKEN) throw new Error('Configura BOT_TOKEN no servidor.');
  if (!CHAT_ID) throw new Error('Configura CHAT_ID no servidor.');

  await garantirTabelas();

  server = app.listen(PORT, () => console.log(`[HTTP] Porta ${PORT}`));

  await sincronizar();

  syncTimer = setInterval(() => {
    sincronizar().catch((e) => console.error('[SYNC TIMER]', e.message));
  }, INTERVALO_SYNC_MS);

  console.log('[BOT] CAPUNGA DADOS iniciado');
}

async function encerrar() {
  if (encerrando) return;
  encerrando = true;

  if (syncTimer) clearInterval(syncTimer);
  if (server) await new Promise((r) => server.close(r));
  if (pool) await pool.end();

  console.log('[BOT] Encerrado');
}

process.on('SIGTERM', () => encerrar().catch((e) => { console.error(e); process.exitCode = 1; }));
process.on('SIGINT', () => encerrar().catch((e) => { console.error(e); process.exitCode = 1; }));

iniciar().catch((erro) => {
  console.error('[FATAL]', erro.message);
  process.exitCode = 1;
});

'use strict';

/**
 * CAPUNGA DADOS — BAC BO
 * Express + PostgreSQL + Telegram, com o motor Capunga Mesa (pasta ./bot).
 *
 * Variáveis obrigatórias:
 *   DATABASE_URL, BOT_TOKEN, CHAT_ID
 *
 * Opcionais:
 *   PORT (10000)
 *   INTERVALO_SYNC_MS (6000, 4000–30000)
 *   KEEP_ALIVE_MS (60000, 15000–900000)
 *   KEEP_ALIVE_URL
 *   KEEP_ALIVE_URLS (lista separada por vírgulas)
 *   PAINEL_TOKEN
 *   DATABASE_SSL=false (local)
 *
 * A pasta ./bot é ESM, por isso é carregada com import().
 */

const express = require('express');
const cors = require('cors');

const app = express();

app.disable('x-powered-by');
app.use(cors());
app.use(express.json({ limit: '1mb' }));

const PORT = Number(process.env.PORT || 10000);
const DATABASE_URL = process.env.DATABASE_URL || '';
const BOT_TOKEN = process.env.BOT_TOKEN || '';
const CHAT_ID = process.env.CHAT_ID || '';
const PAINEL_TOKEN = (process.env.PAINEL_TOKEN || '').trim();

const INTERVALO_SYNC_MS = limitar(process.env.INTERVALO_SYNC_MS, 6000, 4000, 30000);
const KEEP_ALIVE_MS = limitar(process.env.KEEP_ALIVE_MS, 60000, 15000, 900000);

const KEEP_ALIVE_URLS = [...new Set(
  [
    process.env.KEEP_ALIVE_URL,
    process.env.RENDER_EXTERNAL_URL,
    process.env.PUBLIC_URL,
    ...(process.env.KEEP_ALIVE_URLS || '').split(',').map((x) => x.trim())
  ]
    .filter(Boolean)
    .map((url) => normalizarUrlSaude(url))
)];

function limitar(valor, padrao, minimo, maximo) {
  const n = Number.parseInt(valor, 10);
  if (!Number.isFinite(n)) return padrao;
  return Math.max(minimo, Math.min(maximo, n));
}

function normalizarUrlSaude(url) {
  if (!url) return '';
  const base = String(url).trim().replace(/\/+$/, '');
  if (!base) return '';
  return base.includes('/health') ? base : `${base}/health`;
}

const runtime = {
  iniciadoEm: new Date().toISOString(),
  ultimoPainel: null,
  ultimoErro: null,
  ciclos: 0,
  ultimoKeepAlive: null,
  ultimoKeepAliveUrl: null,
  keepAliveAtivo: false
};

let bot = null;
let server = null;
let timer = null;
let encerrando = false;
let keepAliveTimer = null;

/* =====================================================
   KEEP-ALIVE ROBUSTO
===================================================== */

async function keepAliveTick() {
  if (!KEEP_ALIVE_URLS.length) return;

  runtime.keepAliveAtivo = true;

  let sucesso = false;
  let ultimaUrl = null;

  for (const url of KEEP_ALIVE_URLS) {
    try {
      await fetch(url, {
        method: 'GET',
        headers: {
          'User-Agent': 'capunga-dados-keepalive/1.0'
        },
        signal: AbortSignal.timeout(10000)
      });
      sucesso = true;
      ultimaUrl = url;
      runtime.ultimoKeepAlive = new Date().toISOString();
      runtime.ultimoKeepAliveUrl = url;
      break;
    } catch (erro) {
      // tenta o próximo alvo
    }
  }

  if (!sucesso) {
    runtime.ultimoKeepAlive = null;
    runtime.ultimoKeepAliveUrl = null;
    runtime.keepAliveAtivo = false;
  } else {
    runtime.keepAliveAtivo = true;
    runtime.ultimoKeepAliveUrl = ultimaUrl;
  }
}

/* =====================================================
   CICLO DO BOT
===================================================== */

async function ciclo() {
  if (encerrando) return;

  try {
    if (!bot) {
      throw new Error('Bot ainda não inicializado');
    }

    const painel = await bot.tickAndSnapshot();
    runtime.ultimoPainel = painel;
    runtime.ultimoErro = painel.erro || null;
    runtime.ciclos++;
    if (painel.erro) console.error('[TICK]', painel.erro);
  } catch (erro) {
    runtime.ultimoErro = erro.message;
    console.error('[TICK]', erro.message);
  } finally {
    if (!encerrando) {
      timer = setTimeout(ciclo, INTERVALO_SYNC_MS);
    }
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

function saude(_req, res) {
  const p = runtime.ultimoPainel;

  res.json({
    sucesso: true,
    estado: encerrando ? 'encerrando' : 'online',
    baseConfigurada: Boolean(DATABASE_URL),
    telegramConfigurado: Boolean(BOT_TOKEN && CHAT_ID),
    hora: p ? p.hora : null,
    motivo: p ? p.motivo : 'iniciando',
    aoVivo: p ? p.aoVivo : false,
    retomado: p ? p.retomado : false,
    ultimaSincronizacao: p ? p.sincronizadoEm : null,
    ultimoErro: runtime.ultimoErro,
    ultimoKeepAlive: runtime.ultimoKeepAlive,
    ultimoKeepAliveUrl: runtime.ultimoKeepAliveUrl,
    keepAliveAtivo: runtime.keepAliveAtivo
  });
}

app.get('/health', saude);
app.get('/api/health', saude);

app.get('/status', async (_req, res) => {
  let dbOk = false;
  let totais = null;

  try {
    if (!bot) throw new Error('bot nao inicializado');

    const sql = await bot.getSql();
    const linhas = await sql.query(
      'SELECT estado, COUNT(*)::int AS quantidade FROM bacbo_sinais GROUP BY estado'
    );
    dbOk = true;
    totais = linhas;
  } catch (erro) {
    runtime.ultimoErro = erro.message;
  }

  const p = runtime.ultimoPainel;

  res.json({
    nome: 'CAPUNGA DADOS — BAC BO',
    estado: 'online',
    iniciadoEm: runtime.iniciadoEm,
    ciclos: runtime.ciclos,
    ultimaSincronizacao: p ? p.sincronizadoEm : null,
    motivo: p ? p.motivoHumano : null,
    sinalAberto: p ? p.aberto : null,
    folhaDoDia: p ? p.folha : null,
    totalRodadas: p ? p.totais.rodadas : null,
    baseDadosOK: dbOk,
    totaisSinais: totais,
    ultimoErro: runtime.ultimoErro
  });
});

app.get('/api/painel', (req, res) => {
  if (PAINEL_TOKEN) {
    const ok =
      req.query.token === PAINEL_TOKEN ||
      req.headers.authorization === `Bearer ${PAINEL_TOKEN}`;
    if (!ok) return res.status(401).json({ sucesso: false, erro: 'nao_autorizado' });
  }

  res.json(runtime.ultimoPainel || { sucesso: false, erro: 'iniciando' });
});

/* =====================================================
   ARRANQUE E ENCERRAMENTO
===================================================== */

async function iniciar() {
  if (!DATABASE_URL) throw new Error('Configura DATABASE_URL no servidor.');
  if (!BOT_TOKEN) throw new Error('Configura BOT_TOKEN no servidor.');
  if (!CHAT_ID) throw new Error('Configura CHAT_ID no servidor.');

  const [tick, tg, db, mig] = await Promise.all([
    import('./bot/tick.js'),
    import('./bot/telegram.js'),
    import('./bot/db.js'),
    import('./bot/migrate.js')
  ]);

  bot = {
    tickAndSnapshot: tick.tickAndSnapshot,
    telegramConfigurado: tg.telegramConfigurado,
    getSql: db.getSql,
    closeDb: db.closeDb
  };

  await mig.migrate();
  console.log('[DB] Tabelas verificadas');

  server = app.listen(PORT, '0.0.0.0', () => {
    console.log(`[HTTP] Servidor iniciado na porta ${PORT}`);
  });

  // Keep-alive interno para manter o processo vivo e avisar o Render/monitor
  if (KEEP_ALIVE_URLS.length) {
    keepAliveTimer = setInterval(() => {
      keepAliveTick().catch(() => {});
    }, KEEP_ALIVE_MS);

    if (typeof keepAliveTimer.unref === 'function') keepAliveTimer.unref();
  }

  // primeira checagem rápida
  setTimeout(() => {
    keepAliveTick().catch(() => {});
  }, 15000);

  ciclo();
  console.log('[BOT] CAPUNGA DADOS iniciado');
}

async function encerrar() {
  if (encerrando) return;
  encerrando = true;

  if (timer) {
    clearTimeout(timer);
    timer = null;
  }

  if (keepAliveTimer) {
    clearInterval(keepAliveTimer);
    keepAliveTimer = null;
  }

  if (server) {
    await new Promise((resolve) => server.close(resolve));
  }

  if (bot && typeof bot.closeDb === 'function') {
    await bot.closeDb().catch(() => undefined);
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

process.on('unhandledRejection', (erro) => {
  console.error('[UNHANDLED]', erro);
});

process.on('uncaughtException', (erro) => {
  console.error('[UNCAUGHT]', erro);
});

iniciar().catch((erro) => {
  console.error('[FATAL]', erro.message);
  process.exitCode = 1;
});
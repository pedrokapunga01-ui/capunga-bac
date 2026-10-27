'use strict';

require('dotenv').config();
const express = require('express');
const cors = require('cors');
const axios = require('axios');
const { Pool } = require('pg');

const app = express();
app.disable('x-powered-by');
app.use(cors());
app.use(express.json({ limit: '32kb' }));

function numEnv(name, fallback, min, max) {
  const n = Number(process.env[name] ?? fallback);
  return Number.isFinite(n) ? Math.min(max, Math.max(min, n)) : fallback;
}

function round(n, places = 2) {
  const p = 10 ** places;
  return Math.round((Number(n) + Number.EPSILON) * p) / p;
}

function errText(e) {
  return String(e?.message || e || 'Unknown error').slice(0, 400);
}

const CFG = Object.freeze({
  port: numEnv('PORT', 3000, 1, 65535),
  symbol: String(process.env.SYMBOL || 'BTCUSDT').toUpperCase(),
  category: 'spot',
  bybitUrl: (process.env.BYBIT_BASE_URL || 'https://api.bybit.com').replace(/\/$/, ''),
  loopMs: numEnv('LOOP_MS', 60000, 15000, 3600000),
  minScore: numEnv('MIN_SCORE', 5, 3, 8),
  minRR: numEnv('MIN_RR', 1.4, 1, 5),
  riskPct: numEnv('RISK_PCT', 1, 0.1, 1) / 100,
  maxDeviationPct: numEnv('MAX_ENTRY_DEVIATION_PCT', 0.35, 0.05, 3) / 100,
  tpAtr: numEnv('ATR_TP_MULT', 1.8, 0.5, 10),
  slAtr: numEnv('ATR_SL_MULT', 1.2, 0.5, 10),
  tpMinPct: numEnv('TP_MIN_PCT', 0.25, 0.05, 5) / 100,
  slMinPct: numEnv('SL_MIN_PCT', 0.20, 0.05, 5) / 100,
  expiryMs: numEnv('SIGNAL_EXPIRY_MIN', 240, 15, 10080) * 60000,
  antiSpamMs: numEnv('ANTI_SPAM_MIN', 60, 1, 1440) * 60000,
  candleLimit: numEnv('CANDLE_LIMIT', 200, 100, 1000),
  apiKey: process.env.API_KEY || '',
  botToken: process.env.BOT_TOKEN || '',
  chatId: process.env.CHAT_ID || '',
  keepaliveUrl: process.env.KEEPALIVE_URL || '',
  databaseUrl: process.env.DATABASE_URL || '',
  databaseSsl: String(process.env.DATABASE_SSL || 'true').toLowerCase() !== 'false',
  telegramPollMs: numEnv('TELEGRAM_POLL_MS', 4000, 1500, 30000),
  allowTelegramCommands: String(process.env.TELEGRAM_COMMANDS || 'true').toLowerCase() !== 'false'
});

const pool = CFG.databaseUrl ? new Pool({
  connectionString: CFG.databaseUrl,
  ssl: CFG.databaseSsl ? { rejectUnauthorized: false } : false,
  max: 8,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 10000
}) : null;

let server;
let scanTimer;
let keepaliveTimer;
let telegramTimer;
let scanning = false;
let shuttingDown = false;
let lastAnalysis = null;
let lastError = null;
let lastSource = 'none';
let lastSignalAt = 0;
let telegramOffset = 0;
let consecutiveErrors = 0;

async function logEvent(type, message, data = {}) {
  if (!pool) return;

  try {
    await pool.query(
      'INSERT INTO trade_eventos(tipo,mensagem,dados) VALUES($1,$2,$3)',
      [type, message, JSON.stringify(data)]
    );
  } catch (e) {
    console.error('[DB event]', errText(e));
  }
}

async function initDB() {
  if (!pool) {
    throw new Error(
      'DATABASE_URL não configurado. PostgreSQL é obrigatório para histórico e controlo de duplicados.'
    );
  }

  await pool.query(`
    CREATE TABLE IF NOT EXISTS trade_sinais (
      id BIGSERIAL PRIMARY KEY,
      symbol TEXT NOT NULL,
      direcao TEXT NOT NULL DEFAULT 'BUY',
      preco_entrada NUMERIC NOT NULL,
      preco_tp NUMERIC NOT NULL,
      preco_sl NUMERIC NOT NULL,
      score INTEGER NOT NULL,
      estado TEXT NOT NULL DEFAULT 'PENDENTE',
      tp_pct NUMERIC NOT NULL,
      sl_pct NUMERIC NOT NULL,
      preco_saida NUMERIC,
      pnl_pct NUMERIC,
      criado_em TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      resolvido_em TIMESTAMPTZ,
      expiracao_em TIMESTAMPTZ NOT NULL,
      metadados JSONB NOT NULL DEFAULT '{}'::jsonb
    );

    CREATE INDEX IF NOT EXISTS trade_sinais_estado_idx
      ON trade_sinais(estado, criado_em DESC);

    CREATE INDEX IF NOT EXISTS trade_sinais_criado_idx
      ON trade_sinais(criado_em DESC);

    CREATE TABLE IF NOT EXISTS trade_eventos (
      id BIGSERIAL PRIMARY KEY,
      tipo TEXT NOT NULL,
      mensagem TEXT NOT NULL,
      dados JSONB NOT NULL DEFAULT '{}'::jsonb,
      criado_em TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS trade_config (
      chave TEXT PRIMARY KEY,
      valor TEXT NOT NULL,
      atualizado_em TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  // Migrações para bases de dados de versões anteriores.
  await pool.query(`
    ALTER TABLE trade_sinais ADD COLUMN IF NOT EXISTS preco_tp NUMERIC;
    ALTER TABLE trade_sinais ADD COLUMN IF NOT EXISTS preco_sl NUMERIC;
    ALTER TABLE trade_sinais ADD COLUMN IF NOT EXISTS tp_pct NUMERIC;
    ALTER TABLE trade_sinais ADD COLUMN IF NOT EXISTS sl_pct NUMERIC;
    ALTER TABLE trade_sinais ADD COLUMN IF NOT EXISTS preco_saida NUMERIC;
    ALTER TABLE trade_sinais ADD COLUMN IF NOT EXISTS pnl_pct NUMERIC;
    ALTER TABLE trade_sinais ADD COLUMN IF NOT EXISTS resolvido_em TIMESTAMPTZ;
    ALTER TABLE trade_sinais ADD COLUMN IF NOT EXISTS expiracao_em TIMESTAMPTZ;
    ALTER TABLE trade_sinais ADD COLUMN IF NOT EXISTS metadados JSONB NOT NULL DEFAULT '{}'::jsonb;
  `);

  await pool.query(`
    UPDATE trade_sinais
    SET expiracao_em = COALESCE(
      expiracao_em,
      criado_em + INTERVAL '4 hours'
    )
    WHERE expiracao_em IS NULL
  `);

  const { rows } = await pool.query(
    'SELECT EXTRACT(EPOCH FROM MAX(criado_em))*1000 AS last_ms FROM trade_sinais'
  );

  lastSignalAt = Number(rows[0]?.last_ms || 0);
}

async function telegramSend(text) {
  if (!CFG.botToken || !CFG.chatId) {
    return { skipped: true, reason: 'Telegram not configured' };
  }

  try {
    const { data } = await axios.post(
      `https://api.telegram.org/bot${CFG.botToken}/sendMessage`,
      {
        chat_id: CFG.chatId,
        text,
        disable_web_page_preview: true
      },
      { timeout: 12000 }
    );

    if (!data?.ok) {
      throw new Error(data?.description || 'Telegram API rejected message');
    }

    return { sent: true };
  } catch (e) {
    await logEvent('telegram_error', errText(e));
    console.error('[Telegram send]', errText(e));

    return {
      sent: false,
      error: errText(e)
    };
  }
}

async function fetchBybitCandles(interval) {
  const { data } = await axios.get(
    `${CFG.bybitUrl}/v5/market/kline`,
    {
      params: {
        category: 'spot',
        symbol: CFG.symbol,
        interval: String(interval),
        limit: CFG.candleLimit
      },
      timeout: 12000
    }
  );

  if (data?.retCode !== 0 || !Array.isArray(data?.result?.list)) {
    throw new Error(`Bybit kline: ${data?.retMsg || 'resposta inválida'}`);
  }

  const ms = Number(interval) * 60000;

  return data.result.list
    .map(r => ({
      time: Number(r[0]),
      open: Number(r[1]),
      high: Number(r[2]),
      low: Number(r[3]),
      close: Number(r[4]),
      volume: Number(r[5])
    }))
    .filter(c =>
      [c.time, c.open, c.high, c.low, c.close, c.volume]
        .every(Number.isFinite) && c.close > 0
    )
    .sort((a, b) => a.time - b.time)
    .filter(c => c.time + ms <= Date.now());
}

async function fetchOkxCandles(interval) {
  const instId = CFG.symbol.replace(/USDT$/, '-USDT');

  const { data } = await axios.get(
    'https://www.okx.com/api/v5/market/candles',
    {
      params: {
        instId,
        bar: `${interval}m`,
        limit: String(Math.min(CFG.candleLimit, 300))
      },
      timeout: 12000
    }
  );

  if (data?.code !== '0' || !Array.isArray(data?.data)) {
    throw new Error('OKX candles response invalid');
  }

  const ms = Number(interval) * 60000;

  return data.data
    .map(r => ({
      time: Number(r[0]),
      open: Number(r[1]),
      high: Number(r[2]),
      low: Number(r[3]),
      close: Number(r[4]),
      volume: Number(r[5])
    }))
    .filter(c =>
      [c.time, c.open, c.high, c.low, c.close, c.volume]
        .every(Number.isFinite) && c.close > 0
    )
    .sort((a, b) => a.time - b.time)
    .filter(c => c.time + ms <= Date.now());
}

async function fetchMarketBundle() {
  // Utiliza a mesma corretora nos três períodos.
  try {
    const [m1, m5, m15] = await Promise.all([
      fetchBybitCandles(1),
      fetchBybitCandles(5),
      fetchBybitCandles(15)
    ]);

    if ([m1, m5, m15].some(a => a.length < 60)) {
      throw new Error('Bybit returned insufficient closed candles');
    }

    lastSource = 'bybit-spot';
    return { m1, m5, m15, source: lastSource };
  } catch (bybitError) {
    try {
      const [m1, m5, m15] = await Promise.all([
        fetchOkxCandles(1),
        fetchOkxCandles(5),
        fetchOkxCandles(15)
      ]);

      if ([m1, m5, m15].some(a => a.length < 60)) {
        throw new Error('OKX returned insufficient closed candles');
      }

      lastSource = 'okx-spot-fallback';
      return { m1, m5, m15, source: lastSource };
    } catch (okxError) {
      throw new Error(
        `Market data unavailable. Bybit: ${errText(bybitError)}; OKX: ${errText(okxError)}`
      );
    }
  }
}

async function fetchPrice(source = 'bybit-spot') {
  if (source === 'okx-spot-fallback') {
    const instId = CFG.symbol.replace(/USDT$/, '-USDT');

    const { data } = await axios.get(
      'https://www.okx.com/api/v5/market/ticker',
      {
        params: { instId },
        timeout: 10000
      }
    );

    const p = Number(data?.data?.[0]?.last);

    if (data?.code !== '0' || !Number.isFinite(p) || p <= 0) {
      throw new Error('OKX ticker unavailable');
    }

    return p;
  }

  const { data } = await axios.get(
    `${CFG.bybitUrl}/v5/market/tickers`,
    {
      params: {
        category: 'spot',
        symbol: CFG.symbol
      },
      timeout: 10000
    }
  );

  const p = Number(data?.result?.list?.[0]?.lastPrice);

  if (data?.retCode !== 0 || !Number.isFinite(p) || p <= 0) {
    throw new Error('Bybit ticker unavailable');
  }

  return p;
}
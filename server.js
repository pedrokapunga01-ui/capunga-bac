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
function ema(values, period) {
  if (values.length < period) return null;

  const k = 2 / (period + 1);
  let v = values.slice(0, period).reduce((a, b) => a + b, 0) / period;

  for (let i = period; i < values.length; i++) {
    v = values[i] * k + v * (1 - k);
  }

  return v;
}

function rsi(values, period = 14) {
  if (values.length < period + 1) return null;

  let gains = 0;
  let losses = 0;

  for (let i = values.length - period; i < values.length; i++) {
    const d = values[i] - values[i - 1];

    if (d > 0) gains += d;
    else losses -= d;
  }

  if (losses === 0) return gains === 0 ? 50 : 100;

  const rs = (gains / period) / (losses / period);

  return 100 - (100 / (1 + rs));
}

function atr(candles, period = 14) {
  if (candles.length < period + 1) return null;

  const tr = [];

  for (let i = 1; i < candles.length; i++) {
    const c = candles[i];
    const pc = candles[i - 1].close;

    tr.push(
      Math.max(
        c.high - c.low,
        Math.abs(c.high - pc),
        Math.abs(c.low - pc)
      )
    );
  }

  return tr.slice(-period).reduce((a, b) => a + b, 0) / period;
}

function adxApprox(candles, period = 14) {
  if (candles.length < period + 2) return null;

  let plus = 0;
  let minus = 0;
  let tr = 0;

  for (let i = candles.length - period; i < candles.length; i++) {
    const c = candles[i];
    const p = candles[i - 1];

    const up = c.high - p.high;
    const down = p.low - c.low;

    plus += up > down && up > 0 ? up : 0;
    minus += down > up && down > 0 ? down : 0;

    tr += Math.max(
      c.high - c.low,
      Math.abs(c.high - p.close),
      Math.abs(c.low - p.close)
    );
  }

  if (tr <= 0) return 0;

  const pdi = 100 * plus / tr;
  const mdi = 100 * minus / tr;

  return pdi + mdi === 0
    ? 0
    : 100 * Math.abs(pdi - mdi) / (pdi + mdi);
}

function getFeatures(candles) {
  const closes = candles.map(c => c.close);
  const last = candles[candles.length - 1];

  return {
    close: last.close,
    ema9: ema(closes, 9),
    ema21: ema(closes, 21),
    ema50: ema(closes, 50),
    rsi: rsi(closes),
    atr: atr(candles),
    adx: adxApprox(candles),
    candle: last
  };
}

function analyze(m1, m5, m15) {
  const a = getFeatures(m1);
  const b = getFeatures(m5);
  const c = getFeatures(m15);

  const reasons = [];
  let score = 0;

  if (b.ema9 > b.ema21) {
    score++;
    reasons.push('EMA9 acima da EMA21 em 5m');
  }

  if (c.ema9 > c.ema21 && c.ema21 > c.ema50) {
    score += 2;
    reasons.push('Tendência ascendente confirmada em 15m');
  }

  if (b.rsi >= 50 && b.rsi <= 68) {
    score++;
    reasons.push('RSI de 5m em zona favorável');
  }

  if (b.close > b.ema9 && b.ema9 > b.ema21) {
    score++;
    reasons.push('Preço acima das médias curtas');
  }

  if (b.adx >= 18) {
    score++;
    reasons.push('Força de tendência suficiente');
  }

  if (a.close > a.ema9 && a.ema9 > a.ema21) {
    score++;
    reasons.push('Confirmação de momentum em 1m');
  }

  if (b.rsi > 72) {
    score -= 2;
    reasons.push('Penalização por RSI sobrecomprado');
  }

  if (c.ema9 < c.ema21) {
    score -= 2;
    reasons.push('Tendência de 15m não confirma compra');
  }

  if (
    !Number.isFinite(b.atr) ||
    b.atr <= 0 ||
    !Number.isFinite(b.close)
  ) {
    return {
      ok: false,
      score,
      reasons: ['Indicadores insuficientes'],
      features: { m1: a, m5: b, m15: c }
    };
  }

  const entry = b.close;

  const slDist = Math.max(
    b.atr * CFG.slAtr,
    entry * CFG.slMinPct
  );

  const tpDist = Math.max(
    b.atr * CFG.tpAtr,
    entry * CFG.tpMinPct
  );

  const rr = tpDist / slDist;

  const ok =
    score >= CFG.minScore &&
    rr >= CFG.minRR &&
    entry > 0;

  return {
    ok,
    direction: 'BUY',
    score,
    entry,
    tp: entry + tpDist,
    sl: entry - slDist,
    tpPct: tpDist / entry * 100,
    slPct: slDist / entry * 100,
    rr,
    reasons,
    candleTime: b.candle.time,

    features: {
      m1: {
        close: a.close,
        ema9: a.ema9,
        ema21: a.ema21
      },
      m5: {
        close: b.close,
        ema9: b.ema9,
        ema21: b.ema21,
        rsi: b.rsi,
        atr: b.atr,
        adx: b.adx
      },
      m15: {
        close: c.close,
        ema9: c.ema9,
        ema21: c.ema21,
        ema50: c.ema50
      }
    }
  };
}

async function pendingSignal(client = pool) {
  const { rows } = await client.query(
    "SELECT * FROM trade_sinais WHERE estado='PENDENTE' ORDER BY id DESC LIMIT 1"
  );

  return rows[0] || null;
}

async function resolvePending(price, candles5m) {
  const signal = await pendingSignal();

  if (!signal) return;

  const createdAt = new Date(signal.criado_em).getTime();
  const expiresAt = new Date(signal.expiracao_em).getTime();

  const sl = Number(signal.preco_sl);
  const tp = Number(signal.preco_tp);
  const entry = Number(signal.preco_entrada);

  if (![createdAt, expiresAt, sl, tp, entry].every(Number.isFinite)) {
    await logEvent(
      'signal_data_error',
      'Sinal pendente tem dados inválidos',
      { id: signal.id }
    );

    return;
  }

  // Analisa as velas fechadas disponíveis desde a criação do sinal.
  // Se TP e SL forem atingidos dentro da mesma vela, assume LOSS
  // para evitar classificar o resultado de forma excessivamente otimista.

  const eligibleCandles = (candles5m || []).filter(c => {
    const candleCloseTime = c.time + 5 * 60000;

    return candleCloseTime > createdAt && c.time < expiresAt;
  });

  for (const candle of eligibleCandles) {
    let state = null;
    let exit = null;

    if (candle.low <= sl) {
      state = 'LOSS';
      exit = sl;
    } else if (candle.high >= tp) {
      state = 'GREEN';
      exit = tp;
    }

    if (!state) continue;

    const pnl = (exit - entry) / entry * 100;

    const result = await pool.query(
      `UPDATE trade_sinais
       SET estado=$2,
           preco_saida=$3,
           pnl_pct=$4,
           resolvido_em=NOW()
       WHERE id=$1 AND estado='PENDENTE'
       RETURNING id`,
      [signal.id, state, exit, round(pnl, 5)]
    );

    if (!result.rowCount) return;

    await logEvent('signal_resolved', `Sinal ${state}`, {
      id: signal.id,
      exit,
      pnlPct: round(pnl, 5),
      candleTime: candle.time
    });

    await telegramSend(
      `${state === 'GREEN' ? '🟢 GREEN' : '🔴 LOSS'} — CAPUNGA TRADE\n` +
      `Sinal #${signal.id} · BTC/USDT Spot\n` +
      `Entrada de referência: ${round(entry, 2)}\n` +
      `Saída simulada: ${round(exit, 2)}\n` +
      `Variação bruta: ${round(pnl, 3)}%\n` +
      `Resultado por vela fechada; taxas e slippage excluídos.`
    );

    return;
  }

  if (Date.now() >= expiresAt) {
    const result = await pool.query(
      `UPDATE trade_sinais
       SET estado='EXPIRADO',
           preco_saida=$2,
           resolvido_em=NOW()
       WHERE id=$1 AND estado='PENDENTE'
       RETURNING id`,
      [signal.id, price]
    );

    if (result.rowCount) {
      await logEvent(
        'signal_expired',
        'Sinal expirado',
        { id: signal.id, price }
      );

      await telegramSend(
        `⌛ CAPUNGA TRADE — SINAL EXPIRADO\n` +
        `#${signal.id} · BTC/USDT Spot\n` +
        `Preço de referência: ${round(price, 2)}\n` +
        `Expirado sem TP/SL confirmado nas velas disponíveis.`
      );
    }
  }
}

async function createSignal(a, source) {
  const client = await pool.connect();
  let signal;

  try {
    await client.query('BEGIN');

    await client.query(
      "SELECT pg_advisory_xact_lock(hashtext('capunga_trade_signal_engine'))"
    );

    const pending = await pendingSignal(client);

    if (pending) {
      await client.query('COMMIT');

      return {
        opened: false,
        reason: 'pending_exists',
        id: pending.id
      };
    }

    const recent = await client.query(
      'SELECT criado_em FROM trade_sinais ORDER BY id DESC LIMIT 1'
    );

    if (
      recent.rowCount &&
      Date.now() - new Date(recent.rows[0].criado_em).getTime()
        < CFG.antiSpamMs
    ) {
      await client.query('COMMIT');

      return {
        opened: false,
        reason: 'cooldown'
      };
    }

    const expires = new Date(Date.now() + CFG.expiryMs);

    const result = await client.query(
      `INSERT INTO trade_sinais
       (symbol,direcao,preco_entrada,preco_tp,preco_sl,score,estado,
        tp_pct,sl_pct,expiracao_em,metadados)
       VALUES($1,'BUY',$2,$3,$4,$5,'PENDENTE',$6,$7,$8,$9::jsonb)
       RETURNING id,criado_em`,
      [
        CFG.symbol,
        a.entry,
        a.tp,
        a.sl,
        a.score,
        round(a.tpPct, 5),
        round(a.slPct, 5),
        expires,
        JSON.stringify({
          reasons: a.reasons,
          rr: a.rr,
          features: a.features,
          source
        })
      ]
    );

    signal = result.rows[0];

    await client.query('COMMIT');
    lastSignalAt = Date.now();
  } catch (e) {
    try {
      await client.query('ROLLBACK');
    } catch (_) {}

    throw e;
  } finally {
    client.release();
  }

  await logEvent('signal_opened', 'Novo sinal BUY', {
    id: signal.id,
    entry: a.entry,
    tp: a.tp,
    sl: a.sl,
    score: a.score,
    source
  });

  const msg =
    `📣 CAPUNGA TRADE — NOVO SINAL\n\n` +
    `🟢 ATIVO: ${CFG.symbol.replace('USDT', '/USDT')} Spot\n` +
    `📍 DIREÇÃO: BUY / COMPRA\n` +
    `🆔 SINAL: #${signal.id}\n\n` +
    `🎯 Entrada de referência: ${round(a.entry, 2)} USDT\n` +
    `✅ Take Profit: ${round(a.tp, 2)} USDT (+${round(a.tpPct, 2)}%)\n` +
    `🛑 Stop Loss: ${round(a.sl, 2)} USDT (-${round(a.slPct, 2)}%)\n` +
    `⚖️ Relação R:R: ${round(a.rr, 2)}\n` +
    `📊 Score: ${a.score}/8\n` +
    `⏳ Validade: ${Math.round(CFG.expiryMs / 60000)} minutos\n\n` +
    `🔎 Confirmações: ${a.reasons.join('; ')}\n` +
    `📡 Fonte: ${source}\n\n` +
    `⚠️ Sinal informativo para execução manual. Não é uma ordem; preço, taxas e slippage podem alterar o resultado.`;

  await telegramSend(msg);

  return {
    opened: true,
    id: signal.id
  };
}
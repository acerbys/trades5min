// collector.js — пишет в Railway PostgreSQL напрямую
const WebSocket = require('ws');
const fetch = require('node-fetch');
const { Pool } = require('pg');

const pool = new Pool({ connectionString: process.env.DATABASE_URL });
const WS_URL = 'wss://ws-subscriptions-clob.polymarket.com/ws/market';

let currentSlug = null;
let currentConditionId = null;
let currentTokenUp = null;
let currentTokenDown = null;
let nextSlug = null;
let nextConditionId = null;
let nextTokenUp = null;
let nextTokenDown = null;
let ws = null;
let reconnectTimer = null;
let agg = null;
let upsertCounter = 0;

function getCurrentBucket() {
  return Math.floor(Date.now() / 1000 / 300) * 300;
}

function slugFromBucket(ts) {
  return `btc-updown-5m-${ts}`;
}

function getMinute(elapsed) {
  // 0-60s = m1, 60-120 = m2, etc
  if (elapsed < 60) return 1;
  if (elapsed < 120) return 2;
  if (elapsed < 180) return 3;
  if (elapsed < 240) return 4;
  return 5;
}

async function getMarketData(slug) {
  try {
    const res = await fetch(`https://gamma-api.polymarket.com/markets?slug=${slug}`);
    const data = await res.json();
    if (!data || !data[0]) return null;
    const market = data[0];
    let tokenUp = null, tokenDown = null;
    if (market.clobTokenIds) {
      const tokens = JSON.parse(market.clobTokenIds);
      tokenUp = tokens[0];
      tokenDown = tokens[1];
    }
    console.log(`[getMarketData] ${slug} ok`);
    return { conditionId: market.conditionId, tokenUp, tokenDown };
  } catch (err) {
    console.error(`[getMarketData] Error:`, err.message);
    return null;
  }
}

async function saveTrade(trade) {
  try {
    await pool.query(
      `INSERT INTO trades5min (slug, condition_id, timestamp, outcome, price, size, side)
       VALUES ($1, $2, $3, $4, $5, $6, $7)`,
      [trade.slug, trade.condition_id, trade.timestamp, trade.outcome, trade.price, trade.size, trade.side]
    );
  } catch (err) {
    console.error('[saveTrade] Error:', err.message);
  }
}

async function upsertAgg() {
  if (!agg) return;
  try {
    await pool.query(
      `INSERT INTO markets_agg (
        slug, market_start,
        up_start, up_min, up_min_time, up_max, up_max_time,
        up_min_m1, up_max_m1, up_min_m2, up_max_m2, up_min_m3, up_max_m3, up_min_m4, up_max_m4, up_min_m5, up_max_m5,
        down_start, down_min, down_min_time, down_max, down_max_time,
        down_min_m1, down_max_m1, down_min_m2, down_max_m2, down_min_m3, down_max_m3, down_min_m4, down_max_m4, down_min_m5, down_max_m5,
        outcome, updated_at
      ) VALUES (
        $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,
        $18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,NOW()
      )
      ON CONFLICT (slug) DO UPDATE SET
        up_start=EXCLUDED.up_start, up_min=EXCLUDED.up_min, up_min_time=EXCLUDED.up_min_time,
        up_max=EXCLUDED.up_max, up_max_time=EXCLUDED.up_max_time,
        up_min_m1=EXCLUDED.up_min_m1, up_max_m1=EXCLUDED.up_max_m1,
        up_min_m2=EXCLUDED.up_min_m2, up_max_m2=EXCLUDED.up_max_m2,
        up_min_m3=EXCLUDED.up_min_m3, up_max_m3=EXCLUDED.up_max_m3,
        up_min_m4=EXCLUDED.up_min_m4, up_max_m4=EXCLUDED.up_max_m4,
        up_min_m5=EXCLUDED.up_min_m5, up_max_m5=EXCLUDED.up_max_m5,
        down_start=EXCLUDED.down_start, down_min=EXCLUDED.down_min, down_min_time=EXCLUDED.down_min_time,
        down_max=EXCLUDED.down_max, down_max_time=EXCLUDED.down_max_time,
        down_min_m1=EXCLUDED.down_min_m1, down_max_m1=EXCLUDED.down_max_m1,
        down_min_m2=EXCLUDED.down_min_m2, down_max_m2=EXCLUDED.down_max_m2,
        down_min_m3=EXCLUDED.down_min_m3, down_max_m3=EXCLUDED.down_max_m3,
        down_min_m4=EXCLUDED.down_min_m4, down_max_m4=EXCLUDED.down_max_m4,
        down_min_m5=EXCLUDED.down_min_m5, down_max_m5=EXCLUDED.down_max_m5,
        outcome=EXCLUDED.outcome, updated_at=NOW()`,
      [
        agg.slug, agg.market_start,
        agg.up_start, agg.up_min, agg.up_min_time, agg.up_max, agg.up_max_time,
        agg.up_min_m1, agg.up_max_m1, agg.up_min_m2, agg.up_max_m2,
        agg.up_min_m3, agg.up_max_m3, agg.up_min_m4, agg.up_max_m4,
        agg.up_min_m5, agg.up_max_m5,
        agg.down_start, agg.down_min, agg.down_min_time, agg.down_max, agg.down_max_time,
        agg.down_min_m1, agg.down_max_m1, agg.down_min_m2, agg.down_max_m2,
        agg.down_min_m3, agg.down_max_m3, agg.down_min_m4, agg.down_max_m4,
        agg.down_min_m5, agg.down_max_m5,
        agg.outcome
      ]
    );
  } catch (err) {
    console.error('[upsertAgg] Error:', err.message);
  }
}

function initAgg(slug, marketStart) {
  agg = {
    slug, market_start: marketStart,
    up_start: null, up_min: null, up_min_time: null, up_max: null, up_max_time: null,
    up_min_m1: null, up_max_m1: null, up_min_m2: null, up_max_m2: null,
    up_min_m3: null, up_max_m3: null, up_min_m4: null, up_max_m4: null,
    up_min_m5: null, up_max_m5: null,
    down_start: null, down_min: null, down_min_time: null, down_max: null, down_max_time: null,
    down_min_m1: null, down_max_m1: null, down_min_m2: null, down_max_m2: null,
    down_min_m3: null, down_max_m3: null, down_min_m4: null, down_max_m4: null,
    down_min_m5: null, down_max_m5: null,
    outcome: null
  };
}

function updateAgg(trade, marketStart) {
  if (!agg) return;
  const price = trade.price;
  const elapsed = (new Date(trade.timestamp).getTime() / 1000) - marketStart;

  if (elapsed > 300) {
    if (trade.outcome === 'Up' && price > 0.95) agg.outcome = 'UP';
    else if (trade.outcome === 'Up' && price < 0.05) agg.outcome = 'DOWN';
    else if (trade.outcome === 'Down' && price > 0.95) agg.outcome = 'DOWN';
    else if (trade.outcome === 'Down' && price < 0.05) agg.outcome = 'UP';
    return;
  }

  const m = getMinute(elapsed);

  if (trade.outcome === 'Up') {
    if (agg.up_start === null) agg.up_start = price;
    if (agg.up_min === null || price < agg.up_min) { agg.up_min = price; agg.up_min_time = elapsed; }
    if (agg.up_max === null || price > agg.up_max) { agg.up_max = price; agg.up_max_time = elapsed; }
    if (agg[`up_min_m${m}`] === null || price < agg[`up_min_m${m}`]) agg[`up_min_m${m}`] = price;
    if (agg[`up_max_m${m}`] === null || price > agg[`up_max_m${m}`]) agg[`up_max_m${m}`] = price;
  } else if (trade.outcome === 'Down') {
    if (agg.down_start === null) agg.down_start = price;
    if (agg.down_min === null || price < agg.down_min) { agg.down_min = price; agg.down_min_time = elapsed; }
    if (agg.down_max === null || price > agg.down_max) { agg.down_max = price; agg.down_max_time = elapsed; }
    if (agg[`down_min_m${m}`] === null || price < agg[`down_min_m${m}`]) agg[`down_min_m${m}`] = price;
    if (agg[`down_max_m${m}`] === null || price > agg[`down_max_m${m}`]) agg[`down_max_m${m}`] = price;
  }
}

function isTrade(msg) {
  return msg.asset_id && msg.price !== undefined && msg.size !== undefined &&
         !msg.price_changes && !msg.bids && !msg.asks;
}

function connect(slug, conditionId, tokenUp, tokenDown) {
  if (ws) { ws.removeAllListeners(); ws.terminate(); ws = null; }
  if (!tokenUp || !tokenDown) { console.error('[connect] No tokenIds'); return; }

  const marketStart = parseInt(slug.split('-').pop());
  currentSlug = slug; currentConditionId = conditionId;
  currentTokenUp = tokenUp; currentTokenDown = tokenDown;
  initAgg(slug, marketStart);

  console.log(`[connect] Connecting for ${slug}`);
  ws = new WebSocket(WS_URL);

  ws.on('open', () => {
    console.log('[ws] Connected');
    ws.send(JSON.stringify({ auth: {}, type: 'Market', assets_ids: [tokenUp, tokenDown] }));
    console.log(`[ws] Subscribed to ${slug}`);
  });

  ws.on('message', async (data) => {
    try {
      const messages = JSON.parse(data.toString());
      const arr = Array.isArray(messages) ? messages : [messages];
      for (const msg of arr) {
        if (!isTrade(msg)) continue;
        let outcome = 'Unknown';
        if (msg.asset_id === currentTokenUp) outcome = 'Up';
        else if (msg.asset_id === currentTokenDown) outcome = 'Down';
        const ts = msg.timestamp
          ? (msg.timestamp > 1e12 ? new Date(Number(msg.timestamp)).toISOString() : new Date(Number(msg.timestamp) * 1000).toISOString())
          : new Date().toISOString();
        const trade = {
          slug: currentSlug, condition_id: currentConditionId,
          timestamp: ts, outcome,
          price: parseFloat(msg.price), size: parseFloat(msg.size), side: msg.side || null
        };
        // trades5min отключён — пишем только агрегат
        // await saveTrade(trade);
        updateAgg(trade, marketStart);
        upsertCounter++;
        if (upsertCounter % 10 === 0) await upsertAgg();
        console.log(`[trade] ${trade.outcome} | ${trade.price}`);
      }
    } catch (err) { console.error('[ws message] Error:', err.message); }
  });

  ws.on('error', (err) => console.error('[ws] Error:', err.message));
  ws.on('close', () => {
    console.log('[ws] Disconnected, reconnecting in 5s...');
    upsertAgg();
    clearTimeout(reconnectTimer);
    reconnectTimer = setTimeout(() => connect(currentSlug, currentConditionId, currentTokenUp, currentTokenDown), 5000);
  });
}

async function prefetchNext() {
  const nextBucket = getCurrentBucket() + 300;
  const slug = slugFromBucket(nextBucket);
  if (slug === nextSlug) return;
  const market = await getMarketData(slug);
  if (market && market.tokenUp) {
    nextSlug = slug; nextConditionId = market.conditionId;
    nextTokenUp = market.tokenUp; nextTokenDown = market.tokenDown;
    console.log(`[prefetch] Ready: ${slug}`);
  }
}

async function checkMarket() {
  const nowSec = Math.floor(Date.now() / 1000);
  const currentBucket = getCurrentBucket();
  const slug = slugFromBucket(currentBucket);
  const secsToNext = 300 - (nowSec - currentBucket);
  if (slug !== currentSlug) {
    console.log(`[checkMarket] Switching to ${slug}`);
    await upsertAgg();
    if (nextSlug === slug && nextTokenUp) {
      connect(slug, nextConditionId, nextTokenUp, nextTokenDown); nextSlug = null;
    } else {
      const market = await getMarketData(slug);
      if (market && market.tokenUp) connect(slug, market.conditionId, market.tokenUp, market.tokenDown);
    }
  }
  if (secsToNext <= 30) await prefetchNext();
}

setInterval(async () => { await upsertAgg(); }, 30000);

// All coins to track
const COINS = ['btc', 'eth', 'sol', 'xrp', 'doge', 'hype', 'bnb'];

// Per-coin state
const coinState = {};
for (const coin of COINS) {
  coinState[coin] = {
    currentSlug: null, conditionId: null, tokenUp: null, tokenDown: null,
    nextSlug: null, nextConditionId: null, nextTokenUp: null, nextTokenDown: null,
    nextWs: null, nextWsTokenUp: null, nextWsTokenDown: null,
    ws: null, reconnectTimer: null, agg: null, upsertCounter: 0
  };
}

function slugFromBucketCoin(ts, coin) {
  return `${coin}-updown-5m-${ts}`;
}

async function getMarketDataCoin(slug) {
  try {
    const res = await fetch(`https://gamma-api.polymarket.com/markets?slug=${slug}`);
    const data = await res.json();
    if (!data || !data[0]) return null;
    const market = data[0];
    let tokenUp = null, tokenDown = null;
    if (market.clobTokenIds) {
      const tokens = JSON.parse(market.clobTokenIds);
      tokenUp = tokens[0]; tokenDown = tokens[1];
    }
    return { conditionId: market.conditionId, tokenUp, tokenDown };
  } catch (err) {
    console.error(`[getMarketData] Error for ${slug}:`, err.message);
    return null;
  }
}

function initAggCoin(coin, slug, marketStart) {
  coinState[coin].agg = {
    slug, coin, market_start: marketStart,
    up_start: null,
    up_min: null, up_min_time: null, up_max: null, up_max_time: null,
    up_min_m1: null, up_max_m1: null, up_min_m2: null, up_max_m2: null,
    up_min_m3: null, up_max_m3: null, up_min_m4: null, up_max_m4: null,
    up_min_m5: null, up_max_m5: null,
    down_start: null,
    down_min: null, down_min_time: null, down_max: null, down_max_time: null,
    down_min_m1: null, down_max_m1: null, down_min_m2: null, down_max_m2: null,
    down_min_m3: null, down_max_m3: null, down_min_m4: null, down_max_m4: null,
    down_min_m5: null, down_max_m5: null,
    outcome: null
  };
}

function updateAggCoin(coin, trade, marketStart) {
  const a = coinState[coin].agg;
  if (!a) return;
  const price = trade.price;
  const elapsed = (new Date(trade.timestamp).getTime() / 1000) - marketStart;

  if (elapsed > 300) {
    if (trade.outcome === 'Up' && price > 0.95) a.outcome = 'UP';
    else if (trade.outcome === 'Up' && price < 0.05) a.outcome = 'DOWN';
    else if (trade.outcome === 'Down' && price > 0.95) a.outcome = 'DOWN';
    else if (trade.outcome === 'Down' && price < 0.05) a.outcome = 'UP';
    return;
  }

  // Accept trades up to 3 seconds before market start (clock skew)
  if (elapsed < -3) return;

  const effectiveElapsed = Math.max(0, elapsed);
  const m = effectiveElapsed < 60 ? 1 : effectiveElapsed < 120 ? 2 : effectiveElapsed < 180 ? 3 : effectiveElapsed < 240 ? 4 : 5;

  // Ignore extreme prices (post-market settlement trades: >95c or <5c)
  if (price > 0.95 || price < 0.05) return;

  if (trade.outcome === 'Up') {
    if (a.up_start === null) a.up_start = price;
    if (a.up_min === null || price < a.up_min) { a.up_min = price; a.up_min_time = effectiveElapsed; }
    if (a.up_max === null || price > a.up_max) { a.up_max = price; a.up_max_time = effectiveElapsed; }
    if (a[`up_min_m${m}`] === null || price < a[`up_min_m${m}`]) a[`up_min_m${m}`] = price;
    if (a[`up_max_m${m}`] === null || price > a[`up_max_m${m}`]) a[`up_max_m${m}`] = price;
  } else if (trade.outcome === 'Down') {
    if (a.down_start === null) a.down_start = price;
    if (a.down_min === null || price < a.down_min) { a.down_min = price; a.down_min_time = effectiveElapsed; }
    if (a.down_max === null || price > a.down_max) { a.down_max = price; a.down_max_time = effectiveElapsed; }
    if (a[`down_min_m${m}`] === null || price < a[`down_min_m${m}`]) a[`down_min_m${m}`] = price;
    if (a[`down_max_m${m}`] === null || price > a[`down_max_m${m}`]) a[`down_max_m${m}`] = price;
  }
}

async function upsertAggCoin(coin) {
  const a = coinState[coin].agg;
  if (!a) return;
  try {
    await pool.query(
      `INSERT INTO markets_agg (
        slug, coin, market_start,
        up_start, up_min, up_min_time, up_max, up_max_time,
        up_min_m1, up_max_m1, up_min_m2, up_max_m2, up_min_m3, up_max_m3, up_min_m4, up_max_m4, up_min_m5, up_max_m5,
        down_start, down_min, down_min_time, down_max, down_max_time,
        down_min_m1, down_max_m1, down_min_m2, down_max_m2, down_min_m3, down_max_m3, down_min_m4, down_max_m4, down_min_m5, down_max_m5,
        outcome, updated_at
      ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,NOW())
      ON CONFLICT (slug) DO UPDATE SET
        up_start=EXCLUDED.up_start, up_min=EXCLUDED.up_min, up_min_time=EXCLUDED.up_min_time,
        up_max=EXCLUDED.up_max, up_max_time=EXCLUDED.up_max_time,
        up_min_m1=EXCLUDED.up_min_m1, up_max_m1=EXCLUDED.up_max_m1,
        up_min_m2=EXCLUDED.up_min_m2, up_max_m2=EXCLUDED.up_max_m2,
        up_min_m3=EXCLUDED.up_min_m3, up_max_m3=EXCLUDED.up_max_m3,
        up_min_m4=EXCLUDED.up_min_m4, up_max_m4=EXCLUDED.up_max_m4,
        up_min_m5=EXCLUDED.up_min_m5, up_max_m5=EXCLUDED.up_max_m5,
        down_start=EXCLUDED.down_start, down_min=EXCLUDED.down_min, down_min_time=EXCLUDED.down_min_time,
        down_max=EXCLUDED.down_max, down_max_time=EXCLUDED.down_max_time,
        down_min_m1=EXCLUDED.down_min_m1, down_max_m1=EXCLUDED.down_max_m1,
        down_min_m2=EXCLUDED.down_min_m2, down_max_m2=EXCLUDED.down_max_m2,
        down_min_m3=EXCLUDED.down_min_m3, down_max_m3=EXCLUDED.down_max_m3,
        down_min_m4=EXCLUDED.down_min_m4, down_max_m4=EXCLUDED.down_max_m4,
        down_min_m5=EXCLUDED.down_min_m5, down_max_m5=EXCLUDED.down_max_m5,
        outcome=EXCLUDED.outcome, updated_at=NOW()`,
      [a.slug, coin, a.market_start,
       a.up_start, a.up_min, a.up_min_time, a.up_max, a.up_max_time,
       a.up_min_m1, a.up_max_m1, a.up_min_m2, a.up_max_m2,
       a.up_min_m3, a.up_max_m3, a.up_min_m4, a.up_max_m4, a.up_min_m5, a.up_max_m5,
       a.down_start, a.down_min, a.down_min_time, a.down_max, a.down_max_time,
       a.down_min_m1, a.down_max_m1, a.down_min_m2, a.down_max_m2,
       a.down_min_m3, a.down_max_m3, a.down_min_m4, a.down_max_m4, a.down_min_m5, a.down_max_m5,
       a.outcome]
    );
  } catch (err) {
    console.error(`[upsertAgg] ${coin} Error:`, err.message);
  }
}

function connectCoin(coin, slug, conditionId, tokenUp, tokenDown) {
  const state = coinState[coin];
  if (state.ws) { state.ws.removeAllListeners(); state.ws.terminate(); state.ws = null; }
  if (!tokenUp || !tokenDown) return;

  const marketStart = parseInt(slug.split('-').pop());
  state.currentSlug = slug; state.conditionId = conditionId;
  state.tokenUp = tokenUp; state.tokenDown = tokenDown;
  initAggCoin(coin, slug, marketStart);

  console.log(`[${coin}] Connecting for ${slug}`);
  const ws = new WebSocket(WS_URL);
  state.ws = ws;

  ws.on('open', () => {
    ws.send(JSON.stringify({ auth: {}, type: 'Market', assets_ids: [tokenUp, tokenDown] }));
    console.log(`[${coin}] Subscribed to ${slug}`);
  });

  ws.on('message', async (data) => {
    try {
      const messages = JSON.parse(data.toString());
      const arr = Array.isArray(messages) ? messages : [messages];
      for (const msg of arr) {
        if (!msg.asset_id || msg.price === undefined || msg.size === undefined || msg.price_changes || msg.bids || msg.asks) continue;
        let outcome = 'Unknown';
        if (msg.asset_id === state.tokenUp) outcome = 'Up';
        else if (msg.asset_id === state.tokenDown) outcome = 'Down';
        const ts = msg.timestamp
          ? (msg.timestamp > 1e12 ? new Date(Number(msg.timestamp)).toISOString() : new Date(Number(msg.timestamp) * 1000).toISOString())
          : new Date().toISOString();
        const mStart = parseInt(state.currentSlug.split('-').pop());
        const trade = { slug: state.currentSlug, condition_id: state.conditionId, timestamp: ts, outcome, price: parseFloat(msg.price), size: parseFloat(msg.size), side: msg.side || null };
        updateAggCoin(coin, trade, mStart);
        state.upsertCounter++;
        if (state.upsertCounter % 10 === 0) await upsertAggCoin(coin);
      }
    } catch (err) { console.error(`[${coin}] message error:`, err.message); }
  });

  ws.on('error', (err) => console.error(`[${coin}] WS error:`, err.message));
  ws.on('close', () => {
    console.log(`[${coin}] Disconnected, reconnecting in 5s...`);
    upsertAggCoin(coin);
    clearTimeout(state.reconnectTimer);
    state.reconnectTimer = setTimeout(() => connectCoin(coin, state.currentSlug, state.conditionId, state.tokenUp, state.tokenDown), 5000);
  });
}

async function checkAllMarkets() {
  const nowSec = Math.floor(Date.now() / 1000);
  const bucket = getCurrentBucket();
  const secsToNext = 300 - (nowSec - bucket);

  // Check which coins need new market
  const coinsNeedSwitch = COINS.filter(coin => slugFromBucketCoin(bucket, coin) !== coinState[coin].currentSlug);

  if (coinsNeedSwitch.length > 0) {
    // FIRST: connect immediately all coins that have prefetched data - no waiting
    const coinsNeedFetch = [];
    for (const coin of coinsNeedSwitch) {
      const state = coinState[coin];
      const slug = slugFromBucketCoin(bucket, coin);
      if (state.nextSlug === slug && state.nextTokenUp) {
        // Use pre-connected WS if available
        if (state.nextWs && state.nextWs.readyState <= 1) {
          if (state.ws) { state.ws.removeAllListeners(); state.ws.terminate(); }
          const mStart = parseInt(slug.split('-').pop());
          state.currentSlug = slug; state.conditionId = state.nextConditionId;
          state.tokenUp = state.nextTokenUp; state.tokenDown = state.nextTokenDown;
          state.ws = state.nextWs; state.nextWs = null;
          initAggCoin(coin, slug, mStart);
          // Set up message handler on pre-connected ws
          state.ws.removeAllListeners('message');
          state.ws.removeAllListeners('error');
          state.ws.removeAllListeners('close');
          state.ws.on('message', async (data) => {
            try {
              const messages = JSON.parse(data.toString());
              const arr = Array.isArray(messages) ? messages : [messages];
              for (const msg of arr) {
                if (!msg.asset_id || msg.price === undefined || msg.size === undefined || msg.price_changes || msg.bids || msg.asks) continue;
                let outcome = 'Unknown';
                if (msg.asset_id === state.tokenUp) outcome = 'Up';
                else if (msg.asset_id === state.tokenDown) outcome = 'Down';
                const ts = msg.timestamp ? (msg.timestamp > 1e12 ? new Date(Number(msg.timestamp)).toISOString() : new Date(Number(msg.timestamp)*1000).toISOString()) : new Date().toISOString();
                const ms2 = parseInt(state.currentSlug.split('-').pop());
                const trade = { slug: state.currentSlug, condition_id: state.conditionId, timestamp: ts, outcome, price: parseFloat(msg.price), size: parseFloat(msg.size), side: msg.side || null };
                updateAggCoin(coin, trade, ms2);
                state.upsertCounter++;
                if (state.upsertCounter % 10 === 0) await upsertAggCoin(coin);
              }
            } catch(err) { console.error(`[${coin}] message error:`, err.message); }
          });
          state.ws.on('error', (err) => console.error(`[${coin}] WS error:`, err.message));
          state.ws.on('close', () => {
            upsertAggCoin(coin);
            clearTimeout(state.reconnectTimer);
            state.reconnectTimer = setTimeout(() => connectCoin(coin, state.currentSlug, state.conditionId, state.tokenUp, state.tokenDown), 5000);
          });
          console.log(`[${coin}] Switched to pre-connected ${slug}`);
        } else {
          connectCoin(coin, slug, state.nextConditionId, state.nextTokenUp, state.nextTokenDown);
        }
        state.nextSlug = null;
      } else {
        coinsNeedFetch.push(coin);
      }
    }

    // THEN: upsert old data in background (don't await)
    Promise.all(coinsNeedSwitch.map(coin => upsertAggCoin(coin))).catch(e => console.error('[upsert bg]', e.message));

    // Fetch missing in parallel and connect
    if (coinsNeedFetch.length > 0) {
      Promise.all(
        coinsNeedFetch.map(coin => {
          const slug = slugFromBucketCoin(bucket, coin);
          return getMarketDataCoin(slug).then(market => {
            if (market && market.tokenUp) connectCoin(coin, slug, market.conditionId, market.tokenUp, market.tokenDown);
          });
        })
      ).catch(e => console.error('[fetch bg]', e.message));
    }
  }

  // Prefetch next markets 30s before switch - all in parallel
  if (secsToNext <= 60) {
    const coinsToPrefetch = COINS.filter(coin => {
      const nextSlug = slugFromBucketCoin(bucket + 300, coin);
      return coinState[coin].nextSlug !== nextSlug;
    });

    Promise.all(
      coinsToPrefetch.map(coin => {
        const nextSlug = slugFromBucketCoin(bucket + 300, coin);
        return getMarketDataCoin(nextSlug).then(market => {
          if (market && market.tokenUp) {
            coinState[coin].nextSlug = nextSlug;
            coinState[coin].nextConditionId = market.conditionId;
            coinState[coin].nextTokenUp = market.tokenUp;
            coinState[coin].nextTokenDown = market.tokenDown;
            console.log(`[${coin}] Prefetched: ${nextSlug}`);


          }
        });
      })
    );
  }

  // Pre-connect new WS 5 seconds before market start
  if (secsToNext <= 5 && secsToNext > 0) {
    for (const coin of COINS) {
      const state = coinState[coin];
      const nextSlug = slugFromBucketCoin(bucket + 300, coin);
      if (state.nextSlug === nextSlug && state.nextTokenUp && !state.nextWs) {
        const nextWs = new WebSocket(WS_URL);
        state.nextWs = nextWs;
        nextWs.on('open', () => {
          nextWs.send(JSON.stringify({ auth: {}, type: 'Market', assets_ids: [state.nextTokenUp, state.nextTokenDown] }));
          console.log(`[${coin}] Pre-connected to ${nextSlug}`);
        });
        nextWs.on('error', () => { state.nextWs = null; });
      }
    }
  }

  // Extra upsert 10 seconds after market close to capture outcome
  const secsAfterClose = (nowSec - bucket) - 300;
  if (secsAfterClose >= 10 && secsAfterClose <= 15) {
    await Promise.all(COINS.map(coin => upsertAggCoin(coin)));
    console.log('[checkAllMarkets] Post-close upsert for outcomes');
  }
}

// Periodic upsert every 30s
setInterval(async () => {
  for (const coin of COINS) await upsertAggCoin(coin);
}, 30000);

async function main() {
  console.log('[main] Starting multi-coin collector...');
  const bucket = getCurrentBucket();

  // Fetch all market data in parallel, then connect all at once
  const results = await Promise.all(
    COINS.map(coin => {
      const slug = slugFromBucketCoin(bucket, coin);
      return getMarketDataCoin(slug).then(market => ({ coin, slug, market }));
    })
  );

  for (const { coin, slug, market } of results) {
    if (market && market.tokenUp) {
      connectCoin(coin, slug, market.conditionId, market.tokenUp, market.tokenDown);
    }
  }

  setInterval(checkAllMarkets, 1000);
}

main().catch(err => { console.error('[main] Fatal:', err); process.exit(1); });

// collector.js
const WebSocket = require('ws');
const fetch = require('node-fetch');

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
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

// In-memory агрегат текущего маркета
let agg = null;

function getCurrentBucket() {
  return Math.floor(Date.now() / 1000 / 300) * 300;
}

function slugFromBucket(ts) {
  return `btc-updown-5m-${ts}`;
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
    const res = await fetch(`${SUPABASE_URL}/rest/v1/trades5min`, {
      method: 'POST',
      headers: {
        'apikey': SUPABASE_KEY,
        'Authorization': `Bearer ${SUPABASE_KEY}`,
        'Content-Type': 'application/json',
        'Prefer': 'return=minimal'
      },
      body: JSON.stringify(trade)
    });
    if (!res.ok) console.error('[saveTrade] Error:', await res.text());
  } catch (err) {
    console.error('[saveTrade]:', err.message);
  }
}

async function upsertAgg() {
  if (!agg) return;
  try {
    const res = await fetch(`${SUPABASE_URL}/rest/v1/markets_agg`, {
      method: 'POST',
      headers: {
        'apikey': SUPABASE_KEY,
        'Authorization': `Bearer ${SUPABASE_KEY}`,
        'Content-Type': 'application/json',
        'Prefer': 'resolution=merge-duplicates,return=minimal'
      },
      body: JSON.stringify({
        slug: agg.slug,
        market_start: agg.market_start,
        up_start: agg.up_start,
        up_min: agg.up_min,
        up_min_time: agg.up_min_time,
        up_max: agg.up_max,
        up_max_time: agg.up_max_time,
        down_start: agg.down_start,
        down_min: agg.down_min,
        down_min_time: agg.down_min_time,
        down_max: agg.down_max,
        down_max_time: agg.down_max_time,
        outcome: agg.outcome,
        updated_at: new Date().toISOString()
      })
    });
    if (!res.ok) console.error('[upsertAgg] Error:', await res.text());
  } catch (err) {
    console.error('[upsertAgg]:', err.message);
  }
}

function initAgg(slug, marketStart) {
  agg = {
    slug, market_start: marketStart,
    up_start: null, up_min: null, up_min_time: null,
    up_max: null, up_max_time: null,
    down_start: null, down_min: null, down_min_time: null,
    down_max: null, down_max_time: null,
    outcome: null
  };
}

function updateAgg(trade, marketStart) {
  if (!agg) return;
  const price = trade.price;
  const elapsed = (new Date(trade.timestamp).getTime() / 1000) - marketStart;

  if (trade.outcome === 'Up') {
    if (agg.up_start === null) agg.up_start = price;
    if (agg.up_min === null || price < agg.up_min) { agg.up_min = price; agg.up_min_time = elapsed; }
    if (agg.up_max === null || price > agg.up_max) { agg.up_max = price; agg.up_max_time = elapsed; }
    // Определяем исход по последней цене
    if (price > 0.95) agg.outcome = 'UP';
    else if (price < 0.05) agg.outcome = 'DOWN';
  } else if (trade.outcome === 'Down') {
    if (agg.down_start === null) agg.down_start = price;
    if (agg.down_min === null || price < agg.down_min) { agg.down_min = price; agg.down_min_time = elapsed; }
    if (agg.down_max === null || price > agg.down_max) { agg.down_max = price; agg.down_max_time = elapsed; }
    if (price > 0.95) agg.outcome = 'DOWN';
    else if (price < 0.05) agg.outcome = 'UP';
  }
}

let upsertCounter = 0;

function isTrade(msg) {
  return msg.asset_id &&
         msg.price !== undefined &&
         msg.size !== undefined &&
         !msg.price_changes &&
         !msg.bids &&
         !msg.asks;
}

function connect(slug, conditionId, tokenUp, tokenDown) {
  if (ws) {
    ws.removeAllListeners();
    ws.terminate();
    ws = null;
  }

  if (!tokenUp || !tokenDown) {
    console.error('[connect] No tokenIds');
    return;
  }

  const marketStart = parseInt(slug.split('-').pop());
  currentSlug = slug;
  currentConditionId = conditionId;
  currentTokenUp = tokenUp;
  currentTokenDown = tokenDown;

  initAgg(slug, marketStart);
  console.log(`[connect] Connecting for ${slug}`);

  ws = new WebSocket(WS_URL);

  ws.on('open', () => {
    console.log('[ws] Connected');
    ws.send(JSON.stringify({
      auth: {},
      type: 'Market',
      assets_ids: [tokenUp, tokenDown]
    }));
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
          ? (msg.timestamp > 1e12
              ? new Date(Number(msg.timestamp)).toISOString()
              : new Date(Number(msg.timestamp) * 1000).toISOString())
          : new Date().toISOString();

        const trade = {
          slug: currentSlug,
          condition_id: currentConditionId,
          timestamp: ts,
          outcome,
          price: parseFloat(msg.price),
          size: parseFloat(msg.size),
          side: msg.side || null
        };

        // Сохраняем сырую сделку
        await saveTrade(trade);

        // Обновляем агрегат в памяти
        updateAgg(trade, marketStart);

        // Пишем агрегат в БД каждые 10 сделок
        upsertCounter++;
        if (upsertCounter % 10 === 0) {
          await upsertAgg();
        }

        console.log(`[trade] ${trade.outcome} | ${trade.price} | ${trade.size}`);
      }
    } catch (err) {
      console.error('[ws message] Error:', err.message);
    }
  });

  ws.on('error', (err) => console.error('[ws] Error:', err.message));

  ws.on('close', () => {
    console.log('[ws] Disconnected, reconnecting in 5s...');
    // Сохраняем агрегат перед реконнектом
    upsertAgg();
    clearTimeout(reconnectTimer);
    reconnectTimer = setTimeout(() => connect(currentSlug, currentConditionId, currentTokenUp, currentTokenDown), 5000);
  });
}

async function prefetchNext() {
  const nextBucket = getCurrentBucket() + 300;
  const slug = slugFromBucket(nextBucket);
  if (slug === nextSlug) return;
  console.log(`[prefetch] Loading next: ${slug}`);
  const market = await getMarketData(slug);
  if (market && market.tokenUp) {
    nextSlug = slug;
    nextConditionId = market.conditionId;
    nextTokenUp = market.tokenUp;
    nextTokenDown = market.tokenDown;
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
    // Финальный upsert старого маркета
    await upsertAgg();

    if (nextSlug === slug && nextTokenUp) {
      connect(slug, nextConditionId, nextTokenUp, nextTokenDown);
      nextSlug = null;
    } else {
      const market = await getMarketData(slug);
      if (market && market.tokenUp) {
        connect(slug, market.conditionId, market.tokenUp, market.tokenDown);
      }
    }
  }

  // Prefetch за 30 секунд до смены маркета
  if (secsToNext <= 30) {
    await prefetchNext();
  }
}

// Периодический upsert каждые 30 секунд
setInterval(async () => {
  await upsertAgg();
}, 30000);

async function main() {
  console.log('[main] Starting collector...');
  console.log(`[main] Supabase: ${SUPABASE_URL}`);

  const bucket = getCurrentBucket();
  const slug = slugFromBucket(bucket);
  const market = await getMarketData(slug);
  if (market && market.tokenUp) {
    connect(slug, market.conditionId, market.tokenUp, market.tokenDown);
  }

  setInterval(checkMarket, 5000);
}

main().catch(err => {
  console.error('[main] Fatal:', err);
  process.exit(1);
});

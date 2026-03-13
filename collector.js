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
let nextTokenUp = null;
let nextTokenDown = null;
let nextConditionId = null;
let ws = null;
let reconnectTimer = null;

// Текущий bucket (активный маркет)
function getCurrentBucket() {
  return Math.floor(Date.now() / 1000 / 300) * 300;
}

// Следующий bucket
function getNextBucket() {
  return getCurrentBucket() + 300;
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
    console.log(`[getMarketData] ${slug} tokenUp=${tokenUp ? tokenUp.slice(0,8) : 'null'}...`);
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
    if (!res.ok) {
      const err = await res.text();
      console.error('[saveTrade] Error:', err);
    }
  } catch (err) {
    console.error('[saveTrade] Fetch error:', err.message);
  }
}

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
    console.error('[connect] No tokenIds, skipping');
    return;
  }

  currentSlug = slug;
  currentConditionId = conditionId;
  currentTokenUp = tokenUp;
  currentTokenDown = tokenDown;

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
        console.log(`[trade] ${trade.outcome} | price=${trade.price} | size=${trade.size}`);
        await saveTrade(trade);
      }
    } catch (err) {
      console.error('[ws message] Error:', err.message);
    }
  });

  ws.on('error', (err) => console.error('[ws] Error:', err.message));

  ws.on('close', () => {
    console.log('[ws] Disconnected, reconnecting in 5s...');
    clearTimeout(reconnectTimer);
    reconnectTimer = setTimeout(() => connect(currentSlug, currentConditionId, currentTokenUp, currentTokenDown), 5000);
  });
}

// Предзагрузить данные следующего маркета
async function prefetchNext() {
  const nextBucket = getNextBucket();
  const slug = slugFromBucket(nextBucket);
  if (slug === nextSlug) return; // уже загружен
  console.log(`[prefetch] Loading next market: ${slug}`);
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
  const secsIntoMarket = nowSec - currentBucket;
  const secsToNext = 300 - secsIntoMarket;

  // Переключаемся на новый маркет
  if (slug !== currentSlug) {
    console.log(`[checkMarket] Switching to ${slug}`);

    // Если следующий маркет уже prefetch'нут — используем
    if (nextSlug === slug && nextTokenUp) {
      connect(slug, nextConditionId, nextTokenUp, nextTokenDown);
      nextSlug = null;
    } else {
      // Иначе загружаем сейчас
      const market = await getMarketData(slug);
      if (market && market.tokenUp) {
        connect(slug, market.conditionId, market.tokenUp, market.tokenDown);
      }
    }
  }

  // Предзагружаем следующий маркет за 30 секунд до старта
  if (secsToNext <= 30) {
    await prefetchNext();
  }
}

async function main() {
  console.log('[main] Starting BTC 5min WebSocket collector...');
  console.log(`[main] Supabase URL: ${SUPABASE_URL}`);

  // Предзагрузить текущий маркет сразу
  const currentBucket = getCurrentBucket();
  const slug = slugFromBucket(currentBucket);
  const market = await getMarketData(slug);
  if (market && market.tokenUp) {
    connect(slug, market.conditionId, market.tokenUp, market.tokenDown);
  }

  // Проверять каждые 5 секунд
  setInterval(checkMarket, 5000);
}

main().catch(err => {
  console.error('[main] Fatal error:', err);
  process.exit(1);
});

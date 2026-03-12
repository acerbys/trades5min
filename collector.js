// collector.js
// WebSocket коллектор для BTC 5min маркетов Polymarket
// Подписывается на tokenIds (UP/DOWN), пишет сделки в Supabase trades5min

const WebSocket = require('ws');
const fetch = require('node-fetch');

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const WS_URL = 'wss://ws-subscriptions-clob.polymarket.com/ws/market';

let currentSlug = null;
let currentConditionId = null;
let currentTokenUp = null;
let currentTokenDown = null;
let ws = null;
let reconnectTimer = null;

function getCurrentSlug() {
  const now = Math.floor(Date.now() / 1000);
  const bucket = Math.floor(now / 300) * 300;
  return `btc-updown-5m-${bucket}`;
}

async function getMarketData(slug) {
  try {
    const res = await fetch(`https://gamma-api.polymarket.com/markets?slug=${slug}`);
    const data = await res.json();
    if (!data || !data[0]) return null;
    const market = data[0];
    let tokenUp = null;
    let tokenDown = null;
    if (market.clobTokenIds) {
      const tokens = JSON.parse(market.clobTokenIds);
      tokenUp = tokens[0];
      tokenDown = tokens[1];
    }
    console.log(`[getMarketData] conditionId=${market.conditionId} tokenUp=${tokenUp ? tokenUp.slice(0,10) : 'null'} tokenDown=${tokenDown ? tokenDown.slice(0,10) : 'null'}`);
    return { conditionId: market.conditionId, tokenUp, tokenDown };
  } catch (err) {
    console.error(`[getMarketData] Error for ${slug}:`, err.message);
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
      console.error('[saveTrade] Supabase error:', err);
    }
  } catch (err) {
    console.error('[saveTrade] Fetch error:', err.message);
  }
}

function connect() {
  if (ws) {
    ws.removeAllListeners();
    ws.terminate();
    ws = null;
  }

  if (!currentTokenUp || !currentTokenDown) {
    console.error('[connect] No tokenIds, skipping');
    return;
  }

  console.log(`[connect] Connecting for ${currentSlug}`);
  ws = new WebSocket(WS_URL);

  ws.on('open', () => {
    console.log('[ws] Connected');
    const sub = {
      auth: {},
      type: 'Market',
      assets_ids: [currentTokenUp, currentTokenDown]
    };
    ws.send(JSON.stringify(sub));
    console.log(`[ws] Subscribed UP=${currentTokenUp.slice(0,10)}... DOWN=${currentTokenDown.slice(0,10)}...`);
  });

  ws.on('message', async (data) => {
    try {
      const messages = JSON.parse(data.toString());
      const arr = Array.isArray(messages) ? messages : [messages];
      for (const msg of arr) {
        console.log('[ws raw]', JSON.stringify(msg).slice(0, 200));
        const etype = msg.event_type || msg.type || '';
        if (etype !== 'trade') continue;
        let outcome = 'Unknown';
        if (msg.asset_id === currentTokenUp) outcome = 'Up';
        else if (msg.asset_id === currentTokenDown) outcome = 'Down';
        const trade = {
          slug: currentSlug,
          condition_id: currentConditionId,
          timestamp: new Date(Number(msg.timestamp) * 1000).toISOString(),
          outcome,
          price: parseFloat(msg.price),
          size: parseFloat(msg.size),
          side: msg.side || null
        };
        console.log(`[trade] ${trade.slug} | ${trade.outcome} | ${trade.price} | ${trade.size}`);
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
    reconnectTimer = setTimeout(connect, 5000);
  });
}

async function checkMarket() {
  const newSlug = getCurrentSlug();
  if (newSlug === currentSlug) return;
  console.log(`[checkMarket] New market: ${newSlug}`);
  const market = await getMarketData(newSlug);
  if (!market || !market.tokenUp) {
    console.error(`[checkMarket] No market data for ${newSlug}`);
    return;
  }
  currentSlug = newSlug;
  currentConditionId = market.conditionId;
  currentTokenUp = market.tokenUp;
  currentTokenDown = market.tokenDown;
  connect();
}

async function main() {
  console.log('[main] Starting BTC 5min WebSocket collector...');
  console.log(`[main] Supabase URL: ${SUPABASE_URL}`);
  await checkMarket();
  setInterval(checkMarket, 10_000);
}

main().catch(err => {
  console.error('[main] Fatal error:', err);
  process.exit(1);
});

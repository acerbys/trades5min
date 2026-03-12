// collector.js
// WebSocket коллектор для BTC 5min маркетов Polymarket
// Пишет каждую сделку в Supabase trades5min в реальном времени

const WebSocket = require('ws');
const fetch = require('node-fetch');

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const WS_URL = 'wss://ws-subscriptions-clob.polymarket.com/ws/market';

let currentSlug = null;
let currentConditionId = null;
let ws = null;
let reconnectTimer = null;
let marketCheckTimer = null;

// Получить текущий slug BTC 5min маркета
function getCurrentSlug() {
  const now = Math.floor(Date.now() / 1000);
  const bucket = Math.floor(now / 300) * 300;
  return `btc-updown-5m-${bucket}`;
}

// Получить conditionId маркета из Gamma API
async function getConditionId(slug) {
  try {
    const res = await fetch(`https://gamma-api.polymarket.com/markets?slug=${slug}`);
    const data = await res.json();
    if (!data || !data[0]) return null;
    return data[0].conditionId;
  } catch (err) {
    console.error(`[getConditionId] Error for ${slug}:`, err.message);
    return null;
  }
}

// Записать сделку в Supabase
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

// Подключиться к WebSocket и подписаться на маркет
async function connect(conditionId, slug) {
  if (ws) {
    ws.removeAllListeners();
    ws.terminate();
    ws = null;
  }

  console.log(`[connect] Connecting to WebSocket for ${slug} (${conditionId})`);

  ws = new WebSocket(WS_URL);

  ws.on('open', () => {
    console.log('[ws] Connected');
    const sub = {
      auth: {},
      type: 'Market',
      assets_ids: [conditionId]
    };
    ws.send(JSON.stringify(sub));
    console.log(`[ws] Subscribed to ${slug}`);
  });

  ws.on('message', async (data) => {
    try {
      const raw = data.toString();
      const messages = JSON.parse(raw);
      const arr = Array.isArray(messages) ? messages : [messages];

      for (const msg of arr) {
        // Логируем первые сообщения чтобы увидеть реальный формат
        console.log('[ws raw]', JSON.stringify(msg).slice(0, 300));

        // Polymarket WebSocket: event_type может быть 'trade', 'last_trade_price' и др.
        const etype = msg.event_type || msg.type || '';
        if (etype !== 'trade') continue;

        // asset_id это tokenId одного из исходов
        // outcome определяем по asset_id
        const outcome = msg.asset_id === currentConditionId ? 'Up' : 
                        (msg.outcome ? msg.outcome : 
                        (msg.outcome_index === 0 ? 'Up' : 'Down'));

        const trade = {
          slug: currentSlug,
          condition_id: currentConditionId,
          timestamp: new Date(Number(msg.timestamp) * 1000).toISOString(),
          outcome: outcome,
          price: parseFloat(msg.price),
          size: parseFloat(msg.size),
          side: msg.side || null
        };

        console.log(`[trade] ${trade.slug} | ${trade.outcome} | ${trade.price} | ${trade.size}`);
        await saveTrade(trade);
      }
    } catch (err) {
      console.error('[ws message] Parse error:', err.message);
    }
  });

  ws.on('error', (err) => {
    console.error('[ws] Error:', err.message);
  });

  ws.on('close', () => {
    console.log('[ws] Disconnected, reconnecting in 5s...');
    clearTimeout(reconnectTimer);
    reconnectTimer = setTimeout(() => connect(currentConditionId, currentSlug), 5000);
  });
}

// Проверить не сменился ли маркет и переподписаться если нужно
async function checkMarket() {
  const newSlug = getCurrentSlug();
  if (newSlug === currentSlug) return;

  console.log(`[checkMarket] New market: ${newSlug}`);
  const conditionId = await getConditionId(newSlug);

  if (!conditionId) {
    console.error(`[checkMarket] Could not get conditionId for ${newSlug}`);
    return;
  }

  currentSlug = newSlug;
  currentConditionId = conditionId;

  await connect(conditionId, newSlug);
}

// Старт
async function main() {
  console.log('[main] Starting BTC 5min WebSocket collector...');
  console.log(`[main] Supabase URL: ${SUPABASE_URL}`);

  await checkMarket();

  // Проверять смену маркета каждые 10 секунд
  marketCheckTimer = setInterval(checkMarket, 10_000);
}

main().catch(err => {
  console.error('[main] Fatal error:', err);
  process.exit(1);
});

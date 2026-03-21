const WebSocket = require('ws');
const fetch = require('node-fetch');
const { Pool } = require('pg');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

const COINS = ['btc', 'eth', 'sol', 'xrp', 'doge', 'hype', 'bnb'];
const INTERVALS = [
  { name: '5m',  seconds: 300,  slugSuffix: 'updown-5m'  },
  { name: '15m', seconds: 900,  slugSuffix: 'updown-15m' },
];

// coinState[coin][interval] = { slug, ws, agg, nextSlug, nextWs, ... }
const coinState = {};
for (const coin of COINS) {
  coinState[coin] = {};
  for (const iv of INTERVALS) {
    coinState[coin][iv.name] = {
      slug: null, ws: null, agg: null,
      nextSlug: null, nextWs: null, nextAgg: null,
      prefetched: false, preconnected: false,
      upsertTimer: null, upsertCount: 0,
    };
  }
}

function currentMarketStart(intervalSec) {
  return Math.floor(Date.now() / 1000 / intervalSec) * intervalSec;
}

function makeSlug(coin, ivSuffix, ts) {
  return `${coin}-${ivSuffix}-${ts}`;
}

async function fetchMarketInfo(slug) {
  try {
    const res = await fetch(`https://gamma-api.polymarket.com/markets?slug=${slug}`);
    const data = await res.json();
    if (!data || !data[0]) return null;
    const m = data[0];
    const tokens = JSON.parse(m.clobTokenIds || m.clobTokenIds || '[]');
    return {
      conditionId: m.conditionId,
      tokenUp: tokens[0],
      tokenDown: tokens[1],
      startTime: m.eventStartTime ? new Date(m.eventStartTime).getTime() / 1000 : null,
    };
  } catch (e) {
    console.error(`[fetchMarketInfo] ${slug}:`, e.message);
    return null;
  }
}

function makeAgg(slug, coin, intervalName, marketStart) {
  return {
    slug, coin, interval: intervalName, market_start: marketStart,
    up_start: null, up_min: null, up_min_time: null, up_max: null, up_max_time: null,
    up_min_m1: null, up_max_m1: null, up_min_m2: null, up_max_m2: null,
    up_min_m3: null, up_max_m3: null, up_min_m4: null, up_max_m4: null,
    up_min_m5: null, up_max_m5: null,
    down_start: null, down_min: null, down_min_time: null, down_max: null, down_max_time: null,
    down_min_m1: null, down_max_m1: null, down_min_m2: null, down_max_m2: null,
    down_min_m3: null, down_max_m3: null, down_min_m4: null, down_max_m4: null,
    down_min_m5: null, down_max_m5: null,
    outcome: null,
  };
}

function processTradeIntoAgg(agg, price, outcome, tradeTs, intervalSec) {
  const elapsed = tradeTs - agg.market_start;
  if (elapsed < -3) return;
  const effectiveElapsed = Math.max(0, elapsed);
  const p = Math.round(price * 100);
  const isUp = outcome === 'Up';
  const side = isUp ? 'up' : 'down';

  // Filter extremes
  if (price > 0.95 || price < 0.05) {
    // Only use for outcome detection
    if (elapsed > intervalSec) {
      agg.outcome = isUp ? 'UP' : 'DOWN';
    }
    return;
  }

  // Start price
  if (agg[`${side}_start`] === null) agg[`${side}_start`] = p;

  // Min/Max overall
  if (agg[`${side}_min`] === null || p < agg[`${side}_min`]) {
    agg[`${side}_min`] = p;
    agg[`${side}_min_time`] = Math.round(effectiveElapsed);
  }
  if (agg[`${side}_max`] === null || p > agg[`${side}_max`]) {
    agg[`${side}_max`] = p;
    agg[`${side}_max_time`] = Math.round(effectiveElapsed);
  }

  // Per-minute min/max (up to M5 for 5m, up to M15 for 15m — but we store only M1-M5)
  const minuteNum = Math.min(5, Math.ceil((effectiveElapsed + 1) / 60));
  if (minuteNum >= 1 && minuteNum <= 5) {
    const mk = `m${minuteNum}`;
    if (agg[`${side}_min_${mk}`] === null || p < agg[`${side}_min_${mk}`]) agg[`${side}_min_${mk}`] = p;
    if (agg[`${side}_max_${mk}`] === null || p > agg[`${side}_max_${mk}`]) agg[`${side}_max_${mk}`] = p;
  }

  // Outcome
  if (elapsed > intervalSec) {
    agg.outcome = isUp ? 'UP' : 'DOWN';
  }
}

async function upsertAgg(agg) {
  try {
    await pool.query(`
      INSERT INTO markets_agg (
        slug, coin, interval, market_start,
        up_start, up_min, up_min_time, up_max, up_max_time,
        up_min_m1, up_max_m1, up_min_m2, up_max_m2, up_min_m3, up_max_m3, up_min_m4, up_max_m4, up_min_m5, up_max_m5,
        down_start, down_min, down_min_time, down_max, down_max_time,
        down_min_m1, down_max_m1, down_min_m2, down_max_m2, down_min_m3, down_max_m3, down_min_m4, down_max_m4, down_min_m5, down_max_m5,
        outcome, updated_at
      ) VALUES (
        $1,$2,$3,$4,
        $5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,
        $20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,
        $35, NOW()
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
        outcome=EXCLUDED.outcome, updated_at=NOW()
    `, [
      agg.slug, agg.coin, agg.interval, agg.market_start,
      agg.up_start, agg.up_min, agg.up_min_time, agg.up_max, agg.up_max_time,
      agg.up_min_m1, agg.up_max_m1, agg.up_min_m2, agg.up_max_m2,
      agg.up_min_m3, agg.up_max_m3, agg.up_min_m4, agg.up_max_m4,
      agg.up_min_m5, agg.up_max_m5,
      agg.down_start, agg.down_min, agg.down_min_time, agg.down_max, agg.down_max_time,
      agg.down_min_m1, agg.down_max_m1, agg.down_min_m2, agg.down_max_m2,
      agg.down_min_m3, agg.down_max_m3, agg.down_min_m4, agg.down_max_m4,
      agg.down_min_m5, agg.down_max_m5,
      agg.outcome,
    ]);
  } catch (e) {
    console.error(`[upsert] ${agg.slug}:`, e.message);
  }
}

function connectWS(coin, iv, slug, marketInfo, agg, isNext = false) {
  const state = coinState[coin][iv.name];
  const ws = new WebSocket('wss://ws-subscriptions-clob.polymarket.com/ws/market');
  const tag = `[${coin}/${iv.name}]`;

  ws.on('open', () => {
    ws.send(JSON.stringify({
      auth: {},
      type: 'Market',
      assets_ids: [marketInfo.tokenUp, marketInfo.tokenDown],
    }));
    console.log(`${tag} WS connected: ${slug}`);
    if (isNext) state.preconnected = true;
  });

  ws.on('message', (raw) => {
    let events;
    try { events = JSON.parse(raw); } catch { return; }
    if (!Array.isArray(events)) events = [events];

    const currentAgg = isNext ? state.nextAgg : state.agg;
    if (!currentAgg) return;

    let changed = false;
    for (const ev of events) {
      if (ev.event_type !== 'trade') continue;
      const price = parseFloat(ev.price);
      const outcome = ev.outcome;
      const ts = parseInt(ev.timestamp || Date.now() / 1000);
      processTradeIntoAgg(currentAgg, price, outcome, ts, iv.seconds);
      changed = true;
    }

    if (changed) {
      state.upsertCount = (state.upsertCount || 0) + 1;
      if (state.upsertCount % 10 === 0) {
        upsertAgg(currentAgg);
      }
    }
  });

  ws.on('error', (e) => console.error(`${tag} WS error:`, e.message));
  ws.on('close', () => {
    console.log(`${tag} WS closed: ${slug}`);
    if (!isNext && state.slug === slug) {
      setTimeout(() => startMarket(coin, iv), 5000);
    }
  });

  return ws;
}

async function startMarket(coin, iv) {
  const state = coinState[coin][iv.name];
  const ts = currentMarketStart(iv.seconds);
  const slug = makeSlug(coin, iv.slugSuffix, ts);
  const tag = `[${coin}/${iv.name}]`;

  console.log(`${tag} Starting market: ${slug}`);

  // If pre-connected ws is ready
  if (state.preconnected && state.nextSlug === slug && state.nextWs) {
    console.log(`${tag} Switched to pre-connected`);
    state.slug = slug;
    state.agg = state.nextAgg;
    state.ws = state.nextWs;
    state.nextSlug = null; state.nextWs = null; state.nextAgg = null;
    state.preconnected = false; state.prefetched = false;
    state.upsertCount = 0;
    return;
  }

  const info = await fetchMarketInfo(slug);
  if (!info) {
    console.error(`${tag} Failed to fetch info for ${slug}, retry in 10s`);
    setTimeout(() => startMarket(coin, iv), 10000);
    return;
  }

  if (state.ws) { try { state.ws.terminate(); } catch {} }
  state.slug = slug;
  state.agg = makeAgg(slug, coin, iv.name, ts);
  state.upsertCount = 0;
  state.ws = connectWS(coin, iv, slug, info, state.agg, false);

  // Schedule periodic upsert
  if (state.upsertTimer) clearInterval(state.upsertTimer);
  state.upsertTimer = setInterval(() => {
    if (state.agg) upsertAgg(state.agg);
  }, 30000);
}

async function prefetchNext(coin, iv) {
  const state = coinState[coin][iv.name];
  const nextTs = currentMarketStart(iv.seconds) + iv.seconds;
  const nextSlug = makeSlug(coin, iv.slugSuffix, nextTs);
  const tag = `[${coin}/${iv.name}]`;

  if (state.prefetched && state.nextSlug === nextSlug) return;
  state.prefetched = true;
  state.nextSlug = nextSlug;

  console.log(`${tag} Prefetching: ${nextSlug}`);
  const info = await fetchMarketInfo(nextSlug);
  if (!info) {
    state.prefetched = false;
    return;
  }

  state.nextAgg = makeAgg(nextSlug, coin, iv.name, nextTs);
  console.log(`${tag} Prefetched OK`);

  // Pre-connect 5 seconds before start
  const now = Date.now() / 1000;
  const secsUntilStart = nextTs - now;
  const preconnectDelay = Math.max(0, (secsUntilStart - 5) * 1000);

  setTimeout(() => {
    console.log(`${tag} Pre-connecting: ${nextSlug}`);
    state.nextWs = connectWS(coin, iv, nextSlug, info, state.nextAgg, true);
  }, preconnectDelay);
}

async function checkAllMarkets() {
  const now = Date.now() / 1000;

  for (const coin of COINS) {
    for (const iv of INTERVALS) {
      const state = coinState[coin][iv.name];
      const ts = currentMarketStart(iv.seconds);
      const slug = makeSlug(coin, iv.slugSuffix, ts);

      // Need to start/switch market?
      if (state.slug !== slug) {
        await startMarket(coin, iv);
      }

      // Prefetch next if within 60s of end
      const secsUntilEnd = (ts + iv.seconds) - now;
      if (secsUntilEnd <= 60 && !state.prefetched) {
        prefetchNext(coin, iv);
      }

      // Final upsert 10s after close
      if (state.slug === slug && state.agg) {
        const elapsed = now - ts;
        if (elapsed > iv.seconds + 10 && !state.finalUpserted) {
          state.finalUpserted = true;
          upsertAgg(state.agg);
        }
      } else {
        state.finalUpserted = false;
      }
    }
  }
}

// Boot
(async () => {
  console.log('[collector] Starting — coins:', COINS.join(', '), '| intervals: 5m, 15m');

  // Stagger startup to avoid hammering API
  for (const coin of COINS) {
    for (const iv of INTERVALS) {
      await startMarket(coin, iv);
      await new Promise(r => setTimeout(r, 200));
    }
  }

  // Check every second
  setInterval(checkAllMarkets, 1000);
})();

// api/data.js
// Читает trades5min из Supabase и агрегирует min/max/start для каждого маркета

export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Cache-Control', 'no-store');

  const SUPABASE_URL = process.env.NEXT_PUBLIC_SUPABASE_URL;
  const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

  if (!SUPABASE_URL || !SUPABASE_KEY) {
    return res.status(500).json({ error: 'Missing env vars' });
  }

  try {
    // Берём последние 48 часов сделок
    const since = new Date(Date.now() - 48 * 60 * 60 * 1000).toISOString();

    const response = await fetch(
      `${SUPABASE_URL}/rest/v1/trades5min?select=slug,timestamp,outcome,price,size&timestamp=gte.${since}&order=timestamp.asc&limit=50000`,
      {
        headers: {
          'apikey': SUPABASE_KEY,
          'Authorization': `Bearer ${SUPABASE_KEY}`
        }
      }
    );

    const trades = await response.json();
    if (!Array.isArray(trades)) return res.status(500).json({ error: 'Bad response', raw: trades });

    // Агрегируем по slug
    const markets = {};

    for (const t of trades) {
      const slug = t.slug;
      if (!markets[slug]) {
        // Извлекаем timestamp из slug: btc-updown-5m-{ts}
        const ts = parseInt(slug.split('-').pop());
        markets[slug] = {
          slug,
          market_start: ts,
          up_start: null, up_min: null, up_min_time: null,
          up_max: null, up_max_time: null,
          down_start: null, down_min: null, down_min_time: null,
          down_max: null, down_max_time: null,
          outcome: null
        };
      }

      const m = markets[slug];
      const price = parseFloat(t.price);
      const tradeTs = new Date(t.timestamp).getTime() / 1000;
      const elapsed = tradeTs - m.market_start; // секунды от старта маркета

      if (t.outcome === 'Up') {
        if (m.up_start === null) m.up_start = price;
        if (m.up_min === null || price < m.up_min) { m.up_min = price; m.up_min_time = elapsed; }
        if (m.up_max === null || price > m.up_max) { m.up_max = price; m.up_max_time = elapsed; }
      } else if (t.outcome === 'Down') {
        if (m.down_start === null) m.down_start = price;
        if (m.down_min === null || price < m.down_min) { m.down_min = price; m.down_min_time = elapsed; }
        if (m.down_max === null || price > m.down_max) { m.down_max = price; m.down_max_time = elapsed; }
      }
    }

    // Определяем исход: маркет завершён если текущее время > market_start + 300
    const now = Math.floor(Date.now() / 1000);
    for (const m of Object.values(markets)) {
      if (now > m.market_start + 300) {
        // Исход: у победителя цена близка к 1 (>0.9)
        // Берём последнюю сделку по slug для определения исхода
        const slugTrades = trades.filter(t => t.slug === m.slug);
        const last = slugTrades[slugTrades.length - 1];
        if (last) {
          if (last.outcome === 'Up' && parseFloat(last.price) > 0.85) m.outcome = 'UP';
          else if (last.outcome === 'Down' && parseFloat(last.price) > 0.85) m.outcome = 'DOWN';
          else if (last.outcome === 'Up' && parseFloat(last.price) < 0.15) m.outcome = 'DOWN';
          else if (last.outcome === 'Down' && parseFloat(last.price) < 0.15) m.outcome = 'UP';
        }
      }
    }

    // Сортируем по времени (новые первые)
    const result = Object.values(markets).sort((a, b) => b.market_start - a.market_start);

    return res.status(200).json(result);
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
}

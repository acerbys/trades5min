// api/data.js
export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Cache-Control', 'no-store');

  const SUPABASE_URL = process.env.NEXT_PUBLIC_SUPABASE_URL;
  const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

  if (!SUPABASE_URL || !SUPABASE_KEY) {
    return res.status(500).json({ error: 'Missing env vars' });
  }

  try {
    const headers = {
      'apikey': SUPABASE_KEY,
      'Authorization': `Bearer ${SUPABASE_KEY}`
    };

    // Получаем все уникальные slugи
    const slugRes = await fetch(
      `${SUPABASE_URL}/rest/v1/trades5min?select=slug&order=slug.desc`,
      { headers }
    );
    const slugRows = await slugRes.json();
    if (!Array.isArray(slugRows)) return res.status(500).json({ error: 'Bad slugs', raw: slugRows });

    const slugs = [...new Set(slugRows.map(r => r.slug))];

    // Для каждого slug агрегируем данные батчами по 5
    const markets = [];

    for (const slug of slugs) {
      const ts = parseInt(slug.split('-').pop());

      // Берём все сделки по этому slug
      const tradesRes = await fetch(
        `${SUPABASE_URL}/rest/v1/trades5min?select=timestamp,outcome,price&slug=eq.${slug}&order=timestamp.asc&limit=10000`,
        { headers }
      );
      const trades = await tradesRes.json();
      if (!Array.isArray(trades) || trades.length === 0) continue;

      let up_start = null, up_min = null, up_min_time = null;
      let up_max = null, up_max_time = null;
      let down_start = null, down_min = null, down_min_time = null;
      let down_max = null, down_max_time = null;

      for (const t of trades) {
        const price = parseFloat(t.price);
        const elapsed = (new Date(t.timestamp).getTime() / 1000) - ts;

        if (t.outcome === 'Up') {
          if (up_start === null) up_start = price;
          if (up_min === null || price < up_min) { up_min = price; up_min_time = elapsed; }
          if (up_max === null || price > up_max) { up_max = price; up_max_time = elapsed; }
        } else if (t.outcome === 'Down') {
          if (down_start === null) down_start = price;
          if (down_min === null || price < down_min) { down_min = price; down_min_time = elapsed; }
          if (down_max === null || price > down_max) { down_max = price; down_max_time = elapsed; }
        }
      }

      // Определяем исход по последним сделкам
      let outcome = null;
      const now = Math.floor(Date.now() / 1000);
      if (now > ts + 300) {
        const last = trades[trades.length - 1];
        if (last) {
          const p = parseFloat(last.price);
          if (last.outcome === 'Up' && p > 0.85) outcome = 'UP';
          else if (last.outcome === 'Down' && p > 0.85) outcome = 'DOWN';
          else if (last.outcome === 'Up' && p < 0.15) outcome = 'DOWN';
          else if (last.outcome === 'Down' && p < 0.15) outcome = 'UP';
        }
      }

      markets.push({
        slug, market_start: ts,
        up_start, up_min, up_min_time,
        up_max, up_max_time,
        down_start, down_min, down_min_time,
        down_max, down_max_time,
        outcome
      });
    }

    // Сортируем новые первые
    markets.sort((a, b) => b.market_start - a.market_start);

    return res.status(200).json(markets);
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
}

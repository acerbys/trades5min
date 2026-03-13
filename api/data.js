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
      'Authorization': `Bearer ${SUPABASE_KEY}`,
      'Content-Type': 'application/json'
    };

    // Один запрос — вся агрегация в SQL функции
    const response = await fetch(
      `${SUPABASE_URL}/rest/v1/rpc/get_markets_summary`,
      { method: 'POST', headers, body: '{}' }
    );

    const rows = await response.json();
    if (!Array.isArray(rows)) return res.status(500).json({ error: 'Bad response', raw: rows });

    const now = Math.floor(Date.now() / 1000);

    const markets = rows.map(r => {
      let outcome = null;
      const ts = parseInt(r.market_start);
      if (now > ts + 300 && r.last_outcome && r.last_price !== null) {
        const p = parseFloat(r.last_price);
        if (r.last_outcome === 'Up' && p > 0.85) outcome = 'UP';
        else if (r.last_outcome === 'Down' && p > 0.85) outcome = 'DOWN';
        else if (r.last_outcome === 'Up' && p < 0.15) outcome = 'DOWN';
        else if (r.last_outcome === 'Down' && p < 0.15) outcome = 'UP';
      }

      return {
        slug: r.slug,
        market_start: ts,
        up_start: r.up_start,
        up_min: r.up_min,
        up_min_time: r.up_min_time,
        up_max: r.up_max,
        up_max_time: r.up_max_time,
        down_start: r.down_start,
        down_min: r.down_min,
        down_min_time: r.down_min_time,
        down_max: r.down_max,
        down_max_time: r.down_max_time,
        outcome
      };
    });

    return res.status(200).json(markets);
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
}

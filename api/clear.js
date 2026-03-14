// api/clear.js — удаление из markets_agg
export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });

  const SUPABASE_URL = process.env.NEXT_PUBLIC_SUPABASE_URL;
  const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
  if (!SUPABASE_URL || !SUPABASE_KEY) return res.status(500).json({ error: 'Missing env vars' });

  const headers = {
    'apikey': SUPABASE_KEY,
    'Authorization': `Bearer ${SUPABASE_KEY}`,
    'Content-Type': 'application/json'
  };

  try {
    const body = req.body;

    if (body.all) {
      // Удалить все записи
      const r = await fetch(`${SUPABASE_URL}/rest/v1/markets_agg?market_start=gt.0`, { method: 'DELETE', headers });
      if (!r.ok) return res.status(500).json({ error: await r.text() });
      return res.status(200).json({ ok: true });
    }

    if (body.slug) {
      // Удалить один маркет
      const r = await fetch(`${SUPABASE_URL}/rest/v1/markets_agg?slug=eq.${encodeURIComponent(body.slug)}`, { method: 'DELETE', headers });
      if (!r.ok) return res.status(500).json({ error: await r.text() });
      return res.status(200).json({ ok: true });
    }

    return res.status(400).json({ error: 'No action specified' });
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
}

// api/clear.js
import pg from 'pg';
const { Pool } = pg;

const pool = new Pool({ connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false } });

export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });
  try {
    const body = req.body;
    if (body.all) {
      await pool.query('DELETE FROM markets_agg');
      return res.status(200).json({ ok: true });
    }
    if (body.slug) {
      await pool.query('DELETE FROM markets_agg WHERE slug=$1', [body.slug]);
      return res.status(200).json({ ok: true });
    }
    return res.status(400).json({ error: 'No action' });
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
}

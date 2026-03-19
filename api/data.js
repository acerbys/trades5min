import pg from 'pg';
const { Pool } = pg;
const pool = new Pool({ connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false } });

export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Cache-Control', 'no-store');
  const coin = req.query.coin || 'btc';
  try {
    const result = await pool.query(
      'SELECT * FROM markets_agg WHERE coin=$1 ORDER BY market_start DESC LIMIT 200',
      [coin]
    );
    return res.status(200).json(result.rows);
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
}

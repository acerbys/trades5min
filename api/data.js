const { Pool } = require('pg');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();

  const coin = req.query.coin || 'btc';
  const interval = req.query.interval || '5m';

  try {
    const { rows } = await pool.query(
      `SELECT * FROM markets_agg
       WHERE coin = $1 AND interval = $2
       ORDER BY market_start DESC
       LIMIT 200`,
      [coin, interval]
    );
    res.status(200).json(rows);
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: e.message });
  }
};

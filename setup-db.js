const { Pool } = require('pg');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

async function setup() {
  const client = await pool.connect();
  try {
    // Create main table
    await client.query(`
      CREATE TABLE IF NOT EXISTS markets_agg (
        slug TEXT PRIMARY KEY,
        coin TEXT NOT NULL DEFAULT 'btc',
        interval TEXT NOT NULL DEFAULT '5m',
        market_start BIGINT NOT NULL,
        up_start NUMERIC, up_min NUMERIC, up_min_time NUMERIC, up_max NUMERIC, up_max_time NUMERIC,
        up_min_m1 NUMERIC, up_max_m1 NUMERIC,
        up_min_m2 NUMERIC, up_max_m2 NUMERIC,
        up_min_m3 NUMERIC, up_max_m3 NUMERIC,
        up_min_m4 NUMERIC, up_max_m4 NUMERIC,
        up_min_m5 NUMERIC, up_max_m5 NUMERIC,
        down_start NUMERIC, down_min NUMERIC, down_min_time NUMERIC, down_max NUMERIC, down_max_time NUMERIC,
        down_min_m1 NUMERIC, down_max_m1 NUMERIC,
        down_min_m2 NUMERIC, down_max_m2 NUMERIC,
        down_min_m3 NUMERIC, down_max_m3 NUMERIC,
        down_min_m4 NUMERIC, down_max_m4 NUMERIC,
        down_min_m5 NUMERIC, down_max_m5 NUMERIC,
        outcome TEXT,
        updated_at TIMESTAMPTZ DEFAULT NOW()
      )
    `);

    // Add columns if missing (for existing deployments)
    const alterCols = [
      `ALTER TABLE markets_agg ADD COLUMN IF NOT EXISTS coin TEXT NOT NULL DEFAULT 'btc'`,
      `ALTER TABLE markets_agg ADD COLUMN IF NOT EXISTS interval TEXT NOT NULL DEFAULT '5m'`,
    ];
    for (const sql of alterCols) {
      await client.query(sql).catch(() => {});
    }

    // Indexes
    await client.query(`CREATE INDEX IF NOT EXISTS idx_markets_agg_coin ON markets_agg(coin)`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_markets_agg_interval ON markets_agg(interval)`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_markets_agg_market_start ON markets_agg(market_start)`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_markets_agg_coin_interval ON markets_agg(coin, interval)`);

    console.log('[setup-db] Done');
  } finally {
    client.release();
    await pool.end();
  }
}

setup().catch(e => { console.error(e); process.exit(1); });

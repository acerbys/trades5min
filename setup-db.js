const { Client } = require('pg');

async function setup() {
  const client = new Client({ connectionString: process.env.DATABASE_URL });
  await client.connect();
  console.log('[setup] Connected to PostgreSQL');

  await client.query(`
    CREATE TABLE IF NOT EXISTS trades5min (
      id SERIAL PRIMARY KEY,
      slug TEXT NOT NULL,
      condition_id TEXT NOT NULL,
      timestamp TIMESTAMPTZ NOT NULL,
      outcome TEXT, price NUMERIC, size NUMERIC, side TEXT,
      created_at TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_trades5min_slug ON trades5min(slug);

    CREATE TABLE IF NOT EXISTS markets_agg (
      slug TEXT PRIMARY KEY,
      coin TEXT NOT NULL DEFAULT 'btc',
      market_start BIGINT NOT NULL,
      up_start NUMERIC, up_min NUMERIC, up_min_time NUMERIC, up_max NUMERIC, up_max_time NUMERIC,
      up_min_m1 NUMERIC, up_max_m1 NUMERIC, up_min_m2 NUMERIC, up_max_m2 NUMERIC,
      up_min_m3 NUMERIC, up_max_m3 NUMERIC, up_min_m4 NUMERIC, up_max_m4 NUMERIC,
      up_min_m5 NUMERIC, up_max_m5 NUMERIC,
      down_start NUMERIC, down_min NUMERIC, down_min_time NUMERIC, down_max NUMERIC, down_max_time NUMERIC,
      down_min_m1 NUMERIC, down_max_m1 NUMERIC, down_min_m2 NUMERIC, down_max_m2 NUMERIC,
      down_min_m3 NUMERIC, down_max_m3 NUMERIC, down_min_m4 NUMERIC, down_max_m4 NUMERIC,
      down_min_m5 NUMERIC, down_max_m5 NUMERIC,
      outcome TEXT, updated_at TIMESTAMPTZ DEFAULT NOW()
    );

    -- Add coin column if not exists (for existing tables)
    DO $$ BEGIN
      ALTER TABLE markets_agg ADD COLUMN IF NOT EXISTS coin TEXT NOT NULL DEFAULT 'btc';
    EXCEPTION WHEN others THEN NULL; END $$;

    CREATE INDEX IF NOT EXISTS idx_markets_agg_coin ON markets_agg(coin);
    CREATE INDEX IF NOT EXISTS idx_markets_agg_market_start ON markets_agg(market_start);
  `);

  console.log('[setup] Tables ready');
  await client.end();
}

setup().catch(err => { console.error('[setup] Error:', err.message); process.exit(1); });

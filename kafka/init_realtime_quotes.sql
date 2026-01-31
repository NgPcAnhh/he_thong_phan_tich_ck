-- =====================================================
-- Database Schema for Real-time Stock Quotes
-- =====================================================
-- This script creates the realtime_quotes table to store
-- streaming stock market data from Kafka consumers
-- =====================================================

-- Drop table if exists (CAUTION: Remove in production)
-- DROP TABLE IF EXISTS realtime_quotes CASCADE;

-- Create main table
CREATE TABLE IF NOT EXISTS realtime_quotes (
    -- Primary identifiers
    symbol           VARCHAR(20)     NOT NULL,
    ts               TIMESTAMP       NOT NULL,
    
    -- Price information
    last_price       NUMERIC(18, 4),
    avg_price        NUMERIC(18, 4),
    ref_price        NUMERIC(18, 4),
    ceil_price       NUMERIC(18, 4),
    floor_price      NUMERIC(18, 4),
    high_price       NUMERIC(18, 4),
    low_price        NUMERIC(18, 4),
    
    -- Price changes
    change_percent   NUMERIC(10, 4),
    change_value     NUMERIC(18, 4),
    
    -- Volume information
    last_volume      BIGINT,
    total_volume     BIGINT,
    total_value      NUMERIC(20, 2),
    
    -- Foreign trading
    foreign_buy_qty  BIGINT,
    foreign_sell_qty BIGINT,
    foreign_buy_val  NUMERIC(20, 2),
    foreign_sell_val NUMERIC(20, 2),
    
    -- Bid levels (3 levels)
    bid1_price       NUMERIC(18, 4),
    bid1_qty         BIGINT,
    bid2_price       NUMERIC(18, 4),
    bid2_qty         BIGINT,
    bid3_price       NUMERIC(18, 4),
    bid3_qty         BIGINT,
    
    -- Ask levels (3 levels)
    ask1_price       NUMERIC(18, 4),
    ask1_qty         BIGINT,
    ask2_price       NUMERIC(18, 4),
    ask2_qty         BIGINT,
    ask3_price       NUMERIC(18, 4),
    ask3_qty         BIGINT,
    
    -- Constraints
    CONSTRAINT pk_realtime_quotes PRIMARY KEY (symbol, ts)
);

-- =====================================================
-- Indexes for query performance
-- =====================================================

-- Index for symbol lookups
CREATE INDEX IF NOT EXISTS idx_realtime_quotes_symbol 
ON realtime_quotes(symbol);

-- Index for time-based queries (most recent first)
CREATE INDEX IF NOT EXISTS idx_realtime_quotes_ts 
ON realtime_quotes(ts DESC);

-- Composite index for symbol + time range queries
CREATE INDEX IF NOT EXISTS idx_realtime_quotes_symbol_ts 
ON realtime_quotes(symbol, ts DESC);

-- =====================================================
-- Comments for documentation
-- =====================================================

COMMENT ON TABLE realtime_quotes IS 
'Real-time stock market quotes streamed from Kafka. Contains tick-level price and volume data.';

COMMENT ON COLUMN realtime_quotes.symbol IS 'Stock symbol (e.g. VNM, HPG)';
COMMENT ON COLUMN realtime_quotes.ts IS 'Timestamp of the quote (millisecond precision converted to TIMESTAMP)';
COMMENT ON COLUMN realtime_quotes.last_price IS 'Last traded price';
COMMENT ON COLUMN realtime_quotes.total_volume IS 'Total trading volume for the day';
COMMENT ON COLUMN realtime_quotes.foreign_buy_qty IS 'Foreign buy quantity';
COMMENT ON COLUMN realtime_quotes.foreign_sell_qty IS 'Foreign sell quantity';

-- =====================================================
-- Optional: Partition by time (for large datasets)
-- =====================================================
-- Uncomment if you need to partition by month/quarter
-- This helps with query performance and data management

-- ALTER TABLE realtime_quotes PARTITION BY RANGE (ts);
-- 
-- CREATE TABLE realtime_quotes_2026_q1 PARTITION OF realtime_quotes
--     FOR VALUES FROM ('2026-01-01') TO ('2026-04-01');
-- 
-- CREATE TABLE realtime_quotes_2026_q2 PARTITION OF realtime_quotes
--     FOR VALUES FROM ('2026-04-01') TO ('2026-07-01');

-- =====================================================
-- Verify table created successfully
-- =====================================================

SELECT 
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as total_size
FROM pg_tables
WHERE tablename = 'realtime_quotes';

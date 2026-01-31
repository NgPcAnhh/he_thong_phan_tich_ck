"""
Setup realtime_quotes table in correct schema
Database: postgres
Schema: hethong_phantich_chungkhoan
"""
import asyncio
import asyncpg
import config

async def main():
    print("🔧 Connecting to PostgreSQL...")
    print(f"   DSN: {config.DB_DSN}")
    print(f"   Schema: {config.DB_SCHEMA}")
    
    try:
        conn = await asyncpg.connect(config.DB_DSN)
        print(f"✅ Connected successfully")
        
        # Create schema if not exists
        print(f"\n📋 Creating schema '{config.DB_SCHEMA}'...")
        await conn.execute(f"CREATE SCHEMA IF NOT EXISTS {config.DB_SCHEMA}")
        print("✅ Schema created/verified")
        
        # Create table in schema
        print(f"\n📋 Creating table '{config.DB_SCHEMA}.realtime_quotes'...")
        
        await conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {config.DB_SCHEMA}.realtime_quotes (
                symbol           VARCHAR(20)     NOT NULL,
                ts               TIMESTAMP       NOT NULL,
                last_price       NUMERIC(18, 4),
                avg_price        NUMERIC(18, 4),
                ref_price        NUMERIC(18, 4),
                ceil_price       NUMERIC(18, 4),
                floor_price      NUMERIC(18, 4),
                high_price       NUMERIC(18, 4),
                low_price        NUMERIC(18, 4),
                change_percent   NUMERIC(10, 4),
                change_value     NUMERIC(18, 4),
                last_volume      BIGINT,
                total_volume     BIGINT,
                total_value      NUMERIC(20, 2),
                foreign_buy_qty  BIGINT,
                foreign_sell_qty BIGINT,
                foreign_buy_val  NUMERIC(20, 2),
                foreign_sell_val NUMERIC(20, 2),
                bid1_price       NUMERIC(18, 4),
                bid1_qty         BIGINT,
                bid2_price       NUMERIC(18, 4),
                bid2_qty         BIGINT,
                bid3_price       NUMERIC(18, 4),
                bid3_qty         BIGINT,
                ask1_price       NUMERIC(18, 4),
                ask1_qty         BIGINT,
                ask2_price       NUMERIC(18, 4),
                ask2_qty         BIGINT,
                ask3_price       NUMERIC(18, 4),
                ask3_qty         BIGINT,
                CONSTRAINT pk_realtime_quotes PRIMARY KEY (symbol, ts)
            );
        """)
        
        print("✅ Table created/verified")
        
        # Create candles_1m table
        print(f"\n📋 Creating table '{config.DB_SCHEMA}.candles_1m'...")
        
        await conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {config.DB_SCHEMA}.candles_1m (
                symbol         VARCHAR(20)     NOT NULL,
                bucket_time    TIMESTAMP       NOT NULL,
                open_price     NUMERIC(18, 4),
                high_price     NUMERIC(18, 4),
                low_price      NUMERIC(18, 4),
                close_price    NUMERIC(18, 4),
                volume         BIGINT,
                CONSTRAINT pk_candles_1m PRIMARY KEY (symbol, bucket_time)
            );
        """)
        
        print("✅ Candles table created/verified")
        
        print("\n📊 Creating indexes...")
        
        # Indexes for realtime_quotes
        
        await conn.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_realtime_quotes_symbol 
            ON {config.DB_SCHEMA}.realtime_quotes(symbol);
        """)
        
        await conn.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_realtime_quotes_ts 
            ON {config.DB_SCHEMA}.realtime_quotes(ts DESC);
        """)
        
        await conn.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_realtime_quotes_symbol_ts 
            ON {config.DB_SCHEMA}.realtime_quotes(symbol, ts DESC);
        """)
        
        print("✅ Indexes created")
        
        # Indexes for candles_1m
        await conn.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_candles_1m_symbol 
            ON {config.DB_SCHEMA}.candles_1m(symbol);
        """)
        
        await conn.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_candles_1m_bucket_time 
            ON {config.DB_SCHEMA}.candles_1m(bucket_time DESC);
        """)
        
        await conn.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_candles_1m_symbol_bucket 
            ON {config.DB_SCHEMA}.candles_1m(symbol, bucket_time DESC);
        """)
        
        print("✅ Candles indexes created")
        
        # Check table info
        row = await conn.fetchrow(f"""
            SELECT COUNT(*) as count FROM {config.DB_SCHEMA}.realtime_quotes;
        """)
        
        print(f"\n📈 Current records in realtime_quotes: {row['count']}")
        
        row_candles = await conn.fetchrow(f"""
            SELECT COUNT(*) as count FROM {config.DB_SCHEMA}.candles_1m;
        """)
        
        print(f"📈 Current records in candles_1m: {row_candles['count']}")
        
        await conn.close()
        print("\n✅ Setup complete!")
        print(f"   Tables: {config.DB_SCHEMA}.realtime_quotes, {config.DB_SCHEMA}.candles_1m")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        print(f"\n⚠️  Make sure:")
        print(f"   1. PostgreSQL container 'dwh-postgres' is running")
        print(f"   2. Database 'postgres' exists")
        print(f"   3. User 'admin' has permission to create schema/tables")
        raise

if __name__ == "__main__":
    asyncio.run(main())

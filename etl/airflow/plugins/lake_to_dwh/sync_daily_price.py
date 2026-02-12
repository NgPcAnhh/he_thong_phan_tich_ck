from contextlib import closing
import pandas as pd
from psycopg2.extras import execute_values
from lake_to_dwh.utils import (
    get_latest_partition,
    read_all_csvs_from_folder,
    get_postgres_connection,
    ensure_schema,
    clean_dataframe,
    standardize_ticker,
    parse_trading_date
)


def sync_daily_price_to_db(
    db_url: str,
    schema: str,
    bucket: str,
    minio_conn_id: str = "minio_finance",
    folder_prefix: str = "daily_price/",
    table: str = "history_price"
) -> str:
    print("=" * 70)
    print("📊 SYNC DAILY PRICE TO DATABASE")
    print("=" * 70)
    
    # Step 1: Find latest partition
    print("\n[1/4] Finding latest partition...")
    latest_partition = get_latest_partition(bucket, folder_prefix, minio_conn_id)
    
    if not latest_partition:
        return "❌ No partition found"
    
    # Step 2: Read all CSV files from latest partition
    print("\n[2/4] Reading CSV files...")
    df = read_all_csvs_from_folder(bucket, latest_partition, minio_conn_id)
    
    if df.empty:
        return "⚠️ No data found"
    
    print(f"Loaded {len(df)} rows")
    
    # Step 3: Clean and transform data
    print("\n[3/4] Cleaning and transforming data...")
    
    # Normalize column names
    df.columns = df.columns.str.lower().str.strip()
    
    # Ensure required columns exist
    required_cols = ['ticker', 'trading_date', 'open', 'high', 'low', 'close', 'volume']
    
    # Handle date column variations
    if 'date' in df.columns and 'trading_date' not in df.columns:
        df.rename(columns={'date': 'trading_date'}, inplace=True)
    if 'time' in df.columns and 'trading_date' not in df.columns:
        df.rename(columns={'time': 'trading_date'}, inplace=True)
    
    # Check required columns
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        return f"❌ Missing columns: {missing_cols}"
    
    # Select only required columns
    df = df[required_cols].copy()
    
    # Clean data
    df = clean_dataframe(df, required_columns=['ticker', 'trading_date'])
    df = standardize_ticker(df, 'ticker')
    df = parse_trading_date(df, 'trading_date')
    
    # Remove duplicates
    df = df.drop_duplicates(subset=['ticker', 'trading_date'])
    
    print(f"After cleaning: {len(df)} rows")
    
    if df.empty:
        return "⚠️ No data after cleaning"
    
    # Step 4: Insert into database
    print("\n[4/4] Inserting into database...")
    
    with closing(get_postgres_connection(db_url)) as conn:
        conn.autocommit = False
        
        try:
            # Ensure schema exists
            ensure_schema(conn, schema)
            
            # Prepare data for insertion
            rows = [
                (
                    row['ticker'],
                    row['trading_date'],
                    row.get('open'),
                    row.get('high'),
                    row.get('low'),
                    row.get('close'),
                    row.get('volume'),
                )
                for _, row in df.iterrows()
            ]
            
            # UPSERT pattern using ON CONFLICT
            with conn.cursor() as cur:
                # Insert with ON CONFLICT DO UPDATE
                upsert_sql = f"""
                    INSERT INTO {schema}.{table}
                    (ticker, trading_date, open, high, low, close, volume)
                    VALUES %s
                    ON CONFLICT (ticker, trading_date)
                    DO UPDATE SET
                        open = EXCLUDED.open,
                        high = EXCLUDED.high,
                        low = EXCLUDED.low,
                        close = EXCLUDED.close,
                        volume = EXCLUDED.volume;
                """
                execute_values(cur, upsert_sql, rows)
            
            conn.commit()
            print(f"✅ Inserted/Updated {len(rows)} rows")
            
            return f"✅ Success: {len(rows)} rows"
            
        except Exception as e:
            conn.rollback()
            print(f"❌ Error: {str(e)}")
            raise
    
    print("=" * 70)

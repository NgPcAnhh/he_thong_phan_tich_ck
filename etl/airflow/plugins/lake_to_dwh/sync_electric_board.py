"""
Sync electric_board data from MinIO to PostgreSQL database.
Strategy: UPSERT (DELETE by ticker+trading_date, then INSERT)
Data source: electric_board_per_day/{date}/batch_*.csv
"""
from contextlib import closing
import pandas as pd
from psycopg2.extras import execute_values
from lake_to_dwh.utils import (
    get_latest_partition,
    read_all_csvs_from_folder,
    get_postgres_connection,
    ensure_schema,
)


# Column mapping from CSV to DB
COLUMN_MAPPING = {
    'listing_symbol': 'ticker',
    'listing_exchange': 'exchange',
    'listing_ref_price': 'ref_price',
    'match_match_price': 'match_price',
    'match_accumulated_volume': 'accumulated_volume',
    'match_highest': 'highest_price',
    'match_lowest': 'lowest_price',
    'match_foreign_buy_volume': 'foreign_buy_volume',
    'match_foreign_sell_volume': 'foreign_sell_volume',
    'bid_ask_bid_1_price': 'bid_1_price',
    'bid_ask_bid_1_volume': 'bid_1_volume',
    'bid_ask_bid_2_price': 'bid_2_price',
    'bid_ask_bid_2_volume': 'bid_2_volume',
    'bid_ask_bid_3_price': 'bid_3_price',
    'bid_ask_bid_3_volume': 'bid_3_volume',
    'bid_ask_ask_1_price': 'ask_1_price',
    'bid_ask_ask_1_volume': 'ask_1_volume',
    'bid_ask_ask_2_price': 'ask_2_price',
    'bid_ask_ask_2_volume': 'ask_2_volume',
    'bid_ask_ask_3_price': 'ask_3_price',
    'bid_ask_ask_3_volume': 'ask_3_volume',
    'trading_date': 'trading_date',
}

# Database columns (order matters for INSERT)
DB_COLUMNS = [
    'ticker', 'exchange', 'trading_date', 'ref_price', 'match_price',
    'accumulated_volume', 'highest_price', 'lowest_price',
    'foreign_buy_volume', 'foreign_sell_volume',
    'bid_1_price', 'bid_1_volume', 'bid_2_price', 'bid_2_volume',
    'bid_3_price', 'bid_3_volume', 'ask_1_price', 'ask_1_volume',
    'ask_2_price', 'ask_2_volume', 'ask_3_price', 'ask_3_volume',
]

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS {schema}.electric_board (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(20) NOT NULL,
    exchange VARCHAR(10),
    trading_date DATE NOT NULL,
    ref_price NUMERIC(18,2),
    match_price NUMERIC(18,2),
    accumulated_volume BIGINT,
    highest_price NUMERIC(18,2),
    lowest_price NUMERIC(18,2),
    foreign_buy_volume BIGINT,
    foreign_sell_volume BIGINT,
    bid_1_price NUMERIC(18,2),
    bid_1_volume BIGINT,
    bid_2_price NUMERIC(18,2),
    bid_2_volume BIGINT,
    bid_3_price NUMERIC(18,2),
    bid_3_volume BIGINT,
    ask_1_price NUMERIC(18,2),
    ask_1_volume BIGINT,
    ask_2_price NUMERIC(18,2),
    ask_2_volume BIGINT,
    ask_3_price NUMERIC(18,2),
    ask_3_volume BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_electric_board_ticker_date 
ON {schema}.electric_board (ticker, trading_date);
"""


def ensure_table_exists(conn, schema: str):
    """Create electric_board table if not exists"""
    try:
        with conn.cursor() as cur:
            cur.execute(CREATE_TABLE_SQL.format(schema=schema))
        conn.commit()
        print(f"✓ Table {schema}.electric_board ensured")
    except Exception as e:
        conn.rollback()
        print(f"❌ Error creating table: {str(e)}")
        raise


def transform_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Transform and clean electric board dataframe"""
    if df.empty:
        return df
    
    df = df.copy()
    
    # Rename columns using mapping
    df = df.rename(columns=COLUMN_MAPPING)
    
    # Keep only mapped columns that exist in the dataframe
    available_cols = [col for col in DB_COLUMNS if col in df.columns]
    df = df[available_cols].copy()
    
    # Check required columns — if ticker is missing, data is incomplete
    if 'ticker' not in df.columns or 'trading_date' not in df.columns:
        missing = [c for c in ['ticker', 'trading_date'] if c not in df.columns]
        print(f"⚠️ Missing required columns after mapping: {missing}")
        print(f"  Available columns: {list(df.columns)}")
        return pd.DataFrame()
    
    # Clean ticker
    df['ticker'] = df['ticker'].astype(str).str.upper().str.strip()
    
    # Parse trading_date
    df['trading_date'] = pd.to_datetime(df['trading_date'], errors='coerce').dt.date
    df = df.dropna(subset=['trading_date'])
    
    # Clean exchange
    if 'exchange' in df.columns:
        df['exchange'] = df['exchange'].astype(str).str.upper().str.strip()
    
    # Convert numeric columns
    numeric_cols = [
        'ref_price', 'match_price', 'highest_price', 'lowest_price',
        'accumulated_volume', 'foreign_buy_volume', 'foreign_sell_volume',
        'bid_1_price', 'bid_1_volume', 'bid_2_price', 'bid_2_volume',
        'bid_3_price', 'bid_3_volume', 'ask_1_price', 'ask_1_volume',
        'ask_2_price', 'ask_2_volume', 'ask_3_price', 'ask_3_volume',
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Remove duplicates
    df = df.drop_duplicates(subset=['ticker', 'trading_date'])
    
    # Drop rows without ticker
    df = df.dropna(subset=['ticker'])
    
    return df


def sync_electric_board_to_db(
    db_url: str,
    schema: str,
    bucket: str,
    minio_conn_id: str = "minio_finance",
    folder_prefix: str = "electric_board_per_day/",
    table: str = "electric_board"
) -> str:
    """
    Sync electric board data from MinIO to PostgreSQL.
    
    Args:
        db_url: PostgreSQL connection URL
        schema: Database schema name
        bucket: MinIO bucket name
        minio_conn_id: MinIO connection ID
        folder_prefix: Folder prefix in MinIO
        table: Database table name
    
    Returns:
        Status message
    """
    print("=" * 70)
    print("📊 SYNC ELECTRIC BOARD TO DATABASE")
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
    
    # Step 3: Transform data
    print("\n[3/4] Transforming data...")
    df = transform_dataframe(df)
    
    print(f"After cleaning: {len(df)} rows")
    
    if df.empty:
        return "⚠️ No data after cleaning"
    
    # Step 4: Insert into database
    print("\n[4/4] Inserting into database...")
    
    with closing(get_postgres_connection(db_url)) as conn:
        conn.autocommit = False
        
        try:
            # Ensure schema and table exist
            ensure_schema(conn, schema)
            ensure_table_exists(conn, schema)
            
            # Prepare data for insertion
            available_cols = [col for col in DB_COLUMNS if col in df.columns]
            rows = [
                tuple(
                    row[col] if col in row.index and pd.notna(row[col]) else None
                    for col in available_cols
                )
                for _, row in df.iterrows()
            ]
            
            # UPSERT pattern using ON CONFLICT
            with conn.cursor() as cur:
                # Build dynamic column list for UPDATE (exclude key columns)
                update_cols = [col for col in available_cols 
                              if col not in ['ticker', 'exchange', 'trading_date']]
                update_set = ', '.join([f"{col} = EXCLUDED.{col}" for col in update_cols])
                
                # UPSERT with ON CONFLICT
                upsert_sql = f"""
                    INSERT INTO {schema}.{table}
                    ({', '.join(available_cols)})
                    VALUES %s
                    ON CONFLICT (ticker, exchange, trading_date)
                    DO UPDATE SET
                        {update_set};
                """
                execute_values(cur, upsert_sql, rows, page_size=1000)
            
            conn.commit()
            print(f"✅ Inserted/Updated {len(rows)} rows")
            
            return f"✅ Success: {len(rows)} rows"
            
        except Exception as e:
            conn.rollback()
            print(f"❌ Error: {str(e)}")
            raise
    
    print("=" * 70)

"""
Sync global_index data (USD/VND, DXY, USD/CNY, EUR/USD, US Bond) from MinIO to PostgreSQL database.
Strategy: APPEND (with duplicate check on date, asset_type)
Target table: macro_economy (same as macro_economy sync)
"""
from contextlib import closing
import pandas as pd
from psycopg2.extras import execute_values
from lake_to_dwh.utils import (
    get_minio_hook,
    get_latest_partition,
    read_csv_from_minio,
    get_postgres_connection,
    ensure_schema,
    clean_dataframe
)


def sync_global_index_to_db(
    db_url: str,
    schema: str,
    bucket: str,
    minio_conn_id: str = "minio_finance",
    folder_prefix: str = "global_index/",
    table: str = "macro_economy"
) -> str:
    """
    Sync global index data from MinIO to PostgreSQL.
    Reads from subfolders: usd_vnd/, dxy_index/, usd_cny/, eur_usd/, us_bond_10y/
    
    Args:
        minio_conn_id: MinIO connection ID
        bucket: MinIO bucket name
        folder_prefix: Folder prefix in MinIO
        db_url: PostgreSQL connection URL
        schema: Database schema name
        table: Database table name
    
    Returns:
        Status message
    """
    print("=" * 70)
    print("📊 SYNC GLOBAL INDEX TO DATABASE")
    print("=" * 70)
    
    # Asset types and their folders
    asset_folders = {
        'USD_VND': 'usd_vnd',
        'DXY': 'dxy_index',
        'USD_CNY': 'usd_cny',
        'EUR_USD': 'eur_usd',
        'US_BOND_10Y': 'us_bond_10y'
    }
    
    all_dfs = []
    
    for asset_type, folder_name in asset_folders.items():
        print(f"\n--- Processing {asset_type} ---")
        
        # Find latest partition for this asset
        asset_prefix = f"{folder_prefix}{folder_name}/"
        latest_partition = get_latest_partition(bucket, asset_prefix, minio_conn_id)
        
        if not latest_partition:
            print(f"⚠️ No partition found for {asset_type}")
            continue
        
        # Read CSV files
        hook = get_minio_hook(minio_conn_id)
        keys = hook.list_keys(bucket_name=bucket, prefix=latest_partition)
        csv_files = [k for k in keys if k.endswith('.csv')]
        
        if not csv_files:
            print(f"⚠️ No CSV files found for {asset_type}")
            continue
        
        for csv_file in csv_files:
            df = read_csv_from_minio(bucket, csv_file, minio_conn_id)
            
            if not df.empty:
                # Add asset_type column
                df['asset_type'] = asset_type
                all_dfs.append(df)
    
    if not all_dfs:
        return "⚠️ No data found"
    
    # Concatenate all dataframes
    df = pd.concat(all_dfs, ignore_index=True)
    print(f"\nTotal loaded: {len(df)} rows")
    
    # Clean and transform data
    print("\nCleaning and transforming data...")
    
    # Normalize column names
    df.columns = df.columns.str.lower().str.strip()
    
    # Handle date column variations
    if 'time' in df.columns and 'date' not in df.columns:
        df.rename(columns={'time': 'date'}, inplace=True)
    
    # Ensure required columns exist
    required_cols = ['date', 'asset_type']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        return f"❌ Missing columns: {missing_cols}"
    
    # Select columns that exist
    available_cols = ['date', 'open', 'high', 'low', 'close', 'volume', 'asset_type']
    df = df[[col for col in available_cols if col in df.columns]].copy()
    
    # Parse date
    df['date'] = pd.to_datetime(df['date'], errors='coerce').dt.date
    df = df.dropna(subset=['date'])
    
    # Clean data
    df = clean_dataframe(df, required_columns=['date', 'asset_type'])
    
    # Remove duplicates
    df = df.drop_duplicates(subset=['date', 'asset_type'])
    
    print(f"After cleaning: {len(df)} rows")
    
    if df.empty:
        return "⚠️ No data after cleaning"
    
    # Insert into database
    print("\nInserting into database...")
    
    with closing(get_postgres_connection(db_url)) as conn:
        conn.autocommit = False
        
        try:
            # Ensure schema exists
            ensure_schema(conn, schema)
            
            # Prepare data for insertion
            rows = [
                (
                    row['date'],
                    row.get('open'),
                    row.get('high'),
                    row.get('low'),
                    row.get('close'),
                    row.get('volume'),
                    row['asset_type'],
                )
                for _, row in df.iterrows()
            ]
            
            
            # UPSERT pattern using ON CONFLICT
            with conn.cursor() as cur:
                upsert_sql = f"""
                    INSERT INTO {schema}.{table}
                    (date, open, high, low, close, volume, asset_type)
                    VALUES %s
                    ON CONFLICT (date, asset_type)
                    DO UPDATE SET
                        open = EXCLUDED.open,
                        high = EXCLUDED.high,
                        low = EXCLUDED.low,
                        close = EXCLUDED.close,
                        volume = EXCLUDED.volume;
                """
                execute_values(cur, upsert_sql, rows)
            
            conn.commit()
            print(f"✅ Inserted {len(rows)} rows")
            
            return f"✅ Success: {len(rows)} rows"
            
        except Exception as e:
            conn.rollback()
            print(f"❌ Error: {str(e)}")
            raise
    
    print("=" * 70)

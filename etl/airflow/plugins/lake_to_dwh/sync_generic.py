from contextlib import closing
from typing import Dict, List, Optional, Any
import pandas as pd
from psycopg2.extras import execute_values
from lake_to_dwh.utils import (
    read_all_csvs_from_folder,
    get_postgres_connection,
    ensure_schema,
    clean_dataframe,
    standardize_ticker
)


# Topic configuration mapping
TOPIC_CONFIG = {
    "bctc": {
        "table": "bctc",
        "mode": "upsert",
        "pk": ["ticker", "year", "quarter", "ind_code"],
        "required_columns": ["ticker", "quarter", "year", "ind_name", "value"]
    },
    "daily-price": {
        "table": "daily_price",
        "mode": "append",
        "pk": ["ticker", "trading_date"],
        "required_columns": ["ticker", "trading_date", "open", "high", "low", "close", "volume"]
    },
    "financial-ratios": {
        "table": "financial_ratio",
        "mode": "replace",
        "pk": [],
        "required_columns": ["ticker"]
    },
    "news": {
        "table": "news",
        "mode": "upsert",
        "pk": ["source", "published_date", "title"],
        "required_columns": ["title", "source", "published_date"]
    },
    "overview": {
        "table": "overview",
        "mode": "upsert",
        "pk": ["ticker"],
        "required_columns": ["ticker"]
    },
    "people": {
        "table": "people",
        "mode": "delete_insert",
        "pk": [],
        "required_columns": ["ticker", "name"]
    },
    "electric-board": {
        "table": "electric_board",
        "mode": "upsert",
        "pk": ["ticker", "trading_date"],
        "required_columns": ["ticker", "trading_date"]
    },
    "global-index": {
        "table": "global_index",
        "mode": "append",
        "pk": ["symbol", "trading_date"],
        "required_columns": ["symbol", "trading_date"]
    },
    "index-price": {
        "table": "index_price",
        "mode": "append",
        "pk": ["index_code", "trading_date"],
        "required_columns": ["index_code", "trading_date"]
    },
    "macro-economy": {
        "table": "macro_economy",
        "mode": "append",
        "pk": ["indicator", "date"],
        "required_columns": ["indicator", "date"]
    },
}


def sync_folder_to_db(
    db_url: str,
    schema: str,
    bucket: str,
    folder_path: str,
    topic: str,
    minio_conn_id: str = "minio_finance",
    sync_mode: Optional[str] = None,
    target_table: Optional[str] = None,
    dry_run: bool = False
) -> str:
    """
    Generic sync function that reads data from a MinIO folder and syncs to database.
    
    Args:
        db_url: PostgreSQL connection URL
        schema: Database schema name
        bucket: MinIO bucket name
        folder_path: Full path to folder in MinIO (e.g., 'bctc/date=2026-02-04/')
        topic: Topic name for configuration lookup
        minio_conn_id: MinIO connection ID
        sync_mode: Override sync mode (upsert/replace/append/delete_insert)
        target_table: Override target table name
        dry_run: If True, only preview without writing to DB
        
    Returns:
        Status message
    """
    print("=" * 70)
    print(f"📊 GENERIC SYNC: {topic} → Database")
    print("=" * 70)
    
    # Step 1: Get topic configuration
    print("\n[1/7] Getting topic configuration...")
    
    config = TOPIC_CONFIG.get(topic, {})
    
    if not config and not target_table:
        return f"❌ Unknown topic '{topic}' and no target_table specified"
    
    # Use overrides or defaults
    table = target_table or config.get("table", topic.replace("-", "_"))
    mode = sync_mode or config.get("mode", "append")
    pk_columns = config.get("pk", [])
    required_cols = config.get("required_columns", [])
    
    print(f"✓ Topic: {topic}")
    print(f"✓ Target table: {schema}.{table}")
    print(f"✓ Sync mode: {mode}")
    print(f"✓ Primary keys: {pk_columns if pk_columns else 'None (append mode)'}")
    
    # Step 2: Read CSV files from folder
    print(f"\n[2/7] Reading CSV files from {folder_path}...")
    df = read_all_csvs_from_folder(bucket, folder_path, minio_conn_id)
    
    if df.empty:
        return "⚠️ No data found in folder"
    
    print(f"Loaded {len(df)} rows")
    
    # Step 3: Clean and transform data
    print("\n[3/7] Cleaning and transforming data...")
    
    # Normalize column names
    df.columns = df.columns.str.lower().str.strip()
    
    # Check required columns (if configured)
    if required_cols:
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            print(f"⚠️ Warning: Missing expected columns: {missing_cols}")
    
    # Clean data
    df = clean_dataframe(df)
    
    # Standardize ticker column if exists
    if 'ticker' in df.columns:
        df = standardize_ticker(df, 'ticker')
    
    # Handle date columns
    for date_col in ['trading_date', 'published_date', 'date']:
        if date_col in df.columns:
            df[date_col] = pd.to_datetime(df[date_col], errors='coerce').dt.date
            df = df.dropna(subset=[date_col])
    
    # Handle numeric columns
    numeric_candidates = ['value', 'open', 'high', 'low', 'close', 'volume', 'year']
    for col in numeric_candidates:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    rows_after_cleaning = len(df)
    print(f"After cleaning: {rows_after_cleaning} rows")
    
    if df.empty:
        return "⚠️ No data after cleaning"
    
    # Step 4: Deduplication (CRITICAL - prevents constraint violations)
    print("\n[4/7] Deduplication...")
    
    # Always deduplicate if PK columns are defined, regardless of sync mode
    # This prevents "duplicate key value violates unique constraint" errors
    if pk_columns:
        # Check which PK columns exist in dataframe
        existing_pk_cols = [col for col in pk_columns if col in df.columns]
        
        if existing_pk_cols:
            # First, drop rows with NULL values in ANY PK column
            # (NULL values can't be part of unique constraint)
            rows_before_null_drop = len(df)
            df = df.dropna(subset=existing_pk_cols)
            null_dropped = rows_before_null_drop - len(df)
            
            if null_dropped > 0:
                print(f"⚠️ Dropped {null_dropped} rows with NULL in PK columns: {existing_pk_cols}")
            
            # Now check for duplicates based on PK columns
            duplicate_count = df.duplicated(subset=existing_pk_cols, keep='last').sum()
            
            if duplicate_count > 0:
                print(f"⚠️ Found {duplicate_count} duplicate rows based on PK: {existing_pk_cols}")
                # Keep last occurrence (most recent data)
                df = df.drop_duplicates(subset=existing_pk_cols, keep='last')
                print(f"✓ Removed duplicates, keeping last occurrence")
                print(f"✓ Rows after deduplication: {len(df)}")
            else:
                print(f"✓ No duplicates found based on PK columns: {existing_pk_cols}")
        else:
            print(f"⚠️ Warning: None of PK columns {pk_columns} found in data")
            print(f"   Available columns: {list(df.columns)}")
    else:
        print(f"✓ No PK columns defined - skipping deduplication")
    
    rows_after_dedup = len(df)
    
    if df.empty:
        return "⚠️ No data after deduplication"
    
    # Step 5: Preview data
    print("\n[5/7] Data preview...")
    print(f"Columns: {list(df.columns)}")
    print(f"First row sample: {df.iloc[0].to_dict() if len(df) > 0 else {}}")
    
    # Step 6: Dry run check
    if dry_run:
        print("\n[DRY RUN MODE]")
        print(f"✓ Would sync {len(df)} rows to {schema}.{table}")
        print(f"✓ Sync mode: {mode}")
        print(f"✓ Columns: {list(df.columns)}")
        return f"✅ DRY RUN: Would sync {len(df)} rows"
    
    # Step 7: Insert into database
    print(f"\n[6/7] Syncing to database (mode: {mode})...")
    
    with closing(get_postgres_connection(db_url)) as conn:
        conn.autocommit = False
        
        try:
            # Ensure schema exists
            ensure_schema(conn, schema)
            
            with conn.cursor() as cur:
                # Prepare column list and values
                columns = list(df.columns)
                rows = [tuple(row) for row in df.itertuples(index=False, name=None)]
                
                # Generate SQL based on sync mode
                if mode == "upsert":
                    if not pk_columns:
                        return f"❌ Cannot use upsert mode without primary key columns"
                    
                    # Build upsert SQL
                    cols_str = ", ".join(columns)
                    pk_str = ", ".join(pk_columns)
                    update_cols = [col for col in columns if col not in pk_columns]
                    update_str = ", ".join([f"{col} = EXCLUDED.{col}" for col in update_cols])
                    
                    sql = f"""
                        INSERT INTO {schema}.{table} ({cols_str})
                        VALUES %s
                        ON CONFLICT ({pk_str})
                        DO UPDATE SET {update_str}
                    """
                    execute_values(cur, sql, rows, page_size=1000)
                    print(f"✓ Upserted {len(rows)} rows")
                
                elif mode == "replace":
                    # Truncate then insert
                    cur.execute(f"TRUNCATE TABLE {schema}.{table}")
                    print(f"✓ Truncated table {schema}.{table}")
                    
                    cols_str = ", ".join(columns)
                    sql = f"INSERT INTO {schema}.{table} ({cols_str}) VALUES %s"
                    execute_values(cur, sql, rows, page_size=1000)
                    print(f"✓ Inserted {len(rows)} rows")
                
                elif mode == "append":
                    # Simple insert
                    cols_str = ", ".join(columns)
                    sql = f"INSERT INTO {schema}.{table} ({cols_str}) VALUES %s"
                    execute_values(cur, sql, rows, page_size=1000)
                    print(f"✓ Appended {len(rows)} rows")
                
                elif mode == "delete_insert":
                    # Delete matching records then insert
                    # For delete_insert, we delete all records and insert new ones
                    # This is typically used for reference data like people/ownership
                    cur.execute(f"DELETE FROM {schema}.{table}")
                    deleted_count = cur.rowcount
                    print(f"✓ Deleted {deleted_count} existing rows")
                    
                    cols_str = ", ".join(columns)
                    sql = f"INSERT INTO {schema}.{table} ({cols_str}) VALUES %s"
                    execute_values(cur, sql, rows, page_size=1000)
                    print(f"✓ Inserted {len(rows)} rows")
                
                else:
                    return f"❌ Unknown sync mode: {mode}"
            
            conn.commit()
            
            # Summary
            print("=" * 50)
            print(f"📥 LOADED from MinIO: {rows_after_cleaning} rows")
            if rows_after_dedup < rows_after_cleaning:
                print(f"🔄 DEDUPLICATED: {rows_after_cleaning - rows_after_dedup} duplicates removed")
            print(f"📤 SYNCED to DB ({mode}): {len(rows)} rows")
            print("=" * 50)
            
            return f"✅ Success: {mode.upper()} {len(rows)} rows to {table}"
            
        except Exception as e:
            conn.rollback()
            print(f"❌ Error: {str(e)}")
            raise
    
    print("=" * 70)


def list_available_topics() -> List[str]:
    """List all configured topics."""
    return list(TOPIC_CONFIG.keys())


def get_topic_info(topic: str) -> Optional[Dict[str, Any]]:
    """Get configuration info for a topic."""
    return TOPIC_CONFIG.get(topic)

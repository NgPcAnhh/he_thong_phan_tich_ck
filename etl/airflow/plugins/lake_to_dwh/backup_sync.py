from contextlib import closing
import pandas as pd
from psycopg2.extras import execute_values
from lake_to_dwh.utils import (
    get_all_partitions,
    read_all_csvs_from_folder,
    get_postgres_connection,
    ensure_schema,
    clean_dataframe,
    standardize_ticker,
    parse_trading_date
)

# Batch size for chunked processing (adjust based on available memory)
BATCH_SIZE = 50000  # Process 50k rows at a time


def process_and_clean_partition(
    df: pd.DataFrame,
    column_mapping: dict = None,
    required_columns: list = None,
    ticker_column: str = None,
    date_column: str = None
) -> pd.DataFrame:
    """
    Clean and transform a partition DataFrame.
    
    This is extracted to a separate function for reusability.
    """
    if df.empty:
        return df
    
    # Normalize column names
    df.columns = df.columns.str.lower().str.strip()
    
    # Apply column mapping
    if column_mapping:
        df.rename(columns=column_mapping, inplace=True)
    
    # Clean data
    if required_columns:
        df = clean_dataframe(df, required_columns=required_columns)
    else:
        df = clean_dataframe(df)
    
    # Standardize ticker
    if ticker_column and ticker_column in df.columns:
        df = standardize_ticker(df, ticker_column)
    
    # Parse dates
    if date_column and date_column in df.columns:
        df = parse_trading_date(df, date_column)
    
    # Remove duplicates within partition
    df = df.drop_duplicates()
    
    return df


def insert_dataframe_in_batches(
    df: pd.DataFrame,
    conn,
    schema: str,
    table: str,
    batch_size: int = BATCH_SIZE
) -> int:
    """
    Insert DataFrame into database in batches to avoid memory issues.
    
    Returns:
        Number of rows inserted
    """
    if df.empty:
        return 0
    
    columns = df.columns.tolist()
    columns_str = ', '.join(columns)
    total_rows = len(df)
    inserted = 0
    
    # Process in batches
    for start_idx in range(0, total_rows, batch_size):
        end_idx = min(start_idx + batch_size, total_rows)
        batch_df = df.iloc[start_idx:end_idx]
        
        # Convert batch to tuples
        rows = []
        for _, row in batch_df.iterrows():
            row_data = tuple(row[col] if pd.notna(row[col]) else None for col in columns)
            rows.append(row_data)
        
        # Insert batch
        with conn.cursor() as cur:
            insert_sql = f"""
                INSERT INTO {schema}.{table}
                ({columns_str})
                VALUES %s
                ON CONFLICT DO NOTHING;
            """
            execute_values(cur, insert_sql, rows, page_size=1000)
            inserted += len(rows)
        
        # Clear batch from memory
        del batch_df
        del rows
        
        if (start_idx + batch_size) % (batch_size * 10) == 0:
            print(f"  📊 Progress: {inserted:,} / {total_rows:,} rows inserted")
    
    return inserted


def backup_sync_generic_table(
    db_url: str,
    schema: str,
    table: str,
    bucket: str,
    folder_prefix: str,
    minio_conn_id: str = "minio_finance",
    column_mapping: dict = None,
    required_columns: list = None,
    ticker_column: str = None,
    date_column: str = None
) -> str:
    """
    Memory-efficient backup sync that processes partitions one at a time.
    
    OPTIMIZATION CHANGES:
    1. Processes each partition separately instead of concatenating all
    2. Inserts data partition-by-partition with batching
    3. Uses ON CONFLICT DO NOTHING for deduplication in DB
    4. Explicit memory cleanup between partitions
    
    This avoids loading 5.9M+ rows into memory at once.
    
    Args:
        db_url: PostgreSQL connection URL
        schema: Database schema name
        table: Target table name
        bucket: MinIO bucket name
        folder_prefix: Folder prefix in MinIO (e.g., "bctc/")
        minio_conn_id: MinIO connection ID
        column_mapping: Dict to rename columns
        required_columns: Columns that cannot be null
        ticker_column: Name of ticker column (for standardization)
        date_column: Name of date column (for parsing)
    
    Returns:
        Status message
    """
    print("="*70)
    print(f"📦 BACKUP SYNC: {table.upper()} (Memory-Optimized)")
    print("="*70)
    
    # Step 1: Get all partitions (doesn't load data yet)
    print(f"\n[1/4] Discovering partitions in {folder_prefix}...")
    partitions = get_all_partitions(bucket, folder_prefix, minio_conn_id)
    
    if not partitions:
        return f"⚠️ {table}: No partitions found"
    
    print(f"✓ Found {len(partitions)} partitions to process")
    
    # Step 2: Truncate table once before starting
    print(f"\n[2/4] Preparing database...")
    with closing(get_postgres_connection(db_url)) as conn:
        conn.autocommit = False
        try:
            ensure_schema(conn, schema)
            with conn.cursor() as cur:
                truncate_sql = f"TRUNCATE TABLE {schema}.{table} CASCADE;"
                cur.execute(truncate_sql)
                print(f"✓ Truncated {schema}.{table}")
            conn.commit()
        except Exception as e:
            conn.rollback()
            return f"❌ {table}: Failed to prepare database: {str(e)}"
    
    # Step 3: Process each partition separately
    print(f"\n[3/4] Processing partitions one at a time...")
    total_inserted = 0
    total_removed_dupes = 0
    
    for i, partition in enumerate(partitions, 1):
        print(f"\n📂 [{i}/{len(partitions)}] Processing: {partition}")
        
        # Read partition data
        df = read_all_csvs_from_folder(bucket, partition, minio_conn_id)
        
        if df.empty:
            print(f"  ⚠️ Empty partition, skipping")
            continue
        
        partition_original = len(df)
        print(f"  ✓ Read {partition_original:,} rows")
        
        # Clean and transform
        before_clean = len(df)
        df = process_and_clean_partition(
            df, column_mapping, required_columns, ticker_column, date_column
        )
        removed_in_clean = before_clean - len(df)
        
        if removed_in_clean > 0:
            print(f"  🧹 Removed {removed_in_clean:,} invalid/duplicate rows in cleaning")
            total_removed_dupes += removed_in_clean
        
        if df.empty:
            print(f"  ⚠️ No valid data after cleaning, skipping")
            continue
        
        # Insert partition data in batches
        print(f"  💾 Inserting {len(df):,} rows in batches...")
        with closing(get_postgres_connection(db_url)) as conn:
            conn.autocommit = False
            try:
                inserted = insert_dataframe_in_batches(df, conn, schema, table)
                conn.commit()
                total_inserted += inserted
                print(f"  ✅ Inserted {inserted:,} rows from this partition")
            except Exception as e:
                conn.rollback()
                print(f"  ❌ Error inserting partition: {str(e)}")
                # Continue with next partition instead of failing completely
                continue
        
        # Explicit cleanup
        del df
    
    # Step 4: Final deduplication in database (if needed)
    print(f"\n[4/4] Final database cleanup...")
    print(f"✓ Inserted {total_inserted:,} total rows from {len(partitions)} partitions")
    
    result_msg = f"✅ {table}: {total_inserted:,} rows inserted, {total_removed_dupes:,} duplicates removed during cleaning"
    print(f"\n{result_msg}")
    print("="*70)
    
    return result_msg

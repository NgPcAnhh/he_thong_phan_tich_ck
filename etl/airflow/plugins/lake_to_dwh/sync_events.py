"""
Sync events data from MinIO to PostgreSQL.
Sync strategy: REPLACE (DELETE partition + INSERT)
This ensures complete refresh of events data on each sync.
"""

from contextlib import closing
from datetime import datetime
from typing import Optional

import pandas as pd
from psycopg2.extras import execute_values

from lake_to_dwh.utils import (
    clean_dataframe,
    ensure_schema,
    get_latest_partition,
    get_postgres_connection,
    read_all_csvs_from_folder,
)


def _transform_events(df: pd.DataFrame) -> pd.DataFrame:
    """Transform events DataFrame to match events table schema."""
    if df.empty:
        return pd.DataFrame()
    
    df = df.copy()
    df.columns = df.columns.str.lower().str.strip()
    
    # Map columns to standard names
    column_mappings = {
        "event_title": "event_title",
        "public_date": "public_date",
        "source_url": "source_url",
        "event_list_name": "event_list_name",
    }
    
    for old_col, new_col in column_mappings.items():
        if old_col in df.columns and new_col not in df.columns:
            df.rename(columns={old_col: new_col}, inplace=True)
    
    return df


def sync_events_to_db(
    db_url: str,
    schema: str,
    bucket: str,
    minio_conn_id: str = "minio_finance",
    folder_prefix: str = "news/events/",
    table: str = "events"
) -> str:
    """
    Sync events from MinIO to PostgreSQL using REPLACE strategy.
    
    REPLACE strategy:
    1. Find latest partition date
    2. DELETE all rows with that partition_date
    3. INSERT new rows with partition_date
    
    This ensures events data is completely refreshed on each sync,
    which is appropriate for events that may be updated/cancelled.
    
    Args:
        db_url: PostgreSQL connection URL
        schema: Database schema name
        bucket: MinIO bucket name
        minio_conn_id: MinIO connection ID
        folder_prefix: MinIO folder prefix
        table: Target table name
    
    Returns:
        Status message
    """
    print("=" * 70)
    print("📅 SYNC EVENTS TO DATABASE (REPLACE STRATEGY)")
    print("=" * 70)
    
    # Step 1: Find latest partition
    print("\n[1/5] Finding latest partition...")
    latest_partition = get_latest_partition(bucket, folder_prefix, minio_conn_id)
    
    if not latest_partition:
        return "❌ No partition found"
    
    # Extract partition date from path
    # Format: news/events/2026-02-05/ or news/events/date=2026-02-05/
    partition_date_str = None
    parts = latest_partition.rstrip("/").split("/")
    for part in reversed(parts):
        if part.startswith("date="):
            partition_date_str = part[5:]
            break
        elif len(part) == 10 and part[4] == "-" and part[7] == "-":
            partition_date_str = part
            break
    
    if not partition_date_str:
        return f"❌ Could not extract date from partition: {latest_partition}"
    
    partition_date = pd.to_datetime(partition_date_str).date()
    print(f"Partition date: {partition_date}")
    
    # Step 2: Read all CSVs from latest partition
    print("\n[2/5] Reading files...")
    df = read_all_csvs_from_folder(bucket, latest_partition, minio_conn_id)
    
    if df.empty:
        return "⚠️ No data found"
    
    print(f"Total loaded: {len(df)} rows")
    
    # Step 3: Transform data
    print("\n[3/5] Transforming data...")
    df = _transform_events(df)
    df = clean_dataframe(df, required_columns=["event_id", "event_title"])
    df = df.drop_duplicates(subset=["event_id"])
    
    # Parse dates
    if "public_date" in df.columns:
        df["public_date"] = pd.to_datetime(df["public_date"], errors="coerce")
    
    if "fetched_at" in df.columns:
        df["fetched_at"] = pd.to_datetime(df["fetched_at"], errors="coerce")
    
    # Add partition_date column
    df["partition_date"] = partition_date
    
    print(f"After cleaning: {len(df)} rows")
    
    if df.empty:
        return "⚠️ No data after cleaning"
    
    # Step 4: Delete existing data for this partition
    # Step 5: Insert new data
    print("\n[4/5] Replacing data in database...")
    
    with closing(get_postgres_connection(db_url)) as conn:
        conn.autocommit = False
        
        try:
            ensure_schema(conn, schema)
            
            # DELETE existing data for this partition date
            delete_sql = f"DELETE FROM {schema}.{table} WHERE partition_date = %s;"
            
            with conn.cursor() as cur:
                cur.execute(delete_sql, (partition_date,))
                deleted = cur.rowcount
            
            print(f"Deleted {deleted} existing rows for partition {partition_date}")
            
            # Prepare rows for INSERT
            rows = []
            for _, row in df.iterrows():
                rows.append((
                    row.get("event_id"),
                    row.get("ticker"),
                    row.get("event_title"),
                    row.get("public_date"),
                    row.get("source_url"),
                    row.get("event_list_name"),
                    row.get("fetched_at"),
                    row.get("partition_date"),
                ))
            
            # INSERT new data
            insert_sql = f"""
                INSERT INTO {schema}.{table}
                (event_id, ticker, event_title, public_date, source_url, 
                 event_list_name, fetched_at, partition_date)
                VALUES %s;
            """
            
            print("\n[5/5] Inserting new data...")
            
            with conn.cursor() as cur:
                execute_values(cur, insert_sql, rows)
                inserted = cur.rowcount
            
            conn.commit()
            print(f"✅ Inserted {inserted} new rows")
            
            return f"✅ Success: Deleted {deleted}, Inserted {inserted} rows for partition {partition_date}"
            
        except Exception as e:
            conn.rollback()
            print(f"❌ Error: {str(e)}")
            raise


def sync_events_full_replace(
    db_url: str,
    schema: str,
    bucket: str,
    minio_conn_id: str = "minio_finance",
    folder_prefix: str = "news/events/",
    table: str = "events"
) -> str:
    """
    Full replace: TRUNCATE table and reload all data from all partitions.
    Use this for periodic full refresh (e.g., weekly).
    
    Args:
        db_url: PostgreSQL connection URL
        schema: Database schema name
        bucket: MinIO bucket name
        minio_conn_id: MinIO connection ID
        folder_prefix: MinIO folder prefix
        table: Target table name
    
    Returns:
        Status message
    """
    from lake_to_dwh.utils import read_all_csvs_from_all_partitions
    
    print("=" * 70)
    print("📅 SYNC EVENTS - FULL REPLACE (TRUNCATE + INSERT)")
    print("=" * 70)
    
    # Step 1: Read all data from all partitions
    print("\n[1/3] Reading all partitions...")
    df = read_all_csvs_from_all_partitions(bucket, folder_prefix, minio_conn_id)
    
    if df.empty:
        return "⚠️ No data found in any partition"
    
    # Step 2: Transform data
    print("\n[2/3] Transforming data...")
    df = _transform_events(df)
    df = clean_dataframe(df, required_columns=["event_id", "event_title"])
    df = df.drop_duplicates(subset=["event_id"])
    
    # Parse dates
    if "public_date" in df.columns:
        df["public_date"] = pd.to_datetime(df["public_date"], errors="coerce")
    
    if "fetched_at" in df.columns:
        df["fetched_at"] = pd.to_datetime(df["fetched_at"], errors="coerce")
    
    # Set partition_date from fetched_at or today
    if "partition_date" not in df.columns:
        if "fetched_at" in df.columns:
            df["partition_date"] = pd.to_datetime(df["fetched_at"]).dt.date
        else:
            df["partition_date"] = datetime.now().date()
    
    print(f"Total rows after cleaning: {len(df)}")
    
    if df.empty:
        return "⚠️ No data after cleaning"
    
    # Step 3: TRUNCATE and INSERT
    print("\n[3/3] Replacing all data in database...")
    
    with closing(get_postgres_connection(db_url)) as conn:
        conn.autocommit = False
        
        try:
            ensure_schema(conn, schema)
            
            # TRUNCATE table
            with conn.cursor() as cur:
                cur.execute(f"TRUNCATE TABLE {schema}.{table};")
            
            print("Truncated existing table")
            
            # Prepare rows for INSERT
            rows = []
            for _, row in df.iterrows():
                rows.append((
                    row.get("event_id"),
                    row.get("ticker"),
                    row.get("event_title"),
                    row.get("public_date"),
                    row.get("source_url"),
                    row.get("event_list_name"),
                    row.get("fetched_at"),
                    row.get("partition_date"),
                ))
            
            # INSERT all data
            insert_sql = f"""
                INSERT INTO {schema}.{table}
                (event_id, ticker, event_title, public_date, source_url, 
                 event_list_name, fetched_at, partition_date)
                VALUES %s;
            """
            
            with conn.cursor() as cur:
                execute_values(cur, insert_sql, rows)
                inserted = cur.rowcount
            
            conn.commit()
            print(f"✅ Inserted {inserted} rows")
            
            return f"✅ Full replace success: {inserted} total rows"
            
        except Exception as e:
            conn.rollback()
            print(f"❌ Error: {str(e)}")
            raise

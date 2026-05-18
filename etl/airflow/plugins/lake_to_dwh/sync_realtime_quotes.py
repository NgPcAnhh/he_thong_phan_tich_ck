"""
Sync realtime_quotes data from MinIO (Parquet files) to PostgreSQL database.
Strategy: UPSERT (ON CONFLICT symbol, ts DO UPDATE)
Data source: realtime/{YYYY-MM-DD}/quotes_*.parquet

Also provides a cleanup function to retain only today's and yesterday's data.
"""
import io
import logging
from contextlib import closing
from datetime import datetime, timedelta, timezone

import pandas as pd
from psycopg2.extras import execute_values

from lake_to_dwh.utils import (
    get_minio_hook,
    get_postgres_connection,
    ensure_schema,
)

logger = logging.getLogger("airflow.task")

# Vietnam timezone (UTC+7)
VN_TZ = timezone(timedelta(hours=7))

# ============================================================================
# Database columns for realtime_quotes (order matters for INSERT)
# ============================================================================
DB_COLUMNS = [
    "symbol", "ts",
    "last_price", "avg_price",
    "last_volume", "total_volume", "total_value",
    "foreign_buy_qty", "foreign_sell_qty",
    "foreign_buy_val", "foreign_sell_val",
    "bid1_price", "bid1_qty",
    "bid2_price", "bid2_qty",
    "bid3_price", "bid3_qty",
    "ask1_price", "ask1_qty",
    "ask2_price", "ask2_qty",
    "ask3_price", "ask3_qty",
    "ref_price", "ceil_price", "floor_price",
    "change_percent", "change_value",
    "high_price", "low_price",
]

# Numeric columns that need explicit type casting
NUMERIC_COLS = [
    "last_price", "avg_price", "total_value",
    "foreign_buy_val", "foreign_sell_val",
    "bid1_price", "bid2_price", "bid3_price",
    "ask1_price", "ask2_price", "ask3_price",
    "ref_price", "ceil_price", "floor_price",
    "change_percent", "change_value",
    "high_price", "low_price",
]

INT_COLS = [
    "last_volume", "total_volume",
    "foreign_buy_qty", "foreign_sell_qty",
    "bid1_qty", "bid2_qty", "bid3_qty",
    "ask1_qty", "ask2_qty", "ask3_qty",
]


# ============================================================================
# MinIO Helpers
# ============================================================================

def list_parquet_files(
    bucket: str,
    folder: str,
    conn_id: str = "minio_finance"
) -> list:
    """List all .parquet files in a MinIO folder."""
    hook = get_minio_hook(conn_id)

    try:
        keys = hook.list_keys(bucket_name=bucket, prefix=folder)
        if not keys:
            logger.info(f"⚠️ No objects found in {bucket}/{folder}")
            return []

        parquet_files = [k for k in keys if k.endswith(".parquet")]
        logger.info(f"✓ Found {len(parquet_files)} Parquet files in {bucket}/{folder}")
        return parquet_files
    except Exception as e:
        logger.error(f"❌ Error listing Parquet files: {str(e)}")
        return []


def read_parquet_from_minio(
    bucket: str,
    key: str,
    conn_id: str = "minio_finance"
) -> pd.DataFrame:
    """
    Read a single Parquet file from MinIO into a Pandas DataFrame.

    Uses S3Hook to download the file as bytes, then reads it
    with pandas + pyarrow engine.
    """
    hook = get_minio_hook(conn_id)

    try:
        # Download the parquet file as a file object
        file_obj = hook.get_key(key=key, bucket_name=bucket)
        if file_obj is None:
            logger.warning(f"⚠️ File not found: {key}")
            return pd.DataFrame()

        # Read into bytes buffer
        body = file_obj.get()["Body"].read()
        parquet_buffer = io.BytesIO(body)

        # Parse parquet
        df = pd.read_parquet(parquet_buffer, engine="pyarrow")
        logger.info(f"✓ Read {len(df)} rows from {key.split('/')[-1]}")
        return df
    except Exception as e:
        logger.error(f"❌ Error reading Parquet file {key}: {str(e)}")
        return pd.DataFrame()


# ============================================================================
# Data Transformation
# ============================================================================

def transform_realtime_quotes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean, transform, and deduplicate realtime quotes DataFrame.

    Steps:
      1. Normalize column names to lowercase.
      2. Convert 'ts' from milliseconds epoch to Vietnam local TIMESTAMP.
      3. Clean 'symbol' field (uppercase, trim).
      4. Cast numeric/integer columns.
      5. Keep only columns that exist in DB_COLUMNS.
      6. Deduplicate on (symbol, ts) keeping the last occurrence.
    """
    if df.empty:
        return df

    df = df.copy()

    # 1. Normalize column names
    df.columns = df.columns.str.lower().str.strip()

    # 2. Convert timestamp
    if "ts" in df.columns:
        # Check if ts values look like millisecond epochs (large numbers)
        sample_ts = df["ts"].dropna().iloc[0] if len(df["ts"].dropna()) > 0 else None
        if sample_ts is not None and isinstance(sample_ts, (int, float)) and sample_ts > 1e12:
            # Milliseconds epoch → convert to datetime
            df["ts"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
            # Convert to Vietnam timezone then drop tz info for naive TIMESTAMP
            df["ts"] = df["ts"].dt.tz_convert("Asia/Ho_Chi_Minh").dt.tz_localize(None)
        elif sample_ts is not None and isinstance(sample_ts, (int, float)) and sample_ts > 1e9:
            # Seconds epoch
            df["ts"] = pd.to_datetime(df["ts"], unit="s", utc=True)
            df["ts"] = df["ts"].dt.tz_convert("Asia/Ho_Chi_Minh").dt.tz_localize(None)
        else:
            # Already datetime or string → try parsing directly
            df["ts"] = pd.to_datetime(df["ts"], errors="coerce")

        # Round to microseconds to prevent duplicate values due to nanosecond resolution truncation in Postgres
        df["ts"] = df["ts"].dt.round("us")

    # 3. Clean symbol
    if "symbol" in df.columns:
        df["symbol"] = df["symbol"].astype(str).str.upper().str.strip()

    # 4. Cast numeric columns
    for col in NUMERIC_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in INT_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            # Convert to nullable integer (Int64) to handle NaN
            df[col] = df[col].astype("Int64")

    # 5. Keep only DB columns that exist
    available_cols = [col for col in DB_COLUMNS if col in df.columns]
    df = df[available_cols].copy()

    # 6. Drop rows with NULL in primary key columns
    df = df.dropna(subset=["symbol", "ts"])

    # 7. Deduplicate on (symbol, ts), keeping last occurrence
    before_dedup = len(df)
    df = df.drop_duplicates(subset=["symbol", "ts"], keep="last")
    dedup_count = before_dedup - len(df)

    if dedup_count > 0:
        logger.info(f"🔄 Removed {dedup_count} duplicate rows (kept last)")

    logger.info(f"✓ Transformed DataFrame: {len(df)} rows, {len(available_cols)} columns")
    return df


# ============================================================================
# Database Sync
# ============================================================================

def sync_realtime_quotes_to_db(
    db_url: str,
    schema: str,
    bucket: str,
    target_date: str = None,
    minio_conn_id: str = "minio_finance",
    table: str = "realtime_quotes"
) -> str:
    """
    Sync realtime quotes from MinIO Parquet files to PostgreSQL.

    Reads all Parquet files from `realtime/{target_date}/` in MinIO,
    transforms the data, and performs a batch UPSERT into the
    realtime_quotes table.

    Args:
        db_url: PostgreSQL connection URL
        schema: Database schema name
        bucket: MinIO bucket name
        target_date: Date string (YYYY-MM-DD). Defaults to today in VN timezone.
        minio_conn_id: MinIO Airflow connection ID
        table: Target database table name

    Returns:
        Status message string
    """
    logger.info("=" * 70)
    logger.info("📊 SYNC REALTIME QUOTES: MinIO (Parquet) → PostgreSQL")
    logger.info("=" * 70)

    # Determine target date
    if target_date is None:
        target_date = datetime.now(VN_TZ).strftime("%Y-%m-%d")

    folder_prefix = f"realtime/{target_date}/"
    logger.info(f"📅 Target date: {target_date}")
    logger.info(f"📁 MinIO folder: {bucket}/{folder_prefix}")

    # Step 1: List all Parquet files for the target date
    logger.info("\n[1/4] Listing Parquet files...")
    parquet_files = list_parquet_files(bucket, folder_prefix, minio_conn_id)

    if not parquet_files:
        msg = f"⚠️ No Parquet files found in {bucket}/{folder_prefix}"
        logger.info(msg)
        return msg

    # Step 2: Read and concatenate all Parquet files
    logger.info(f"\n[2/4] Reading {len(parquet_files)} Parquet files...")
    all_dfs = []

    for i, pf in enumerate(parquet_files, 1):
        logger.info(f"  [{i}/{len(parquet_files)}] Reading: {pf.split('/')[-1]}")
        df = read_parquet_from_minio(bucket, pf, minio_conn_id)
        if not df.empty:
            all_dfs.append(df)

    if not all_dfs:
        msg = "⚠️ No valid data found in any Parquet files"
        logger.info(msg)
        return msg

    combined_df = pd.concat(all_dfs, ignore_index=True)
    logger.info(f"✓ Combined: {len(combined_df)} total rows from {len(all_dfs)} files")

    # Step 3: Transform data
    logger.info("\n[3/4] Transforming data...")
    df = transform_realtime_quotes(combined_df)

    if df.empty:
        msg = "⚠️ No data remaining after transformation"
        logger.info(msg)
        return msg

    # Step 4: UPSERT into database
    logger.info(f"\n[4/4] Upserting {len(df)} rows into {schema}.{table}...")

    with closing(get_postgres_connection(db_url)) as conn:
        conn.autocommit = False

        try:
            ensure_schema(conn, schema)

            # Build column list
            available_cols = [col for col in DB_COLUMNS if col in df.columns]
            cols_str = ", ".join(available_cols)

            # Build UPDATE SET clause (exclude PK columns: symbol, ts)
            update_cols = [col for col in available_cols if col not in ("symbol", "ts")]
            update_set = ", ".join([f"{col} = EXCLUDED.{col}" for col in update_cols])

            upsert_sql = f"""
                INSERT INTO {schema}.{table} ({cols_str})
                VALUES %s
                ON CONFLICT (symbol, ts)
                DO UPDATE SET {update_set}
            """

            # Prepare rows as tuples
            rows = []
            for _, row in df.iterrows():
                row_tuple = tuple(
                    row[col] if col in row.index and pd.notna(row[col]) else None
                    for col in available_cols
                )
                rows.append(row_tuple)

            # Execute batch upsert
            with conn.cursor() as cur:
                execute_values(cur, upsert_sql, rows, page_size=1000)

            conn.commit()

            logger.info("=" * 70)
            logger.info(f"✅ Successfully upserted {len(rows)} rows into {schema}.{table}")
            logger.info(f"📅 Date: {target_date}")
            logger.info(f"📁 Source files: {len(parquet_files)}")
            logger.info("=" * 70)

            return f"✅ Success: UPSERT {len(rows)} rows to {schema}.{table} (date={target_date})"

        except Exception as e:
            conn.rollback()
            logger.error(f"❌ Error upserting data: {str(e)}")
            raise


# ============================================================================
# Data Retention / Cleanup
# ============================================================================

def cleanup_old_realtime_quotes(
    db_url: str,
    schema: str,
    table: str = "realtime_quotes"
) -> str:
    """
    Delete realtime_quotes data older than yesterday (N-2 and older).

    Retains only today's (N) and yesterday's (N-1) data.
    Uses Vietnam timezone (UTC+7) to calculate the cutoff boundary.

    If no old data exists, the function completes successfully
    with 0 rows deleted (no error raised).

    Args:
        db_url: PostgreSQL connection URL
        schema: Database schema name
        table: Target database table name

    Returns:
        Status message string
    """
    logger.info("=" * 70)
    logger.info("🗑️ CLEANUP REALTIME QUOTES: Delete N-2 and older")
    logger.info("=" * 70)

    # Calculate cutoff: start of yesterday (00:00:00) in Vietnam timezone
    now_vn = datetime.now(VN_TZ)
    yesterday_start = (now_vn - timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    # Convert to naive datetime for comparison with TIMESTAMP column
    cutoff_dt = yesterday_start.replace(tzinfo=None)

    logger.info(f"📅 Current time (VN): {now_vn.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"🔒 Keeping data from: {cutoff_dt} onwards")
    logger.info(f"🗑️ Deleting data before: {cutoff_dt}")

    with closing(get_postgres_connection(db_url)) as conn:
        conn.autocommit = False

        try:
            with conn.cursor() as cur:
                # Count records to be deleted (for logging)
                count_sql = f"""
                    SELECT COUNT(*)
                    FROM {schema}.{table}
                    WHERE ts < %s
                """
                cur.execute(count_sql, (cutoff_dt,))
                count_to_delete = cur.fetchone()[0]

                logger.info(f"📊 Records to delete: {count_to_delete}")

                if count_to_delete == 0:
                    logger.info("✅ No old data to delete. Table is already clean.")
                    return f"✅ No old data found (cutoff={cutoff_dt}). 0 rows deleted."

                # Perform the deletion
                delete_sql = f"""
                    DELETE FROM {schema}.{table}
                    WHERE ts < %s
                """
                cur.execute(delete_sql, (cutoff_dt,))
                deleted_count = cur.rowcount

            conn.commit()

            logger.info("=" * 70)
            logger.info(f"✅ Deleted {deleted_count} rows from {schema}.{table}")
            logger.info(f"📅 Cutoff date: {cutoff_dt}")
            logger.info("=" * 70)

            return f"✅ Success: Deleted {deleted_count} rows older than {cutoff_dt}"

        except Exception as e:
            conn.rollback()
            logger.error(f"❌ Error cleaning up data: {str(e)}")
            raise

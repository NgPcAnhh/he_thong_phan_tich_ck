"""
Sync news data from MinIO to PostgreSQL.
Combines vnstock_news and paper sources into news_v2 table.
Sync strategy: UPSERT (ON CONFLICT DO UPDATE)
"""

import hashlib
from contextlib import closing
from datetime import datetime
from typing import Optional

import pandas as pd
from psycopg2.extras import execute_values

from lake_to_dwh.utils import (
    clean_dataframe,
    ensure_schema,
    get_latest_partition,
    get_minio_hook,
    get_postgres_connection,
    read_all_csvs_from_folder,
)


def _generate_news_id(title: str, source_link: str) -> str:
    """Generate unique news_id from title and source_link."""
    content = f"{title}|{source_link}"
    return hashlib.md5(content.encode()).hexdigest()


def _transform_vnstock_news(df: pd.DataFrame) -> pd.DataFrame:
    """Transform vnstock news DataFrame to match news_v2 schema."""
    if df.empty:
        return pd.DataFrame()
    
    df = df.copy()
    df.columns = df.columns.str.lower().str.strip()
    
    # Map columns to standard names
    column_mappings = {
        "news_title": "news_title",
        "news_image_url": "news_image_url",
        "news_source_link": "news_source_link",
        "news_short_content": "news_short_content",
        "news_full_content": "news_full_content",
        "public_date": "public_date",
    }
    
    for old_col, new_col in column_mappings.items():
        if old_col in df.columns and new_col not in df.columns:
            df.rename(columns={old_col: new_col}, inplace=True)
    
    # Ensure news_id exists
    if "news_id" not in df.columns:
        df["news_id"] = df.apply(
            lambda row: _generate_news_id(
                str(row.get("news_title", "")),
                str(row.get("news_source_link", ""))
            ),
            axis=1
        )
    
    # Set source_type
    if "source_type" not in df.columns:
        df["source_type"] = "vnstock"
    
    return df


def _transform_paper_news(df: pd.DataFrame) -> pd.DataFrame:
    """Transform paper news DataFrame to match news_v2 schema."""
    if df.empty:
        return pd.DataFrame()
    
    df = df.copy()
    df.columns = df.columns.str.lower().str.strip()
    
    # Paper crawl columns: source, url, title, sapo, content, publish_time
    column_mappings = {
        "title": "news_title",
        "url": "news_source_link",
        "sapo": "news_short_content",
        "content": "news_full_content",
        "publish_time": "public_date",
    }
    
    for old_col, new_col in column_mappings.items():
        if old_col in df.columns:
            df.rename(columns={old_col: new_col}, inplace=True)
    
    # Generate news_id
    df["news_id"] = df.apply(
        lambda row: _generate_news_id(
            str(row.get("news_title", "")),
            str(row.get("news_source_link", ""))
        ),
        axis=1
    )
    
    # Set source_type
    df["source_type"] = "paper"
    
    # No ticker for paper news
    if "ticker" not in df.columns:
        df["ticker"] = None
    
    # No image for paper
    if "news_image_url" not in df.columns:
        df["news_image_url"] = None
    
    return df


def sync_vnstock_news_to_db(
    db_url: str,
    schema: str,
    bucket: str,
    minio_conn_id: str = "minio_finance",
    folder_prefix: str = "news/vnstock_news/",
    table: str = "news_v2"
) -> str:
    """
    Sync vnstock news from MinIO to PostgreSQL using UPSERT.
    
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
    print("📰 SYNC VNSTOCK NEWS TO DATABASE")
    print("=" * 70)
    
    # Step 1: Find latest partition
    print("\n[1/4] Finding latest partition...")
    latest_partition = get_latest_partition(bucket, folder_prefix, minio_conn_id)
    
    if not latest_partition:
        return "❌ No partition found"
    
    # Step 2: Read all CSVs from latest partition
    print("\n[2/4] Reading files...")
    df = read_all_csvs_from_folder(bucket, latest_partition, minio_conn_id)
    
    if df.empty:
        return "⚠️ No data found"
    
    print(f"Total loaded: {len(df)} rows")
    
    # Step 3: Transform data
    print("\n[3/4] Transforming data...")
    df = _transform_vnstock_news(df)
    df = clean_dataframe(df, required_columns=["news_id", "news_title"])
    df = df.drop_duplicates(subset=["news_id"])
    
    # Parse dates
    if "public_date" in df.columns:
        df["public_date"] = pd.to_datetime(df["public_date"], errors="coerce")
    
    if "fetched_at" in df.columns:
        df["fetched_at"] = pd.to_datetime(df["fetched_at"], errors="coerce")
    
    print(f"After cleaning: {len(df)} rows")
    
    if df.empty:
        return "⚠️ No data after cleaning"
    
    # Step 4: Upsert into database
    print("\n[4/4] Upserting into database...")
    
    with closing(get_postgres_connection(db_url)) as conn:
        conn.autocommit = False
        
        try:
            ensure_schema(conn, schema)
            
            # Prepare rows
            rows = []
            for _, row in df.iterrows():
                rows.append((
                    row.get("news_id"),
                    row.get("ticker"),
                    row.get("news_title"),
                    row.get("news_image_url"),
                    row.get("news_source_link"),
                    row.get("news_short_content"),
                    row.get("news_full_content"),
                    row.get("public_date"),
                    row.get("source_type", "vnstock"),
                    row.get("fetched_at"),
                ))
            
            # UPSERT: ON CONFLICT DO UPDATE
            upsert_sql = f"""
                INSERT INTO {schema}.{table}
                (news_id, ticker, news_title, news_image_url, news_source_link,
                 news_short_content, news_full_content, public_date, source_type, fetched_at)
                VALUES %s
                ON CONFLICT (news_id) DO UPDATE SET
                    ticker = EXCLUDED.ticker,
                    news_title = EXCLUDED.news_title,
                    news_image_url = EXCLUDED.news_image_url,
                    news_source_link = EXCLUDED.news_source_link,
                    news_short_content = EXCLUDED.news_short_content,
                    news_full_content = EXCLUDED.news_full_content,
                    public_date = EXCLUDED.public_date,
                    source_type = EXCLUDED.source_type,
                    fetched_at = EXCLUDED.fetched_at,
                    import_time = CURRENT_TIMESTAMP;
            """
            
            with conn.cursor() as cur:
                execute_values(cur, upsert_sql, rows)
                affected = cur.rowcount
            
            conn.commit()
            print(f"✅ Upserted {affected} rows")
            
            return f"✅ Success: {affected} rows upserted"
            
        except Exception as e:
            conn.rollback()
            print(f"❌ Error: {str(e)}")
            raise


def sync_paper_news_to_db(
    db_url: str,
    schema: str,
    bucket: str,
    minio_conn_id: str = "minio_finance",
    folder_prefix: str = "news/paper/",
    table: str = "news_v2"
) -> str:
    """
    Sync paper news from MinIO to PostgreSQL using UPSERT.
    
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
    print("📄 SYNC PAPER NEWS TO DATABASE")
    print("=" * 70)
    
    # Step 1: Find latest partition
    print("\n[1/4] Finding latest partition...")
    latest_partition = get_latest_partition(bucket, folder_prefix, minio_conn_id)
    
    if not latest_partition:
        return "❌ No partition found"
    
    # Step 2: Read all CSVs from latest partition
    print("\n[2/4] Reading files...")
    df = read_all_csvs_from_folder(bucket, latest_partition, minio_conn_id)
    
    if df.empty:
        return "⚠️ No data found"
    
    print(f"Total loaded: {len(df)} rows")
    
    # Step 3: Transform data
    print("\n[3/4] Transforming data...")
    df = _transform_paper_news(df)
    df = clean_dataframe(df, required_columns=["news_id", "news_title"])
    df = df.drop_duplicates(subset=["news_id"])
    
    # Parse dates
    if "public_date" in df.columns:
        df["public_date"] = pd.to_datetime(df["public_date"], errors="coerce")
    
    if "fetched_at" in df.columns:
        df["fetched_at"] = pd.to_datetime(df["fetched_at"], errors="coerce")
    
    print(f"After cleaning: {len(df)} rows")
    
    if df.empty:
        return "⚠️ No data after cleaning"
    
    # Step 4: Upsert into database
    print("\n[4/4] Upserting into database...")
    
    with closing(get_postgres_connection(db_url)) as conn:
        conn.autocommit = False
        
        try:
            ensure_schema(conn, schema)
            
            # Prepare rows
            rows = []
            for _, row in df.iterrows():
                rows.append((
                    row.get("news_id"),
                    row.get("ticker"),
                    row.get("news_title"),
                    row.get("news_image_url"),
                    row.get("news_source_link"),
                    row.get("news_short_content"),
                    row.get("news_full_content"),
                    row.get("public_date"),
                    row.get("source_type", "paper"),
                    row.get("fetched_at"),
                ))
            
            # UPSERT: ON CONFLICT DO UPDATE
            upsert_sql = f"""
                INSERT INTO {schema}.{table}
                (news_id, ticker, news_title, news_image_url, news_source_link,
                 news_short_content, news_full_content, public_date, source_type, fetched_at)
                VALUES %s
                ON CONFLICT (news_id) DO UPDATE SET
                    ticker = EXCLUDED.ticker,
                    news_title = EXCLUDED.news_title,
                    news_image_url = EXCLUDED.news_image_url,
                    news_source_link = EXCLUDED.news_source_link,
                    news_short_content = EXCLUDED.news_short_content,
                    news_full_content = EXCLUDED.news_full_content,
                    public_date = EXCLUDED.public_date,
                    source_type = EXCLUDED.source_type,
                    fetched_at = EXCLUDED.fetched_at,
                    import_time = CURRENT_TIMESTAMP;
            """
            
            with conn.cursor() as cur:
                execute_values(cur, upsert_sql, rows)
                affected = cur.rowcount
            
            conn.commit()
            print(f"✅ Upserted {affected} rows")
            
            return f"✅ Success: {affected} rows upserted"
            
        except Exception as e:
            conn.rollback()
            print(f"❌ Error: {str(e)}")
            raise

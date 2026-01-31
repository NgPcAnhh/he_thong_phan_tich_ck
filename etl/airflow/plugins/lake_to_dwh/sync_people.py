"""
Sync company people/owner data from MinIO to PostgreSQL database.
Strategy: DELETE old records for ticker, then INSERT new ones
Target table: owner
"""
from contextlib import closing
import pandas as pd
from psycopg2.extras import execute_values
from lake_to_dwh.utils import (
    get_latest_partition,
    read_all_csvs_from_folder,
    get_postgres_connection,
    ensure_schema,
    clean_dataframe,
    standardize_ticker
)


def sync_people_to_db(
    db_url: str,
    schema: str,
    bucket: str,
    minio_conn_id: str = "minio_finance",
    folder_prefix: str = "people/",
    table: str = "owner"
) -> str:
    """
    Sync company people/ownership data from MinIO to PostgreSQL.
    
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
    print("📊 SYNC COMPANY PEOPLE/OWNER TO DATABASE")
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
    
    # Handle column variations (based on schema)
    column_mappings = {
        'symbol': 'ticker',
        'owner_name': 'name',
        'owner_position': 'position',
        'ownership_percent': 'percent',
        'owner_type': 'type',
    }
    
    for old_col, new_col in column_mappings.items():
        if old_col in df.columns and new_col not in df.columns:
            df.rename(columns={old_col: new_col}, inplace=True)
    
    # Ensure required columns exist
    if 'ticker' not in df.columns:
        return "❌ Missing ticker column"
    
    # Clean data
    df = clean_dataframe(df, required_columns=['ticker'])
    df = standardize_ticker(df, 'ticker')
    
    # Select columns that exist in the schema
    schema_cols = ['ticker', 'name', 'position', 'percent', 'type']
    available_cols = [col for col in schema_cols if col in df.columns]
    df = df[available_cols].copy()
    
    print(f"After cleaning: {len(df)} rows")
    
    if df.empty:
        return "⚠️ No data after cleaning"
    
    # Step 4: Insert into database (delete old, insert new)
    print("\n[4/4] Inserting into database...")
    
    with closing(get_postgres_connection(db_url)) as conn:
        conn.autocommit = False
        
        try:
            # Ensure schema exists
            ensure_schema(conn, schema)
            
            # Get unique tickers
            unique_tickers = df['ticker'].unique().tolist()
            
            # Prepare data for insertion
            rows = []
            for _, row in df.iterrows():
                row_data = []
                for col in available_cols:
                    row_data.append(row.get(col))
                rows.append(tuple(row_data))
            
            # Build dynamic INSERT statement
            columns_str = ', '.join(available_cols)
            
            insert_sql = f"""
                INSERT INTO {schema}.{table}
                ({columns_str})
                VALUES %s;
            """
            
            with conn.cursor() as cur:
                # Delete old records for these tickers
                delete_sql = f"DELETE FROM {schema}.{table} WHERE ticker = ANY(%s);"
                cur.execute(delete_sql, (unique_tickers,))
                deleted = cur.rowcount
                print(f"Deleted {deleted} old records for {len(unique_tickers)} tickers")
                
                # Insert new records
                execute_values(cur, insert_sql, rows)
            
            conn.commit()
            print(f"✅ Inserted {len(rows)} rows")
            
            return f"✅ Success: {len(rows)} rows"
            
        except Exception as e:
            conn.rollback()
            print(f"❌ Error: {str(e)}")
            raise
    
    print("=" * 70)

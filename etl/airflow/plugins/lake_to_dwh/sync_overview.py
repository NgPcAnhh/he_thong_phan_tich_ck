"""
Sync company_overview data from MinIO to PostgreSQL database.
Strategy: UPSERT (overwrite duplicates, insert new)
Primary Key: ticker
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


def sync_overview_to_db(
    db_url: str,
    schema: str,
    bucket: str,
    minio_conn_id: str = "minio_finance",
    folder_prefix: str = "overview/",
    table: str = "company_overview"
) -> str:
    """
    Sync company overview data from MinIO to PostgreSQL.
    
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
    print("📊 SYNC COMPANY OVERVIEW TO DATABASE")
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
        'company_overview': 'overview',
        'icbname1': 'icb_name1',
        'icbname2': 'icb_name2',
        'icbname3': 'icb_name3',
    }
    
    for old_col, new_col in column_mappings.items():
        if old_col in df.columns and new_col not in df.columns:
            df.rename(columns={old_col: new_col}, inplace=True)
    
    # Ensure ticker column exists
    if 'ticker' not in df.columns:
        return "❌ Missing ticker column"
    
    # Clean data
    df = clean_dataframe(df, required_columns=['ticker'])
    df = standardize_ticker(df, 'ticker')
    
    # Remove duplicates (keep last)
    df = df.drop_duplicates(subset=['ticker'], keep='last')
    
    # Select columns that exist in the schema
    schema_cols = [
        'ticker', 'overview', 'icb_name1', 'icb_name2', 'icb_name3',
        'exchange', 'type_info', 'organ_short_name', 'organ_name', 'product_group'
    ]
    available_cols = [col for col in schema_cols if col in df.columns]
    df = df[available_cols].copy()
    
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
            rows = []
            for _, row in df.iterrows():
                row_data = [row['ticker']]  # ticker is always first
                
                # Add other columns in order
                for col in available_cols[1:]:  # Skip ticker (already added)
                    row_data.append(row.get(col))
                
                rows.append(tuple(row_data))
            
            # Build dynamic INSERT statement
            columns_str = ', '.join(available_cols)
            
            # UPSERT pattern using ON CONFLICT
            with conn.cursor() as cur:
                # Determine conflict keys based on available columns
                if 'exchange' in available_cols:
                    conflict_keys = '(ticker, exchange)'
                    # Build UPDATE SET clause (exclude key columns)
                    update_cols = [col for col in available_cols 
                                  if col not in ['ticker', 'exchange']]
                else:
                    conflict_keys = '(ticker)'
                    # Build UPDATE SET clause (exclude ticker)
                    update_cols = [col for col in available_cols 
                                  if col != 'ticker']
                
                update_set = ', '.join([f"{col} = EXCLUDED.{col}" for col in update_cols])
                
                # UPSERT
                upsert_sql = f"""
                    INSERT INTO {schema}.{table}
                    ({columns_str})
                    VALUES %s
                    ON CONFLICT {conflict_keys}
                    DO UPDATE SET
                        {update_set};
                """
                execute_values(cur, upsert_sql, rows)
            
            conn.commit()
            print(f"✅ Upserted {len(rows)} rows")
            
            return f"✅ Success: {len(rows)} rows"
            
        except Exception as e:
            conn.rollback()
            print(f"❌ Error: {str(e)}")
            raise
    
    print("=" * 70)

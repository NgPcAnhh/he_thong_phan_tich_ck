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


# INDICATOR_MAPPING removed - data is now fetched in Vietnamese from vnstock with lang='vi' parameter


# apply_indicator_mapping() removed - no longer needed since ind_name is already in Vietnamese


def sync_bctc_to_db(
    db_url: str,
    schema: str,
    bucket: str,
    minio_conn_id: str = "minio_finance",
    folder_prefix: str = "bctc/",
    table: str = "bctc"
) -> str:

    print("=" * 70)
    print("📊 SYNC BCTC TO DATABASE (Vietnamese + Smart IND_CODE)")
    print("=" * 70)
    
    # Step 1: Find latest partition
    print("\n[1/5] Finding latest partition...")
    latest_partition = get_latest_partition(bucket, folder_prefix, minio_conn_id)
    
    if not latest_partition:
        return "❌ No partition found"
    
    # Step 2: Read all CSV files
    print("\n[2/5] Reading CSV files...")
    df = read_all_csvs_from_folder(bucket, latest_partition, minio_conn_id)
    
    if df.empty:
        return "⚠️ No data found"
    
    print(f"Loaded {len(df)} rows")
    
    # Step 3: Clean and transform data
    print("\n[3/5] Cleaning and transforming data...")
    
    # Normalize column names
    df.columns = df.columns.str.lower().str.strip()
    
    # Ensure required columns exist
    required_cols = ['ticker', 'quarter', 'year', 'ind_name', 'value']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        return f"❌ Missing columns: {missing_cols}"
    
    # Clean data
    df = clean_dataframe(df, required_columns=required_cols)
    df = standardize_ticker(df, 'ticker')
    
    # Parse year and quarter  
    # Note: quarter is VARCHAR(10) in database, year is INTEGER
    df['year'] = pd.to_numeric(df['year'], errors='coerce').astype('Int64')
    df['quarter'] = df['quarter'].astype(str).str.strip()  # Keep as string
    df['value'] = pd.to_numeric(df['value'], errors='coerce')
    
    # Remove rows with null year or quarter
    df = df.dropna(subset=['year', 'quarter'])
    
    rows_after_cleaning = len(df)
    print(f"After cleaning: {rows_after_cleaning} rows")
    
    # Step 4: Generate smart ind_code from report_code + indicator acronym
    print("\n[4/5] Generating smart ind_code...")
    
    def generate_ind_code(report_code: str, ind_name: str) -> str:
        import re

        if not ind_name:
            return 'UNKNOWN'

        # Chuẩn hoá chuỗi
        clean_name = re.sub(r'[(),.;\'"\-]+', ' ', str(ind_name))
        clean_name = re.sub(r'\s+', ' ', clean_name).strip().lower()

        # Lấy ký tự đầu chuỗi + ký tự sau space
        acronym_chars = re.findall(r'(^\S)|(?<=\s)\S', clean_name)
        acronym = ''.join(acronym_chars)

        # Ghép với report_code
        code = f"{report_code}_{acronym}" if report_code else acronym

        return code[:50]

    
    # Apply ind_code generation
    if 'ind_code' not in df.columns or df['ind_code'].isnull().all():
        if 'report_code' in df.columns:
            df['ind_code'] = df.apply(
                lambda row: generate_ind_code(
                    str(row.get('report_code', '')),
                    str(row.get('ind_name', ''))
                ),
                axis=1
            )
            print(f"✓ Generated ind_code from report_code + acronym")
        else:
            # Fallback if no report_code
            df['ind_code'] = df['ind_name'].apply(lambda x: generate_ind_code('', str(x)))
            print(f"⚠️ Generated ind_code from ind_name only (no report_code column)")
    else:
        print(f"✓ Using existing ind_code from data")
    
    # Ensure ind_code is not null
    df['ind_code'] = df['ind_code'].fillna('UNKNOWN').astype(str).str[:50]
    
    # Select final columns
    final_cols = ['ticker', 'quarter', 'year', 'ind_name', 'ind_code', 'value', 'report_name', 'report_code']
    df = df[[col for col in final_cols if col in df.columns]].copy()
    
    if df.empty:
        return "⚠️ No data after transformation"
    
    # Step 5: Insert into database
    print("\n[5/5] Inserting into database...")
    
    with closing(get_postgres_connection(db_url)) as conn:
        conn.autocommit = False
        
        try:
            # Ensure schema exists
            ensure_schema(conn, schema)
            
            # Prepare data for insertion
            rows = []
            for _, row in df.iterrows():
                rows.append((
                    row.get('ticker'),
                    row.get('quarter'),
                    row.get('year'),
                    row.get('ind_name'),
                    row.get('ind_code'),
                    row.get('value'),
                    row.get('report_name'),
                    row.get('report_code'),
                ))
            
            
            # Use modern PostgreSQL UPSERT with ON CONFLICT for PRIMARY KEY
            # Assumes table has PRIMARY KEY (ticker, year, quarter, ind_code)
            
            with conn.cursor() as cur:
                # Modern upsert: INSERT ... ON CONFLICT DO UPDATE
                upsert_sql = f"""
                    INSERT INTO {schema}.{table}
                    (ticker, quarter, year, ind_name, ind_code, value, report_name, report_code)
                    VALUES %s
                    ON CONFLICT (ticker, year, quarter, ind_code)
                    DO UPDATE SET
                        ind_name = EXCLUDED.ind_name,
                        value = EXCLUDED.value,
                        report_name = EXCLUDED.report_name,
                        report_code = EXCLUDED.report_code;
                """
                
                execute_values(cur, upsert_sql, rows, page_size=1000)
                print(f"✓ Upserted {len(rows)} rows using ON CONFLICT (efficient)")
            
            conn.commit()
            
            # Summary log
            print("="*50)
            print(f"📥 LOADED from MinIO: {rows_after_cleaning} rows")
            print(f"📤 UPSERTED to DB: {len(rows)} rows")
            print("="*50)
            
            return f"✅ Success: Loaded {rows_after_cleaning} | Upserted {len(rows)} rows"
            
        except Exception as e:
            conn.rollback()
            print(f"❌ Error: {str(e)}")
            raise
    
    print("=" * 70)

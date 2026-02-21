from contextlib import closing
import pandas as pd
import numpy as np
import re
import unicodedata
from psycopg2.extras import execute_values
from lake_to_dwh.utils import (
    get_latest_partition,
    read_all_csvs_from_folder,
    get_postgres_connection,
    ensure_schema
)


# =========================
# COLUMN MAPPING (from chiso_taichinh.py)
# Maps CSV column names to database column names
# Database schema: cp, nam, ky (not ticker, year, quarter)
# =========================
COLUMN_MAPPING = {
    # --- META DATA ---
    "ticker": "cp",
    "ind_code": "cp",  # Some files may have ind_code instead
    "Meta_CP": "cp",
    "Meta_Năm": "nam",
    "Meta_Kỳ": "ky",
    "period_type": "period_type",
    "extracted_at": "extracted_at",

    # --- CƠ CẤU NGUỒN VỐN ---
    "Chỉ tiêu cơ cấu nguồn vốn_Nợ/VCSH": "no_vcsh",
    "Chỉ tiêu cơ cấu nguồn vốn_TSCĐ / Vốn CSH": "tsc_von_csh",
    "Chỉ tiêu cơ cấu nguồn vốn_Vốn CSH/Vốn điều lệ": "von_csh_von_ieu_le",
    "Chỉ tiêu cơ cấu nguồn vốn_(Vay NH+DH)/VCSH": "vay_nh_dh_vcsh",

    # --- HIỆU QUẢ HOẠT ĐỘNG ---
    "Chỉ tiêu hiệu quả hoạt động_Vòng quay tài sản": "vong_quay_tai_san",
    "Chỉ tiêu hiệu quả hoạt động_Vòng quay TSCĐ": "vong_quay_tsc",
    "Chỉ tiêu hiệu quả hoạt động_Số ngày thu tiền bình quân": "so_ngay_thu_tien_binh_quan",
    "Chỉ tiêu hiệu quả hoạt động_Số ngày tồn kho bình quân": "so_ngay_ton_kho_binh_quan",
    "Chỉ tiêu hiệu quả hoạt động_Số ngày thanh toán bình quân": "so_ngay_thanh_toan_binh_quan",
    "Chỉ tiêu hiệu quả hoạt động_Chu kỳ tiền": "chu_ky_tien",
    "Chỉ tiêu hiệu quả hoạt động_Vòng quay hàng tồn kho": "vong_quay_hang_ton_kho",

    # --- KHẢ NĂNG SINH LỜI ---
    "Chỉ tiêu khả năng sinh lợi_Biên EBIT (%)": "bien_ebit",
    "Chỉ tiêu khả năng sinh lợi_Biên lợi nhuận gộp (%)": "bien_loi_nhuan_gop",
    "Chỉ tiêu khả năng sinh lợi_Biên lợi nhuận ròng (%)": "bien_loi_nhuan_rong",
    "Chỉ tiêu khả năng sinh lợi_ROE (%)": "roe",
    "Chỉ tiêu khả năng sinh lợi_ROIC (%)": "roic",
    "Chỉ tiêu khả năng sinh lợi_ROA (%)": "roa",
    "Chỉ tiêu khả năng sinh lợi_EBITDA (Tỷ đồng)": "ebitda_ty_ong",
    "Chỉ tiêu khả năng sinh lợi_EBIT (Tỷ đồng)": "ebit_ty_ong",
    "Chỉ tiêu khả năng sinh lợi_Tỷ suất cổ tức (%)": "ty_suat_co_tuc",

    # --- THANH KHOẢN & ĐÒN BẨY ---
    "Chỉ tiêu thanh khoản_Chỉ số thanh toán hiện thời": "chi_so_thanh_toan_hien_thoi",
    "Chỉ tiêu thanh khoản_Chỉ số thanh toán tiền mặt": "chi_so_thanh_toan_tien_mat",
    "Chỉ tiêu thanh khoản_Chỉ số thanh toán nhanh": "chi_so_thanh_toan_nhanh",
    "Chỉ tiêu thanh khoản_Đòn bẩy tài chính": "on_bay_tai_chinh",
    "Chỉ tiêu thanh khoản_Khả năng chi trả lãi vay": "kha_nang_chi_tra_lai_vay",

    # --- ĐỊNH GIÁ ---
    "Chỉ tiêu định giá_Vốn hóa (Tỷ đồng)": "von_hoa_ty_ong",
    "Chỉ tiêu định giá_Số CP lưu hành (Triệu CP)": "so_cp_luu_hanh_trieu_cp",
    "Chỉ tiêu định giá_P/E": "p_e",
    "Chỉ tiêu định giá_P/B": "p_b",
    "Chỉ tiêu định giá_P/S": "p_s",
    "Chỉ tiêu định giá_P/Cash Flow": "p_cash_flow",
    "Chỉ tiêu định giá_EPS (VND)": "eps_vnd",
    "Chỉ tiêu định giá_BVPS (VND)": "bvps_vnd",
    "Chỉ tiêu định giá_EV/EBITDA": "ev_ebitda"
}


def clean_header_key(text):
    """Làm sạch key để so sánh mapping dễ hơn (bỏ dấu, lowercase)"""
    if not isinstance(text, str):
        return str(text)
    text = unicodedata.normalize("NFKD", text)
    text = text.encode("ascii", "ignore").decode("ascii")
    return text.lower().strip().replace(" ", "_").replace("/", "_")


def get_mapped_columns(raw_columns):
    """
    Mapping cột từ file CSV sang tên cột DB.
    Trả về: (list tên cột mới, list tên cột cũ cần giữ lại)
    """
    new_columns = []
    keep_columns = []
    
    # Tạo bản sao normalized của mapping keys để đối chiếu linh hoạt hơn
    normalized_mapping = {clean_header_key(k): v for k, v in COLUMN_MAPPING.items()}
    
    for col in raw_columns:
        clean_col = clean_header_key(col)
        
        # 1. Tìm trong mapping chính xác
        if col in COLUMN_MAPPING:
            new_columns.append(COLUMN_MAPPING[col])
            keep_columns.append(col)
        # 2. Tìm trong mapping đã normalized
        elif clean_col in normalized_mapping:
            new_columns.append(normalized_mapping[clean_col])
            keep_columns.append(col)
        else:
            # Bỏ qua cột không map được
            print(f"⚠️ Bỏ qua cột không map được: {col}")
            
    return new_columns, keep_columns


def process_multiheader_csv(df_raw):
    """
    Xử lý CSV có header đa cấp (2 dòng đầu)
    Dòng 0: Parent header (forward fill)
    Dòng 1: Child header
    Kết hợp: "Parent_Child"
    """
    if df_raw.shape[0] < 3:
        raise ValueError("CSV không đủ dữ liệu (cần ít nhất 3 dòng)")
    
    # Lấy dòng 0 và 1 làm header
    row_0 = df_raw.iloc[0].astype(str).replace('nan', '').str.strip()
    row_1 = df_raw.iloc[1].astype(str).replace('nan', '').str.strip()
    
    # Forward fill cho dòng 0
    last_valid = ""
    filled_row_0 = []
    for val in row_0:
        if val:
            last_valid = val
        filled_row_0.append(last_valid)
    
    # Kết hợp Header: "Parent_Child"
    combined_header = []
    for h1, h2 in zip(filled_row_0, row_1):
        if h1 and h2:
            combined_header.append(f"{h1}_{h2}")
        elif h2:
            combined_header.append(h2)
        else:
            combined_header.append(h1)
    
    # Gán header và lấy data từ dòng 2 trở đi
    df_raw.columns = combined_header
    df = df_raw.iloc[2:].reset_index(drop=True)
    
    return df


def clean_dataframe(df):
    """Clean and convert dataframe values"""
    # Thay thế rác thành NaN
    df = df.replace(['', ' ', 'N/A', 'NA', 'null', 'NULL', '-', '#DIV/0!'], np.nan)
    
    # Loại bỏ dấu phẩy trong số (VD: 1,000.50 -> 1000.50)
    for col in df.columns:
        try:
            # Check if accessing the column returns a Series (not DataFrame due to duplicates)
            col_data = df[col]
            if isinstance(col_data, pd.Series) and col_data.dtype == object:
                df[col] = col_data.str.replace(',', '', regex=False)
        except Exception as e:
            print(f"⚠️ Warning: Could not process column '{col}': {str(e)}")
            continue
    
    # Convert numeric (exclude cp and ky which should stay as string types for VARCHAR columns)
    for col in df.columns:
        if col not in ['cp', 'ky']:  # Don't convert cp and ky
            try:
                col_data = df[col]
                if isinstance(col_data, pd.Series):
                    df[col] = pd.to_numeric(col_data, errors='ignore')
            except Exception as e:
                print(f"⚠️ Warning: Could not convert column '{col}' to numeric: {str(e)}")
                continue
    
    return df


def sync_financial_ratio_to_db(
    db_url: str,
    schema: str,
    bucket: str,
    minio_conn_id: str = "minio_finance",
    folder_prefix: str = "financial_ratios/",
    table: str = "financial_ratio"
) -> str:

    print("=" * 70)
    print("📊 SYNC FINANCIAL RATIO TO DATABASE (UPSERT MODE)")
    print("=" * 70)
    
    # Step 1: Find latest partition
    print("\n[1/4] Finding latest partition...")
    latest_partition = get_latest_partition(bucket, folder_prefix, minio_conn_id)
    
    if not latest_partition:
        return "❌ No partition found"
    
    print(f"Latest partition: {latest_partition}")
    
    # Step 2: Read CSV with multi-header processing
    print("\n[2/4] Reading and processing CSV files...")
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    import io
    
    s3_hook = S3Hook(aws_conn_id=minio_conn_id)
    
    # List all CSV files in partition
    objects = s3_hook.list_keys(bucket_name=bucket, prefix=latest_partition)
    csv_files = [obj for obj in (objects or []) if obj.endswith('.csv')]
    
    if not csv_files:
        return "⚠️ No CSV files found"
    
    print(f"Found {len(csv_files)} CSV files")
    
    all_dfs = []
    for csv_file in csv_files:
        try:
            # Read raw CSV
            csv_obj = s3_hook.get_key(csv_file, bucket_name=bucket)
            csv_content = csv_obj.get()['Body'].read()
            
            # Try different encodings
            for encoding in ['utf-8', 'utf-8-sig', 'latin1']:
                try:
                    df_raw = pd.read_csv(io.BytesIO(csv_content), header=None, encoding=encoding)
                    break
                except UnicodeDecodeError:
                    continue
            else:
                print(f"⚠️ Cannot decode {csv_file}, skipping")
                continue
            
            # Process multi-header if applicable
            try:
                df = process_multiheader_csv(df_raw)
                print(f"✓ Processed multi-header CSV: {csv_file}")
            except:
                # If not multi-header, use first row as header
                df = df_raw
                df.columns = df.iloc[0]
                df = df.iloc[1:].reset_index(drop=True)
                print(f"✓ Processed single-header CSV: {csv_file}")
            
            # Apply column mapping
            new_cols, valid_cols = get_mapped_columns(df.columns)
            
            if not new_cols:
                print(f"⚠️ No mapped columns in {csv_file}, skipping")
                continue
            
            # Keep only mapped columns and rename
            df = df[valid_cols]
            df.columns = new_cols
            
            # Remove duplicate columns (keep first occurrence)
            if df.columns.duplicated().any():
                duplicated_cols = df.columns[df.columns.duplicated()].unique()
                print(f"⚠️ Warning: Duplicate columns detected in {csv_file}: {list(duplicated_cols)}")
                # Keep only first occurrence of each column
                df = df.loc[:, ~df.columns.duplicated(keep='first')]
                print(f"  ✓ Removed duplicates, keeping {len(df.columns)} unique columns")
            
            # Clean data
            df = clean_dataframe(df)
            
            all_dfs.append(df)
            print(f"  ✓ {csv_file}: {len(df)} rows, {len(df.columns)} columns")
            
        except Exception as e:
            print(f"  ✗ {csv_file}: {str(e)}")
            continue
    
    if not all_dfs:
        return "⚠️ No data after processing"
    
    # Combine all dataframes
    df = pd.concat(all_dfs, ignore_index=True)
    print(f"\nTotal: {len(df)} rows")
    
    # Step 3: Additional cleaning
    print("\n[3/4] Final data cleaning...")
    
    df = df.dropna(how='all')
    
    # Filter out rows with null required columns (cp/ticker is NOT NULL in DB)
    if 'cp' in df.columns:
        before_filter = len(df)
        df = df.dropna(subset=['cp'])
        df = df[df['cp'].astype(str).str.strip() != '']
        if len(df) < before_filter:
            print(f"⚠️ Removed {before_filter - len(df)} rows with null/empty cp")
    
    # Ensure ky is integer type to match database schema (INTEGER)
    if 'ky' in df.columns:
        df['ky'] = pd.to_numeric(df['ky'], errors='coerce').astype('Int64')
    
    # Ensure nam is integer type to match database schema (INTEGER)
    if 'nam' in df.columns:
        df['nam'] = pd.to_numeric(df['nam'], errors='coerce').astype('Int64')
    
    # Deduplicate using correct column names
    if 'cp' in df.columns and 'nam' in df.columns and 'ky' in df.columns:
        # Filter out rows with null PK values before dedup
        df = df.dropna(subset=['cp', 'nam', 'ky'])
        before_dedup = len(df)
        df = df.drop_duplicates(subset=['cp', 'nam', 'ky'], keep='last')
        if len(df) < before_dedup:
            print(f"⚠️ Removed {before_dedup - len(df)} duplicate PK rows")
    
    rows_after_cleaning = len(df)
    print(f"After cleaning: {rows_after_cleaning} rows")
    print(f"Columns: {list(df.columns)[:10]}...")  # Show first 10
    
    if df.empty:
        return "⚠️ No data after cleaning"
    
    # Step 4: Upsert to database
    print("\n[4/4] Upserting data to database...")
    
    # Rename DataFrame columns to match DB schema
    # COLUMN_MAPPING uses cp/nam/ky but DB table has ticker/year/quarter
    db_rename = {'cp': 'ticker', 'nam': 'year', 'ky': 'quarter'}
    df = df.rename(columns={k: v for k, v in db_rename.items() if k in df.columns})
    print(f"Renamed columns: {db_rename}")
    
    with closing(get_postgres_connection(db_url)) as conn:
        conn.autocommit = False
        
        try:
            ensure_schema(conn, schema)
            
            with conn.cursor() as cur:
                # Get database columns
                cur.execute(f"""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_schema = '{schema}' 
                      AND table_name = '{table}'
                      AND column_name != 'id'
                    ORDER BY ordinal_position;
                """)
                db_columns = [row[0] for row in cur.fetchall()]
                
                # Match columns
                df_columns = [col for col in df.columns if col in db_columns]
                
                if not df_columns:
                    raise ValueError("No matching columns between CSV and database")
                
                print(f"Matched {len(df_columns)}/{len(df.columns)} columns")
                
                # Prepare rows
                rows = [
                    tuple(row[col] if col in row.index and pd.notna(row[col]) else None 
                          for col in df_columns)
                    for _, row in df.iterrows()
                ]
                
                # DELETE+INSERT pattern using temp table (avoids massive IN clause)
                if 'ticker' in df_columns and 'year' in df_columns and 'quarter' in df_columns:
                    # Create temp table with keys to delete
                    cur.execute("""
                        CREATE TEMP TABLE IF NOT EXISTS _fr_keys_to_delete (
                            ticker VARCHAR(50),
                            year INTEGER,
                            quarter INTEGER
                        ) ON COMMIT DROP;
                    """)
                    cur.execute("TRUNCATE _fr_keys_to_delete;")
                    
                    # Get unique keys from incoming data
                    ti = df_columns.index('ticker')
                    yi = df_columns.index('year')
                    qi = df_columns.index('quarter')
                    keys = list(set(
                        (r[ti], r[yi], r[qi])
                        for r in rows
                        if r[ti] is not None
                    ))
                    
                    if keys:
                        # Bulk insert keys into temp table
                        execute_values(
                            cur,
                            "INSERT INTO _fr_keys_to_delete (ticker, year, quarter) VALUES %s",
                            keys, page_size=1000
                        )
                        print(f"✓ Loaded {len(keys)} key combos into temp table")
                        
                        # Delete using join (efficient for large datasets)
                        cur.execute(f"""
                            DELETE FROM {schema}.{table} t
                            USING _fr_keys_to_delete k
                            WHERE t.ticker = k.ticker 
                              AND t.year = k.year 
                              AND t.quarter = k.quarter;
                        """)
                        deleted = cur.rowcount
                        print(f"✓ Deleted {deleted} existing rows")
                    
                    # Insert all rows
                    columns_str = ', '.join(df_columns)
                    insert_sql = f"""
                        INSERT INTO {schema}.{table} ({columns_str})
                        VALUES %s;
                    """
                    execute_values(cur, insert_sql, rows, page_size=1000)
                    print(f"✓ Inserted {len(rows)} rows")
                else:
                    # Fallback to simple insert if keys are missing
                    columns_str = ', '.join(df_columns)
                    insert_sql = f"""
                        INSERT INTO {schema}.{table} ({columns_str})
                        VALUES %s;
                    """
                    execute_values(cur, insert_sql, rows, page_size=1000)
                    print(f"✓ Inserted {len(rows)} rows")
            
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

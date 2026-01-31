from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.models import Variable

DB_URL = Variable.get(
    "dwh_db_url",
    default_var="postgresql+psycopg2://admin:123456@dwh-postgres:5432/postgres"
)

SCHEMA = Variable.get(
    "dwh_schema",
    default_var="hethong_phantich_chungkhoan"
)

MINIO_BUCKET = Variable.get(
    "minio_bucket",
    default_var="thongtin-congty-va-bctc"
)

MINIO_CONN_ID = "minio_finance"

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="backup_db",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    max_active_runs=1,
    tags=["sync", "minio", "database", "etl", "dwh", "backup", "full"],
    description="Full backup: Process ALL partitions, TRUNCATE first, deduplicate last",
)
def backup_db_full_sync():
    # ========================================================================
    # TASK 1: TRUNCATE ALL TABLES
    # ========================================================================
    
    @task(task_id="truncate_all_tables")
    def task_truncate_all_tables():
        """Truncate all tables in schema before syncing data"""
        from sqlalchemy import create_engine, text
        import logging
        logger = logging.getLogger("airflow.task")
        
        logger.info("=" * 70)
        logger.info(f"🗑️ TRUNCATING ALL TABLES IN SCHEMA: {SCHEMA}")
        logger.info("=" * 70)
        
        engine = create_engine(DB_URL)
        
        truncate_all_sql = f"""
        DO $$
        DECLARE
            r RECORD;
            truncated_count INTEGER := 0;
        BEGIN
            FOR r IN
                SELECT tablename
                FROM pg_tables
                WHERE schemaname = '{SCHEMA}'
                AND tablename NOT IN ('indicator_mapping_4bctc')
            LOOP
                EXECUTE format(
                    'TRUNCATE TABLE %I.%I RESTART IDENTITY CASCADE',
                    '{SCHEMA}',
                    r.tablename
                );
                truncated_count := truncated_count + 1;
            END LOOP;
            RAISE NOTICE 'Total tables truncated: %', truncated_count;
        END $$;
        """
        
        try:
            with engine.begin() as conn:
                conn.execute(text(truncate_all_sql))
                logger.info("✅ Successfully truncated all tables")
            return "success"
        except Exception as e:
            logger.error(f"❌ Error: {str(e)}")
            raise
        finally:
            engine.dispose()
    
    # ========================================================================
    # HELPER: Scan ALL subfolders and partitions
    # ========================================================================
    
    import re
    
    def is_date_partition(folder_name: str) -> bool:
        """
        Check if folder name is a valid date partition.
        Valid formats: yyyy-mm-dd or date=yyyy-mm-dd
        """
        # Pattern for yyyy-mm-dd
        pattern1 = r'^\d{4}-\d{2}-\d{2}/?$'
        # Pattern for date=yyyy-mm-dd
        pattern2 = r'^date=\d{4}-\d{2}-\d{2}/?$'
        
        folder_base = folder_name.rstrip('/').split('/')[-1]
        return bool(re.match(pattern1, folder_base)) or bool(re.match(pattern2, folder_base))
    
    def is_year_partition(folder_name: str) -> bool:
        """Check if folder name is a year partition (year=yyyy)"""
        folder_base = folder_name.rstrip('/').split('/')[-1]
        pattern = r'^year=\d{4}/?$'
        return bool(re.match(pattern, folder_base))
    
    def get_all_partitions_standard(folder_prefix: str):
        """
        CASE 1: Standard partition scanning
        For: sync_daily_price, sync_history_price, sync_financial_ratio, 
             sync_index_price, sync_overview, sync_people
        
        Folder structure: folder_prefix/yyyy-mm-dd/ or folder_prefix/date=yyyy-mm-dd/
        Both formats can co-exist.
        Scans all files inside partition folders.
        """
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        import logging
        logger = logging.getLogger("airflow.task")
        
        s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
        all_partitions = []
        
        # Ensure folder_prefix ends with /
        if not folder_prefix.endswith('/'):
            folder_prefix = folder_prefix + '/'
        
        try:
            # List all objects with common prefixes (subfolders)
            result = s3_hook.get_conn().list_objects_v2(
                Bucket=MINIO_BUCKET,
                Prefix=folder_prefix,
                Delimiter='/'
            )
            
            if 'CommonPrefixes' in result:
                for prefix in result['CommonPrefixes']:
                    subfolder = prefix['Prefix']
                    # Check if this is a date partition folder
                    if is_date_partition(subfolder):
                        all_partitions.append(subfolder)
                        logger.debug(f"Found partition: {subfolder}")
                    
        except Exception as e:
            logger.error(f"Error scanning {folder_prefix}: {str(e)}")
        
        logger.info(f"Found {len(all_partitions)} partitions in {folder_prefix}")
        return all_partitions
    
    def get_all_partitions_two_level(folder_prefix: str):
        """
        CASE 2: Two-level partition scanning
        For: global_index, macro_economy
        
        Folder structure: folder_prefix/topic/yyyy-mm-dd/ or folder_prefix/topic/date=yyyy-mm-dd/
        Level 1: topic folders (e.g., xau, oil, dowjone, usd_vnd, dxy_index, etc.)
        Level 2: date partition folders
        """
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        import logging
        logger = logging.getLogger("airflow.task")
        
        s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
        all_partitions = []
        
        # Ensure folder_prefix ends with /
        if not folder_prefix.endswith('/'):
            folder_prefix = folder_prefix + '/'
        
        try:
            # First, get all topic folders
            result = s3_hook.get_conn().list_objects_v2(
                Bucket=MINIO_BUCKET,
                Prefix=folder_prefix,
                Delimiter='/'
            )
            
            topic_folders = []
            if 'CommonPrefixes' in result:
                for prefix in result['CommonPrefixes']:
                    topic_folders.append(prefix['Prefix'])
            
            logger.info(f"Found {len(topic_folders)} topic folders in {folder_prefix}")
            
            # For each topic folder, get all partition folders
            for topic in topic_folders:
                try:
                    topic_result = s3_hook.get_conn().list_objects_v2(
                        Bucket=MINIO_BUCKET,
                        Prefix=topic,
                        Delimiter='/'
                    )
                    
                    if 'CommonPrefixes' in topic_result:
                        for prefix in topic_result['CommonPrefixes']:
                            subfolder = prefix['Prefix']
                            # Check if this is a date partition folder
                            if is_date_partition(subfolder):
                                all_partitions.append(subfolder)
                                logger.debug(f"Found partition: {subfolder}")
                                
                except Exception as e:
                    logger.error(f"Error scanning topic {topic}: {str(e)}")
                    continue
                    
        except Exception as e:
            logger.error(f"Error scanning {folder_prefix}: {str(e)}")
        
        logger.info(f"Found {len(all_partitions)} partitions in {folder_prefix}")
        return all_partitions
    
    def get_all_partitions_bctc(folder_prefix: str):
        """
        CASE 3: BCTC partition scanning with year subfolders
        For: bctc
        
        Folder structure:
        - folder_prefix/yyyy-mm-dd/  (files directly inside)
        - folder_prefix/date=yyyy-mm-dd/year=yyyy/ (files inside year subfolder)
        
        Both formats can co-exist.
        """
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        import logging
        logger = logging.getLogger("airflow.task")
        
        s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
        all_partitions = []
        
        # Ensure folder_prefix ends with /
        if not folder_prefix.endswith('/'):
            folder_prefix = folder_prefix + '/'
        
        try:
            # List all objects with common prefixes (subfolders)
            result = s3_hook.get_conn().list_objects_v2(
                Bucket=MINIO_BUCKET,
                Prefix=folder_prefix,
                Delimiter='/'
            )
            
            if 'CommonPrefixes' in result:
                for prefix in result['CommonPrefixes']:
                    subfolder = prefix['Prefix']
                    folder_base = subfolder.rstrip('/').split('/')[-1]
                    
                    # Check if this is a date partition folder
                    if is_date_partition(subfolder):
                        # Check if format is date=yyyy-mm-dd (need to scan year= subfolders)
                        if folder_base.startswith('date='):
                            # Scan for year= subfolders inside
                            try:
                                year_result = s3_hook.get_conn().list_objects_v2(
                                    Bucket=MINIO_BUCKET,
                                    Prefix=subfolder,
                                    Delimiter='/'
                                )
                                
                                found_year_folders = False
                                if 'CommonPrefixes' in year_result:
                                    for year_prefix in year_result['CommonPrefixes']:
                                        year_folder = year_prefix['Prefix']
                                        if is_year_partition(year_folder):
                                            all_partitions.append(year_folder)
                                            found_year_folders = True
                                            logger.debug(f"Found BCTC year partition: {year_folder}")
                                
                                # If no year= subfolders found, add the date folder itself
                                if not found_year_folders:
                                    all_partitions.append(subfolder)
                                    logger.debug(f"Found BCTC date partition (no year subfolders): {subfolder}")
                                    
                            except Exception as e:
                                logger.error(f"Error scanning year folders in {subfolder}: {str(e)}")
                                # Fallback: add the date folder itself
                                all_partitions.append(subfolder)
                        else:
                            # Format is yyyy-mm-dd (files directly inside)
                            all_partitions.append(subfolder)
                            logger.debug(f"Found BCTC partition: {subfolder}")
                    
        except Exception as e:
            logger.error(f"Error scanning {folder_prefix}: {str(e)}")
        
        logger.info(f"Found {len(all_partitions)} BCTC partitions in {folder_prefix}")
        return all_partitions
    
    # ========================================================================
    # SYNC TASKS - Process ALL partitions
    # ========================================================================
    
    @task(task_id="sync_bctc")
    def task_sync_bctc():
        """Sync ALL BCTC partitions with indicator mapping"""
        from lake_to_dwh.utils import read_all_csvs_from_folder, get_postgres_connection
        from lake_to_dwh.backup_sync import process_and_clean_partition
        from lake_to_dwh.sync_bctc import apply_indicator_mapping
        from psycopg2.extras import execute_values
        from contextlib import closing
        import pandas as pd
        import logging
        logger = logging.getLogger("airflow.task")
        
        logger.info("Processing ALL BCTC partitions")
        partitions = get_all_partitions_bctc("bctc/")
        
        if not partitions:
            return "⚠️ BCTC: No partitions found"
        
        logger.info(f"Found {len(partitions)} BCTC partitions")
        total_rows = 0
        
        for partition in partitions:
            try:
                df = read_all_csvs_from_folder(MINIO_BUCKET, partition, MINIO_CONN_ID)
                if df.empty:
                    continue
                
                required_cols = ['ticker', 'quarter', 'year', 'ind_name', 'value']
                df = process_and_clean_partition(df, required_columns=required_cols)
                if df.empty:
                    continue
                
                df['year'] = pd.to_numeric(df['year'], errors='coerce').astype('Int64')
                df['quarter'] = df['quarter'].astype(str).str.strip()
                df['value'] = pd.to_numeric(df['value'], errors='coerce')
                df = df.dropna(subset=['year', 'quarter'])
                
                df = apply_indicator_mapping(df)
                
                if 'ind_code' not in df.columns:
                    df['ind_code'] = df['ind_name'].str.upper().str.replace(' ', '_').str.replace('/', '_')
                df['ind_code'] = df['ind_code'].astype(str).str[:50]
                
                final_cols = ['ticker', 'quarter', 'year', 'ind_name', 'ind_code', 'value', 'report_name', 'report_code']
                df = df[[col for col in final_cols if col in df.columns]].copy()
                
                if df.empty:
                    continue
                
                with closing(get_postgres_connection(DB_URL)) as conn:
                    conn.autocommit = False
                    try:
                        rows = [
                            (row.get('ticker'), row.get('quarter'), row.get('year'), row.get('ind_name'),
                             row.get('ind_code'), row.get('value'),
                             row.get('report_name') if pd.notna(row.get('report_name')) else None,
                             row.get('report_code') if pd.notna(row.get('report_code')) else None)
                            for _, row in df.iterrows()
                        ]
                        
                        delete_keys = set()
                        for row in rows:
                            ticker, quarter, year, ind_code = row[0], str(row[1]) if pd.notna(row[1]) else None, int(row[2]) if pd.notna(row[2]) else None, row[4]
                            if ticker and quarter and year and ind_code:
                                delete_keys.add((ticker, quarter, year, ind_code))
                        
                        with conn.cursor() as cur:
                            if delete_keys:
                                cur.execute(f"""
                                    CREATE TEMP TABLE IF NOT EXISTS temp_bctc_delete_keys (
                                        ticker VARCHAR(10), quarter VARCHAR(10), year INTEGER, ind_code VARCHAR(50)
                                    ) ON COMMIT DROP;
                                """)
                                execute_values(cur, "INSERT INTO temp_bctc_delete_keys VALUES %s", list(delete_keys))
                                cur.execute(f"""
                                    DELETE FROM {SCHEMA}.bctc t USING temp_bctc_delete_keys tmp
                                    WHERE t.ticker = tmp.ticker AND t.quarter = tmp.quarter 
                                      AND t.year = tmp.year AND t.ind_code = tmp.ind_code;
                                """)
                            
                            execute_values(cur, f"""
                                INSERT INTO {SCHEMA}.bctc (ticker, quarter, year, ind_name, ind_code, value, report_name, report_code)
                                VALUES %s;
                            """, rows, page_size=1000)
                        
                        conn.commit()
                        total_rows += len(rows)
                        logger.info(f"Processed {partition}: {len(rows)} rows")
                    except Exception as e:
                        conn.rollback()
                        logger.error(f"Error processing {partition}: {str(e)}")
            except Exception as e:
                logger.error(f"Error reading {partition}: {str(e)}")
                continue
        
        return f"✅ BCTC: {total_rows:,} rows from {len(partitions)} partitions"
    
    @task(task_id="sync_daily_price")
    def task_sync_daily_price():
        """Sync ALL daily price partitions with memory optimization"""
        from lake_to_dwh.utils import read_all_csvs_from_folder, get_postgres_connection
        from psycopg2.extras import execute_values
        from contextlib import closing
        import pandas as pd
        import logging
        import gc
        logger = logging.getLogger("airflow.task")
        
        logger.info("Processing ALL daily_price partitions")
        partitions = get_all_partitions_standard("daily_price/")
        
        if not partitions:
            return "⚠️ Daily Price: No partitions found"
        
        total_partitions = len(partitions)
        logger.info(f"Found {total_partitions} daily_price partitions")
        total_rows = 0
        processed_partitions = 0
        BATCH_SIZE = 10000
        
        for idx, partition in enumerate(partitions, 1):
            try:
                if idx % 10 == 0 or idx == 1:
                    logger.info(f"📊 Progress: {idx}/{total_partitions} ({idx*100//total_partitions}%), {total_rows:,} rows")
                
                df = read_all_csvs_from_folder(MINIO_BUCKET, partition, MINIO_CONN_ID)
                if df.empty:
                    continue
                
                if 'symbol' in df.columns:
                    df.rename(columns={'symbol': 'ticker'}, inplace=True)
                if 'date' in df.columns and 'trading_date' not in df.columns:
                    df.rename(columns={'date': 'trading_date'}, inplace=True)
                if 'time' in df.columns and 'trading_date' not in df.columns:
                    df.rename(columns={'time': 'trading_date'}, inplace=True)
                
                df['trading_date'] = pd.to_datetime(df['trading_date'], errors='coerce').dt.date.astype(str)
                df = df.dropna(subset=['ticker', 'trading_date'])
                
                available_cols = ['ticker', 'trading_date', 'open', 'high', 'low', 'close', 'volume']
                df = df[[col for col in available_cols if col in df.columns]].copy()
                df = df.drop_duplicates(subset=['ticker', 'trading_date'])
                
                if df.empty:
                    del df
                    gc.collect()
                    continue
                
                with closing(get_postgres_connection(DB_URL)) as conn:
                    conn.autocommit = False
                    try:
                        rows = [tuple(row[col] if col in row.index else None for col in available_cols) for _, row in df.iterrows()]
                        
                        for batch_start in range(0, len(rows), BATCH_SIZE):
                            batch_rows = rows[batch_start:batch_start + BATCH_SIZE]
                            tickers = [str(row[0]) for row in batch_rows]
                            dates = [str(row[1]) for row in batch_rows]
                            
                            with conn.cursor() as cur:
                                cur.execute(f"""
                                    DELETE FROM {SCHEMA}.history_price
                                    WHERE (ticker, trading_date) IN (
                                        SELECT DISTINCT ticker, trading_date 
                                        FROM unnest(%s::text[], %s::text[]) AS t(ticker, trading_date)
                                    );
                                """, (tickers, dates))
                                
                                execute_values(cur, f"""
                                    INSERT INTO {SCHEMA}.history_price ({', '.join(available_cols)})
                                    VALUES %s;
                                """, batch_rows, page_size=1000)
                        
                        conn.commit()
                        total_rows += len(rows)
                        processed_partitions += 1
                        logger.info(f"Processed {partition}: {len(rows)} rows")
                    except Exception as e:
                        conn.rollback()
                        logger.error(f"Error processing {partition}: {str(e)}")
                
                del df, rows
                gc.collect()
                
            except Exception as e:
                logger.error(f"Error reading {partition}: {str(e)}")
                gc.collect()
                continue
        
        return f"✅ Daily Price: {total_rows:,} rows from {processed_partitions} partitions"
    
    @task(task_id="sync_history_price")
    def task_sync_history_price():
        """Sync ALL history price partitions (backfill) with memory optimization"""
        from lake_to_dwh.utils import read_all_csvs_from_folder, get_postgres_connection
        from psycopg2.extras import execute_values
        from contextlib import closing
        import pandas as pd
        import logging
        import gc
        logger = logging.getLogger("airflow.task")
        
        logger.info("Processing ALL history_price partitions")
        partitions = get_all_partitions_standard("history_price/")
        
        if not partitions:
            return "⚠️ History Price: No partitions found"
        
        total_partitions = len(partitions)
        logger.info(f"Found {total_partitions} history_price partitions")
        total_rows = 0
        processed_partitions = 0
        
        # Process in batches to avoid memory issues
        BATCH_SIZE = 10000  # Rows per batch insert
        
        for idx, partition in enumerate(partitions, 1):
            try:
                # Log progress every 10 partitions
                if idx % 10 == 0 or idx == 1:
                    logger.info(f"📊 Progress: {idx}/{total_partitions} partitions ({idx*100//total_partitions}%), {total_rows:,} total rows")
                
                df = read_all_csvs_from_folder(MINIO_BUCKET, partition, MINIO_CONN_ID)
                if df.empty:
                    continue
                
                if 'symbol' in df.columns:
                    df.rename(columns={'symbol': 'ticker'}, inplace=True)
                if 'date' in df.columns and 'trading_date' not in df.columns:
                    df.rename(columns={'date': 'trading_date'}, inplace=True)
                if 'time' in df.columns and 'trading_date' not in df.columns:
                    df.rename(columns={'time': 'trading_date'}, inplace=True)
                
                df['trading_date'] = pd.to_datetime(df['trading_date'], errors='coerce').dt.date.astype(str)
                df = df.dropna(subset=['ticker', 'trading_date'])
                
                available_cols = ['ticker', 'trading_date', 'open', 'high', 'low', 'close', 'volume']
                df = df[[col for col in available_cols if col in df.columns]].copy()
                df = df.drop_duplicates(subset=['ticker', 'trading_date'])
                
                if df.empty:
                    del df
                    gc.collect()
                    continue
                
                with closing(get_postgres_connection(DB_URL)) as conn:
                    conn.autocommit = False
                    try:
                        rows = [tuple(row[col] if col in row.index else None for col in available_cols) for _, row in df.iterrows()]
                        
                        # Process in batches
                        for batch_start in range(0, len(rows), BATCH_SIZE):
                            batch_rows = rows[batch_start:batch_start + BATCH_SIZE]
                            tickers = [str(row[0]) for row in batch_rows]
                            dates = [str(row[1]) for row in batch_rows]
                            
                            with conn.cursor() as cur:
                                cur.execute(f"""
                                    DELETE FROM {SCHEMA}.history_price
                                    WHERE (ticker, trading_date) IN (
                                        SELECT DISTINCT ticker, trading_date 
                                        FROM unnest(%s::text[], %s::text[]) AS t(ticker, trading_date)
                                    );
                                """, (tickers, dates))
                                
                                execute_values(cur, f"""
                                    INSERT INTO {SCHEMA}.history_price ({', '.join(available_cols)})
                                    VALUES %s;
                                """, batch_rows, page_size=1000)
                        
                        conn.commit()
                        total_rows += len(rows)
                        processed_partitions += 1
                        logger.info(f"Processed {partition}: {len(rows)} rows")
                        
                    except Exception as e:
                        conn.rollback()
                        logger.error(f"Error processing {partition}: {str(e)}")
                
                # Cleanup memory after each partition
                del df, rows
                gc.collect()
                
            except Exception as e:
                logger.error(f"Error reading {partition}: {str(e)}")
                gc.collect()
                continue
        
        logger.info(f"✅ Completed: {processed_partitions}/{total_partitions} partitions, {total_rows:,} rows")
        return f"✅ History Price (Backfill): {total_rows:,} rows from {processed_partitions} partitions"
    
    @task(task_id="sync_financial_ratio")
    def task_sync_financial_ratio():
        """Sync ALL financial ratio partitions with comprehensive column mapping"""
        from lake_to_dwh.utils import read_all_csvs_from_folder, get_postgres_connection
        from psycopg2.extras import execute_values
        from contextlib import closing
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        import pandas as pd
        import numpy as np
        import re, unicodedata
        import io
        import logging
        logger = logging.getLogger("airflow.task")
        
        # =========================
        # COLUMN MAPPING (from chiso_taichinh.py)
        # =========================
        COLUMN_MAPPING = {
            # --- META DATA ---
            "ticker": "ticker",
            "ind_code": "ticker",
            "Meta_CP": "ticker",
            "Meta_Năm": "year",
            "Meta_Kỳ": "quarter",
            "period_type": "period_type",
            "extracted_at": "extracted_at",

            # --- CƠ CẤU NGUỒN VỐN ---
            "Chỉ tiêu cơ cấu nguồn vốn_Nợ/VCSH": "debt_to_equity",
            "Chỉ tiêu cơ cấu nguồn vốn_TSCĐ / Vốn CSH": "fixed_asset_to_equity",
            "Chỉ tiêu cơ cấu nguồn vốn_Vốn CSH/Vốn điều lệ": "equity_to_charter_capital",
            "Chỉ tiêu cơ cấu nguồn vốn_(Vay NH+DH)/VCSH": "long_short_term_debt_on_equity",

            # --- HIỆU QUẢ HOẠT ĐỘNG ---
            "Chỉ tiêu hiệu quả hoạt động_Vòng quay tài sản": "asset_turnover",
            "Chỉ tiêu hiệu quả hoạt động_Vòng quay TSCĐ": "fixed_asset_turnover",
            "Chỉ tiêu hiệu quả hoạt động_Số ngày thu tiền bình quân": "receivable_days",
            "Chỉ tiêu hiệu quả hoạt động_Số ngày tồn kho bình quân": "inventory_days",
            "Chỉ tiêu hiệu quả hoạt động_Số ngày thanh toán bình quân": "payable_days",
            "Chỉ tiêu hiệu quả hoạt động_Chu kỳ tiền": "cash_conversion_cycle",
            "Chỉ tiêu hiệu quả hoạt động_Vòng quay hàng tồn kho": "inventory_turnover",

            # --- KHẢ NĂNG SINH LỜI ---
            "Chỉ tiêu khả năng sinh lợi_Biên EBIT (%)": "ebit_margin",
            "Chỉ tiêu khả năng sinh lợi_Biên lợi nhuận gộp (%)": "gross_margin",
            "Chỉ tiêu khả năng sinh lợi_Biên lợi nhuận ròng (%)": "net_margin",
            "Chỉ tiêu khả năng sinh lợi_ROE (%)": "roe",
            "Chỉ tiêu khả năng sinh lợi_ROIC (%)": "roic",
            "Chỉ tiêu khả năng sinh lợi_ROA (%)": "roa",
            "Chỉ tiêu khả năng sinh lợi_EBITDA (Tỷ đồng)": "ebitda_value",
            "Chỉ tiêu khả năng sinh lợi_EBIT (Tỷ đồng)": "ebit_value",
            "Chỉ tiêu khả năng sinh lợi_Tỷ suất cổ tức (%)": "dividend_yield",

            # --- THANH KHOẢN & ĐÒN BẨY ---
            "Chỉ tiêu thanh khoản_Chỉ số thanh toán hiện thời": "current_ratio",
            "Chỉ tiêu thanh khoản_Chỉ số thanh toán tiền mặt": "cash_ratio",
            "Chỉ tiêu thanh khoản_Chỉ số thanh toán nhanh": "quick_ratio",
            "Chỉ tiêu thanh khoản_Đòn bẩy tài chính": "financial_leverage",
            "Chỉ tiêu thanh khoản_Khả năng chi trả lãi vay": "interest_coverage_ratio",

            # --- ĐỊNH GIÁ ---
            "Chỉ tiêu định giá_Vốn hóa (Tỷ đồng)": "market_cap",
            "Chỉ tiêu định giá_Số CP lưu hành (Triệu CP)": "outstanding_shares",
            "Chỉ tiêu định giá_P/E": "pe",
            "Chỉ tiêu định giá_P/B": "pb",
            "Chỉ tiêu định giá_P/S": "ps",
            "Chỉ tiêu định giá_P/Cash Flow": "p_cashflow",
            "Chỉ tiêu định giá_EPS (VND)": "eps",
            "Chỉ tiêu định giá_BVPS (VND)": "bvps",
            "Chỉ tiêu định giá_EV/EBITDA": "ev_ebitda"
        }
        
        def clean_header_key(text):
            """Làm sạch key để so sánh mapping dễ hơn"""
            if not isinstance(text, str):
                return str(text)
            text = unicodedata.normalize("NFKD", text)
            text = text.encode("ascii", "ignore").decode("ascii")
            return text.lower().strip().replace(" ", "_").replace("/", "_")
        
        def get_mapped_columns(raw_columns):
            """Mapping cột từ CSV sang DB"""
            new_columns = []
            keep_columns = []
            normalized_mapping = {clean_header_key(k): v for k, v in COLUMN_MAPPING.items()}
            
            for col in raw_columns:
                clean_col = clean_header_key(col)
                if col in COLUMN_MAPPING:
                    new_columns.append(COLUMN_MAPPING[col])
                    keep_columns.append(col)
                elif clean_col in normalized_mapping:
                    new_columns.append(normalized_mapping[clean_col])
                    keep_columns.append(col)
                else:
                    logger.warning(f"⚠️ Bỏ qua cột không map được: {col}")
            
            return new_columns, keep_columns
        
        def process_multiheader_csv(df_raw):
            """Xử lý CSV header đa cấp"""
            if df_raw.shape[0] < 3:
                raise ValueError("CSV không đủ dữ liệu")
            
            row_0 = df_raw.iloc[0].astype(str).replace('nan', '').str.strip()
            row_1 = df_raw.iloc[1].astype(str).replace('nan', '').str.strip()
            
            # Forward fill row 0
            last_valid = ""
            filled_row_0 = []
            for val in row_0:
                if val:
                    last_valid = val
                filled_row_0.append(last_valid)
            
            # Combine headers
            combined_header = []
            for h1, h2 in zip(filled_row_0, row_1):
                if h1 and h2:
                    combined_header.append(f"{h1}_{h2}")
                elif h2:
                    combined_header.append(h2)
                else:
                    combined_header.append(h1)
            
            df_raw.columns = combined_header
            return df_raw.iloc[2:].reset_index(drop=True)
        
        def clean_dataframe(df):
            """Clean dataframe values"""
            df = df.replace(['', ' ', 'N/A', 'NA', 'null', 'NULL', '-', '#DIV/0!'], np.nan)
            
            for col in df.columns:
                if df[col].dtype == object:
                    try:
                        df[col] = df[col].str.replace(',', '', regex=False)
                    except:
                        pass
            
            for col in df.columns:
                if col not in ['ticker']:
                    try:
                        df[col] = pd.to_numeric(df[col], errors='ignore')
                    except:
                        pass
            
            return df
        
        logger.info("Processing ALL financial_ratios partitions")
        partitions = get_all_partitions_standard("financial_ratios/")
        
        if not partitions:
            return "⚠️ Financial Ratio: No partitions found"
        
        logger.info(f"Found {len(partitions)} financial_ratios partitions")
        total_rows = 0
        s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
        
        for partition in partitions:
            try:
                # List CSV files in partition
                objects = s3_hook.list_keys(bucket_name=MINIO_BUCKET, prefix=partition)
                csv_files = [obj for obj in (objects or []) if obj.endswith('.csv')]
                
                if not csv_files:
                    logger.info(f"No CSV files in {partition}")
                    continue
                
                all_dfs = []
                for csv_file in csv_files:
                    try:
                        # Read raw CSV
                        csv_obj = s3_hook.get_key(csv_file, bucket_name=MINIO_BUCKET)
                        csv_content = csv_obj.get()['Body'].read()
                        
                        # Try different encodings
                        for encoding in ['utf-8', 'utf-8-sig', 'latin1']:
                            try:
                                df_raw = pd.read_csv(io.BytesIO(csv_content), header=None, encoding=encoding)
                                break
                            except UnicodeDecodeError:
                                continue
                        else:
                            logger.warning(f"Cannot decode {csv_file}")
                            continue
                        
                        # Process multi-header if applicable
                        try:
                            df = process_multiheader_csv(df_raw)
                        except:
                            df = df_raw
                            df.columns = df.iloc[0]
                            df = df.iloc[1:].reset_index(drop=True)
                        
                        # Apply column mapping
                        new_cols, valid_cols = get_mapped_columns(df.columns)
                        
                        if not new_cols:
                            continue
                        
                        df = df[valid_cols]
                        df.columns = new_cols
                        df = clean_dataframe(df)
                        all_dfs.append(df)
                        
                    except Exception as e:
                        logger.error(f"Error processing {csv_file}: {str(e)}")
                        continue
                
                if not all_dfs:
                    continue
                
                df = pd.concat(all_dfs, ignore_index=True)
                df = df.dropna(how='all')
                
                if 'ticker' in df.columns and 'year' in df.columns and 'quarter' in df.columns:
                    df = df.drop_duplicates(subset=['ticker', 'year', 'quarter'], keep='last')
                
                if df.empty:
                    continue
                
                # Insert to database
                with closing(get_postgres_connection(DB_URL)) as conn:
                    conn.autocommit = False
                    try:
                        with conn.cursor() as cur:
                            cur.execute(f"""
                                SELECT column_name FROM information_schema.columns 
                                WHERE table_schema = '{SCHEMA}' AND table_name = 'financial_ratio'
                                  AND column_name != 'id' ORDER BY ordinal_position;
                            """)
                            db_columns = [row[0] for row in cur.fetchall()]
                            df_columns = [col for col in df.columns if col in db_columns]
                            
                            if not df_columns:
                                continue
                            
                            rows = [
                                tuple(row[col] if col in row.index and pd.notna(row[col]) else None 
                                      for col in df_columns)
                                for _, row in df.iterrows()
                            ]
                            
                            # Delete existing by (ticker, year, quarter)
                            if 'ticker' in df_columns and 'year' in df_columns and 'quarter' in df_columns:
                                delete_keys = set()
                                ticker_idx = df_columns.index('ticker')
                                year_idx = df_columns.index('year')
                                quarter_idx = df_columns.index('quarter')
                                
                                for row in rows:
                                    if row[ticker_idx] and row[year_idx] and row[quarter_idx]:
                                        delete_keys.add((row[ticker_idx], row[year_idx], row[quarter_idx]))
                                
                                if delete_keys:
                                    cur.execute(f"""
                                        CREATE TEMP TABLE temp_fr_delete_keys (
                                            ticker VARCHAR(20), year INTEGER, quarter VARCHAR(10)
                                        ) ON COMMIT DROP;
                                    """)
                                    execute_values(cur, "INSERT INTO temp_fr_delete_keys VALUES %s", list(delete_keys))
                                    cur.execute(f"""
                                        DELETE FROM {SCHEMA}.financial_ratio t 
                                        USING temp_fr_delete_keys tmp
                                        WHERE t.ticker = tmp.ticker 
                                          AND t.year = tmp.year 
                                          AND t.quarter = tmp.quarter;
                                    """)
                            
                            # Insert new data
                            execute_values(cur, f"""
                                INSERT INTO {SCHEMA}.financial_ratio ({', '.join(df_columns)})
                                VALUES %s;
                            """, rows, page_size=1000)
                        
                        conn.commit()
                        total_rows += len(rows)
                        logger.info(f"Processed {partition}: {len(rows)} rows")
                    except Exception as e:
                        conn.rollback()
                        logger.error(f"Error processing {partition}: {str(e)}")
            except Exception as e:
                logger.error(f"Error reading {partition}: {str(e)}")
                continue
        
        return f"✅ Financial Ratio: {total_rows:,} rows from {len(partitions)} partitions"

    
    @task(task_id="sync_macro_economy")
    def task_sync_macro_economy():
        """Sync ALL macro economy partitions from ALL subfolders (xau, oil, dowjone)"""
        from lake_to_dwh.utils import read_all_csvs_from_folder, get_postgres_connection
        from psycopg2.extras import execute_values
        from contextlib import closing
        import pandas as pd
        import logging
        logger = logging.getLogger("airflow.task")
        
        logger.info("Processing ALL macro_economy partitions")
        
        # Get all partitions from two-level folders (xau/, oil/, dowjone/)
        partitions = get_all_partitions_two_level("macro_economy/")
        
        if not partitions:
            return "⚠️ Macro Economy: No partitions found"
        
        logger.info(f"Found {len(partitions)} macro_economy partitions")
        
        # Asset type mapping
        asset_type_mapping = {
            'xau': 'XAU',
            'oil': 'OIL',
            'dowjone': 'DJI',
            'dowjones': 'DJI',
        }
        
        total_rows = 0
        
        # Process each partition
        for partition in partitions:
            try:
                # Read data
                df = read_all_csvs_from_folder(MINIO_BUCKET, partition, MINIO_CONN_ID)
                
                if df.empty:
                    continue
                
                # Extract asset_type from partition path
                partition_lower = partition.lower()
                asset_type = None
                for folder_name, asset_code in asset_type_mapping.items():
                    if f'/{folder_name}/' in partition_lower:
                        asset_type = asset_code
                        break
                
                if not asset_type:
                    logger.warning(f"Cannot determine asset_type for {partition}")
                    continue
                
                # Add asset_type column
                df['asset_type'] = asset_type
                
                # Handle column variations
                if 'time' in df.columns and 'date' not in df.columns:
                    df.rename(columns={'time': 'date'}, inplace=True)
                
                # Clean data
                df['date'] = pd.to_datetime(df['date'], errors='coerce').dt.date
                df = df.dropna(subset=['date'])
                
                available_cols = ['date', 'open', 'high', 'low', 'close', 'volume', 'asset_type']
                df = df[[col for col in available_cols if col in df.columns]].copy()
                
                df = df.drop_duplicates(subset=['date', 'asset_type'])
                
                if df.empty:
                    continue
                
                # Insert to database
                with closing(get_postgres_connection(DB_URL)) as conn:
                    conn.autocommit = False
                    try:
                        rows = [
                            (row['date'], row.get('open'), row.get('high'), row.get('low'),
                             row.get('close'), row.get('volume'), row['asset_type'])
                            for _, row in df.iterrows()
                        ]
                        
                        dates = [row[0] for row in rows]
                        asset_types = [row[6] for row in rows]
                        
                        with conn.cursor() as cur:
                            # Delete existing
                            delete_sql = f"""
                                DELETE FROM {SCHEMA}.macro_economy
                                WHERE (date, asset_type) IN (
                                    SELECT DISTINCT unnest(%s::date[]), unnest(%s::varchar[])
                                );
                            """
                            cur.execute(delete_sql, (dates, asset_types))
                            
                            # Insert new
                            insert_sql = f"""
                                INSERT INTO {SCHEMA}.macro_economy
                                (date, open, high, low, close, volume, asset_type)
                                VALUES %s;
                            """
                            execute_values(cur, insert_sql, rows)
                        
                        conn.commit()
                        total_rows += len(rows)
                        logger.info(f"Processed {partition}: {len(rows)} rows")
                        
                    except Exception as e:
                        conn.rollback()
                        logger.error(f"Error processing {partition}: {str(e)}")
                        
            except Exception as e:
                logger.error(f"Error reading {partition}: {str(e)}")
                continue
        
        return f"✅ Macro Economy: {total_rows:,} rows from {len(partitions)} partitions"
    
    @task(task_id="sync_global_index")
    def task_sync_global_index():
        """Sync ALL global index partitions from ALL subfolders (usd_vnd, dxy_index, etc.)"""
        from lake_to_dwh.utils import read_all_csvs_from_folder, get_postgres_connection
        from psycopg2.extras import execute_values
        from contextlib import closing
        import pandas as pd
        import logging
        logger = logging.getLogger("airflow.task")
        
        logger.info("Processing ALL global_index partitions")
        
        # Get all partitions recursively
        partitions = get_all_partitions_two_level("global_index/")
        
        if not partitions:
            return "⚠️ Global Index: No partitions found"
        
        logger.info(f"Found {len(partitions)} global_index partitions")
        
        # Asset type mapping
        asset_type_mapping = {
            'usd_vnd': 'USD_VND',
            'dxy_index': 'DXY',
            'usd_cny': 'USD_CNY',
            'eur_usd': 'EUR_USD',
            'us_bond_10y': 'US_BOND_10Y',
        }
        
        total_rows = 0
        
        # Process each partition
        for partition in partitions:
            try:
                # Read data
                df = read_all_csvs_from_folder(MINIO_BUCKET, partition, MINIO_CONN_ID)
                
                if df.empty:
                    continue
                
                # Extract asset_type from partition path
                partition_lower = partition.lower()
                asset_type = None
                for folder_name, asset_code in asset_type_mapping.items():
                    if f'/{folder_name}/' in partition_lower:
                        asset_type = asset_code
                        break
                
                if not asset_type:
                    logger.warning(f"Cannot determine asset_type for {partition}")
                    continue
                
                # Add asset_type column
                df['asset_type'] = asset_type
                
                # Handle column variations
                if 'time' in df.columns and 'date' not in df.columns:
                    df.rename(columns={'time': 'date'}, inplace=True)
                
                # Clean data
                df['date'] = pd.to_datetime(df['date'], errors='coerce').dt.date
                df = df.dropna(subset=['date'])
                
                available_cols = ['date', 'open', 'high', 'low', 'close', 'volume', 'asset_type']
                df = df[[col for col in available_cols if col in df.columns]].copy()
                
                df = df.drop_duplicates(subset=['date', 'asset_type'])
                
                if df.empty:
                    continue
                
                # Insert to database (same table as macro_economy)
                with closing(get_postgres_connection(DB_URL)) as conn:
                    conn.autocommit = False
                    try:
                        rows = [
                            (row['date'], row.get('open'), row.get('high'), row.get('low'),
                             row.get('close'), row.get('volume'), row['asset_type'])
                            for _, row in df.iterrows()
                        ]
                        
                        dates = [row[0] for row in rows]
                        asset_types = [row[6] for row in rows]
                        
                        with conn.cursor() as cur:
                            # Delete existing
                            delete_sql = f"""
                                DELETE FROM {SCHEMA}.macro_economy
                                WHERE (date, asset_type) IN (
                                    SELECT DISTINCT unnest(%s::date[]), unnest(%s::varchar[])
                                );
                            """
                            cur.execute(delete_sql, (dates, asset_types))
                            
                            # Insert new
                            insert_sql = f"""
                                INSERT INTO {SCHEMA}.macro_economy
                                (date, open, high, low, close, volume, asset_type)
                                VALUES %s;
                            """
                            execute_values(cur, insert_sql, rows)
                        
                        conn.commit()
                        total_rows += len(rows)
                        logger.info(f"Processed {partition}: {len(rows)} rows")
                        
                    except Exception as e:
                        conn.rollback()
                        logger.error(f"Error processing {partition}: {str(e)}")
                        
            except Exception as e:
                logger.error(f"Error reading {partition}: {str(e)}")
                continue
        
        return f"✅ Global Index: {total_rows:,} rows from {len(partitions)} partitions"
    
    @task(task_id="sync_index_price")
    def task_sync_index_price():
        """Sync ALL index price partitions"""
        from lake_to_dwh.utils import read_all_csvs_from_folder, get_postgres_connection
        from psycopg2.extras import execute_values
        from contextlib import closing
        import pandas as pd
        import logging
        logger = logging.getLogger("airflow.task")
        
        logger.info("Processing ALL index_price partitions")
        partitions = get_all_partitions_standard("index_price/")
        
        if not partitions:
            return "⚠️ Market Index: No partitions found"
        
        logger.info(f"Found {len(partitions)} index_price partitions")
        total_rows = 0
        
        for partition in partitions:
            try:
                df = read_all_csvs_from_folder(MINIO_BUCKET, partition, MINIO_CONN_ID)
                if df.empty:
                    continue
                
                if 'index_code' in df.columns:
                    df.rename(columns={'index_code': 'ticker'}, inplace=True)
                if 'date' in df.columns and 'trading_date' not in df.columns:
                    df.rename(columns={'date': 'trading_date'}, inplace=True)
                if 'time' in df.columns and 'trading_date' not in df.columns:
                    df.rename(columns={'time': 'trading_date'}, inplace=True)
                
                df['trading_date'] = pd.to_datetime(df['trading_date'], errors='coerce').dt.date.astype(str)
                df = df.dropna(subset=['ticker', 'trading_date'])
                
                available_cols = ['ticker', 'trading_date', 'open', 'high', 'low', 'close', 'volume']
                df = df[[col for col in available_cols if col in df.columns]].copy()
                df = df.drop_duplicates(subset=['ticker', 'trading_date'])
                
                if df.empty:
                    continue
                
                with closing(get_postgres_connection(DB_URL)) as conn:
                    conn.autocommit = False
                    try:
                        rows = [tuple(row[col] if col in row.index else None for col in available_cols) for _, row in df.iterrows()]
                        tickers = [str(row[0]) for row in rows]
                        dates = [str(row[1]) for row in rows]
                        
                        with conn.cursor() as cur:
                            cur.execute(f"""
                                DELETE FROM {SCHEMA}.market_index
                                WHERE (ticker, trading_date) IN (
                                    SELECT DISTINCT ticker, trading_date 
                                    FROM unnest(%s::text[], %s::text[]) AS t(ticker, trading_date)
                                );
                            """, (tickers, dates))
                            
                            execute_values(cur, f"""
                                INSERT INTO {SCHEMA}.market_index ({', '.join(available_cols)})
                                VALUES %s;
                            """, rows)
                        
                        conn.commit()
                        total_rows += len(rows)
                        logger.info(f"Processed {partition}: {len(rows)} rows")
                    except Exception as e:
                        conn.rollback()
                        logger.error(f"Error processing {partition}: {str(e)}")
            except Exception as e:
                logger.error(f"Error reading {partition}: {str(e)}")
                continue
        
        return f"✅ Market Index: {total_rows:,} rows from {len(partitions)} partitions"
    
    @task(task_id="sync_news")
    def task_sync_news():
        """Sync ALL news partitions from ALL subfolders (daily, site)"""
        from lake_to_dwh.utils import read_all_csvs_from_folder, get_postgres_connection
        from psycopg2.extras import execute_values
        from contextlib import closing
        import pandas as pd
        import hashlib
        import logging
        logger = logging.getLogger("airflow.task")
        
        logger.info("Processing ALL news partitions")
        
        # Get all partitions from two-level folders (daily/, site/)
        partitions = get_all_partitions_two_level("news/")
        
        if not partitions:
            return "⚠️ News: No partitions found"
        
        logger.info(f"Found {len(partitions)} news partitions")
        
        total_rows = 0
        
        # Process each partition
        for partition in partitions:
            try:
                # Read data
                df = read_all_csvs_from_folder(MINIO_BUCKET, partition, MINIO_CONN_ID)
                
                if df.empty:
                    continue
                
                # Column mappings
                if 'related_ticker' in df.columns:
                    df.rename(columns={'related_ticker': 'ticker'}, inplace=True)
                if 'symbol' in df.columns:
                    df.rename(columns={'symbol': 'ticker'}, inplace=True)
                if 'news_source_link' in df.columns:
                    df.rename(columns={'news_source_link': 'source_link'}, inplace=True)
                if 'friendly_sub_title' in df.columns:
                    df.rename(columns={'friendly_sub_title': 'sub_title'}, inplace=True)
                if 'news_short_content' in df.columns:
                    df.rename(columns={'news_short_content': 'short_content'}, inplace=True)
                if 'news_full_content' in df.columns:
                    df.rename(columns={'news_full_content': 'full_content'}, inplace=True)
                
                # Generate news_id from title if not exists
                if 'news_id' not in df.columns and 'title' in df.columns:
                    df['news_id'] = df['title'].apply(
                        lambda x: hashlib.md5(str(x).encode()).hexdigest()[:50] if pd.notna(x) else None
                    )
                
                # Clean data
                df = df.dropna(subset=['news_id', 'title'])
                
                # Handle public_date
                if 'public_date' in df.columns:
                    df['public_date'] = pd.to_datetime(df['public_date'], errors='coerce')
                
                df = df.drop_duplicates(subset=['news_id'], keep='last')
                
                if df.empty:
                    continue
                
                # Insert to database
                with closing(get_postgres_connection(DB_URL)) as conn:
                    conn.autocommit = False
                    try:
                        rows = [
                            (row.get('news_id'), row.get('ticker'), row.get('title'),
                             row.get('sub_title'), row.get('short_content'), row.get('full_content'),
                             row.get('image_url'), row.get('source_link'), row.get('source_name'),
                             row.get('lang_code', 'vi'), row.get('public_date'),
                             row.get('close_price'), row.get('ref_price'), row.get('floor_price'),
                             row.get('ceiling_price'), row.get('price_change_pct'))
                            for _, row in df.iterrows()
                        ]
                        
                        news_ids = [row[0] for row in rows]
                        
                        with conn.cursor() as cur:
                            # Delete existing by news_id
                            delete_sql = f"""
                                DELETE FROM {SCHEMA}.news
                                WHERE news_id = ANY(%s);
                            """
                            cur.execute(delete_sql, (news_ids,))
                            
                            # Insert new
                            insert_sql = f"""
                                INSERT INTO {SCHEMA}.news
                                (news_id, ticker, title, sub_title, short_content, full_content,
                                 image_url, source_link, source_name, lang_code, public_date,
                                 close_price, ref_price, floor_price, ceiling_price, price_change_pct)
                                VALUES %s;
                            """
                            execute_values(cur, insert_sql, rows)
                        
                        conn.commit()
                        total_rows += len(rows)
                        logger.info(f"Processed {partition}: {len(rows)} rows")
                        
                    except Exception as e:
                        conn.rollback()
                        logger.error(f"Error processing {partition}: {str(e)}")
                        
            except Exception as e:
                logger.error(f"Error reading {partition}: {str(e)}")
                continue
        
        return f"✅ News: {total_rows:,} rows from {len(partitions)} partitions"
    
    @task(task_id="sync_overview")
    def task_sync_overview():
        """Sync ALL company overview partitions"""
        from lake_to_dwh.utils import read_all_csvs_from_folder, get_postgres_connection
        from psycopg2.extras import execute_values
        from contextlib import closing
        import pandas as pd
        import logging
        logger = logging.getLogger("airflow.task")
        
        logger.info("Processing ALL overview partitions")
        partitions = get_all_partitions_standard("overview/")
        
        if not partitions:
            return "⚠️ Company Overview: No partitions found"
        
        logger.info(f"Found {len(partitions)} overview partitions")
        total_rows = 0
        
        for partition in partitions:
            try:
                df = read_all_csvs_from_folder(MINIO_BUCKET, partition, MINIO_CONN_ID)
                if df.empty:
                    continue
                
                column_mappings = {
                    'symbol': 'ticker', 'company_profile': 'overview',
                    'icbname1': 'icb_name1', 'icbname2': 'icb_name2', 'icbname3': 'icb_name3',
                }
                for old_col, new_col in column_mappings.items():
                    if old_col in df.columns and new_col not in df.columns:
                        df.rename(columns={old_col: new_col}, inplace=True)
                
                df = df.dropna(subset=['ticker'])
                df = df.drop_duplicates(subset=['ticker'], keep='last')
                
                schema_cols = ['ticker', 'overview', 'icb_name1', 'icb_name2', 'icb_name3',
                              'exchange', 'type_info', 'organ_short_name', 'organ_name', 'product_group']
                available_cols = [col for col in schema_cols if col in df.columns]
                df = df[available_cols].copy()
                
                if df.empty:
                    continue
                
                with closing(get_postgres_connection(DB_URL)) as conn:
                    conn.autocommit = False
                    try:
                        rows = [tuple(row[col] if col in row.index else None for col in available_cols) for _, row in df.iterrows()]
                        tickers = [row[0] for row in rows]
                        
                        with conn.cursor() as cur:
                            cur.execute(f"DELETE FROM {SCHEMA}.company_overview WHERE ticker = ANY(%s);", (tickers,))
                            execute_values(cur, f"""
                                INSERT INTO {SCHEMA}.company_overview ({', '.join(available_cols)})
                                VALUES %s;
                            """, rows)
                        
                        conn.commit()
                        total_rows += len(rows)
                        logger.info(f"Processed {partition}: {len(rows)} rows")
                    except Exception as e:
                        conn.rollback()
                        logger.error(f"Error processing {partition}: {str(e)}")
            except Exception as e:
                logger.error(f"Error reading {partition}: {str(e)}")
                continue
        
        return f"✅ Company Overview: {total_rows:,} rows from {len(partitions)} partitions"
    
    @task(task_id="sync_people")
    def task_sync_people():
        """Sync ALL people/owner partitions"""
        from lake_to_dwh.utils import read_all_csvs_from_folder, get_postgres_connection
        from psycopg2.extras import execute_values
        from contextlib import closing
        import pandas as pd
        import logging
        logger = logging.getLogger("airflow.task")
        
        logger.info("Processing ALL people partitions")
        partitions = get_all_partitions_standard("people/")
        
        if not partitions:
            return "⚠️ Company People: No partitions found"
        
        logger.info(f"Found {len(partitions)} people partitions")
        total_rows = 0
        
        for partition in partitions:
            try:
                df = read_all_csvs_from_folder(MINIO_BUCKET, partition, MINIO_CONN_ID)
                if df.empty:
                    continue
                
                if 'symbol' in df.columns:
                    df.rename(columns={'symbol': 'ticker'}, inplace=True)
                if 'owner_name' in df.columns:
                    df.rename(columns={'owner_name': 'name'}, inplace=True)
                if 'ownership_percent' in df.columns:
                    df.rename(columns={'ownership_percent': 'percent'}, inplace=True)
                
                df = df.dropna(subset=['ticker'])
                
                available_cols = ['ticker', 'name', 'position', 'percent', 'type']
                df = df[[col for col in available_cols if col in df.columns]].copy()
                
                if df.empty:
                    continue
                
                with closing(get_postgres_connection(DB_URL)) as conn:
                    conn.autocommit = False
                    try:
                        rows = [tuple(row[col] if col in row.index else None for col in available_cols) for _, row in df.iterrows()]
                        tickers = list(set(row[0] for row in rows))
                        
                        with conn.cursor() as cur:
                            cur.execute(f"DELETE FROM {SCHEMA}.owner WHERE ticker = ANY(%s);", (tickers,))
                            execute_values(cur, f"""
                                INSERT INTO {SCHEMA}.owner ({', '.join(available_cols)})
                                VALUES %s;
                            """, rows)
                        
                        conn.commit()
                        total_rows += len(rows)
                        logger.info(f"Processed {partition}: {len(rows)} rows")
                    except Exception as e:
                        conn.rollback()
                        logger.error(f"Error processing {partition}: {str(e)}")
            except Exception as e:
                logger.error(f"Error reading {partition}: {str(e)}")
                continue
        
        return f"✅ Company People: {total_rows:,} rows from {len(partitions)} partitions"
    
    @task(task_id="sync_electric_board")
    def task_sync_electric_board():
        """Sync ALL electric board (price board) partitions"""
        from lake_to_dwh.utils import read_all_csvs_from_folder, get_postgres_connection, ensure_schema
        from lake_to_dwh.sync_electric_board import (
            COLUMN_MAPPING, DB_COLUMNS, CREATE_TABLE_SQL, transform_dataframe
        )
        from psycopg2.extras import execute_values
        from contextlib import closing
        import pandas as pd
        import logging
        logger = logging.getLogger("airflow.task")
        
        logger.info("Processing ALL electric_board partitions")
        partitions = get_all_partitions_standard("electric_board_per_day/")
        
        if not partitions:
            return "⚠️ Electric Board: No partitions found"
        
        logger.info(f"Found {len(partitions)} electric_board partitions")
        total_rows = 0
        
        # Ensure table exists
        with closing(get_postgres_connection(DB_URL)) as conn:
            conn.autocommit = False
            try:
                ensure_schema(conn, SCHEMA)
                with conn.cursor() as cur:
                    cur.execute(CREATE_TABLE_SQL.format(schema=SCHEMA))
                conn.commit()
                logger.info(f"✓ Table {SCHEMA}.electric_board ensured")
            except Exception as e:
                conn.rollback()
                logger.error(f"Error creating table: {str(e)}")
                raise
        
        for partition in partitions:
            try:
                df = read_all_csvs_from_folder(MINIO_BUCKET, partition, MINIO_CONN_ID)
                if df.empty:
                    continue
                
                # Transform data
                df = transform_dataframe(df)
                
                if df.empty:
                    continue
                
                with closing(get_postgres_connection(DB_URL)) as conn:
                    conn.autocommit = False
                    try:
                        available_cols = [col for col in DB_COLUMNS if col in df.columns]
                        rows = [
                            tuple(
                                row[col] if col in row.index and pd.notna(row[col]) else None
                                for col in available_cols
                            )
                            for _, row in df.iterrows()
                        ]
                        
                        tickers = [str(row[0]) for row in rows]
                        dates = [str(row[2]) for row in rows]  # trading_date is at index 2
                        
                        with conn.cursor() as cur:
                            # Delete existing
                            cur.execute(f"""
                                DELETE FROM {SCHEMA}.electric_board
                                WHERE (ticker, trading_date) IN (
                                    SELECT DISTINCT ticker, trading_date::date 
                                    FROM unnest(%s::text[], %s::text[]) AS t(ticker, trading_date)
                                );
                            """, (tickers, dates))
                            
                            # Insert new
                            execute_values(cur, f"""
                                INSERT INTO {SCHEMA}.electric_board ({', '.join(available_cols)})
                                VALUES %s;
                            """, rows, page_size=1000)
                        
                        conn.commit()
                        total_rows += len(rows)
                        logger.info(f"Processed {partition}: {len(rows)} rows")
                    except Exception as e:
                        conn.rollback()
                        logger.error(f"Error processing {partition}: {str(e)}")
            except Exception as e:
                logger.error(f"Error reading {partition}: {str(e)}")
                continue
        
        return f"✅ Electric Board: {total_rows:,} rows from {len(partitions)} partitions"
    
    # ========================================================================
    # TASK: REMOVE DUPLICATES
    # ========================================================================
    
    @task(task_id="remove_all_duplicates")
    def task_remove_all_duplicates():
        """Remove duplicate records from ALL tables using ctid approach with parallel processing"""
        from sqlalchemy import create_engine, text
        from concurrent.futures import ThreadPoolExecutor, as_completed
        import logging
        import time
        logger = logging.getLogger("airflow.task")
        
        logger.info("=" * 70)
        logger.info("🧹 REMOVING DUPLICATE RECORDS FROM ALL TABLES")
        logger.info("=" * 70)
        
        tables_config = {
            'bctc': ['ticker', 'quarter', 'year', 'ind_name', 'ind_code', 'value', 'report_name', 'report_code'],
            'company_overview': ['ticker', 'overview', 'icb_name1', 'icb_name2', 'icb_name3', 'exchange', 'type_info', 'organ_short_name', 'organ_name', 'product_group'],
            'electric_board': ['ticker', 'trading_date', 'exchange', 'ref_price', 'match_price', 'accumulated_volume'],
            'financial_ratio': ['cp', 'nam', 'ky', 'id'],
            'history_price': ['ticker', 'trading_date'],
            'macro_economy': ['date', 'asset_type'],
            'market_index': ['ticker', 'trading_date'],
            'news': ['news_id'],
            'owner': ['ticker', 'name', 'position', 'percent', 'type'],
        }
        
        def process_table(table: str, group_cols: list) -> dict:
            """Process a single table for duplicate removal"""
            result = {
                'table': table,
                'status': 'success',
                'before': 0,
                'after': 0,
                'removed': 0,
                'time_seconds': 0,
                'error': None
            }
            
            start_time = time.time()
            engine = create_engine(DB_URL)
            
            try:
                with engine.begin() as conn:
                    # Count before
                    count_before = conn.execute(text(f"SELECT COUNT(*) FROM {SCHEMA}.{table}")).scalar()
                    result['before'] = count_before
                    
                    if count_before == 0:
                        result['status'] = 'empty'
                        return result
                    
                    # Build and execute delete query
                    columns_str = ', '.join(group_cols)
                    delete_sql = f"""
                        DELETE FROM {SCHEMA}.{table}
                            WHERE ctid IN (
                                SELECT ctid
                                FROM (
                                    SELECT ctid,
                                        ROW_NUMBER() OVER (
                                            PARTITION BY {columns_str}
                                            ORDER BY ctid
                                        ) AS rn
                                    FROM {SCHEMA}.{table}
                                ) t
                                WHERE rn > 1
                            );
                    """
                    
                    delete_result = conn.execute(text(delete_sql))
                    deleted = delete_result.rowcount
                    result['removed'] = deleted
                    
                    # Count after
                    count_after = conn.execute(text(f"SELECT COUNT(*) FROM {SCHEMA}.{table}")).scalar()
                    result['after'] = count_after
                    
            except Exception as e:
                result['status'] = 'error'
                result['error'] = str(e)
            finally:
                engine.dispose()
                result['time_seconds'] = round(time.time() - start_time, 2)
            
            return result
        
        # Process tables in parallel using ThreadPoolExecutor
        MAX_WORKERS = 4  # Number of parallel workers
        results = []
        total_start = time.time()
        
        logger.info(f"🚀 Starting parallel deduplication with {MAX_WORKERS} workers...")
        logger.info("-" * 70)
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Submit all tasks
            future_to_table = {
                executor.submit(process_table, table, group_cols): table 
                for table, group_cols in tables_config.items()
            }
            
            # Process results as they complete
            for future in as_completed(future_to_table):
                table = future_to_table[future]
                try:
                    result = future.result()
                    results.append(result)
                    
                    # Log result for this table
                    if result['status'] == 'empty':
                        logger.info(f"📋 {table}: Empty table, skipped")
                    elif result['status'] == 'error':
                        logger.error(f"❌ {table}: Error - {result['error']}")
                    else:
                        pct_removed = (result['removed'] / result['before'] * 100) if result['before'] > 0 else 0
                        logger.info(
                            f"✅ {table}: "
                            f"Before={result['before']:,} → After={result['after']:,} | "
                            f"Removed={result['removed']:,} ({pct_removed:.1f}%) | "
                            f"Time={result['time_seconds']}s"
                        )
                        
                except Exception as e:
                    logger.error(f"❌ {table}: Unexpected error - {str(e)}")
                    results.append({
                        'table': table,
                        'status': 'error',
                        'removed': 0,
                        'error': str(e)
                    })
        
        # Summary
        total_time = round(time.time() - total_start, 2)
        total_removed = sum(r.get('removed', 0) for r in results)
        total_before = sum(r.get('before', 0) for r in results)
        total_after = sum(r.get('after', 0) for r in results)
        success_count = sum(1 for r in results if r['status'] == 'success')
        
        logger.info("-" * 70)
        logger.info("📊 DEDUPLICATION SUMMARY")
        logger.info("-" * 70)
        logger.info(f"📁 Tables processed: {success_count}/{len(tables_config)}")
        logger.info(f"📈 Total rows before: {total_before:,}")
        logger.info(f"📉 Total rows after:  {total_after:,}")
        logger.info(f"🗑️  Total removed:     {total_removed:,}")
        logger.info(f"⏱️  Total time:        {total_time}s")
        logger.info("=" * 70)
        
        return f"✅ Removed {total_removed:,} duplicates from {success_count} tables in {total_time}s"
    
    # ========================================================================
    # TASK: SUMMARY REPORT
    # ========================================================================
    
    @task(task_id="summary_report")
    def task_summary_report(results: list, dedup_result: str):
        """Generate final summary report"""
        import logging
        logger = logging.getLogger("airflow.task")
        
        logger.info("=" * 70)
        logger.info("📊 FULL BACKUP COMPLETE - SUMMARY REPORT")
        logger.info("=" * 70)
        
        task_names = [
            "BCTC",
            "Daily Price",
            "History Price (Backfill)",
            "Financial Ratio",
            "Macro Economy",
            "Global Index",
            "Market Index",
            "News",
            "Company Overview",
            "Company People",
            "Electric Board"
        ]
        
        for idx, (task_name, result) in enumerate(zip(task_names, results)):
            logger.info(f"{idx + 1}. {task_name}: {result}")
        
        logger.info(f"\n{dedup_result}")
        logger.info("=" * 70)
        
        # Count successes
        success_count = sum(1 for r in results if r and '✅' in str(r))
        logger.info(f"✅ Successful: {success_count}/{len(results)}")
        
        if success_count < len(results):
            logger.warning(f"⚠️ Failed: {len(results) - success_count}/{len(results)}")
        
        return f"Completed: {success_count}/{len(results)} successful"
    
    # ========================================================================
    # WORKFLOW
    # ========================================================================
    
    # Step 1: Truncate
    truncate = task_truncate_all_tables()
    
    # Step 2: Sync all data EXCEPT history_price (parallel)
    bctc_result = task_sync_bctc()
    daily_price_result = task_sync_daily_price()
    financial_ratio_result = task_sync_financial_ratio()
    macro_economy_result = task_sync_macro_economy()
    global_index_result = task_sync_global_index()
    index_price_result = task_sync_index_price()
    news_result = task_sync_news()
    overview_result = task_sync_overview()
    people_result = task_sync_people()
    electric_board_result = task_sync_electric_board()
    
    # Step 3: After all other sync tasks complete, run history_price
    # This allows all workers to focus on this heavy task
    history_price_result = task_sync_history_price()
    
    # Step 4: Remove duplicates
    dedup = task_remove_all_duplicates()
    
    # Step 5: Summary
    all_results = [
        bctc_result,
        daily_price_result,
        history_price_result,
        financial_ratio_result,
        macro_economy_result,
        global_index_result,
        index_price_result,
        news_result,
        overview_result,
        people_result,
        electric_board_result,
    ]
    
    summary = task_summary_report(all_results, dedup)
    
    # Dependencies:
    # truncate -> [all sync tasks except history_price] -> history_price -> dedup -> summary
    
    other_sync_tasks = [
        bctc_result,
        daily_price_result,
        financial_ratio_result,
        macro_economy_result,
        global_index_result,
        index_price_result,
        news_result,
        overview_result,
        people_result,
        electric_board_result,
    ]
    
    # Step flow: truncate -> other tasks (parallel) -> history_price -> dedup -> summary
    truncate >> other_sync_tasks
    other_sync_tasks >> history_price_result >> dedup >> summary


# Instantiate the DAG
backup_db_full_sync()

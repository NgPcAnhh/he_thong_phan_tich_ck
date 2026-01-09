import logging
from contextlib import closing
from datetime import datetime

import pandas as pd
from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.decorators import apply_defaults
from psycopg2.extras import execute_values


class DailyPriceImportOperator(BaseOperator):
    """
    Operator để insert dữ liệu giá cổ phiếu vào bảng history_price trong PostgreSQL.
    
    Tự động:
    - Tạo schema nếu chưa tồn tại
    - Tạo bảng history_price với cấu trúc chuẩn nếu chưa có
    - Upsert dữ liệu (ON CONFLICT DO UPDATE)
    """
    
    @apply_defaults
    def __init__(
        self,
        task_id_source: str,
        postgres_conn_id: str = 'dwh-postgres',
        dest_schema: str = 'hethong_phantich_chungkhoan',
        dest_table: str = 'history_price',
        *args, **kwargs
    ):
        super(DailyPriceImportOperator, self).__init__(*args, **kwargs)
        self.task_id_source = task_id_source
        self.postgres_conn_id = postgres_conn_id
        self.dest_schema = dest_schema
        self.dest_table = dest_table

    def execute(self, context):
        ti = context['ti']
        
        # Pull dữ liệu từ task trước
        raw_data = ti.xcom_pull(task_ids=self.task_id_source)

        if not raw_data:
            logging.warning(f"[HISTORY_PRICE] Task {self.task_id_source} không trả về dữ liệu")
            return

        # Convert sang DataFrame
        df = pd.DataFrame(raw_data) if not isinstance(raw_data, pd.DataFrame) else raw_data
        
        if df.empty:
            logging.info("[HISTORY_PRICE] DataFrame rỗng, bỏ qua insert")
            return

        # Chuẩn hóa dữ liệu
        logging.info(f"[HISTORY_PRICE] Xử lý {len(df)} records từ task {self.task_id_source}")
        
        df.columns = [c.lower().strip() for c in df.columns]
        if 'tradingdate' in df.columns:
            df.rename(columns={'tradingdate': 'trading_date'}, inplace=True)
        
        # Đảm bảo có đủ cột
        df['import_time'] = datetime.now()
        target_cols = ['ticker', 'trading_date', 'open', 'high', 'low', 'close', 'volume', 'import_time']
        
        for col in target_cols:
            if col not in df.columns:
                df[col] = None
        
        df = df[target_cols].copy()
        
        # Chuẩn hóa kiểu dữ liệu
        df['ticker'] = df['ticker'].astype(str).str.upper().str.strip()
        df['trading_date'] = pd.to_datetime(df['trading_date'], errors='coerce').dt.date
        df.dropna(subset=['ticker', 'trading_date'], inplace=True)
        
        if df.empty:
            logging.warning("[HISTORY_PRICE] Không có dữ liệu hợp lệ sau khi clean")
            return

        # Chuẩn bị rows để insert
        rows = [
            (
                row['ticker'],
                row['trading_date'],
                row.get('open'),
                row.get('high'),
                row.get('low'),
                row.get('close'),
                row.get('volume'),
                row['import_time'],
            )
            for row in df.to_dict('records')
        ]

        logging.info(f"[HISTORY_PRICE] Chuẩn bị insert {len(rows)} rows vào {self.dest_schema}.{self.dest_table}")
        
        # Insert vào PostgreSQL
        try:
            hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
            with closing(hook.get_conn()) as conn:
                conn.autocommit = True
                with conn.cursor() as cur:
                    # Tạo schema nếu chưa có
                    logging.info(f"[HISTORY_PRICE] Đảm bảo schema {self.dest_schema} tồn tại...")
                    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {self.dest_schema};")
                    
                    # Tạo bảng nếu chưa có
                    logging.info(f"[HISTORY_PRICE] Đảm bảo bảng {self.dest_table} tồn tại...")
                    cur.execute(
                        f"""
                        CREATE TABLE IF NOT EXISTS {self.dest_schema}.{self.dest_table} (
                            ticker TEXT NOT NULL,
                            trading_date DATE NOT NULL,
                            open NUMERIC,
                            high NUMERIC,
                            low NUMERIC,
                            close NUMERIC,
                            volume NUMERIC,
                            import_time TIMESTAMPTZ,
                            PRIMARY KEY (ticker, trading_date)
                        );
                        """
                    )
                    
                    # Insert với upsert
                    logging.info(f"[HISTORY_PRICE] Đang insert dữ liệu...")
                    insert_sql = f"""
                        INSERT INTO {self.dest_schema}.{self.dest_table}
                        (ticker, trading_date, open, high, low, close, volume, import_time)
                        VALUES %s
                        ON CONFLICT (ticker, trading_date) DO UPDATE SET
                            open = EXCLUDED.open,
                            high = EXCLUDED.high,
                            low = EXCLUDED.low,
                            close = EXCLUDED.close,
                            volume = EXCLUDED.volume,
                            import_time = EXCLUDED.import_time;
                    """
                    execute_values(cur, insert_sql, rows)
            
            logging.info(f"[HISTORY_PRICE] ✅ Insert thành công {len(rows)} rows vào {self.dest_schema}.{self.dest_table}")
            
        except Exception as e:
            logging.error(f"[HISTORY_PRICE] ❌ Lỗi khi insert vào database: {str(e)}")
            raise e
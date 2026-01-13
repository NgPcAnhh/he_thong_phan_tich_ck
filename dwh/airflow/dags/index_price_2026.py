from contextlib import closing
from datetime import datetime, timedelta
from io import StringIO

import pandas as pd
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values

DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

POSTGRES_CONN_ID = "dwh_postgres"
TARGET_SCHEMA = "hethong_phantich_chungkhoan"
TARGET_TABLE = "market_index"
MINIO_CONN_ID = "minio_finance"
MINIO_BUCKET = "thongtin-congty-va-bctc"
MINIO_OBJECT_TEMPLATE = "index_price/{{ ds }}/index_price_{{ ts_nodash }}.csv"


@dag(
    dag_id="index_price_2026",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule_interval="0 15 * * 1-5",  # chạy 1 lần/ngày lúc 15:00 từ thứ 2-6
    catchup=False,
    max_active_runs=1,
    tags=["vnstock", "finance", "index", "price", "minio"],
)
def index_price_2026():
    @task
    def persist_index_price(**context):
        from logic.index_price_2026 import get_index_price_2026

        ds_str = context["ds"]
        ts_nodash = context.get("ts_nodash")
        print(f"[INDEX_PRICE] Bắt đầu lấy giá index cho ngày {ds_str}")
        
        df = get_index_price_2026(end_date=ds_str, sleep_time=1.0)

        if df is None or df.empty:
            print(f"[INDEX_PRICE] ⚠️ Không có dữ liệu index từ API")
            return "no_index_price"

        print(f"[INDEX_PRICE] ✓ Lấy được {len(df)} records từ API")

        df = df.copy()
        df["trading_date"] = pd.to_datetime(df["trading_date"], errors="coerce").dt.date
        df.dropna(subset=["trading_date", "ticker"], inplace=True)
        if df.empty:
            print(f"[INDEX_PRICE] ⚠️ Không có dữ liệu sau khi clean")
            return "no_index_price"

        df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()

        # Lưu ra MinIO theo partition ngày
        try:
            object_key = MINIO_OBJECT_TEMPLATE.replace("{{ ds }}", ds_str).replace("{{ ts_nodash }}", ts_nodash)
            csv_buf = StringIO()
            df.to_csv(csv_buf, index=False)
            s3 = S3Hook(aws_conn_id=MINIO_CONN_ID)
            s3.load_string(
                string_data=csv_buf.getvalue(),
                key=object_key,
                bucket_name=MINIO_BUCKET,
                replace=True,
            )
            print(f"[INDEX_PRICE] ✓ Đã ghi CSV lên MinIO: s3://{MINIO_BUCKET}/{object_key}")
        except Exception as e:
            print(f"[INDEX_PRICE] ⚠️ Lỗi ghi CSV lên MinIO: {e}")

        rows = [
            (
                row["ticker"],
                row["trading_date"].isoformat(),
                row.get("open"),
                row.get("high"),
                row.get("low"),
                row.get("close"),
                row.get("volume"),
            )
            for row in df.to_dict("records")
        ]

        if not rows:
            print(f"[INDEX_PRICE] ⚠️ Không có rows để insert")
            return "no_index_price"

        print(f"[INDEX_PRICE] Chuẩn bị insert {len(rows)} rows vào {TARGET_SCHEMA}.{TARGET_TABLE}")
        print(f"[INDEX_PRICE] Danh sách index: {df['ticker'].unique().tolist()}")

        try:
            hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            with closing(hook.get_conn()) as conn:
                conn.autocommit = True
                with conn.cursor() as cur:
                    print(f"[INDEX_PRICE] Đang tạo schema và table nếu chưa tồn tại...")
                    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA};")
                    cur.execute(
                        f"""
                        CREATE TABLE IF NOT EXISTS {TARGET_SCHEMA}.{TARGET_TABLE} (
                            ticker TEXT NOT NULL,
                            trading_date TEXT NOT NULL,
                            open NUMERIC,
                            high NUMERIC,
                            low NUMERIC,
                            close NUMERIC,
                            volume NUMERIC,
                            import_time TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
                            PRIMARY KEY (ticker, trading_date)
                        );
                        """
                    )

                    print(f"[INDEX_PRICE] Đang insert dữ liệu vào database...")
                    insert_sql = f"""
                        INSERT INTO {TARGET_SCHEMA}.{TARGET_TABLE}
                        (ticker, trading_date, open, high, low, close, volume)
                        VALUES %s
                        ON CONFLICT (ticker, trading_date) DO UPDATE SET
                            open = EXCLUDED.open,
                            high = EXCLUDED.high,
                            low = EXCLUDED.low,
                            close = EXCLUDED.close,
                            volume = EXCLUDED.volume,
                            import_time = NOW();
                    """
                    execute_values(cur, insert_sql, rows)

            print(f"[INDEX_PRICE] ✅ Insert thành công {len(rows)} rows")
            return f"loaded:{len(rows)}"
        except Exception as e:
            print(f"[INDEX_PRICE] ❌ Lỗi khi insert vào database: {str(e)}")
            raise

    persist_index_price()


index_price_2026()
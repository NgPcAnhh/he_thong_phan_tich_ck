from contextlib import closing
from datetime import datetime, timedelta

import pandas as pd
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values

DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

POSTGRES_CONN_ID = "dwh_postgres"
TARGET_SCHEMA = "public"
TARGET_TABLE = "index_price_daily"


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
        ds_date = pd.to_datetime(ds_str).date()

        rows = [
            (
                row["ticker"],
                row["trading_date"],
                row.get("open"),
                row.get("high"),
                row.get("low"),
                row.get("close"),
                row.get("volume"),
                ds_date,
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
                            trading_date DATE NOT NULL,
                            open NUMERIC,
                            high NUMERIC,
                            low NUMERIC,
                            close NUMERIC,
                            volume NUMERIC,
                            fetched_for DATE NOT NULL,
                            loaded_at TIMESTAMPTZ DEFAULT NOW(),
                            PRIMARY KEY (ticker, trading_date)
                        );
                        """
                    )

                    print(f"[INDEX_PRICE] Đang insert dữ liệu vào database...")
                    insert_sql = f"""
                        INSERT INTO {TARGET_SCHEMA}.{TARGET_TABLE}
                        (ticker, trading_date, open, high, low, close, volume, fetched_for)
                        VALUES %s
                        ON CONFLICT (ticker, trading_date) DO UPDATE SET
                            open = EXCLUDED.open,
                            high = EXCLUDED.high,
                            low = EXCLUDED.low,
                            close = EXCLUDED.close,
                            volume = EXCLUDED.volume,
                            fetched_for = EXCLUDED.fetched_for,
                            loaded_at = NOW();
                    """
                    execute_values(cur, insert_sql, rows)

            print(f"[INDEX_PRICE] ✅ Insert thành công {len(rows)} rows")
            return f"loaded:{len(rows)}"
        except Exception as e:
            print(f"[INDEX_PRICE] ❌ Lỗi khi insert vào database: {str(e)}")
            raise

    persist_index_price()


index_price_2026()

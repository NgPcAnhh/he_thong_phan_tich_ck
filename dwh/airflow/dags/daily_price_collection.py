from contextlib import closing
from datetime import datetime, timedelta

import pandas as pd
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

POSTGRES_CONN_ID = "dwh-postgres"
TARGET_SCHEMA = "hethong_phantich_chungkhoan"
TARGET_TABLE = "history_price"


@dag(
    dag_id="daily_price_collection",
    default_args=default_args,
    start_date=datetime.now(),  # Bắt đầu từ thời điểm hiện tại
    schedule_interval="0 15 * * 1-5",  # 15:00 từ thứ 2-6
    catchup=False,
    max_active_runs=1,
    tags=["vnstock", "finance", "price", "postgres"],
    params={
        "start_date": "{{ ds }}",  # Ngày hiện tại
        "end_date": "{{ ds }}",  # Ngày hiện tại (chỉ lấy 1 ngày)
    },
)
def daily_price_collection():
    @task
    def get_batches(**context):
        from logic.list_macp import get_ticker_batches

        # Lấy start_date và end_date từ params (mặc định = ngày chạy DAG)
        start_date = context["params"].get("start_date", "{{ ds }}")
        end_date = context["params"].get("end_date", "{{ ds }}")

        # Batch nhỏ để tránh rate limit
        return [
            {
                "symbols": batch,
                "start_date": start_date,
                "end_date": end_date,
            }
            for batch in get_ticker_batches(batch_size=5)
        ]

    @task
    def persist_daily_price_batch(symbols: list, start_date: str, end_date: str, **context):
        """Lấy giá từ history_price logic và lưu trực tiếp vào PostgreSQL"""
        from logic.history_price import get_history_price_batch

        print(f"[DAILY_PRICE] Bắt đầu xử lý batch: {len(symbols)} tickers cho ngày {start_date}")
        print(f"[DAILY_PRICE] Danh sách tickers: {symbols}")

        # Lấy dữ liệu từ API
        df = get_history_price_batch(symbols=symbols, start_date=start_date, end_date=end_date)

        if df is None or df.empty:
            print(f"[DAILY_PRICE] ⚠️ Không có dữ liệu từ API cho batch này")
            return "no_data"

        print(f"[DAILY_PRICE] ✓ Lấy được {len(df)} records từ API")

        # Chuẩn hóa dữ liệu
        df = df.copy()
        df["trading_date"] = pd.to_datetime(df["trading_date"], errors="coerce").dt.date
        df.dropna(subset=["trading_date", "ticker"], inplace=True)
        
        if df.empty:
            print(f"[DAILY_PRICE] ⚠️ Không có dữ liệu sau khi clean")
            return "no_data"

        df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
        df["import_time"] = datetime.now()

        # Chuẩn bị rows để insert
        rows = [
            (
                row["ticker"],
                row["trading_date"],
                row.get("open"),
                row.get("high"),
                row.get("low"),
                row.get("close"),
                row.get("volume"),
                row["import_time"],
            )
            for row in df.to_dict("records")
        ]

        if not rows:
            print(f"[DAILY_PRICE] ⚠️ Không có rows để insert")
            return "no_data"

        print(f"[DAILY_PRICE] Chuẩn bị insert {len(rows)} rows vào {TARGET_SCHEMA}.{TARGET_TABLE}")

        # Insert vào PostgreSQL
        try:
            hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            with closing(hook.get_conn()) as conn:
                conn.autocommit = True
                with conn.cursor() as cur:
                    print(f"[DAILY_PRICE] Đang tạo schema và table nếu chưa tồn tại...")
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
                            import_time TIMESTAMPTZ,
                            PRIMARY KEY (ticker, trading_date)
                        );
                        """
                    )

                    print(f"[DAILY_PRICE] Đang insert dữ liệu vào database...")
                    insert_sql = f"""
                        INSERT INTO {TARGET_SCHEMA}.{TARGET_TABLE}
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

            print(f"[DAILY_PRICE] ✅ Insert thành công {len(rows)} rows")
            return f"loaded:{len(rows)}"
        except Exception as e:
            print(f"[DAILY_PRICE] ❌ Lỗi khi insert vào database: {str(e)}")
            raise

    batches = get_batches()
    persist_daily_price_batch.expand(op_kwargs=batches)


daily_price_collection()

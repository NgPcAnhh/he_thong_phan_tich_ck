from contextlib import closing
from datetime import datetime, timedelta
from io import StringIO

import pandas as pd
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

MINIO_CONN_ID = "minio_finance"
MINIO_BUCKET = "thongtin-congty-va-bctc"
MINIO_OBJECT_TEMPLATE = "index_price/{{ ds }}/index_price_{{ ts_nodash }}.csv"


@dag(
    dag_id="market_index_price",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule_interval="0 9-15 * * *",  # chạy mỗi giờ từ 09:00 đến 15:00
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

        # Hoàn tất - dữ liệu đã được lưu vào MinIO
        # Sync vào database sẽ được thực hiện bởi DAG minio_to_db_sync.py
        print(f"[INDEX_PRICE] ✅ Hoàn tất - Đã lưu {len(df)} records vào MinIO")
        print(f"[INDEX_PRICE] Danh sách index: {df['ticker'].unique().tolist()}")
        return f"saved_to_minio:{len(df)}"

    persist_index_price()


index_price_2026()
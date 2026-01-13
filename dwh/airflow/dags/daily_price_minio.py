"""
DAG for Daily Price Collection - Current Day Only
Tương tự history_price.py nhưng chỉ lấy dữ liệu ngày hiện tại.
Dữ liệu được lưu vào MinIO với partition theo ngày.
"""
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models.baseoperator import chain

from function.datalake_df2csv import DfToCsvOperator

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

MINIO_BUCKET = "thongtin-congty-va-bctc"
MINIO_CONN_ID = "minio_finance"


@dag(
    dag_id="daily_price_minio",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="0 16 * * 1-5",  # 16:00 từ thứ 2-6 (sau giờ đóng cửa thị trường)
    catchup=False,
    tags=["vnstock", "finance", "price", "daily"],
    description="Lấy dữ liệu giá chứng khoán trong ngày hiện tại và lưu vào MinIO",
)
def daily_price_minio_dag():
    """
    ETL Pipeline cho giá chứng khoán hàng ngày.
    
    - Tự động chạy vào 16:00 từ thứ 2 đến thứ 6
    - Chỉ lấy dữ liệu ngày hiện tại ({{ ds }})
    - Lưu vào MinIO bucket: thongtinn-congty-va-bctc
    - Partition theo ngày: daily_price/YYYY-MM-DD/batch_X.csv
    """
    
    @task
    def get_batches(**context):
        """
        Tạo danh sách batch tickers với start_date = ngày hiện tại, end_date = ngày hiện tại + 1.
        end_date phải là ngày mai để API trả về đầy đủ dữ liệu của ngày hiện tại.
        """
        from datetime import datetime, timedelta
        from logic.list_macp import get_ticker_batches

        # Lấy ngày hiện tại từ execution date
        current_date = context["ds"]  # Format: YYYY-MM-DD
        
        # Tính ngày mai (end_date phải lớn hơn start_date 1 ngày)
        next_date = (datetime.strptime(current_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
        
        print(f"[DAILY_PRICE] Lấy dữ liệu từ {current_date} đến {next_date}")

        # Batch nhỏ để tránh rate limit
        batches = [
            {
                "symbols": batch,
                "start_date": current_date,  # Ngày hiện tại
                "end_date": next_date,        # Ngày mai (để lấy được dữ liệu ngày hiện tại)
            }
            for batch in get_ticker_batches(batch_size=5)
        ]
        
        print(f"[DAILY_PRICE] Tổng số batches: {len(batches)}")
        return batches

    batches = get_batches()

    # Sử dụng DfToCsvOperator giống như history_price.py
    ingest_daily_price = DfToCsvOperator.partial(
        task_id="ingest_daily_price",
        logic_file="history_price",  # Sử dụng cùng logic file
        df_name="get_history_price_batch",  # Sử dụng cùng function
        bucket_name=MINIO_BUCKET,
        object_path="daily_price/{{ ds }}/batch_{{ ti.map_index }}.csv",  # Lưu vào folder daily_price
        conn_id=MINIO_CONN_ID,
    ).expand(op_kwargs=batches)

    chain(batches, ingest_daily_price)


daily_price_minio_dag()

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models.baseoperator import chain

from function.datalake_df2csv import DfToCsvOperator

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

MINIO_BUCKET = "thongtin-congty-va-bctc"
MINIO_CONN_ID = "minio_finance"


@dag(
    dag_id="electric_board_daily",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="0 15 * * 1-5",  # 15:00 thứ 2-6
    catchup=False,
    tags=["vnstock", "finance", "electric_board", "daily"],
    description="Lấy dữ liệu bảng giá giao dịch cuối ngày và lưu vào MinIO",
)
def electric_board_daily_dag():
    @task
    def get_batches(**context):
        from logic.list_macp import get_ticker_batches

        # Lấy ngày hiện tại từ execution date
        trading_date = context["ds"]  # Format: YYYY-MM-DD

        print(f"[ELECTRIC_BOARD] Lấy dữ liệu bảng giá cho ngày {trading_date}")

        # Batch size lớn hơn vì price_board hỗ trợ lấy nhiều mã cùng lúc
        batches = [
            {
                "symbols": batch,
                "trading_date": trading_date,
            }
            for batch in get_ticker_batches(batch_size=50)
        ]

        print(f"[ELECTRIC_BOARD] Tổng số batches: {len(batches)}")
        return batches

    batches = get_batches()

    # Sử dụng DfToCsvOperator để lấy dữ liệu và upload lên MinIO
    ingest_electric_board = DfToCsvOperator.partial(
        task_id="ingest_electric_board",
        logic_file="electric_board",
        df_name="get_price_board_batch",
        bucket_name=MINIO_BUCKET,
        object_path="electric_board_per_day/{{ ds }}/batch_{{ ti.map_index }}.csv",
        conn_id=MINIO_CONN_ID,
    ).expand(op_kwargs=batches)

    chain(batches, ingest_electric_board)


electric_board_daily_dag()

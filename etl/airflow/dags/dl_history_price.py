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
    dag_id="history_price",
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["vnstock", "finance", "price"],
    params={
        "start_date": "2026-01-01",  # Ngày bắt đầu lấy dữ liệu
        "end_date": None,  # None = đến hôm nay
    },
)
def history_price_dag():
    @task
    def get_batches(**context):
        from logic.list_macp import get_ticker_batches

        # Lấy start_date và end_date từ params
        start_date = context["params"].get("start_date", "2020-01-01")
        end_date = context["params"].get("end_date")

        # Batch nhỏ để tránh rate limit
        return [
            {
                "symbols": batch,
                "start_date": start_date,
                "end_date": end_date,
            }
            for batch in get_ticker_batches(batch_size=5)
        ]

    batches = get_batches()

    ingest_history = DfToCsvOperator.partial(
        task_id="ingest_history_price",
        logic_file="history_price",
        df_name="get_history_price_batch",
        bucket_name=MINIO_BUCKET,
        object_path="history_price/{{ ds }}/batch_{{ ti.map_index }}.csv",
        conn_id=MINIO_CONN_ID,
    ).expand(op_kwargs=batches)

    chain(batches, ingest_history)


history_price_dag()
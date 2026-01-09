from datetime import datetime, timedelta

import pandas as pd
from airflow.decorators import dag, task
from airflow.models.baseoperator import chain

from function.datalake_df2csv import DfToCsvOperator

# Chỉ chạy một lần, không backfill
default_args = {
    "owner": "airflow",
    "retries": 0,
}

MINIO_BUCKET = "thongtin-congty-va-bctc"
MINIO_CONN_ID = "minio_finance"

@dag(
    dag_id="thongtin_cty_bctc_once",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="@weekly",  # chạy mỗi tuần, dùng execution date hiện tại
    catchup=False,
    tags=["vnstock", "finance", "weekly"],
)
def stock_dag_once():
    @task
    def get_batches():
        from logic.list_macp import get_ticker_batches
        return [{"symbols": batch} for batch in get_ticker_batches(batch_size=3)]

    batches = get_batches()

    ingest_bctc = DfToCsvOperator.partial(
        task_id="ingest_bctc_once",
        logic_file="bctc",
        df_name="get_financial_reports",
        bucket_name=MINIO_BUCKET,
        object_path="bctc_once/{{ ds }}/batch_{{ ti.map_index }}.csv",
        conn_id=MINIO_CONN_ID,
    ).expand(op_kwargs=batches)

    ingest_overview = DfToCsvOperator.partial(
        task_id="ingest_overview_once",
        logic_file="thongtincongty",
        df_name="get_overview_batch",
        bucket_name=MINIO_BUCKET,
        object_path="overview_once/{{ ds }}/batch_{{ ti.map_index }}.csv",
        conn_id=MINIO_CONN_ID,
    ).expand(op_kwargs=batches)

    ingest_people = DfToCsvOperator.partial(
        task_id="ingest_people_once",
        logic_file="thongtincongty",
        df_name="get_people_batch",
        bucket_name=MINIO_BUCKET,
        object_path="people_once/{{ ds }}/batch_{{ ti.map_index }}.csv",
        conn_id=MINIO_CONN_ID,
    ).expand(op_kwargs=batches)

    chain(batches, [ingest_bctc, ingest_overview, ingest_people])

stock_dag_once()

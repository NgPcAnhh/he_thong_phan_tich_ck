from datetime import datetime, timedelta
import io

import pandas as pd
from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from function.datalake_df2csv import DfToCsvOperator
from function.s3_compact_parquet import S3CompactToParquetOperator

# Config chung
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

MINIO_BUCKET = "thongtin-congty-va-bctc"
MINIO_CONN_ID = "minio_finance"

@dag(
    dag_id='thongtin_cty_bctc',
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['vnstock', 'finance']
)
def stock_dag():

    # 1. Task lấy danh sách mã (chia thành các batch, mỗi batch 20 mã)
    @task
    def get_batches():
        from logic.list_macp import get_ticker_batches
        return [{"symbols": batch} for batch in get_ticker_batches(batch_size=3)]
    batches = get_batches()

    # 2. Dynamic Task: Map qua danh sách batches để lấy BCTC
    ingest_bctc = DfToCsvOperator.partial(
        task_id="ingest_bctc",
        logic_file="bctc",
        df_name="get_financial_reports", 
        bucket_name=MINIO_BUCKET,
        object_path="bctc/{{ ds }}/batch_{{ ti.map_index }}.csv",
        conn_id=MINIO_CONN_ID
    ).expand(op_kwargs=batches)

    # 3. Dynamic Task: Lấy thông tin cơ bản (Overview)
    ingest_overview = DfToCsvOperator.partial(
        task_id="ingest_overview",
        logic_file="thongtincongty",
        df_name="get_overview_batch",
        bucket_name=MINIO_BUCKET,
        object_path="overview/{{ ds }}/batch_{{ ti.map_index }}.csv",
        conn_id=MINIO_CONN_ID
    ).expand(op_kwargs=batches)

    # 4. Dynamic Task: Lấy thông tin ban lãnh đạo/cổ đông
    ingest_people = DfToCsvOperator.partial(
        task_id="ingest_people",
        logic_file="thongtincongty",
        df_name="get_people_batch",
        bucket_name=MINIO_BUCKET,
        object_path="people/{{ ds }}/batch_{{ ti.map_index }}.csv",
        conn_id=MINIO_CONN_ID
    ).expand(op_kwargs=batches)

    compact_bctc = S3CompactToParquetOperator(
        task_id="compact_bctc",
        bucket_name=MINIO_BUCKET,
        prefix="bctc",
        conn_id=MINIO_CONN_ID,
        min_history=10,
    )

    compact_overview = S3CompactToParquetOperator(
        task_id="compact_overview",
        bucket_name=MINIO_BUCKET,
        prefix="overview",
        conn_id=MINIO_CONN_ID,
        min_history=10,
    )

    compact_people = S3CompactToParquetOperator(
        task_id="compact_people",
        bucket_name=MINIO_BUCKET,
        prefix="people",
        conn_id=MINIO_CONN_ID,
        min_history=10,
    )

    # Luồng chạy
    chain(
        batches,
        [ingest_bctc, ingest_overview, ingest_people],
        [compact_bctc, compact_overview, compact_people],
    )

stock_dag()
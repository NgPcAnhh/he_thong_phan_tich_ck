from datetime import datetime, timedelta
import logging

from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from function.datalake_df2csv import DfToCsvOperator


# Config chung
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

MINIO_BUCKET = "thongtin-congty-va-bctc"
MINIO_CONN_ID = "minio_finance"


@dag(
    dag_id="company_people_to_minio",
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule_interval="@weekly",
    catchup=False,
    tags=["vnstock", "company", "people"],
)
def company_people_dag():
    @task
    def log_batches_info(batches: list[dict]):
        logger = logging.getLogger("airflow.task")
        logger.info("People run bắt đầu với %d batch", len(batches))
        for idx, batch in enumerate(batches):
            logger.info("Batch %d chứa %d mã: %s", idx, len(batch.get("symbols", [])), batch.get("symbols", []))
        return batches

    @task
    def get_batches():
        from logic.list_macp import get_ticker_batches

        return [{"symbols": batch} for batch in get_ticker_batches(batch_size=20)]

    batches = get_batches()
    batches_logged = log_batches_info(batches)

    ingest_people = DfToCsvOperator.partial(
        task_id="ingest_people",
        logic_file="thongtincongty",
        df_name="get_people_batch",
        bucket_name=MINIO_BUCKET,
        object_path="people/date={{ ds }}/batch_{{ ti.map_index }}.csv",
        conn_id=MINIO_CONN_ID,
    ).expand(op_kwargs=batches_logged)

    @task
    def verify_upload(batches: list[dict], ds: str):
        logger = logging.getLogger("airflow.task")
        hook = S3Hook(aws_conn_id=MINIO_CONN_ID)

        missing = []
        for idx, _ in enumerate(batches):
            key = f"people/date={ds}/batch_{idx}.csv"
            exists = hook.check_for_key(key=key, bucket_name=MINIO_BUCKET)
            logger.info("Kiểm tra upload batch %d -> %s: %s", idx, key, "OK" if exists else "MISSING")
            if not exists:
                missing.append(key)

        if missing:
            logger.warning("Các file chưa thấy trên MinIO: %s", missing)
        else:
            logger.info("Tất cả %d file batch đã có trên MinIO bucket %s", len(batches), MINIO_BUCKET)

    verify_task = verify_upload(batches_logged, ds="{{ ds }}")

    chain(batches, batches_logged, ingest_people, verify_task)


company_people_dag()

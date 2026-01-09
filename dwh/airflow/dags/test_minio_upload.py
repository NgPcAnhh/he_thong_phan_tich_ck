from datetime import datetime
import pandas as pd
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

MINIO_CONN_ID = "minio_finance"
MINIO_BUCKET = "thongtin-congty-va-bctc"
OBJECT_KEY = "test_upload/{{ ds }}/sample.csv"


@dag(
    dag_id="test_minio_upload",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["test", "minio"],
)
def test_minio_upload():
    @task
    def upload_sample():
        df = pd.DataFrame([
            {"symbol": "HPG", "price": 100},
            {"symbol": "FPT", "price": 120},
        ])
        csv_bytes = df.to_csv(index=False).encode("utf-8")

        hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
        hook.load_bytes(
            bytes_data=csv_bytes,
            key=OBJECT_KEY,
            bucket_name=MINIO_BUCKET,
            replace=True,
        )
        return f"Uploaded to s3://{MINIO_BUCKET}/{OBJECT_KEY}"

    upload_sample()


test_minio_upload()

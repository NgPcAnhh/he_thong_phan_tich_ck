from __future__ import annotations

from datetime import datetime, timedelta

import pandas as pd
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from logic.fin_ratio_v2 import load_tickers, run_pipeline_to_dataframe


MINIO_BUCKET = Variable.get("minio_bucket", default_var="thongtin-congty-va-bctc")
MINIO_CONN_ID = "minio_finance"


def _save_to_minio(df: pd.DataFrame, partition_folder: str) -> str:
    object_key = f"fin_ratio_v2/{partition_folder}/fin_ratio_v2.csv"
    csv_bytes = df.to_csv(index=False).encode("utf-8-sig")

    hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
    if not hook.check_for_bucket(MINIO_BUCKET):
        hook.create_bucket(bucket_name=MINIO_BUCKET)

    hook.load_bytes(
        bytes_data=csv_bytes,
        key=object_key,
        bucket_name=MINIO_BUCKET,
        replace=True,
    )
    return object_key


@dag(
    dag_id="fin_ratio_v2",
    start_date=datetime(2026, 1, 1),
    schedule="@monthly",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "airflow",
        "retries": 2,
        "retry_delay": timedelta(minutes=3),
    },
    tags=["vnstock", "fin-ratio", "all-in-one", "minio"],
    description="Financial ratio v2 DAG: scrape SMONEY + Vietstock, merge EPS/BVPS, save MinIO",
)
def fin_ratio_v2_dag():
    @task
    def get_partition_folder() -> str:
        now = datetime.utcnow()
        return f"{now.strftime('%Y-%m-%d_%H:%M:%S')}:{int(now.microsecond / 1000):03d}"

    @task
    def run_all_in_one(partition_folder: str) -> str:
        # Keep defaults identical to merge_smoney_vietstock.py
        max_tickers = 0
        smoney_workers = 12
        smoney_retries = 3
        smoney_retry_delay_seconds = 1.0
        vietstock_workers = 8
        vietstock_wait_seconds = 5
        vietstock_retries = 2

        # Keep behavior identical to merge_smoney_vietstock.py default path:
        # load all tickers from plugins/logic/tickers_cache.txt unless max_tickers is set.
        tickers = load_tickers(max_tickers=max_tickers)

        print("=" * 80)
        print("FIN_RATIO_V2 ALL-IN-ONE START")
        print(f"tickers={len(tickers)}, partition_folder={partition_folder}")
        print("=" * 80)

        _, _, final_df = run_pipeline_to_dataframe(
            tickers=tickers,
            smoney_workers=smoney_workers,
            smoney_retries=smoney_retries,
            smoney_retry_delay_seconds=smoney_retry_delay_seconds,
            vietstock_workers=vietstock_workers,
            vietstock_wait_seconds=vietstock_wait_seconds,
            vietstock_retries=vietstock_retries,
        )

        if final_df.empty:
            return "No rows after merge"

        object_key = _save_to_minio(final_df, partition_folder=partition_folder)
        print(f"Saved to MinIO: s3://{MINIO_BUCKET}/{object_key}")

        print("-" * 80)
        print(f"Rows written to MinIO      : {len(final_df):,}")
        print("Database sync              : delegated to minio_to_db_sync DAG")
        print("=" * 80)

        return f"OK | minio_key={object_key} | rows={len(final_df)}"

    partition_folder = get_partition_folder()
    run_all_in_one(partition_folder)


fin_ratio_v2_dag()

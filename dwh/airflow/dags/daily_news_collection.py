from datetime import datetime, timedelta

from airflow.decorators import dag

from function.datalake_df2csv import DfToCsvOperator

MINIO_BUCKET = "thongtin-congty-va-bctc"
MINIO_CONN_ID = "minio_finance"
OBJECT_PATH = "news/daily/{{ ds }}/news_daily_{{ ts_nodash }}.csv"


def _default_args():
    return {
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    }


@dag(
    dag_id="daily_news_collection",
    default_args=_default_args(),
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["vnstock", "news", "minio"],
)
def daily_news_collection():
    DfToCsvOperator(
        task_id="fetch_news_to_csv",
        logic_file="daily_news",
        df_name="fetch_news_df",
        bucket_name=MINIO_BUCKET,
        object_path=OBJECT_PATH,
        conn_id=MINIO_CONN_ID,
        op_kwargs={"target_date": "{{ ds }}"},
    )


daily_news_collection()
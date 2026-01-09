from datetime import datetime, timedelta

from airflow.decorators import dag

from function.datalake_df2csv import DfToCsvOperator

MINIO_BUCKET = "thongtin-congty-va-bctc"
MINIO_CONN_ID = "minio_finance"
OBJECT_PATH = "news/site/{{ ds }}/news_site_{{ ts_nodash }}.csv"


def _default_args():
    return {
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    }


@dag(
    dag_id="news_site_to_minio",
    default_args=_default_args(),
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 18 * * *",  # chạy mỗi ngày 18:00
    catchup=False,
    tags=["news", "sitemap", "minio"],
)
def news_site_to_minio():
    DfToCsvOperator(
        task_id="crawl_and_upload",
        logic_file="news_site_safe",
        df_name="crawl_news_df",
        bucket_name=MINIO_BUCKET,
        object_path=OBJECT_PATH,
        conn_id=MINIO_CONN_ID,
        op_kwargs={"target_date_str": "{{ ds }}"},
    )


news_site_to_minio()

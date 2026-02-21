from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.models import Variable

from function.datalake_df2csv import DfToCsvOperator

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

MINIO_BUCKET = Variable.get(
    "minio_bucket",
    default_var="thongtin-congty-va-bctc",
)

MINIO_CONN_ID = "minio_finance"


@dag(
    dag_id="daily_news",
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule_interval="0 18 * * *",
    catchup=False,
    tags=["cafef", "vnstock", "VnExpress_KinhDoanh", "stock_news"],
)
def daily_news_dag():
    ingest_news = DfToCsvOperator(
        task_id="ingest_news",
        logic_file="news",
        df_name="get_financial_news_today",
        bucket_name=MINIO_BUCKET,
        object_path="news/{{ ds }}/news.csv",
        conn_id=MINIO_CONN_ID,
    )

    ingest_news


daily_news_dag()

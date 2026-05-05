from datetime import timedelta
import pendulum

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

# Cài đặt múi giờ Việt Nam
local_tz = pendulum.timezone("Asia/Ho_Chi_Minh")

@dag(
    dag_id="daily_news",
    default_args=default_args,
    # Cập nhật start_date với timezone cụ thể để cron expression hiểu đúng múi giờ
    start_date=pendulum.datetime(2023, 1, 1, tz=local_tz),
    # 0 phút, từ 9h-19h cách nhau 2 tiếng, mọi ngày trong tháng, mọi tháng, mọi ngày trong tuần
    schedule_interval="0 9-19/2 * * *", 
    catchup=False,
    tags=["cafef", "vnstock", "VnExpress_KinhDoanh", "stock_news"],
)
def daily_news_dag():
    ingest_news = DfToCsvOperator(
        task_id="ingest_news",
        logic_file="news",
        df_name="get_financial_news_today",
        bucket_name=MINIO_BUCKET,
        object_path="news/{{ ds }}/news_{{ ts_nodash }}.csv",
        conn_id=MINIO_CONN_ID,
    )

    ingest_news

daily_news_dag()
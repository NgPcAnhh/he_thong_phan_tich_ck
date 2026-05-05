from datetime import timedelta
import pendulum

from airflow.decorators import dag, task
from airflow.models import Variable

DB_URL = Variable.get(
    "dwh_db_url",
    default_var="postgresql+psycopg2://admin:123456@dwh-postgres:5432/postgres"
)

SCHEMA = Variable.get(
    "dwh_schema",
    default_var="hethong_phantich_chungkhoan"
)

MINIO_BUCKET = Variable.get(
    "minio_bucket",
    default_var="thongtin-congty-va-bctc"
)

MINIO_CONN_ID = "minio_finance"

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# Cài đặt múi giờ Việt Nam
local_tz = pendulum.timezone("Asia/Ho_Chi_Minh")

@dag(
    dag_id="news_sentiment_sync",
    default_args=default_args,
    # Áp dụng timezone cho start_date
    start_date=pendulum.datetime(2026, 1, 1, tz=local_tz),
    # Chạy từ 10h đến 20h, mỗi 2 tiếng 1 lần
    schedule_interval="0 10-20/2 * * *",  
    catchup=False,
    max_active_runs=1,
    tags=["news", "sentiment", "icb", "nlp", "sync"],
    description="Sync news from MinIO to DB, calculate sentiment using PhoBERT, and classify ICB names.",
)
def news_sentiment_sync_dag():
    
    @task(task_id="sync_news_from_minio")
    def task_sync_news():
        from lake_to_dwh.sync_news import sync_news_to_db
        import logging
        logger = logging.getLogger("airflow.task")
        
        logger.info("Starting sync_news_to_db")
        result = sync_news_to_db(
            db_url=DB_URL,
            schema=SCHEMA,
            bucket=MINIO_BUCKET,
            minio_conn_id=MINIO_CONN_ID,
            folder_prefix="news/",
            table="news"
        )
        logger.info(f"Sync result: {result}")
        return result

    @task(task_id="fill_sentiment_scores")
    def task_fill_sentiment():
        from lake_to_dwh.sentiment_processor import fill_sentiment
        import logging
        logger = logging.getLogger("airflow.task")
        
        logger.info("Starting fill_sentiment")
        result = fill_sentiment(
            db_url=DB_URL,
            schema=SCHEMA,
            table="news",
            batch_size=32
        )
        logger.info(f"Fill sentiment result: {result}")
        return result

    @task(task_id="fill_icb_names")
    def task_fill_icb_name():
        from lake_to_dwh.sentiment_processor import fill_icb_name
        import logging
        logger = logging.getLogger("airflow.task")
        
        logger.info("Starting fill_icb_name")
        result = fill_icb_name(
            db_url=DB_URL,
            schema=SCHEMA,
            table="news"
        )
        logger.info(f"Fill ICB name result: {result}")
        return result

    @task(task_id="summary_report")
    def task_summary_report(sync_result: str, sentiment_result: str, icb_result: str):
        import logging
        logger = logging.getLogger("airflow.task")
        
        logger.info("=" * 70)
        logger.info("📊 NEWS SENTIMENT & ICB SYNC - SUMMARY REPORT")
        logger.info("=" * 70)
        logger.info(f"1. Sync News   : {sync_result}")
        logger.info(f"2. Fill Sentiment: {sentiment_result}")
        logger.info(f"3. Fill ICB Name : {icb_result}")
        logger.info("=" * 70)
        return "Complete"

    # Define DAG flow
    sync_res = task_sync_news()
    sentiment_res = task_fill_sentiment()
    icb_res = task_fill_icb_name()
    
    sync_res >> sentiment_res >> icb_res >> task_summary_report(sync_res, sentiment_res, icb_res)

# Instantiate the DAG
news_sentiment_sync_dag()
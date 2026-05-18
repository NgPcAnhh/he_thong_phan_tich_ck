"""
Airflow DAGs for syncing realtime quotes from MinIO to PostgreSQL
and cleaning up old data (retention policy: keep only today + yesterday).

DAG 1: realtime_quotes_minio_to_db_sync
  - Schedule: Every 1 hour from 9:00 AM to 5:00 PM, Mon-Fri
  - Reads Parquet files from MinIO realtime/{date}/ folder
  - UPSERT into realtime_quotes table

DAG 2: realtime_quotes_database_cleanup
  - Schedule: Daily at 23:59 PM
  - Deletes data older than yesterday (N-2 and older)
"""
from datetime import datetime, timedelta, timezone
from airflow.decorators import dag, task
from airflow.models import Variable
import logging

logger = logging.getLogger("airflow.task")

# ============================================================================
# Configuration
# ============================================================================

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

# Vietnam timezone (UTC+7)
VN_TZ = timezone(timedelta(hours=7))

# ============================================================================
# DAG 1: Sync Realtime Quotes from MinIO to Database
# ============================================================================

default_args_sync = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="realtime_quotes_minio_to_db_sync",
    default_args=default_args_sync,
    start_date=datetime(2026, 1, 1),
    schedule_interval="0 9-17 * * 1-5",  # Every 1 hour from 9 AM to 5 PM, Mon-Fri
    catchup=False,
    max_active_runs=1,
    tags=["sync", "realtime_quotes", "minio", "parquet", "dwh"],
    description="Sync realtime quotes from MinIO Parquet files to PostgreSQL (hourly during trading hours)",
)
def realtime_quotes_minio_to_db_sync():

    @task(task_id="sync_realtime_quotes")
    def task_sync_realtime_quotes():
        """
        Read all Parquet files for today's date from MinIO
        and UPSERT into the realtime_quotes table.
        """
        from lake_to_dwh.sync_realtime_quotes import sync_realtime_quotes_to_db

        # Use today's date in Vietnam timezone
        target_date = datetime.now(VN_TZ).strftime("%Y-%m-%d")

        logger.info(f"🚀 Starting realtime quotes sync for date: {target_date}")

        result = sync_realtime_quotes_to_db(
            db_url=DB_URL,
            schema=SCHEMA,
            bucket=MINIO_BUCKET,
            target_date=target_date,
            minio_conn_id=MINIO_CONN_ID,
        )

        logger.info(f"📋 Result: {result}")
        return result

    @task(task_id="summary_report")
    def task_summary_report(sync_result: str):
        """Generate a summary report of the sync operation."""
        logger.info("=" * 70)
        logger.info("📊 REALTIME QUOTES SYNC - SUMMARY REPORT")
        logger.info("=" * 70)
        logger.info(f"Result: {sync_result}")
        logger.info("=" * 70)
        return sync_result

    # Task dependencies
    sync_result = task_sync_realtime_quotes()
    summary = task_summary_report(sync_result)


# ============================================================================
# DAG 2: Cleanup Old Realtime Quotes (Retention: Today + Yesterday only)
# ============================================================================

default_args_cleanup = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}


@dag(
    dag_id="realtime_quotes_database_cleanup",
    default_args=default_args_cleanup,
    start_date=datetime(2026, 1, 1),
    schedule_interval="0 17 * * *",  # Daily at 17:00 PM (5:00 PM)
    catchup=False,
    max_active_runs=1,
    tags=["cleanup", "realtime_quotes", "retention", "dwh"],
    description="Delete realtime_quotes data older than yesterday (keep only today + yesterday)",
)
def realtime_quotes_database_cleanup():

    @task(task_id="cleanup_old_data")
    def task_cleanup_old_data():
        """
        Delete all realtime_quotes records from N-2 days and older.
        Keeps only today's (N) and yesterday's (N-1) data.
        Handles empty results gracefully (0 rows deleted = success).
        """
        from lake_to_dwh.sync_realtime_quotes import cleanup_old_realtime_quotes

        logger.info("🗑️ Starting realtime quotes cleanup...")

        result = cleanup_old_realtime_quotes(
            db_url=DB_URL,
            schema=SCHEMA,
        )

        logger.info(f"📋 Result: {result}")
        return result

    @task(task_id="summary_report")
    def task_summary_report(cleanup_result: str):
        """Generate a summary report of the cleanup operation."""
        logger.info("=" * 70)
        logger.info("🗑️ REALTIME QUOTES CLEANUP - SUMMARY REPORT")
        logger.info("=" * 70)
        logger.info(f"Result: {cleanup_result}")
        logger.info("=" * 70)
        return cleanup_result

    # Task dependencies
    cleanup_result = task_cleanup_old_data()
    summary = task_summary_report(cleanup_result)


# ============================================================================
# Instantiate the DAGs
# ============================================================================
realtime_quotes_minio_to_db_sync()
realtime_quotes_database_cleanup()

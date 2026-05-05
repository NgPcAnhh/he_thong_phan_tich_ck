from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
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

# ============================================================================


default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="minio_to_db_sync",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="0 8-18/2 * * *",  # Chạy lúc 0 phút, mỗi 2 tiếng từ 8h sáng đến 18h
    catchup=False,
    max_active_runs=1,
    tags=["sync", "minio", "database", "etl", "dwh"],
    description="Synchronize all data from MinIO to PostgreSQL database",
)
def minio_to_db_sync():
    @task(task_id="sync_bctc")
    def task_sync_bctc():
        """Sync BCTC from new bctc_luong2 MinIO flow"""
        from lake_to_dwh.sync_bctc import sync_bctc_to_db
        return sync_bctc_to_db(
            minio_conn_id=MINIO_CONN_ID,
            bucket=MINIO_BUCKET,
            db_url=DB_URL,
            schema=SCHEMA,
            folder_prefix="bctc_luong2/"
        )
    
    @task(task_id="sync_daily_price")
    def task_sync_daily_price():
        """Sync daily price data - Append"""
        from lake_to_dwh.sync_daily_price import sync_daily_price_to_db
        return sync_daily_price_to_db(
            minio_conn_id=MINIO_CONN_ID,
            bucket=MINIO_BUCKET,
            db_url=DB_URL,
            schema=SCHEMA
        )
    
    @task(task_id="sync_financial_ratio")
    def task_sync_financial_ratio():
        """Sync fin_ratio_v2 data - Upsert by (ticker, year, quarter)"""
        from lake_to_dwh.sync_fin_ratio_v2 import sync_fin_ratio_v2_to_db
        return sync_fin_ratio_v2_to_db(
            minio_conn_id=MINIO_CONN_ID,
            bucket=MINIO_BUCKET,
            db_url=DB_URL,
            schema=SCHEMA,
            folder_prefix="fin_ratio_v2/"
        )
    
    @task(task_id="sync_global_index")
    def task_sync_global_index():
        """Sync global indices (USD/VND, DXY, etc.) - Append"""
        from lake_to_dwh.sync_global_index import sync_global_index_to_db
        return sync_global_index_to_db(
            minio_conn_id=MINIO_CONN_ID,
            bucket=MINIO_BUCKET,
            db_url=DB_URL,
            schema=SCHEMA
        )
    
    @task(task_id="sync_index_price")
    def task_sync_index_price():
        """Sync market index prices - Append"""
        from lake_to_dwh.sync_index_price import sync_index_price_to_db
        return sync_index_price_to_db(
            minio_conn_id=MINIO_CONN_ID,
            bucket=MINIO_BUCKET,
            db_url=DB_URL,
            schema=SCHEMA
        )
    
    @task(task_id="sync_macro_economy")
    def task_sync_macro_economy():
        """Sync macro economy data (Gold, Oil, Dow Jones) - Append"""
        from lake_to_dwh.sync_macro_economy import sync_macro_economy_to_db
        return sync_macro_economy_to_db(
            minio_conn_id=MINIO_CONN_ID,
            bucket=MINIO_BUCKET,
            db_url=DB_URL,
            schema=SCHEMA
        )
    
    @task(task_id="sync_overview")
    def task_sync_overview():
        """Sync company overview - Upsert"""
        from lake_to_dwh.sync_overview import sync_overview_to_db
        return sync_overview_to_db(
            minio_conn_id=MINIO_CONN_ID,
            bucket=MINIO_BUCKET,
            db_url=DB_URL,
            schema=SCHEMA
        )
    
    @task(task_id="sync_people")
    def task_sync_people():
        """Sync company people/ownership - Delete then Insert"""
        from lake_to_dwh.sync_people import sync_people_to_db
        return sync_people_to_db(
            minio_conn_id=MINIO_CONN_ID,
            bucket=MINIO_BUCKET,
            db_url=DB_URL,
            schema=SCHEMA
        )
    
    @task(task_id="sync_electric_board")
    def task_sync_electric_board():
        """Sync electric board (price board) data - Upsert"""
        from lake_to_dwh.sync_electric_board import sync_electric_board_to_db
        return sync_electric_board_to_db(
            minio_conn_id=MINIO_CONN_ID,
            bucket=MINIO_BUCKET,
            db_url=DB_URL,
            schema=SCHEMA
        )
    
    # ==================== VN MACRO YEARLY (World Bank) ====================
    
    @task(task_id="sync_vn_macro_yearly")
    def task_sync_vn_macro_yearly():
        """Sync Vietnam macro yearly data - Pivot (years→rows, indicators→cols) then TRUNCATE + INSERT"""
        from lake_to_dwh.sync_vn_macro_yearly import sync_vn_macro_yearly_to_db
        return sync_vn_macro_yearly_to_db(
            minio_conn_id=MINIO_CONN_ID,
            bucket=MINIO_BUCKET,
            db_url=DB_URL,
            schema=SCHEMA
        )
    
    @task(task_id="summary_report")
    def task_summary_report(results: list):
        """Generate summary report of all sync operations"""
        import logging
        logger = logging.getLogger("airflow.task")
        
        logger.info("=" * 70)
        logger.info("📊 MINIO TO DB SYNC - SUMMARY REPORT")
        logger.info("=" * 70)
        
        task_names = [
            "BCTC",
            "Daily Price",
            "Financial Ratio",
            "Global Index",
            "Index Price",
            "Macro Economy",
            "Company Overview",
            "Company People",
            "Electric Board",
            "VN Macro Yearly"
        ]
        
        for idx, (task_name, result) in enumerate(zip(task_names, results)):
            logger.info(f"{idx + 1}. {task_name}: {result}")
        
        logger.info("=" * 70)
        
        # Count successes
        success_count = sum(1 for r in results if r and '✅' in str(r))
        logger.info(f"✅ Successful: {success_count}/{len(results)}")
        
        if success_count < len(results):
            logger.warning(f"⚠️ Failed: {len(results) - success_count}/{len(results)}")
        
        return f"Completed: {success_count}/{len(results)} successful"
    
    # Execute all sync tasks
    bctc_result = task_sync_bctc()
    daily_price_result = task_sync_daily_price()
    financial_ratio_result = task_sync_financial_ratio()
    global_index_result = task_sync_global_index()
    index_price_result = task_sync_index_price()
    macro_economy_result = task_sync_macro_economy()
    overview_result = task_sync_overview()
    # people_result = task_sync_people()
    electric_board_result = task_sync_electric_board()
    vn_macro_yearly_result = task_sync_vn_macro_yearly()
    
    # All sync tasks run in parallel (no dependencies)
    # Then generate summary report
    all_results = [
        bctc_result,
        daily_price_result,
        financial_ratio_result,
        global_index_result,
        index_price_result,
        macro_economy_result,
        overview_result,
        # people_result,
        electric_board_result,
        vn_macro_yearly_result,
    ]
    
    summary = task_summary_report(all_results)


# Instantiate the DAG
minio_to_db_sync()
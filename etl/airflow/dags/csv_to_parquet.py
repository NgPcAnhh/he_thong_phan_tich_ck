from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.models import Variable

MINIO_BUCKET = Variable.get(
    "minio_bucket",
    default_var="thongtin-congty-va-bctc"
)

MINIO_CONN_ID = "minio_finance"

# Parquet compression algorithm: snappy (fast), gzip (smaller), brotli (smallest)
COMPRESSION_ALGORITHM = "snappy"

# ============================================================================

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="csv_to_parquet",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    max_active_runs=1,
    tags=["compression", "parquet", "minio", "optimization"],
    description="Compress CSV files in MinIO to Parquet format for efficient storage and querying",
)
def csv_to_parquet_compression():
    """
    Master DAG to compress CSV files to Parquet format.
    
    Runs manually after all MinIO ETL and DB sync jobs have completed.
    All compression tasks run in parallel since they work on different folders.
    """
    
    @task(task_id="compress_history_price")
    def task_compress_history_price():
        """Compress history_price folder to Parquet"""
        from lake_to_dwh.compress_history_price import compress_history_price
        
        result = compress_history_price(
            bucket=MINIO_BUCKET,
            source_folder="history_price",
            target_folder="parquet",
            conn_id=MINIO_CONN_ID,
            compression=COMPRESSION_ALGORITHM
        )
        
        # Return summary
        if result["successful"] > 0:
            return f"✅ history_price: {result['successful']}/{result['total_partitions']} partitions, {result['total_rows']:,} rows"
        else:
            return f"❌ history_price: Failed - {result.get('errors', ['Unknown error'])[0]}"
    
    @task(task_id="compress_financial_ratio")
    def task_compress_financial_ratio():
        """Compress financial_ratio folder to Parquet"""
        from lake_to_dwh.compress_financial_ratio import compress_financial_ratio
        
        result = compress_financial_ratio(
            bucket=MINIO_BUCKET,
            source_folder="financial_ratios",
            target_folder="parquet",
            conn_id=MINIO_CONN_ID,
            compression=COMPRESSION_ALGORITHM
        )
        
        # Return summary
        if result["successful"] > 0:
            return f"✅ financial_ratio: {result['successful']}/{result['total_partitions']} partitions, {result['total_rows']:,} rows"
        else:
            return f"❌ financial_ratio: Failed - {result.get('errors', ['Unknown error'])[0]}"
    
    @task(task_id="compress_bctc")
    def task_compress_bctc():
        """Compress bctc folder to Parquet"""
        from lake_to_dwh.compress_bctc import compress_bctc
        
        result = compress_bctc(
            bucket=MINIO_BUCKET,
            source_folder="bctc",
            target_folder="parquet",
            conn_id=MINIO_CONN_ID,
            compression=COMPRESSION_ALGORITHM
        )
        
        # Return summary
        if result["successful"] > 0:
            return f"✅ bctc: {result['successful']}/{result['total_partitions']} partitions, {result['total_rows']:,} rows"
        else:
            return f"❌ bctc: Failed - {result.get('errors', ['Unknown error'])[0]}"
    
    @task(task_id="summary_report")
    def task_summary_report(results: list):
        """Generate summary report of all compression operations"""
        import logging
        logger = logging.getLogger("airflow.task")
        
        logger.info("=" * 70)
        logger.info("📦 CSV TO PARQUET COMPRESSION - SUMMARY REPORT")
        logger.info("=" * 70)
        
        folder_names = [
            "History Price",
            "Financial Ratio",
            "BCTC"
        ]
        
        for idx, (folder_name, result) in enumerate(zip(folder_names, results)):
            logger.info(f"{idx + 1}. {folder_name}: {result}")
        
        logger.info("=" * 70)
        
        # Count successes
        success_count = sum(1 for r in results if r and '✅' in str(r))
        logger.info(f"✅ Successful: {success_count}/{len(results)}")
        
        if success_count < len(results):
            logger.warning(f"⚠️ Failed: {len(results) - success_count}/{len(results)}")
        
        logger.info(f"🗜️ Compression algorithm: {COMPRESSION_ALGORITHM}")
        logger.info(f"📁 Output location: {MINIO_BUCKET}/parquet/")
        
        return f"Completed: {success_count}/{len(results)} successful"
    
    # Execute all compression tasks in parallel
    history_price_result = task_compress_history_price()
    financial_ratio_result = task_compress_financial_ratio()
    bctc_result = task_compress_bctc()
    
    # All compression tasks run in parallel (no dependencies)
    # Then generate summary report
    all_results = [
        history_price_result,
        financial_ratio_result,
        bctc_result,
    ]
    
    summary = task_summary_report(all_results)


# Instantiate the DAG
csv_to_parquet_compression()

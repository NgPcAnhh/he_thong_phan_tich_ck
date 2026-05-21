from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.models import Param, Variable

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

def get_minio_topics():
    """Fetch root folders from MinIO to populate the topic dropdown."""
    try:
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
        client = hook.get_conn()
        
        # Note: at parse time, Variable.get might cause issues, so we use a safe default
        try:
            bucket_name = Variable.get("minio_bucket", default_var="thongtin-congty-va-bctc")
        except:
            bucket_name = "thongtin-congty-va-bctc"
            
        response = client.list_objects_v2(Bucket=bucket_name, Delimiter='/')
        prefixes = [p.get('Prefix').strip('/') for p in response.get('CommonPrefixes', [])]
        if prefixes:
            return sorted(prefixes)
    except Exception as e:
        import logging
        logging.getLogger("airflow.processor").warning(f"Could not fetch topics from MinIO: {e}")
    
    # Fallback
    return [
        "bctc", "daily-price", "financial-ratios", "news", 
        "overview", "people", "electric-board", "global-index", 
        "index-price", "macro-economy", "vn-macro-yearly"
    ]

# ============================================================================

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

# Define DAG with UI parameters
with DAG(
    dag_id="minio_to_db_manual_sync",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    max_active_runs=1,
    tags=["sync", "minio", "database", "manual", "etl", "dwh"],
    description="Manual sync of specific MinIO folders/partitions to PostgreSQL database",
    params={
        "topic": Param(
            default="bctc",
            type="string",
            description="Topic/folder name in MinIO (e.g., bctc, daily-price, news)",
            enum=get_minio_topics()
        ),
        "partition_path": Param(
            default="latest",
            type="string",
            description="Partition path. Leave as 'latest' to automatically find the newest partition (e.g., date=YYYY-MM-DD)."
        ),
        "sync_mode": Param(
            default="auto",
            type="string",
            description="Sync mode: auto (detect from topic), upsert, replace, append, delete_insert",
            enum=["auto", "upsert", "replace", "append", "delete_insert"]
        ),
        "target_table": Param(
            default="",
            type=["string", "null"],
            description="(Optional) Override target table name. Leave empty to use default mapping."
        ),
        "dry_run": Param(
            default=False,
            type="boolean",
            description="Dry run mode - preview without writing to database"
        )
    }
) as dag:
    
    @task(task_id="validate_and_prepare")
    def task_validate_and_prepare(**context):
        """Validate parameters and prepare sync configuration."""
        import logging
        logger = logging.getLogger("airflow.task")
        
        # Get parameters
        params = context["params"]
        topic = params["topic"]
        partition_path = params["partition_path"]
        sync_mode = params["sync_mode"]
        target_table = params.get("target_table", "").strip()
        dry_run = params["dry_run"]
        
        logger.info("=" * 70)
        logger.info("📋 PARAMETER VALIDATION")
        logger.info("=" * 70)
        logger.info(f"Topic: {topic}")
        logger.info(f"Partition Path: {partition_path}")
        logger.info(f"Sync Mode: {sync_mode}")
        logger.info(f"Target Table: {target_table or '(auto-detect)'}")
        logger.info(f"Dry Run: {dry_run}")
        
        # Construct full folder path
        if partition_path.lower() == "latest":
            from lake_to_dwh.utils import get_latest_partition
            prefix = f"{topic}/" if not topic.endswith("/") else topic
            latest_path = get_latest_partition(MINIO_BUCKET, prefix, MINIO_CONN_ID)
            
            if not latest_path:
                raise ValueError(f"❌ Could not find any valid date partitions in {MINIO_BUCKET}/{prefix}")
                
            folder_path = latest_path
            logger.info(f"\n✓ Auto-detected latest partition: {MINIO_BUCKET}/{folder_path}")
        else:
            # Ensure partition_path doesn't have leading/trailing slashes
            partition_clean = partition_path.strip("/")
            folder_path = f"{topic}/{partition_clean}/"
            logger.info(f"\n✓ Constructed MinIO path: {MINIO_BUCKET}/{folder_path}")
        
        # Prepare configuration
        config = {
            "topic": topic,
            "folder_path": folder_path,
            "sync_mode": None if sync_mode == "auto" else sync_mode,
            "target_table": target_table if target_table else None,
            "dry_run": dry_run,
            "bucket": MINIO_BUCKET,
            "minio_conn_id": MINIO_CONN_ID,
            "db_url": DB_URL,
            "schema": SCHEMA
        }
        
        logger.info("\n✅ Validation complete")
        logger.info("=" * 70)
        
        return config
    
    @task(task_id="list_partition_files")
    def task_list_partition_files(config: dict, **context):
        """List files in the selected partition for verification."""
        import logging
        from lake_to_dwh.utils import get_minio_hook
        
        logger = logging.getLogger("airflow.task")
        
        hook = get_minio_hook(config["minio_conn_id"])
        folder_path = config["folder_path"]
        bucket = config["bucket"]
        
        logger.info("=" * 70)
        logger.info(f"📂 LISTING FILES IN: {bucket}/{folder_path}")
        logger.info("=" * 70)
        
        try:
            keys = hook.list_keys(bucket_name=bucket, prefix=folder_path)
            
            if not keys:
                logger.warning(f"⚠️ No files found in {bucket}/{folder_path}")
                return {
                    "file_count": 0,
                    "csv_count": 0,
                    "files": []
                }
            
            csv_files = [k for k in keys if k.endswith('.csv')]
            
            logger.info(f"\n✓ Total objects: {len(keys)}")
            logger.info(f"✓ CSV files: {len(csv_files)}")
            
            # Show first 10 files
            logger.info("\nFirst 10 files:")
            for i, file in enumerate(csv_files[:10], 1):
                logger.info(f"  {i}. {file}")
            
            if len(csv_files) > 10:
                logger.info(f"  ... and {len(csv_files) - 10} more CSV files")
            
            logger.info("=" * 70)
            
            return {
                "file_count": len(keys),
                "csv_count": len(csv_files),
                "files": csv_files
            }
            
        except Exception as e:
            logger.error(f"❌ Error listing files: {str(e)}")
            raise
    
    @task(task_id="sync_to_database")
    def task_sync_to_database(config: dict, file_info: dict, **context):
        """Execute the actual sync operation."""
        import logging
        from lake_to_dwh.sync_generic import sync_folder_to_db
        
        logger = logging.getLogger("airflow.task")
        
        # Check if there are files to sync
        if file_info["csv_count"] == 0:
            logger.warning("⚠️ No CSV files found. Skipping sync.")
            return "⚠️ No CSV files to sync"
        
        logger.info("=" * 70)
        logger.info("🚀 STARTING SYNC OPERATION")
        logger.info("=" * 70)
        
        # Execute sync
        result = sync_folder_to_db(
            db_url=config["db_url"],
            schema=config["schema"],
            bucket=config["bucket"],
            folder_path=config["folder_path"],
            topic=config["topic"],
            minio_conn_id=config["minio_conn_id"],
            sync_mode=config["sync_mode"],
            target_table=config["target_table"],
            dry_run=config["dry_run"]
        )
        
        logger.info("\n" + "=" * 70)
        logger.info("📊 SYNC COMPLETE")
        logger.info("=" * 70)
        logger.info(f"Result: {result}")
        
        return result
    
    @task(task_id="summary_report")
    def task_summary_report(config: dict, file_info: dict, sync_result: str, **context):
        """Generate summary report of the sync operation."""
        import logging
        logger = logging.getLogger("airflow.task")
        
        logger.info("=" * 70)
        logger.info("📊 MANUAL SYNC - SUMMARY REPORT")
        logger.info("=" * 70)
        
        logger.info("\n📋 Configuration:")
        logger.info(f"  • Topic: {config['topic']}")
        logger.info(f"  • Folder: {config['folder_path']}")
        logger.info(f"  • Sync Mode: {config['sync_mode'] or 'auto-detect'}")
        logger.info(f"  • Target Table: {config['target_table'] or 'auto-detect'}")
        logger.info(f"  • Dry Run: {config['dry_run']}")
        
        logger.info("\n📂 Files:")
        logger.info(f"  • Total objects: {file_info['file_count']}")
        logger.info(f"  • CSV files: {file_info['csv_count']}")
        
        logger.info("\n📤 Sync Result:")
        logger.info(f"  {sync_result}")
        
        logger.info("\n" + "=" * 70)
        
        if "✅" in sync_result:
            logger.info("✅ Sync completed successfully!")
        elif "⚠️" in sync_result:
            logger.warning("⚠️ Sync completed with warnings")
        else:
            logger.error("❌ Sync failed or incomplete")
        
        logger.info("=" * 70)
        
        return {
            "config": config,
            "file_info": file_info,
            "sync_result": sync_result
        }
    
    # Define task flow
    config = task_validate_and_prepare()
    file_info = task_list_partition_files(config)
    sync_result = task_sync_to_database(config, file_info)
    summary = task_summary_report(config, file_info, sync_result)

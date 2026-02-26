from datetime import datetime, timedelta
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

# Number of days to keep in the database (archive older data)
RETENTION_DAYS = 3

# ============================================================================
# DAG Definition
# ============================================================================

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="orderbook_backto_minio_parquet",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="0 2 * * *",  # Daily at 02:00 AM
    catchup=False,
    max_active_runs=1,
    tags=["archive", "realtime_quotes", "minio", "parquet", "dwh"],
    description="Archive realtime_quotes data older than 3 days to MinIO as parquet files",
)
def archive_realtime_quotes():
    
    @task(task_id="extract_old_data")
    def extract_old_data():
        """Extract data older than RETENTION_DAYS from realtime_quotes table"""
        import pandas as pd
        from sqlalchemy import create_engine, text
        
        logger.info(f"Connecting to database: {DB_URL}")
        engine = create_engine(DB_URL)
        
        # Calculate cutoff date
        cutoff_date = datetime.now() - timedelta(days=RETENTION_DAYS)
        logger.info(f"Extracting data older than: {cutoff_date}")
        
        # Query to extract old data
        query = text(f"""
            SELECT *
            FROM {SCHEMA}.realtime_quotes
            WHERE ts < :cutoff_date
            ORDER BY ts
        """)
        
        try:
            with engine.connect() as conn:
                # Extract data to DataFrame
                df = pd.read_sql(query, conn, params={"cutoff_date": cutoff_date})
                
                logger.info(f"Extracted {len(df)} records from realtime_quotes")
                
                if len(df) == 0:
                    logger.info("No data to archive. Skipping.")
                    return {
                        "status": "skipped",
                        "records_count": 0,
                        "message": "No data older than 3 days found"
                    }
                
                # Convert timestamp columns to appropriate format
                if 'timestamp' in df.columns:
                    df['timestamp'] = pd.to_datetime(df['timestamp'])
                
                # Return metadata along with the DataFrame
                return {
                    "status": "success",
                    "records_count": len(df),
                    "data": df,
                    "min_date": df['timestamp'].min() if 'timestamp' in df.columns else None,
                    "max_date": df['timestamp'].max() if 'timestamp' in df.columns else None
                }
                
        except Exception as e:
            logger.error(f"Error extracting data: {str(e)}")
            raise
        finally:
            engine.dispose()
    
    @task(task_id="save_to_minio")
    def save_to_minio(extract_result: dict):
        """Save extracted data to MinIO as parquet files"""
        import pandas as pd
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        import io
        
        if extract_result["status"] == "skipped":
            logger.info("Skipping MinIO upload - no data to archive")
            return {
                "status": "skipped",
                "message": "No data to upload"
            }
        
        df = extract_result["data"]
        
        # Create S3 Hook (MinIO is S3-compatible)
        s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
        
        # Generate filename with timestamp
        current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_name = f"parquet/order_book/realtime_quotes_{current_time}.parquet"
        
        logger.info(f"Saving {len(df)} records to MinIO: {file_name}")
        
        try:
            # Convert DataFrame to parquet in memory
            parquet_buffer = io.BytesIO()
            df.to_parquet(parquet_buffer, engine='pyarrow', compression='snappy', index=False)
            parquet_buffer.seek(0)
            
            # Upload to MinIO
            s3_hook.load_file_obj(
                file_obj=parquet_buffer,
                key=file_name,
                bucket_name=MINIO_BUCKET,
                replace=True
            )
            
            logger.info(f"✅ Successfully uploaded {file_name} to MinIO")
            
            return {
                "status": "success",
                "file_name": file_name,
                "records_count": len(df),
                "min_date": str(extract_result["min_date"]),
                "max_date": str(extract_result["max_date"]),
                "file_size_mb": round(parquet_buffer.tell() / (1024 * 1024), 2)
            }
            
        except Exception as e:
            logger.error(f"Error uploading to MinIO: {str(e)}")
            raise
    
    @task(task_id="delete_archived_data")
    def delete_archived_data(upload_result: dict):
        """Delete archived data from the database after successful upload"""
        from sqlalchemy import create_engine, text
        
        if upload_result["status"] == "skipped":
            logger.info("Skipping deletion - no data was archived")
            return {
                "status": "skipped",
                "message": "No data to delete"
            }
        
        logger.info("Deleting archived data from realtime_quotes table")
        engine = create_engine(DB_URL)
        
        # Calculate cutoff date
        cutoff_date = datetime.now() - timedelta(days=RETENTION_DAYS)
        
        delete_query = text(f"""
            DELETE FROM {SCHEMA}.realtime_quotes
            WHERE timestamp < :cutoff_date
        """)
        
        try:
            with engine.connect() as conn:
                result = conn.execute(delete_query, {"cutoff_date": cutoff_date})
                conn.commit()
                deleted_count = result.rowcount
                
                logger.info(f"✅ Deleted {deleted_count} records from realtime_quotes")
                
                return {
                    "status": "success",
                    "deleted_count": deleted_count,
                    "cutoff_date": str(cutoff_date)
                }
                
        except Exception as e:
            logger.error(f"Error deleting data: {str(e)}")
            raise
        finally:
            engine.dispose()
    
    @task(task_id="summary_report")
    def summary_report(delete_result: dict, upload_result: dict):
        """Generate summary report of the archival process"""
        logger.info("=" * 70)
        logger.info("📦 REALTIME QUOTES ARCHIVE - SUMMARY REPORT")
        logger.info("=" * 70)
        
        if upload_result["status"] == "skipped":
            logger.info("ℹ️ No data to archive - all data is within retention period")
            return "No data archived - retention period not exceeded"
        
        logger.info(f"📁 File: {upload_result['file_name']}")
        logger.info(f"📊 Records Archived: {upload_result['records_count']}")
        logger.info(f"📅 Date Range: {upload_result['min_date']} to {upload_result['max_date']}")
        logger.info(f"💾 File Size: {upload_result['file_size_mb']} MB")
        logger.info(f"🗑️ Records Deleted: {delete_result['deleted_count']}")
        logger.info("=" * 70)
        logger.info("✅ Archive process completed successfully!")
        
        return f"Archived {upload_result['records_count']} records to {upload_result['file_name']}"
    
    # Task dependencies
    extract_result = extract_old_data()
    upload_result = save_to_minio(extract_result)
    delete_result = delete_archived_data(upload_result)
    summary = summary_report(delete_result, upload_result)


# Instantiate the DAG
archive_realtime_quotes()

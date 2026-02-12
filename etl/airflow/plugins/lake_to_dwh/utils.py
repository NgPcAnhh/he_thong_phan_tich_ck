import io
from typing import List, Optional, Tuple
import pandas as pd
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
import psycopg2
from psycopg2.extensions import connection as PGConnection
# ==================== MinIO Helpers ====================

# lấy hook của minio
def get_minio_hook(conn_id: str = "minio_finance") -> S3Hook:
    return S3Hook(aws_conn_id=conn_id)


def get_latest_partition(bucket: str, prefix: str, conn_id: str = "minio_finance") -> Optional[str]:
    import re
    
    hook = get_minio_hook(conn_id)
    
    # Regex pattern for YYYY-MM-DD format
    DATE_PATTERN = re.compile(r'^\d{4}-\d{2}-\d{2}$')
    
    try:
        # List all objects with the prefix
        keys = hook.list_keys(bucket_name=bucket, prefix=prefix)
        
        if not keys:
            print(f"⚠️ No objects found in {bucket}/{prefix}")
            return None
        
        print(f"📂 Scanning {len(keys)} objects in {bucket}/{prefix}")
        
        # Extract unique date partitions with validation
        partitions = {}  # {date_str: sample_key}
        
        for key in keys:
            # Remove prefix and get first part (date partition)
            relative_path = key.replace(prefix, "").lstrip("/")
            
            if not relative_path or "/" not in relative_path:
                continue
            
            # Get first level folder (partition)
            first_folder = relative_path.split("/")[0]
            
            # Extract date value
            date_value = None
            
            # Case 1: date=YYYY-MM-DD format
            if first_folder.startswith("date="):
                date_value = first_folder[5:]  # Remove 'date=' prefix
            # Case 2: dt=YYYY-MM-DD format (alternative)
            elif first_folder.startswith("dt="):
                date_value = first_folder[3:]  # Remove 'dt=' prefix
            # Case 3: Direct YYYY-MM-DD format
            else:
                date_value = first_folder
            
            # Validate date format (YYYY-MM-DD)
            if date_value and DATE_PATTERN.match(date_value):
                # Store with sample key for path reconstruction
                if date_value not in partitions:
                    partitions[date_value] = key
            else:
                # Skip non-date folders
                continue
        
        if not partitions:
            print(f"⚠️ No valid date partitions (YYYY-MM-DD) found in {bucket}/{prefix}")
            return None
        
        # Get the latest partition (max works correctly for YYYY-MM-DD string format)
        latest_date = max(partitions.keys())
        sample_key = partitions[latest_date]
        
        print(f"📅 Found {len(partitions)} valid date partition(s): {sorted(partitions.keys())}")
        
        # Reconstruct the full path based on original format
        # Check which format was used in the original key
        if f"date={latest_date}" in sample_key:
            latest_path = f"{prefix}date={latest_date}/"
        elif f"dt={latest_date}" in sample_key:
            latest_path = f"{prefix}dt={latest_date}/"
        else:
            latest_path = f"{prefix}{latest_date}/"
        
        print(f"✅ Latest partition selected: {latest_path}")
        return latest_path
        
    except Exception as e:
        print(f"❌ Error finding latest partition: {str(e)}")
        import traceback
        traceback.print_exc()
        return None


def get_all_partitions(bucket: str, prefix: str, conn_id: str = "minio_finance") -> List[str]:
    """
    Find all date partitions in MinIO bucket.
    
    Supports partition formats:
    - YYYY-MM-DD/  (e.g., daily_price/2026-02-04/)
    - date=YYYY-MM-DD/  (e.g., bctc/date=2026-02-04/)
    
    Args:
        bucket: MinIO bucket name
        prefix: Folder prefix to search in
        conn_id: MinIO connection ID
    
    Returns:
        Sorted list of partition paths (oldest to newest)
    """
    import re
    
    hook = get_minio_hook(conn_id)
    
    # Regex pattern for YYYY-MM-DD format
    DATE_PATTERN = re.compile(r'^\d{4}-\d{2}-\d{2}$')
    
    try:
        # List all objects with the prefix
        keys = hook.list_keys(bucket_name=bucket, prefix=prefix)
        
        if not keys:
            print(f"⚠️ No objects found in {bucket}/{prefix}")
            return []
        
        print(f"📂 Scanning {len(keys)} objects in {bucket}/{prefix}")
        
        # Extract unique date partitions with validation
        partitions_dict = {}  # {date_str: partition_path}
        
        for key in keys:
            # Remove prefix and get partition part
            relative_path = key.replace(prefix, "").lstrip("/")
            
            if not relative_path or "/" not in relative_path:
                continue
            
            # Get first level partition (usually date)
            first_folder = relative_path.split("/")[0]
            
            # Extract date value
            date_value = None
            partition_format = None
            
            # Case 1: date=YYYY-MM-DD format
            if first_folder.startswith("date="):
                date_value = first_folder[5:]
                partition_format = "date="
            # Case 2: dt=YYYY-MM-DD format (alternative)
            elif first_folder.startswith("dt="):
                date_value = first_folder[3:]
                partition_format = "dt="
            # Case 3: Direct YYYY-MM-DD format
            else:
                date_value = first_folder
                partition_format = ""
            
            # Validate date format (YYYY-MM-DD)
            if date_value and DATE_PATTERN.match(date_value):
                # Reconstruct partition path
                partition_path = f"{prefix}{partition_format}{date_value}/"
                
                # Store unique partition
                if date_value not in partitions_dict:
                    partitions_dict[date_value] = partition_path
        
        if not partitions_dict:
            print(f"⚠️ No valid date partitions (YYYY-MM-DD) found in {bucket}/{prefix}")
            return []
        
        # Sort by date (oldest first)
        sorted_dates = sorted(partitions_dict.keys())
        all_partitions = [partitions_dict[date] for date in sorted_dates]
        
        print(f"✅ Found {len(all_partitions)} valid partition(s): {sorted_dates[0]} to {sorted_dates[-1]}")
        
        return all_partitions
        
    except Exception as e:
        print(f"❌ Error finding partitions: {str(e)}")
        import traceback
        traceback.print_exc()
        return []


def read_all_csvs_from_all_partitions(
    bucket: str, 
    prefix: str, 
    conn_id: str = "minio_finance"
) -> pd.DataFrame:
    print("="*70)
    print(f"📊 SCANNING ALL PARTITIONS IN {bucket}/{prefix}")
    print("="*70)
    
    # Get all partitions
    partitions = get_all_partitions(bucket, prefix, conn_id)
    
    if not partitions:
        print(f"⚠️ No partitions found in {bucket}/{prefix}")
        return pd.DataFrame()
    
    print(f"\n📂 Found {len(partitions)} partitions to scan:")
    for i, part in enumerate(partitions[:5], 1):  # Show first 5
        print(f"  {i}. {part}")
    if len(partitions) > 5:
        print(f"  ... and {len(partitions) - 5} more")
    
    # Read from all partitions
    all_dfs = []
    for i, partition in enumerate(partitions, 1):
        print(f"\n[{i}/{len(partitions)}] Reading partition: {partition}")
        df = read_all_csvs_from_folder(bucket, partition, conn_id)
        if not df.empty:
            all_dfs.append(df)
            print(f"  ✓ Loaded {len(df)} rows from this partition")
    
    if not all_dfs:
        print("\n⚠️ No data found in any partition")
        return pd.DataFrame()
    
    # Concatenate all DataFrames
    result = pd.concat(all_dfs, ignore_index=True)
    print("\n" + "="*70)
    print(f"✅ TOTAL: {len(result)} rows from {len(all_dfs)} partitions")
    print("="*70)
    
    return result



def list_csv_files(bucket: str, folder: str, conn_id: str = "minio_finance") -> List[str]:
    hook = get_minio_hook(conn_id)
    
    try:
        keys = hook.list_keys(bucket_name=bucket, prefix=folder)
        csv_files = [k for k in keys if k.endswith('.csv')]
        print(f"✓ Found {len(csv_files)} CSV files in {bucket}/{folder}")
        return csv_files
    except Exception as e:
        print(f"❌ Error listing CSV files: {str(e)}")
        return []


def read_csv_from_minio(bucket: str, key: str, conn_id: str = "minio_finance") -> pd.DataFrame:
    hook = get_minio_hook(conn_id)
    
    try:
        content = hook.read_key(key=key, bucket_name=bucket)
        if not content:
            print(f"⚠️ Empty file: {key}")
            return pd.DataFrame()
        
        df = pd.read_csv(io.StringIO(content))
        print(f"✓ Read {len(df)} rows from {key}")
        return df
    except Exception as e:
        print(f"❌ Error reading CSV from MinIO: {str(e)}")
        return pd.DataFrame()


def read_all_csvs_from_folder(bucket: str, folder: str, conn_id: str = "minio_finance") -> pd.DataFrame:
    csv_files = list_csv_files(bucket, folder, conn_id)
    
    if not csv_files:
        print(f"⚠️ No CSV files found in {bucket}/{folder}")
        return pd.DataFrame()
    
    dfs = []
    for csv_file in csv_files:
        df = read_csv_from_minio(bucket, csv_file, conn_id)
        if not df.empty:
            dfs.append(df)
    
    if not dfs:
        print(f"⚠️ No valid data found in CSV files")
        return pd.DataFrame()
    
    result = pd.concat(dfs, ignore_index=True)
    print(f"✓ Concatenated {len(dfs)} files into {len(result)} total rows")
    return result


# ==================== Database Helpers ====================

def get_postgres_connection(
    db_url: str = "postgresql+psycopg2://admin:123456@localhost:5432/postgres"
) -> PGConnection:
    try:
        # Parse the connection URL
        # Format: postgresql+psycopg2://user:password@host:port/database
        db_url = db_url.replace("postgresql+psycopg2://", "postgresql://")
        conn = psycopg2.connect(db_url)
        return conn
    except Exception as e:
        print(f"❌ Error connecting to database: {str(e)}")
        raise


def execute_query(conn: PGConnection, query: str, params: Optional[tuple] = None) -> None:
    try:
        with conn.cursor() as cur:
            if params:
                cur.execute(query, params)
            else:
                cur.execute(query)
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"❌ Error executing query: {str(e)}")
        raise


def ensure_schema(conn: PGConnection, schema: str) -> None:

    try:
        with conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
        conn.commit()
        print(f"✓ Schema {schema} ensured")
    except Exception as e:
        conn.rollback()
        print(f"❌ Error creating schema: {str(e)}")
        raise


# ==================== Data Transformation Helpers ====================

def normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = df.columns.str.lower().str.replace(' ', '_').str.replace('-', '_')
    return df


def clean_dataframe(df: pd.DataFrame, required_columns: Optional[List[str]] = None) -> pd.DataFrame:

    if df.empty:
        return df
    
    df = df.copy()
    
    # Remove rows where all values are null
    df = df.dropna(how='all')
    
    # Remove rows where required columns are null
    if required_columns:
        df = df.dropna(subset=required_columns)
    
    # Remove duplicate rows
    df = df.drop_duplicates()
    
    return df


def standardize_ticker(df: pd.DataFrame, ticker_column: str = 'ticker') -> pd.DataFrame:
    if df.empty or ticker_column not in df.columns:
        return df
    
    df = df.copy()
    df[ticker_column] = df[ticker_column].astype(str).str.upper().str.strip()
    return df


def parse_trading_date(df: pd.DataFrame, date_column: str = 'trading_date') -> pd.DataFrame:
    if df.empty or date_column not in df.columns:
        return df
    
    df = df.copy()
    df[date_column] = pd.to_datetime(df[date_column], errors='coerce').dt.date
    df = df.dropna(subset=[date_column])
    return df

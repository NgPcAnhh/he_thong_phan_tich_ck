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
    hook = get_minio_hook(conn_id)
    
    try:
        # List all objects with the prefix
        keys = hook.list_keys(bucket_name=bucket, prefix=prefix)
        
        if not keys:
            print(f"⚠️ No objects found in {bucket}/{prefix}")
            return None
        
        # Extract unique date partitions
        partitions = set()
        for key in keys:
            # Remove prefix and get first part (date partition)
            relative_path = key.replace(prefix, "").lstrip("/")
            if "/" in relative_path:
                partition = relative_path.split("/")[0]
                # Check if it looks like a date (YYYY-MM-DD or date=YYYY-MM-DD)
                if "=" in partition:
                    partition = partition.split("=")[1]
                partitions.add(partition)
        
        if not partitions:
            print(f"⚠️ No date partitions found in {bucket}/{prefix}")
            return None
        
        # Get the latest partition (assuming YYYY-MM-DD format)
        latest = max(partitions)
        
        # Reconstruct the full path
        # Check if original structure uses "date=" prefix
        sample_key = next(k for k in keys if latest in k)
        if f"date={latest}" in sample_key:
            latest_path = f"{prefix}date={latest}/"
        else:
            latest_path = f"{prefix}{latest}/"
        
        print(f"✓ Latest partition found: {latest_path}")
        return latest_path
        
    except Exception as e:
        print(f"❌ Error finding latest partition: {str(e)}")
        return None


def get_all_partitions(bucket: str, prefix: str, conn_id: str = "minio_finance") -> List[str]:
    hook = get_minio_hook(conn_id)
    
    try:
        # List all objects with the prefix
        keys = hook.list_keys(bucket_name=bucket, prefix=prefix)
        
        if not keys:
            print(f"⚠️ No objects found in {bucket}/{prefix}")
            return []
        
        # Extract unique partitions
        partition_set = set()
        for key in keys:
            # Remove prefix and get partition part
            relative_path = key.replace(prefix, "").lstrip("/")
            
            if not relative_path or not "/" in relative_path:
                continue
            
            # Get first level partition (usually date)
            parts = relative_path.split("/")
            first_partition = parts[0]
            
            # Handle date= prefix if exists
            if "=" in first_partition:
                partition_value = first_partition.split("=")[1]
            else:
                partition_value = first_partition
            
            # Only add if it looks like a date  (YYYY-MM-DD format)
            if len(partition_value) == 10 and partition_value.count("-") == 2:
                # Reconstruct partition path matching original structure
                if f"date={partition_value}" in key:
                    partition_path = f"{prefix}date={partition_value}/"
                else:
                    partition_path = f"{prefix}{partition_value}/"
                partition_set.add(partition_path)
        
        if not partition_set:
            print(f"⚠️ No date partitions found in {bucket}/{prefix}")
            return []
        
        # Sort partitions (oldest first)
        all_partitions = sorted(list(partition_set))
        print(f"✓ Found {len(all_partitions)} partitions in {prefix}")
        
        return all_partitions
        
    except Exception as e:
        print(f"❌ Error finding partitions: {str(e)}")
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

import io
from typing import List, Optional, Dict, Any
import pandas as pd
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def get_minio_hook(conn_id: str = "minio_finance") -> S3Hook:
    """Get S3Hook for MinIO connection."""
    return S3Hook(aws_conn_id=conn_id)


def list_all_partitions(
    bucket: str,
    folder_prefix: str,
    conn_id: str = "minio_finance"
) -> List[str]:
    """
    List all date partitions in a MinIO folder.
    
    Args:
        bucket: MinIO bucket name
        folder_prefix: Folder prefix (e.g., "history_price/", "bctc/")
        conn_id: MinIO connection ID
    
    Returns:
        List of partition paths (e.g., ["history_price/2026-01-13/", "history_price/2026-01-14/"])
    """
    hook = get_minio_hook(conn_id)
    
    try:
        # List all objects with the prefix
        keys = hook.list_keys(bucket_name=bucket, prefix=folder_prefix)
        
        if not keys:
            print(f"⚠️ No objects found in {bucket}/{folder_prefix}")
            return []
        
        # Extract unique date partitions
        partitions = set()
        for key in keys:
            # Remove prefix and get first part (date partition)
            relative_path = key.replace(folder_prefix, "").lstrip("/")
            if "/" in relative_path:
                partition = relative_path.split("/")[0]
                # Check if it looks like a date (YYYY-MM-DD or date=YYYY-MM-DD)
                if "=" in partition:
                    partition_value = partition.split("=")[1]
                    partitions.add(f"{folder_prefix}{partition}/")
                else:
                    # Simple date format
                    partitions.add(f"{folder_prefix}{partition}/")
        
        if not partitions:
            print(f"⚠️ No date partitions found in {bucket}/{folder_prefix}")
            return []
        
        # Sort partitions by date
        sorted_partitions = sorted(list(partitions))
        print(f"✓ Found {len(sorted_partitions)} partitions in {bucket}/{folder_prefix}")
        
        return sorted_partitions
        
    except Exception as e:
        print(f"❌ Error listing partitions: {str(e)}")
        return []


def read_partition_csvs(
    bucket: str,
    partition_path: str,
    conn_id: str = "minio_finance"
) -> pd.DataFrame:
    """
    Read and concatenate all CSV files in a partition.
    
    Args:
        bucket: MinIO bucket name
        partition_path: Full partition path (e.g., "history_price/2026-01-13/")
        conn_id: MinIO connection ID
    
    Returns:
        Concatenated DataFrame from all CSV files in the partition
    """
    hook = get_minio_hook(conn_id)
    
    try:
        # List all CSV files in the partition
        keys = hook.list_keys(bucket_name=bucket, prefix=partition_path)
        csv_files = [k for k in keys if k.endswith('.csv')]
        
        if not csv_files:
            print(f"⚠️ No CSV files found in {bucket}/{partition_path}")
            return pd.DataFrame()
        
        print(f"✓ Found {len(csv_files)} CSV files in {partition_path}")
        
        # Read all CSV files
        dfs = []
        for csv_file in csv_files:
            try:
                content = hook.read_key(key=csv_file, bucket_name=bucket)
                if content:
                    df = pd.read_csv(io.StringIO(content))
                    if not df.empty:
                        dfs.append(df)
                        print(f"  ✓ Read {len(df)} rows from {csv_file.split('/')[-1]}")
            except Exception as e:
                print(f"  ⚠️ Error reading {csv_file}: {str(e)}")
                continue
        
        if not dfs:
            print(f"⚠️ No valid data found in CSV files")
            return pd.DataFrame()
        
        # Concatenate all DataFrames
        result = pd.concat(dfs, ignore_index=True)
        print(f"✓ Consolidated {len(dfs)} files into {len(result)} total rows")
        
        return result
        
    except Exception as e:
        print(f"❌ Error reading partition CSVs: {str(e)}")
        return pd.DataFrame()


def write_parquet_to_minio(
    df: pd.DataFrame,
    bucket: str,
    parquet_path: str,
    conn_id: str = "minio_finance",
    compression: str = "snappy"
) -> bool:
    """
    Write DataFrame as Parquet file to MinIO.
    
    Args:
        df: DataFrame to write
        bucket: MinIO bucket name
        parquet_path: Full path for Parquet file (e.g., "parquet/history_price/2026-01-13/data.parquet")
        conn_id: MinIO connection ID
        compression: Parquet compression algorithm (snappy, gzip, brotli, etc.)
    
    Returns:
        True if successful, False otherwise
    """
    if df.empty:
        print("⚠️ Empty DataFrame, skipping write")
        return False
    
    hook = get_minio_hook(conn_id)
    
    try:
        # Convert DataFrame to Parquet in memory
        parquet_buffer = io.BytesIO()
        df.to_parquet(
            parquet_buffer,
            engine='pyarrow',
            compression=compression,
            index=False
        )
        parquet_buffer.seek(0)
        
        # Get file size for logging
        parquet_size = len(parquet_buffer.getvalue())
        parquet_size_mb = parquet_size / (1024 * 1024)
        
        # Upload to MinIO
        hook.load_bytes(
            bytes_data=parquet_buffer.getvalue(),
            key=parquet_path,
            bucket_name=bucket,
            replace=True
        )
        
        print(f"✅ Wrote Parquet file: {parquet_path}")
        print(f"   Size: {parquet_size_mb:.2f} MB | Rows: {len(df):,} | Compression: {compression}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error writing Parquet to MinIO: {str(e)}")
        return False


def compress_partition_to_parquet(
    bucket: str,
    source_partition: str,
    target_folder: str,
    conn_id: str = "minio_finance",
    compression: str = "snappy"
) -> Dict[str, Any]:
    """
    Compress a single partition from CSV to Parquet.
    
    Args:
        bucket: MinIO bucket name
        source_partition: Source partition path (e.g., "history_price/2026-01-13/")
        target_folder: Target folder name (e.g., "parquet")
        conn_id: MinIO connection ID
        compression: Parquet compression algorithm
    
    Returns:
        Dictionary with compression statistics
    """
    print(f"\n{'='*70}")
    print(f"📦 Compressing partition: {source_partition}")
    print(f"{'='*70}")
    
    stats = {
        "partition": source_partition,
        "success": False,
        "rows": 0,
        "csv_files": 0,
        "parquet_path": None,
        "error": None
    }
    
    try:
        # Step 1: Read all CSV files in the partition
        df = read_partition_csvs(bucket, source_partition, conn_id)
        
        if df.empty:
            stats["error"] = "No data found in partition"
            return stats
        
        stats["rows"] = len(df)
        
        # Step 2: Construct target Parquet path
        # Extract folder name and partition date from source_partition
        # Example: "history_price/2026-01-13/" -> "parquet/history_price/2026-01-13/data.parquet"
        parts = source_partition.rstrip("/").split("/")
        if len(parts) >= 2:
            folder_name = parts[0]
            partition_date = parts[1].replace("date=", "")
            parquet_path = f"{target_folder}/{folder_name}/{partition_date}/data.parquet"
        else:
            stats["error"] = "Invalid partition path format"
            return stats
        
        stats["parquet_path"] = parquet_path
        
        # Step 3: Write Parquet file to MinIO
        success = write_parquet_to_minio(
            df=df,
            bucket=bucket,
            parquet_path=parquet_path,
            conn_id=conn_id,
            compression=compression
        )
        
        stats["success"] = success
        
        if success:
            print(f"✅ Successfully compressed {source_partition}")
        else:
            stats["error"] = "Failed to write Parquet file"
        
        return stats
        
    except Exception as e:
        error_msg = str(e)
        print(f"❌ Error compressing partition: {error_msg}")
        stats["error"] = error_msg
        return stats

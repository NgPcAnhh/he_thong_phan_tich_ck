"""
Compression logic for bctc (financial reports) data from CSV to Parquet.
Consolidates all batch CSV files within each partition into a single Parquet file.
"""
from typing import Dict, Any
from lake_to_dwh.parquet_compression_utils import (
    list_all_partitions,
    compress_partition_to_parquet
)


def compress_bctc(
    bucket: str,
    source_folder: str = "bctc",
    target_folder: str = "parquet",
    conn_id: str = "minio_finance",
    compression: str = "snappy"
) -> Dict[str, Any]:
    """
    Compress all bctc partitions from CSV to Parquet.
    
    Args:
        bucket: MinIO bucket name
        source_folder: Source folder name (default: "bctc")
        target_folder: Target folder name (default: "parquet")
        conn_id: MinIO connection ID
        compression: Parquet compression algorithm
    
    Returns:
        Dictionary with compression results and statistics
    """
    print("=" * 70)
    print("📊 COMPRESSING BCTC DATA TO PARQUET")
    print("=" * 70)
    
    result = {
        "folder": source_folder,
        "total_partitions": 0,
        "successful": 0,
        "failed": 0,
        "total_rows": 0,
        "partition_results": [],
        "errors": []
    }
    
    try:
        # Step 1: List all partitions
        print(f"\n[1/2] Listing partitions in {source_folder}...")
        folder_prefix = f"{source_folder}/"
        partitions = list_all_partitions(bucket, folder_prefix, conn_id)
        
        if not partitions:
            result["errors"].append("No partitions found")
            return result
        
        result["total_partitions"] = len(partitions)
        print(f"✓ Found {len(partitions)} partitions to compress")
        
        # Step 2: Compress each partition
        print(f"\n[2/2] Compressing partitions...")
        for partition in partitions:
            stats = compress_partition_to_parquet(
                bucket=bucket,
                source_partition=partition,
                target_folder=target_folder,
                conn_id=conn_id,
                compression=compression
            )
            
            result["partition_results"].append(stats)
            
            if stats["success"]:
                result["successful"] += 1
                result["total_rows"] += stats["rows"]
            else:
                result["failed"] += 1
                if stats.get("error"):
                    result["errors"].append(f"{partition}: {stats['error']}")
        
        # Summary
        print("\n" + "=" * 70)
        print("📊 BCTC COMPRESSION SUMMARY")
        print("=" * 70)
        print(f"Total partitions: {result['total_partitions']}")
        print(f"✅ Successful: {result['successful']}")
        print(f"❌ Failed: {result['failed']}")
        print(f"📝 Total rows compressed: {result['total_rows']:,}")
        
        if result["errors"]:
            print(f"\n⚠️ Errors encountered:")
            for error in result["errors"]:
                print(f"  - {error}")
        
        print("=" * 70)
        
        return result
        
    except Exception as e:
        error_msg = str(e)
        print(f"❌ Fatal error in bctc compression: {error_msg}")
        result["errors"].append(error_msg)
        return result

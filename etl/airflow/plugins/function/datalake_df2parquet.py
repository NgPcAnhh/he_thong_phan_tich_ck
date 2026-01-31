"""
Custom operator to fetch data, save to CSV, compress, and save as Parquet with partitioning.
"""
from __future__ import annotations
import importlib.util
import io
import gzip
import sys
from pathlib import Path
from typing import Callable, Optional, Any

import pandas as pd
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


class DfToParquetOperator(BaseOperator):
    """
    Load a DataFrame logic file, save to compressed CSV, then save as Parquet to MinIO/S3.
    This operator creates both a compressed CSV and a Parquet file in the same partition.
    """
    template_fields = ("logic_file", "df_name", "object_path", "op_kwargs")

    def __init__(
        self,
        *,
        logic_file: str,
        df_name: str,
        bucket_name: str,
        object_path: str,  # Should be a partition path like "order_book/{{ ds }}"
        conn_id: str = "minio_default",
        op_kwargs: Optional[dict[str, Any]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.logic_file = logic_file
        self.df_name = df_name
        self.bucket_name = bucket_name
        self.object_path = object_path
        self.conn_id = conn_id
        self.op_kwargs = op_kwargs or {}

    def execute(self, context: dict) -> None:
        df = self._load_dataframe()
        if df is None or df.empty:
            self.log.info("DataFrame rỗng, bỏ qua upload.")
            return

        hook = S3Hook(aws_conn_id=self.conn_id)

        if not hook.check_for_bucket(self.bucket_name):
            hook.create_bucket(bucket_name=self.bucket_name)

        # Ensure object_path doesn't end with /
        base_path = self.object_path.rstrip("/")

        # 1. Save as compressed CSV
        csv_data = df.to_csv(index=False).encode("utf-8")
        csv_gz_data = gzip.compress(csv_data)
        csv_key = f"{base_path}/data.csv.gz"
        
        hook.load_bytes(
            bytes_data=csv_gz_data,
            key=csv_key,
            bucket_name=self.bucket_name,
            replace=True
        )
        self.log.info("✅ Đã upload %d dòng CSV (compressed) -> s3://%s/%s", 
                     len(df), self.bucket_name, csv_key)

        # 2. Save as Parquet
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False, engine='pyarrow', compression='snappy')
        parquet_key = f"{base_path}/data.parquet"
        
        hook.load_bytes(
            bytes_data=parquet_buffer.getvalue(),
            key=parquet_key,
            bucket_name=self.bucket_name,
            replace=True
        )
        self.log.info("✅ Đã upload %d dòng Parquet -> s3://%s/%s", 
                     len(df), self.bucket_name, parquet_key)

    def _load_dataframe(self) -> pd.DataFrame:
        """Load the target DataFrame from the logic module."""
        logic_dir = Path(__file__).resolve().parent.parent / "logic"
        module_path = logic_dir / f"{self.logic_file}.py"

        if str(logic_dir) not in sys.path:
            sys.path.append(str(logic_dir))

        if not module_path.exists():
            raise AirflowException(f"Logic file not found: {module_path}")

        spec = importlib.util.spec_from_file_location(self.logic_file, module_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        obj = getattr(module, self.df_name, None)
        if obj is None:
            raise AirflowException(f"{self.df_name} not found in {self.logic_file}.py")

        df = obj(**self.op_kwargs) if isinstance(obj, Callable) else obj

        if not isinstance(df, pd.DataFrame):
            if df is None:
                return pd.DataFrame()
            raise AirflowException(
                f"Object returned is not a pandas DataFrame (Type: {type(df)})"
            )

        return df

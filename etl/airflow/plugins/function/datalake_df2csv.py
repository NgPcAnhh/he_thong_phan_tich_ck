from __future__ import annotations
import importlib.util
import io
import sys
import time
from pathlib import Path
from typing import Callable, Optional, Any

import pandas as pd
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


class DfToCsvOperator(BaseOperator):
    """Load a DataFrame logic file and upload as CSV to MinIO/S3."""
    template_fields = ("logic_file", "df_name", "object_path", "op_kwargs")

    def __init__(
        self,
        *,
        logic_file: str,
        df_name: str,
        bucket_name: str,
        object_path: str,
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
        start_time = time.time()
        self.log.info("="*80)
        self.log.info("🚀 BẮT ĐẦU TASK: %s", self.task_id)
        self.log.info("="*80)
        self.log.info("📋 Thông tin cấu hình:")
        self.log.info("  - Logic file: %s", self.logic_file)
        self.log.info("  - DataFrame name: %s", self.df_name)
        self.log.info("  - Bucket: %s", self.bucket_name)
        self.log.info("  - Object path: %s", self.object_path)
        self.log.info("  - Connection ID: %s", self.conn_id)
        self.log.info("  - Tham số truyền vào: %s", self.op_kwargs)
        self.log.info("-"*80)
        
        try:
            # Load DataFrame
            self.log.info("📥 Bước 1: Đang load dữ liệu từ logic file...")
            df = self._load_dataframe()
            
            if df is None or df.empty:
                self.log.warning("⚠️  DataFrame rỗng hoặc None")
                self.log.info("📊 Thông tin DataFrame:")
                self.log.info("  - Số dòng: 0")
                self.log.info("  - Kết luận: Bỏ qua upload CSV")
                self.log.info("="*80)
                self.log.info("✅ KẾT THÚC TASK (không có dữ liệu): %s", self.task_id)
                self.log.info("="*80)
                return
            
            # Log data quality metrics
            self.log.info("✅ Load dữ liệu thành công!")
            self.log.info("📊 Thông tin DataFrame:")
            self.log.info("  - Số dòng: %d", len(df))
            self.log.info("  - Số cột: %d", len(df.columns))
            self.log.info("  - Tên các cột: %s", list(df.columns))
            self.log.info("  - Kiểu dữ liệu: %s", dict(df.dtypes.astype(str)))
            self.log.info("  - Bộ nhớ sử dụng: %.2f MB", df.memory_usage(deep=True).sum() / 1024 / 1024)
            
            # Check for null values
            null_counts = df.isnull().sum()
            if null_counts.sum() > 0:
                self.log.info("  - Số giá trị NULL:")
                for col, count in null_counts[null_counts > 0].items():
                    self.log.info("    + %s: %d", col, count)
            else:
                self.log.info("  - Không có giá trị NULL")
            
            # Show sample data
            self.log.info("  - Mẫu dữ liệu (5 dòng đầu):")
            for idx, row in df.head(5).iterrows():
                self.log.info("    Row %d: %s", idx, dict(row))
            
            self.log.info("-"*80)

            # Encode CSV
            self.log.info("📝 Bước 2: Đang chuyển đổi DataFrame sang CSV...")
            csv_start = time.time()
            data = df.to_csv(index=False).encode("utf-8-sig")
            csv_time = time.time() - csv_start
            self.log.info("✅ Chuyển đổi CSV thành công!")
            self.log.info("  - Kích thước file: %.2f MB", len(data) / 1024 / 1024)
            self.log.info("  - Thời gian chuyển đổi: %.2f giây", csv_time)
            self.log.info("-"*80)

            # Upload to MinIO
            self.log.info("☁️  Bước 3: Đang upload lên MinIO/S3...")
            upload_start = time.time()
            hook = S3Hook(aws_conn_id=self.conn_id)

            if not hook.check_for_bucket(self.bucket_name):
                self.log.info("  - Bucket '%s' chưa tồn tại, đang tạo mới...", self.bucket_name)
                hook.create_bucket(bucket_name=self.bucket_name)
                self.log.info("  - ✅ Đã tạo bucket thành công")
            else:
                self.log.info("  - Bucket '%s' đã tồn tại", self.bucket_name)

            hook.load_bytes(bytes_data=data, key=self.object_path, bucket_name=self.bucket_name, replace=True)
            upload_time = time.time() - upload_start
            
            self.log.info("✅ Upload thành công!")
            self.log.info("  - Đường dẫn: s3://%s/%s", self.bucket_name, self.object_path)
            self.log.info("  - Số dòng: %d", len(df))
            self.log.info("  - Thời gian upload: %.2f giây", upload_time)
            self.log.info("-"*80)
            
            # Summary
            total_time = time.time() - start_time
            self.log.info("📈 TỔNG KẾT:")
            self.log.info("  - Tổng thời gian thực thi: %.2f giây", total_time)
            self.log.info("  - Tốc độ xử lý: %.0f dòng/giây", len(df) / total_time if total_time > 0 else 0)
            self.log.info("="*80)
            self.log.info("✅ HOÀN THÀNH TASK: %s", self.task_id)
            self.log.info("="*80)
            
        except Exception as e:
            elapsed = time.time() - start_time
            self.log.error("="*80)
            self.log.error("❌ LỖI TRONG TASK: %s", self.task_id)
            self.log.error("="*80)
            self.log.error("⚠️  Loại lỗi: %s", type(e).__name__)
            self.log.error("⚠️  Chi tiết lỗi: %s", str(e))
            self.log.error("⚠️  Thời gian đã chạy trước khi lỗi: %.2f giây", elapsed)
            self.log.error("="*80)
            raise

    def _load_dataframe(self) -> pd.DataFrame:
        """Load the target DataFrame from the logic module."""
        logic_dir = Path(__file__).resolve().parent.parent / "logic"
        module_path = logic_dir / f"{self.logic_file}.py"
        
        self.log.info("  - Logic directory: %s", logic_dir)
        self.log.info("  - Module path: %s", module_path)

        if str(logic_dir) not in sys.path:
            sys.path.append(str(logic_dir))
            self.log.info("  - Đã thêm logic directory vào sys.path")

        if not module_path.exists():
            self.log.error("  - ❌ Không tìm thấy file logic: %s", module_path)
            raise AirflowException(f"Logic file not found: {module_path}")
        
        self.log.info("  - Đang load module '%s'...", self.logic_file)
        spec = importlib.util.spec_from_file_location(self.logic_file, module_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        self.log.info("  - ✅ Module loaded thành công")

        self.log.info("  - Đang tìm DataFrame/function: '%s'...", self.df_name)
        obj = getattr(module, self.df_name, None)
        if obj is None:
            self.log.error("  - ❌ Không tìm thấy DataFrame/function '%s' trong module", self.df_name)
            raise AirflowException(f"{self.df_name} not found in {self.logic_file}.py")
        self.log.info("  - ✅ Tìm thấy object: %s", type(obj).__name__)

        if isinstance(obj, Callable):
            self.log.info("  - Đang gọi function với tham số: %s", self.op_kwargs)
            df = obj(**self.op_kwargs)
        else:
            self.log.info("  - Object là DataFrame tĩnh, không cần gọi function")
            df = obj

        if not isinstance(df, pd.DataFrame):
            if df is None:
                self.log.warning("  - ⚠️  Function trả về None, tạo DataFrame rỗng")
                return pd.DataFrame()
            self.log.error("  - ❌ Object không phải pandas DataFrame (Type: %s)", type(df).__name__)
            raise AirflowException(
                f"Object returned is not a pandas DataFrame (Type: {type(df)})"
            )

        return df
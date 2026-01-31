from __future__ import annotations
import io
from datetime import datetime
from pathlib import Path
from typing import Iterable

import pandas as pd
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


class S3CompactToParquetOperator(BaseOperator):
    template_fields = ("prefix",)

    def __init__(
        self,
        *,
        bucket_name: str,
        prefix: str,
        conn_id: str = "minio_default",
        min_history: int = 10,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.bucket_name = bucket_name
        self.prefix = prefix.rstrip("/")
        self.conn_id = conn_id
        self.min_history = min_history

    def execute(self, context):
        hook = S3Hook(aws_conn_id=self.conn_id)
        keys = hook.list_keys(bucket_name=self.bucket_name, prefix=f"{self.prefix}/") or []
        if not keys:
            return "no_keys"

        def extract_ds(key: str) -> str | None:
            parts = key.split("/")
            return parts[1] if len(parts) >= 2 else None

        ds_list = sorted({extract_ds(k) for k in keys if extract_ds(k)})
        if len(ds_list) <= 1:
            return "only_latest_kept"

        latest = ds_list[-1]
        older = [d for d in ds_list if d != latest]
        if len(older) < self.min_history:
            return f"older<{self.min_history},skip"

        frames = []
        for key in keys:
            ds_part = extract_ds(key)
            if ds_part not in older:
                continue
            content = hook.read_key(key=key, bucket_name=self.bucket_name)
            if not content:
                continue
            frames.append(pd.read_csv(io.StringIO(content)))

        if not frames:
            return "no_frames"

        merged = pd.concat(frames, ignore_index=True)
        out_key = f"{self.prefix}/{older[0]}_{older[-1]}.parquet"

        buffer = io.BytesIO()
        merged.to_parquet(buffer, index=False)

        hook.load_bytes(bytes_data=buffer.getvalue(), key=out_key, bucket_name=self.bucket_name, replace=True)

        to_delete = [k for k in keys if extract_ds(k) in older]
        if to_delete:
            hook.delete_objects(bucket=self.bucket_name, keys=to_delete)

        return f"archived {len(to_delete)} files to {out_key}"

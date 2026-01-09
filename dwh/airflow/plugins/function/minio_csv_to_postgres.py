from __future__ import annotations
import io
from contextlib import closing
from typing import Any, Iterable, Sequence

import pandas as pd
import psycopg2
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extensions import connection as PGConnection
from psycopg2.extras import execute_values


class MinioCSVToPostgresOperator(BaseOperator):
    """Load CSV content from MinIO/S3 into Postgres with idempotent upsert."""

    template_fields = ("object_key", "bucket_name", "target_schema", "target_table")

    def __init__(
        self,
        *,
        bucket_name: str,
        object_key: str,
        target_schema: str,
        target_table: str,
        postgres_conn_id: str | None = None,
        postgres_conn_config: dict[str, Any] | None = None,
        aws_conn_id: str = "minio_default",
        metadata_table: str | None = None,
        expected_columns: Sequence[str] | None = None,
        rename_map: dict[str, str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        if not postgres_conn_id and not postgres_conn_config:
            raise AirflowException("Provide postgres_conn_id or postgres_conn_config")

        self.bucket_name = bucket_name
        self.object_key = object_key
        self.target_schema = target_schema
        self.target_table = target_table
        self.postgres_conn_id = postgres_conn_id
        self.postgres_conn_config = postgres_conn_config
        self.aws_conn_id = aws_conn_id
        self.metadata_table = metadata_table or f"{target_table}_ingest_log"
        self.expected_columns = tuple(expected_columns) if expected_columns else (
            "ticker",
            "trading_date",
            "open",
            "high",
            "low",
            "close",
            "volume",
        )
        self.rename_map = rename_map or {}

    def execute(self, context: dict) -> str:
        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)

        with closing(self._get_pg_conn()) as conn:
            self._ensure_tables(conn)

            if self._is_processed(conn):
                return f"skip:{self.object_key}"

            content = s3_hook.read_key(key=self.object_key, bucket_name=self.bucket_name)
            if not content:
                return f"empty:{self.object_key}"

            df = pd.read_csv(io.StringIO(content))
            df = self._normalize(df)
            if df.empty:
                return f"no_rows:{self.object_key}"

            inserted = self._upsert_rows(conn, df)
            self._mark_processed(conn)
            return f"loaded:{inserted}"

    # Helpers

    def _get_pg_conn(self) -> PGConnection:
        if self.postgres_conn_config:
            conn = psycopg2.connect(**self.postgres_conn_config)
        else:
            hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
            conn = hook.get_conn()
        conn.autocommit = True
        return conn

    def _is_processed(self, conn: PGConnection) -> bool:
        sql = (
            f"SELECT 1 FROM {self.target_schema}.{self.metadata_table} "
            "WHERE object_key = %s LIMIT 1"
        )
        with conn.cursor() as cur:
            cur.execute(sql, (self.object_key,))
            return cur.fetchone() is not None

    def _ensure_tables(self, conn: PGConnection) -> None:
        ddl_schema = f"CREATE SCHEMA IF NOT EXISTS {self.target_schema};"
        ddl_table = f"""
            CREATE TABLE IF NOT EXISTS {self.target_schema}.{self.target_table} (
                ticker TEXT NOT NULL,
                trading_date DATE NOT NULL,
                open NUMERIC,
                high NUMERIC,
                low NUMERIC,
                close NUMERIC,
                volume NUMERIC,
                source_file TEXT,
                loaded_at TIMESTAMPTZ DEFAULT NOW(),
                PRIMARY KEY (ticker, trading_date)
            );
        """
        ddl_log = f"""
            CREATE TABLE IF NOT EXISTS {self.target_schema}.{self.metadata_table} (
                object_key TEXT PRIMARY KEY,
                processed_at TIMESTAMPTZ DEFAULT NOW()
            );
        """
        with conn.cursor() as cur:
            cur.execute(ddl_schema)
            cur.execute(ddl_table)
            cur.execute(ddl_log)

    def _normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame(columns=self.expected_columns)

        df = df.copy()
        rename_map = {
            "date": "trading_date",
            "Time": "trading_date",
            "time": "trading_date",
            **(self.rename_map or {}),
        }
        df.rename(columns=rename_map, inplace=True)

        missing = [c for c in self.expected_columns if c not in df.columns]
        if missing:
            raise AirflowException(f"Missing columns in {self.object_key}: {missing}")

        df = df[list(self.expected_columns)]
        df.dropna(subset=["trading_date", "ticker"], inplace=True)
        df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
        df["trading_date"] = pd.to_datetime(df["trading_date"], errors="coerce").dt.date
        df = df.dropna(subset=["trading_date"])
        df = df.drop_duplicates(subset=["ticker", "trading_date"])
        df["source_file"] = self.object_key
        return df

    def _upsert_rows(self, conn: PGConnection, df: pd.DataFrame) -> int:
        total = len(df)
        rows: Iterable[tuple] = (
            (
                r["ticker"],
                r["trading_date"],
                r.get("open"),
                r.get("high"),
                r.get("low"),
                r.get("close"),
                r.get("volume"),
                r.get("source_file"),
            )
            for r in df.to_dict("records")
        )

        insert_sql = f"""
            INSERT INTO {self.target_schema}.{self.target_table}
            (ticker, trading_date, open, high, low, close, volume, source_file)
            VALUES %s
            ON CONFLICT (ticker, trading_date) DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume,
                source_file = EXCLUDED.source_file,
                loaded_at = NOW();
        """

        with conn.cursor() as cur:
            execute_values(cur, insert_sql, rows)
        return total

    def _mark_processed(self, conn: PGConnection) -> None:
        sql = (
            f"INSERT INTO {self.target_schema}.{self.metadata_table} (object_key) "
            "VALUES (%s) ON CONFLICT (object_key) DO NOTHING"
        )
        with conn.cursor() as cur:
            cur.execute(sql, (self.object_key,))


class HistoryPriceToPostgresOperator(MinioCSVToPostgresOperator):
    """Preset operator for history price ingestion."""

    def __init__(self, *args, **kwargs) -> None:
        metadata_table = kwargs.pop("metadata_table", "history_price_ingest_log")
        expected_columns = kwargs.pop(
            "expected_columns",
            (
                "ticker",
                "trading_date",
                "open",
                "high",
                "low",
                "close",
                "volume",
            ),
        )
        super().__init__(
            *args,
            metadata_table=metadata_table,
            expected_columns=expected_columns,
            **kwargs,
        )


class IndexMarketToPostgresOperator(MinioCSVToPostgresOperator):
    """Preset operator for market index ingestion."""

    def __init__(self, *args, **kwargs) -> None:
        metadata_table = kwargs.pop("metadata_table", "market_index_ingest_log")
        rename_map = kwargs.pop("rename_map", {})
        rename_map = {"index_code": "ticker", "time": "trading_date", **rename_map}
        super().__init__(
            *args,
            metadata_table=metadata_table,
            rename_map=rename_map,
            **kwargs,
        )

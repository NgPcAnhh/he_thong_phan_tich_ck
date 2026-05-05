from contextlib import closing
from datetime import datetime

import pandas as pd
from psycopg2.extras import execute_values

from lake_to_dwh.utils import ensure_schema, get_postgres_connection, read_all_csvs_from_folder


def _extract_partition_from_key(key: str, folder_prefix: str) -> str | None:
    if not key.startswith(folder_prefix):
        return None
    relative = key[len(folder_prefix):].lstrip("/")
    if not relative or "/" not in relative:
        return None
    return relative.split("/", 1)[0]


def _parse_partition_value(partition_value: str) -> datetime | None:
    # Expected format from DAG: YYYY-MM-DD_HH:MM:SS:mmm
    try:
        return datetime.strptime(partition_value, "%Y-%m-%d_%H:%M:%S:%f")
    except ValueError:
        return None


def _get_latest_partition(bucket: str, folder_prefix: str, minio_conn_id: str) -> str | None:
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    hook = S3Hook(aws_conn_id=minio_conn_id)
    keys = hook.list_keys(bucket_name=bucket, prefix=folder_prefix) or []
    if not keys:
        print(f"⚠️ No objects found in {bucket}/{folder_prefix}")
        return None

    partitions: dict[str, datetime] = {}
    for key in keys:
        part = _extract_partition_from_key(key, folder_prefix)
        if not part:
            continue
        ts = _parse_partition_value(part)
        if ts is None:
            continue
        if part not in partitions or ts > partitions[part]:
            partitions[part] = ts

    if not partitions:
        print(f"⚠️ No valid timestamp partition found in {bucket}/{folder_prefix}")
        return None

    latest_part = max(partitions, key=partitions.get)
    latest_path = f"{folder_prefix}{latest_part}/"
    print(f"✅ Latest partition selected: {latest_path}")
    return latest_path


def _normalize_fin_ratio_v2_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [str(c).strip() for c in out.columns]

    required = ["ticker", "year", "quarter"]
    missing = [c for c in required if c not in out.columns]
    if missing:
        raise ValueError(f"Missing required key columns in CSV: {missing}")

    out["ticker"] = out["ticker"].astype(str).str.strip().str.upper()
    out["year"] = pd.to_numeric(out["year"], errors="coerce").astype("Int64")
    out["quarter"] = pd.to_numeric(out["quarter"], errors="coerce").astype("Int64")

    out = out.dropna(subset=["ticker", "year", "quarter"]).copy()
    out = out[out["ticker"] != ""].copy()

    value_cols = [c for c in out.columns if c not in ["ticker", "year", "quarter"]]
    for col in value_cols:
        out[col] = out[col].map(_parse_double_precision_value)
        out[col] = pd.to_numeric(out[col], errors="coerce")

    # Keep latest row in file order for duplicate keys.
    out = out.drop_duplicates(subset=["ticker", "year", "quarter"], keep="last")

    return out


def _parse_double_precision_value(value: object) -> float | None:
    if pd.isna(value):
        return None

    if isinstance(value, (int, float)):
        if pd.isna(value):
            return None
        return float(value)

    text = str(value).strip()
    if text == "":
        return None

    lower = text.lower()
    if lower in {"-", "--", "na", "n/a", "none", "null", "nan", "#n/a"}:
        return None

    # Remove percent symbol before numeric conversion.
    text = text.replace("%", "")
    text = text.replace("\u00a0", "").replace(" ", "")

    # Normalize decimal separator toward dot.
    if "," in text and "." in text:
        # Treat comma as thousands separator when both are present.
        text = text.replace(",", "")
    elif "," in text:
        # Treat comma as decimal separator.
        text = text.replace(",", ".")

    # Keep only valid numeric characters.
    cleaned = []
    dot_seen = False
    sign_seen = False
    for idx, ch in enumerate(text):
        if ch.isdigit():
            cleaned.append(ch)
            continue
        if ch == "." and not dot_seen:
            cleaned.append(ch)
            dot_seen = True
            continue
        if ch == "-" and idx == 0 and not sign_seen:
            cleaned.append(ch)
            sign_seen = True
            continue

    normalized = "".join(cleaned)
    if normalized in {"", ".", "-", "-."}:
        return None

    try:
        return float(normalized)
    except ValueError:
        return None


def sync_fin_ratio_v2_to_db(
    db_url: str,
    schema: str,
    bucket: str,
    minio_conn_id: str = "minio_finance",
    folder_prefix: str = "fin_ratio_v2/",
    table: str = "financial_ratio",
) -> str:
    print("=" * 70)
    print("📊 SYNC FIN_RATIO_V2 TO DATABASE (UPDATE + INSERT)")
    print("=" * 70)

    print("\n[1/4] Finding latest partition...")
    latest_partition = _get_latest_partition(bucket, folder_prefix, minio_conn_id)
    if not latest_partition:
        return "❌ No partition found"

    print("\n[2/4] Reading CSV data from latest partition...")
    df = read_all_csvs_from_folder(bucket=bucket, folder=latest_partition, conn_id=minio_conn_id)
    if df.empty:
        return "⚠️ No CSV rows found"

    print(f"Loaded rows: {len(df):,}")

    print("\n[3/4] Cleaning and preparing data...")
    df = _normalize_fin_ratio_v2_dataframe(df)
    if df.empty:
        return "⚠️ No rows after key cleaning"

    rows_after_cleaning = len(df)
    print(f"Rows after cleaning/dedup: {rows_after_cleaning:,}")

    print("\n[4/4] Upserting to database...")
    with closing(get_postgres_connection(db_url)) as conn:
        conn.autocommit = False

        try:
            ensure_schema(conn, schema)

            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = %s
                      AND table_name = %s
                      AND column_name != 'id'
                    ORDER BY ordinal_position;
                    """,
                    (schema, table),
                )
                db_columns = [row[0] for row in cur.fetchall()]

                if not db_columns:
                    raise ValueError(f"Table {schema}.{table} not found or has no columns")

                key_cols = ["ticker", "year", "quarter"]
                for key_col in key_cols:
                    if key_col not in db_columns:
                        raise ValueError(f"Target table missing key column: {key_col}")

                df_columns = [c for c in df.columns if c in db_columns]
                if not df_columns:
                    raise ValueError("No matching columns between fin_ratio_v2 CSV and target table")

                # Keep only columns that exist in DB.
                df = df[df_columns].copy()

                # Ensure deterministic row tuples.
                rows = [
                    tuple(row[col] if pd.notna(row[col]) else None for col in df_columns)
                    for _, row in df.iterrows()
                ]

                if not rows:
                    return "⚠️ No rows to upsert"

                stage_table = "_fin_ratio_v2_stage"
                cur.execute(
                    f"""
                    CREATE TEMP TABLE {stage_table}
                    AS SELECT {', '.join(df_columns)}
                    FROM {schema}.{table}
                    WHERE 1 = 0;
                    """
                )

                execute_values(
                    cur,
                    f"INSERT INTO {stage_table} ({', '.join(df_columns)}) VALUES %s",
                    rows,
                    page_size=1000,
                )

                non_key_cols = [c for c in df_columns if c not in key_cols]

                updated_count = 0
                if non_key_cols:
                    set_clause = ", ".join([f"{c} = s.{c}" for c in non_key_cols])
                    cur.execute(
                        f"""
                        UPDATE {schema}.{table} AS t
                        SET {set_clause}
                        FROM {stage_table} AS s
                        WHERE t.ticker::text = s.ticker::text
                          AND t.year::text = s.year::text
                          AND t.quarter::text = s.quarter::text;
                        """
                    )
                    updated_count = cur.rowcount

                cur.execute(
                    f"""
                    INSERT INTO {schema}.{table} ({', '.join(df_columns)})
                    SELECT {', '.join([f's.{c}' for c in df_columns])}
                    FROM {stage_table} AS s
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM {schema}.{table} AS t
                        WHERE t.ticker::text = s.ticker::text
                          AND t.year::text = s.year::text
                          AND t.quarter::text = s.quarter::text
                    );
                    """
                )
                inserted_count = cur.rowcount

            conn.commit()

            print("=" * 70)
            print(f"📥 Loaded from MinIO : {rows_after_cleaning:,} rows")
            print(f"🔄 Updated in DB    : {updated_count:,} rows")
            print(f"➕ Inserted in DB   : {inserted_count:,} rows")
            print("=" * 70)

            return (
                f"✅ Success: Loaded {rows_after_cleaning} | "
                f"Updated {updated_count} | Inserted {inserted_count}"
            )

        except Exception as exc:
            conn.rollback()
            print(f"❌ Error: {exc}")
            raise

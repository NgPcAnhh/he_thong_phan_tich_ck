import re
import unicodedata
from contextlib import closing

import pandas as pd
from psycopg2.extras import execute_values

from lake_to_dwh.utils import (
    get_minio_hook,
    get_latest_partition,
    read_csv_from_minio,
    get_postgres_connection,
    ensure_schema,
    clean_dataframe,
)

TABLE_NAME = "vn_macro_yearly"
MINIO_FOLDER = "vn_macro_yearly/"

def _slugify(text: str) -> str:
    """
    Convert a Vietnamese indicator name to a safe, lowercase DB column name.

    Examples:
        "Tăng trưởng GDP (% năm)"           → "tang_truong_gdp"
        "Lạm phát (CPI) (% năm)"            → "lam_phat_cpi"
        "Tăng trưởng Công nghiệp & Xây dựng (% năm)" → "tang_truong_cong_nghiep_xay_dung"
    """
    # Normalize unicode (NFD) then strip accents
    nfkd = unicodedata.normalize("NFKD", text)
    ascii_text = "".join(c for c in nfkd if not unicodedata.combining(c))

    # Remove parenthesized suffixes like "(% năm)", "(% nam)"
    ascii_text = re.sub(r"\(.*?\)", "", ascii_text)

    # Replace & and non-alphanumeric chars with space
    ascii_text = re.sub(r"[^a-zA-Z0-9]+", " ", ascii_text)

    # Collapse whitespace, strip, lowercase, join with underscore
    parts = ascii_text.strip().lower().split()
    slug = "_".join(parts)

    # Trim trailing underscores / ensure non-empty
    slug = slug.strip("_")
    return slug or "unknown"


def _pivot_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Pivot from wide (indicators as rows, years as columns) to
    long (years as rows, indicators as columns).

    Input columns : chi_so, [Series], YR2014 … YR2024
    Output columns: year (int), <slugified indicator names> (float)
    """
    # Keep only chi_so + YR* columns
    year_cols = sorted([c for c in df.columns if c.startswith("YR")])
    if not year_cols:
        raise ValueError(f"No YR* columns found. Available: {list(df.columns)}")

    df = df[["chi_so"] + year_cols].copy()

    # Build slug mapping: chi_so value → safe column name
    slug_map = {}
    for val in df["chi_so"].unique():
        slug = _slugify(str(val))
        # Handle collisions by appending a counter
        base = slug
        counter = 2
        while slug in slug_map.values():
            slug = f"{base}_{counter}"
            counter += 1
        slug_map[val] = slug
        print(f"  {val}  →  {slug}")

    # Set chi_so as index, transpose
    df = df.set_index("chi_so")
    df_t = df.T  # rows = YR2014 … YR2024, columns = chi_so values

    # Rename columns using slug map
    df_t = df_t.rename(columns=slug_map)

    # Convert index (YR2014 etc.) to integer year column
    df_t.index = df_t.index.str.replace("YR", "").astype(int)
    df_t.index.name = "year"
    df_t = df_t.reset_index()

    # Coerce numeric
    for col in df_t.columns:
        if col != "year":
            df_t[col] = pd.to_numeric(df_t[col], errors="coerce")

    return df_t


def _ensure_table(conn, schema: str, table: str, columns: list[str]):
    """Create table if not exists, with year as PK and indicator columns as FLOAT."""
    indicator_cols_ddl = ",\n        ".join(
        f'"{col}" DOUBLE PRECISION' for col in columns if col != "year"
    )

    ddl = f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table} (
            year INTEGER PRIMARY KEY,
            {indicator_cols_ddl}
        );
    """
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()
    print(f"✅ Table {schema}.{table} ensured (year PK + {len(columns) - 1} indicator cols)")


# ---------------------------------------------------------------------------
# Main sync function
# ---------------------------------------------------------------------------

def sync_vn_macro_yearly_to_db(
    db_url: str,
    schema: str,
    bucket: str,
    minio_conn_id: str = "minio_finance",
    folder_prefix: str = MINIO_FOLDER,
    table: str = TABLE_NAME,
) -> str:
    """
    Sync Vietnam macroeconomic yearly data from MinIO to PostgreSQL.

    Reads the latest partition CSV, **pivots** so that each row is a year
    and each indicator becomes a column, then TRUNCATE + INSERT.
    """
    print("=" * 70)
    print("📊 SYNC VN MACRO YEARLY TO DATABASE (PIVOTED)")
    print("=" * 70)

    # 1. Find latest partition
    latest_partition = get_latest_partition(bucket, folder_prefix, minio_conn_id)
    if not latest_partition:
        return "⚠️ No partition found for vn_macro_yearly"

    # 2. Read CSV files from latest partition
    hook = get_minio_hook(minio_conn_id)
    keys = hook.list_keys(bucket_name=bucket, prefix=latest_partition)
    csv_files = [k for k in keys if k.endswith(".csv")]

    if not csv_files:
        return "⚠️ No CSV files found in latest partition"

    all_dfs = []
    for csv_file in csv_files:
        df = read_csv_from_minio(bucket, csv_file, minio_conn_id)
        if not df.empty:
            all_dfs.append(df)

    if not all_dfs:
        return "⚠️ No data found"

    df = pd.concat(all_dfs, ignore_index=True)
    print(f"\nTotal loaded: {len(df)} rows, columns: {list(df.columns)}")

    # 3. Clean data
    print("\n[1/3] Cleaning data...")
    df.columns = df.columns.str.strip()

    if "chi_so" not in df.columns:
        return f"❌ Missing column 'chi_so'. Available: {list(df.columns)}"

    df = clean_dataframe(df, required_columns=["chi_so"])
    df = df.drop_duplicates(subset=["chi_so"])

    # Drop non-numeric, non-chi_so columns (e.g. 'Series')
    keep_cols = [c for c in df.columns if c.startswith("YR") or c == "chi_so"]
    dropped = [c for c in df.columns if c not in keep_cols]
    if dropped:
        print(f"⚠️ Dropping extra columns: {dropped}")
        df = df[keep_cols].copy()

    if df.empty:
        return "⚠️ No data after cleaning"

    # 4. Pivot
    print("\n[2/3] Pivoting table (indicators → columns, years → rows)...")
    df_pivoted = _pivot_dataframe(df)
    print(f"\nPivoted: {len(df_pivoted)} rows × {len(df_pivoted.columns)} columns")
    print(f"Columns: {list(df_pivoted.columns)}")

    # 5. Insert into database (REPLACE = TRUNCATE + INSERT)
    print("\n[3/3] Inserting into database (REPLACE mode)...")

    with closing(get_postgres_connection(db_url)) as conn:
        conn.autocommit = False
        try:
            ensure_schema(conn, schema)

            # Drop + recreate table to handle column changes
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {schema}.{table}")
            conn.commit()
            conn.autocommit = False

            _ensure_table(conn, schema, table, list(df_pivoted.columns))

            rows = [tuple(row) for row in df_pivoted.itertuples(index=False, name=None)]
            cols_str = ", ".join(f'"{c}"' for c in df_pivoted.columns)

            with conn.cursor() as cur:
                sql = f'INSERT INTO {schema}.{table} ({cols_str}) VALUES %s'
                execute_values(cur, sql, rows, page_size=500)

            conn.commit()
            print(f"✅ Inserted {len(rows)} rows into {schema}.{table}")
            return f"✅ Success: REPLACE {len(rows)} rows in {table} (pivoted)"

        except Exception as e:
            conn.rollback()
            print(f"❌ Error: {str(e)}")
            raise

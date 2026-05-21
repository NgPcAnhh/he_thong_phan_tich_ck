import json
from contextlib import closing
from datetime import datetime
from pathlib import Path
from typing import Optional
import pandas as pd
from psycopg2.extras import execute_values
from lake_to_dwh.utils import (
    get_minio_hook,
    read_all_csvs_from_folder,
    get_postgres_connection,
    ensure_schema,
    clean_dataframe,
    standardize_ticker
)


# ==================== Table Definition ====================

CREATE_BCTC_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS {schema}.{table} (
    id SERIAL,
    ticker VARCHAR(20) NOT NULL,
    quarter VARCHAR(10) NOT NULL,
    year INTEGER NOT NULL,
    ind_name TEXT NOT NULL,
    ind_code VARCHAR(50) NOT NULL,
    value NUMERIC,
    report_name TEXT,
    report_code TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_bctc_pk UNIQUE (ticker, year, quarter, ind_code, ind_name)
);

CREATE INDEX IF NOT EXISTS idx_bctc_ticker ON {schema}.{table} (ticker);
CREATE INDEX IF NOT EXISTS idx_bctc_year_quarter ON {schema}.{table} (year, quarter);
"""


def _ensure_bctc_table(conn, schema: str, table: str = "bctc"):
    """Create bctc table if not exists, and ensure unique constraint exists."""
    try:
        with conn.cursor() as cur:
            cur.execute(CREATE_BCTC_TABLE_SQL.format(schema=schema, table=table))
        conn.commit()
        print(f"✓ Table {schema}.{table} ensured (with UNIQUE constraint)")
    except Exception as e:
        conn.rollback()
        err_msg = str(e).lower()
        if "already exists" in err_msg:
            # Table exists but maybe constraint name conflicts or is missing
            print(f"  Table/constraint partial conflict, checking state...")
            _ensure_unique_constraint(conn, schema, table)
        else:
            print(f"❌ Error creating table: {str(e)}")
            raise
    
    # Double-check: if table existed before CREATE TABLE IF NOT EXISTS,
    # the CONSTRAINT clause is silently skipped. Always verify.
    _ensure_unique_constraint(conn, schema, table)


def _ensure_unique_constraint(conn, schema: str, table: str = "bctc"):
    """Add unique constraint if table exists but constraint is missing."""
    constraint_name = f"uq_{table}_pk"
    try:
        with conn.cursor() as cur:
            # Check if constraint already exists
            cur.execute("""
                SELECT 1 FROM pg_constraint c
                JOIN pg_namespace n ON n.oid = c.connamespace
                WHERE c.conname = %s
                AND n.nspname = %s
            """, (constraint_name, schema))
            
            if cur.fetchone() is None:
                # Constraint doesn't exist, create it
                print(f"  Adding UNIQUE constraint {constraint_name}...")
                cur.execute(f"""
                    ALTER TABLE {schema}.{table}
                    ADD CONSTRAINT {constraint_name}
                    UNIQUE (ticker, year, quarter, ind_code, ind_name);
                """)
                conn.commit()
                print(f"  ✓ UNIQUE constraint added successfully")
            else:
                conn.commit()
                print(f"  ✓ UNIQUE constraint already exists")
    except Exception as e:
        conn.rollback()
        if "already exists" in str(e).lower():
            print(f"  ✓ Constraint already exists (race condition)")
        else:
            print(f"❌ Error adding constraint: {str(e)}")
            raise



# INDICATOR_MAPPING removed - data is now fetched in Vietnamese from vnstock with lang='vi' parameter


# apply_indicator_mapping() removed - no longer needed since ind_name is already in Vietnamese


def _parse_partition_datetime(folder_name: str) -> Optional[datetime]:
    """Parse many datetime partition formats and return datetime if valid."""
    if not folder_name:
        return None

    value = str(folder_name).strip().strip("/")

    # Common partition key prefixes
    for prefix in ("date=", "dt=", "ds="):
        if value.startswith(prefix):
            value = value[len(prefix):]
            break

    # Supported examples:
    # 2026-04-04
    # 2026-04-04_14:35:22
    # 2026-04-04_14:35:22:123
    # 2026-04-04_14:35:22.123
    # 2026-04-04 14:35:22.123
    # 20260404143522123
    formats = [
        "%Y-%m-%d_%H:%M:%S:%f",
        "%Y-%m-%d_%H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d_%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y%m%d%H%M%S%f",
        "%Y%m%d%H%M%S",
        "%Y-%m-%d",
        "%Y%m%d",
    ]

    for fmt in formats:
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue

    return None


def _get_latest_bctc_partition(
    bucket: str,
    prefix: str,
    conn_id: str = "minio_finance",
) -> Optional[str]:
    """Get latest BCTC partition folder by parsing datetime from first-level folder names."""
    hook = get_minio_hook(conn_id)

    keys = hook.list_keys(bucket_name=bucket, prefix=prefix)
    if not keys:
        print(f"⚠️ No objects found in {bucket}/{prefix}")
        return None

    print(f"📂 Scanning {len(keys)} objects in {bucket}/{prefix}")

    candidates = []
    seen_folders = set()

    for key in keys:
        relative_path = key.replace(prefix, "", 1).lstrip("/")
        if not relative_path or "/" not in relative_path:
            continue

        first_folder = relative_path.split("/")[0]
        if not first_folder or first_folder in seen_folders:
            continue

        seen_folders.add(first_folder)
        parsed_dt = _parse_partition_datetime(first_folder)
        if parsed_dt is not None:
            candidates.append((parsed_dt, first_folder))

    if not candidates:
        print(
            f"⚠️ No datetime partition folder found in {bucket}/{prefix}. "
            "Supported formats include YYYY-MM-DD, YYYY-MM-DD_HH:MM:SS:ms, date=..."
        )
        return None

    latest_dt, latest_folder = max(candidates, key=lambda item: (item[0], item[1]))
    latest_path = f"{prefix.rstrip('/')}/{latest_folder}/"

    print(f"📅 Datetime partitions found: {len(candidates)}")
    print(f"✅ Latest partition selected: {latest_path} (parsed: {latest_dt.isoformat()})")

    return latest_path


def sync_bctc_to_db(
    db_url: str,
    schema: str,
    bucket: str,
    minio_conn_id: str = "minio_finance",
    folder_prefix: str = "bctc/",
    table: str = "bctc"
) -> str:

    print("=" * 70)
    print("📊 SYNC BCTC TO DATABASE (Vietnamese + Smart IND_CODE)")
    print("=" * 70)
    
    # Step 1: Find latest partition
    print("\n[1/5] Finding latest partition...")
    latest_partition = _get_latest_bctc_partition(bucket, folder_prefix, minio_conn_id)
    
    if not latest_partition:
        return "❌ No partition found"
    
    # Step 2: Read all CSV files
    print("\n[2/5] Reading CSV files...")
    df = read_all_csvs_from_folder(bucket, latest_partition, minio_conn_id)
    
    if df.empty:
        return "⚠️ No data found"
    
    print(f"Loaded {len(df)} rows")
    
    # Step 3: Clean and transform data
    print("\n[3/5] Cleaning and transforming data...")
    
    # Normalize column names
    df.columns = df.columns.str.lower().str.strip()
    
    # Ensure required columns exist
    required_cols = ['ticker', 'quarter', 'year', 'ind_name', 'value']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        return f"❌ Missing columns: {missing_cols}"
    
    # Clean data
    df = clean_dataframe(df, required_columns=required_cols)
    df = standardize_ticker(df, 'ticker')
    
    # Parse year and quarter  
    # Note: quarter is VARCHAR(10) in database, year is INTEGER
    df['year'] = pd.to_numeric(df['year'], errors='coerce').astype('Int64')
    df['quarter'] = df['quarter'].astype(str).str.strip()  # Keep as string
    df['value'] = pd.to_numeric(df['value'], errors='coerce')
    
    # Remove rows with null year or quarter
    df = df.dropna(subset=['year', 'quarter'])
    
    rows_after_cleaning = len(df)
    print(f"After cleaning: {rows_after_cleaning} rows")
    
    # Step 4: Generate ind_code from bctc.md mapping
    print("\n[4/5] Mapping ind_code from bctc.md...")

    # Load mapping from bctc.md
    import re
    _mapping_file = Path(__file__).resolve().parent.parent / "bctc.md"
    _ind_map = {}
    try:
        with open(_mapping_file, "r", encoding="utf-8") as f:
            for entry in json.load(f):
                name = str(entry.get("ind_name", "")).strip()
                code = str(entry.get("ind_code", "")).strip()
                if name and code:
                    _ind_map[name] = code
        print(f"  Loaded {len(_ind_map)} mappings from bctc.md")
    except Exception as exc:
        print(f"  ⚠ Could not load bctc.md: {exc}")

    def _get_ind_code(ind_name: str) -> str:
        name = str(ind_name).strip()
        if name in _ind_map:
            return _ind_map[name]
        # Fallback: slugify lowercase
        slug = re.sub(r"[^A-Za-z0-9]+", "_", name).strip("_")
        return slug.lower() or "unknown"

    # Always regenerate ind_code from mapping
    df['ind_code'] = df['ind_name'].apply(_get_ind_code)
    print(f"✓ Mapped ind_code for {len(df)} rows")
    
    # Ensure ind_code is not null
    df['ind_code'] = df['ind_code'].fillna('UNKNOWN').astype(str).str[:50]
    
    # Select final columns
    final_cols = ['ticker', 'quarter', 'year', 'ind_name', 'ind_code', 'value', 'report_name', 'report_code']
    df = df[[col for col in final_cols if col in df.columns]].copy()
    
    # Deduplicate on PK columns to prevent "ON CONFLICT DO UPDATE cannot affect row a second time"
    pk_cols = ['ticker', 'year', 'quarter', 'ind_code', 'ind_name']
    before_dedup = len(df)
    df = df.drop_duplicates(subset=pk_cols, keep='last')
    if len(df) < before_dedup:
        print(f"⚠️ Removed {before_dedup - len(df)} duplicate PK rows")
    
    rows_after_cleaning = len(df)
    print(f"After final cleaning: {rows_after_cleaning} rows")
    
    if df.empty:
        return "⚠️ No data after transformation"
    
    # Step 5: Insert into database
    print("\n[5/5] Inserting into database...")
    
    with closing(get_postgres_connection(db_url)) as conn:
        conn.autocommit = False
        
        try:
            # Ensure schema and table exist
            ensure_schema(conn, schema)
            _ensure_bctc_table(conn, schema, table)
            
            # Prepare data for insertion
            rows = []
            for _, row in df.iterrows():
                rows.append((
                    row.get('ticker'),
                    row.get('quarter'),
                    row.get('year'),
                    row.get('ind_name'),
                    row.get('ind_code'),
                    row.get('value'),
                    row.get('report_name'),
                    row.get('report_code'),
                ))
            
            
            # Use modern PostgreSQL UPSERT with ON CONFLICT for PRIMARY KEY
            # Assumes table has PRIMARY KEY (ticker, year, quarter, ind_code)
            
            with conn.cursor() as cur:
                # Modern upsert: INSERT ... ON CONFLICT DO UPDATE
                upsert_sql = f"""
                    INSERT INTO {schema}.{table}
                    (ticker, quarter, year, ind_name, ind_code, value, report_name, report_code)
                    VALUES %s
                    ON CONFLICT (ticker, year, quarter, ind_code, ind_name)
                    DO UPDATE SET
                        value = EXCLUDED.value,
                        report_name = EXCLUDED.report_name,
                        report_code = EXCLUDED.report_code;
                """
                
                execute_values(cur, upsert_sql, rows, page_size=1000)
                print(f"✓ Upserted {len(rows)} rows using ON CONFLICT (efficient)")
            
            conn.commit()
            
            # Summary log
            print("="*50)
            print(f"📥 LOADED from MinIO: {rows_after_cleaning} rows")
            print(f"📤 UPSERTED to DB: {len(rows)} rows")
            print("="*50)
            
            return f"✅ Success: Loaded {rows_after_cleaning} | Upserted {len(rows)} rows"
            
        except Exception as e:
            conn.rollback()
            print(f"❌ Error: {str(e)}")
            raise
    
    print("=" * 70)

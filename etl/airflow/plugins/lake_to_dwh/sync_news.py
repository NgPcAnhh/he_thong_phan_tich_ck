import logging
import traceback
from typing import Optional
from contextlib import closing
import pandas as pd
from psycopg2.extras import execute_values
from lake_to_dwh.utils import (
    get_minio_hook,
    get_postgres_connection,
    ensure_schema,
    clean_dataframe,
    get_latest_partition,
    read_all_csvs_from_folder
)

logger = logging.getLogger(__name__)


def sync_news_to_db(
    db_url: str,
    schema: str,
    bucket: str,
    ds: Optional[str] = None,
    minio_conn_id: str = "minio_finance",
    folder_prefix: str = "news/",
    table: str = "news"
) -> str:

    logger.info("START SYNC NEWS")

    try:
        # 1. Get latest partition
        partition = get_latest_partition(bucket, folder_prefix, minio_conn_id)
        if not partition:
            logger.warning("No partition found")
            return "No partition found"

        logger.info(f"Using partition: {partition}")

        # 2. Read data
        df = read_all_csvs_from_folder(bucket, partition, minio_conn_id)
        logger.info(f"Loaded rows: {len(df)}")

        if df.empty:
            return "File is empty"

        # 3. Clean
        df.columns = df.columns.str.lower().str.strip()
        required_cols = ["source", "title", "link", "published", "summary"]

        if not all(col in df.columns for col in required_cols):
            logger.error("Missing required columns")
            return "Missing required columns"

        df = df[required_cols].copy()
        df["published"] = pd.to_datetime(df["published"], errors="coerce")
        df = df.dropna(subset=["source", "title", "link", "published"])
        df = df.drop_duplicates(subset=["source", "title", "link"])
        df = clean_dataframe(df, required_columns=["source", "title", "link", "published"])

        logger.info(f"Rows after cleaning: {len(df)}")

        if df.empty:
            return "No data after cleaning"

        # 4. Insert
        with closing(get_postgres_connection(db_url)) as conn:
            conn.autocommit = False

            try:
                ensure_schema(conn, schema)

                rows = [
                    (
                        r["source"],
                        r["title"],
                        r["link"],
                        r["published"],
                        r["summary"],
                    )
                    for _, r in df.iterrows()
                ]

                # Create unique constraint if not exists to support ON CONFLICT
                try:
                    create_idx_sql = f"""
                        CREATE UNIQUE INDEX IF NOT EXISTS {table}_source_title_link_idx 
                        ON {schema}.{table} (source, title, link);
                    """
                    with conn.cursor() as cur:
                        cur.execute(create_idx_sql)
                    conn.commit()
                except Exception as e:
                    logger.warning(f"Could not create unique index, proceeding anyway. Error: {e}")
                    conn.rollback()

                upsert_sql = f"""
                    INSERT INTO {schema}.{table}
                    (source, title, link, published, summary)
                    VALUES %s
                    ON CONFLICT (source, title, link)
                    DO UPDATE SET
                        published = EXCLUDED.published,
                        summary = EXCLUDED.summary;
                """

                with conn.cursor() as cur:
                    execute_values(cur, upsert_sql, rows)

                conn.commit()
                logger.info(f"Upserted rows: {len(rows)}")
                return f"Success: {len(rows)} rows"

            except Exception as db_error:
                conn.rollback()
                logger.error(f"DB Error: {db_error}")
                logger.error(traceback.format_exc())
                raise

    except Exception as e:
        logger.error(f"Error: {e}")
        logger.error(traceback.format_exc())
        raise

    finally:
        logger.info("END SYNC NEWS")
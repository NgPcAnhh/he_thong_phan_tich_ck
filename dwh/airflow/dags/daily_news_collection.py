from contextlib import closing
from datetime import datetime, timedelta

import pandas as pd
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values

from logic.daily_news import fetch_news_test

POSTGRES_CONN_ID = "dwh_postgres"
NEWS_SCHEMA = "public"
NEWS_TABLE = "daily_company_news"


def _default_args():
    return {
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    }


@dag(
    dag_id="daily_news_collection",
    default_args=_default_args(),
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["vnstock", "news", "minio"],
)
def daily_news_collection():
    @task
    def collect_news():
        # Chạy tuần tự (1 luồng) để tránh rate limit từ VCI API
        records = fetch_news_test()
        return records or []

    @task
    def upload_to_database(records, **context):
        if not records:
            return "no_news_for_target_date"

        df = pd.DataFrame(records)
        if df.empty:
            return "no_news_for_target_date"

        # Bổ sung các cột cần thiết để tránh KeyError
        required_cols = [
            "author",
            "friendly_sub_title",
            "news_source_link",
            "created_at",
            "public_date",
            "updated_at",
            "lang_code",
            "news_short_content",
            "news_full_content",
            "price_change_pct",
            "fetched_at",
            "related_ticker",
            "news_id",
        ]
        for col in required_cols:
            if col not in df.columns:
                df[col] = None

        if "news_id" not in df.columns or df["news_id"].isna().all():
            df["news_id"] = df.get("id")
        else:
            df["news_id"] = df["news_id"].fillna(df.get("id"))

        df.dropna(subset=["news_id", "related_ticker"], inplace=True)
        if df.empty:
            return "no_news_for_target_date"

        df["news_id"] = df["news_id"].astype(str).str.strip()
        df["related_ticker"] = df["related_ticker"].astype(str).str.upper().str.strip()
        df = df[df["related_ticker"] != ""]

        datetime_cols = ["created_at", "updated_at", "fetched_at"]
        for col in datetime_cols:
            df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)
        df["public_date"] = pd.to_datetime(df["public_date"], errors="coerce").dt.date
        df["price_change_pct"] = pd.to_numeric(df["price_change_pct"], errors="coerce")

        df.drop_duplicates(subset=["news_id", "related_ticker"], keep="last", inplace=True)
        if df.empty:
            return "no_news_for_target_date"

        ds_date = pd.to_datetime(context["ds"]).date()
        df["ds"] = ds_date

        rows = list(
            df[
                [
                    "news_id",
                    "related_ticker",
                    "author",
                    "friendly_sub_title",
                    "news_source_link",
                    "created_at",
                    "public_date",
                    "updated_at",
                    "lang_code",
                    "news_short_content",
                    "news_full_content",
                    "price_change_pct",
                    "fetched_at",
                    "ds",
                ]
            ].itertuples(index=False, name=None)
        )

        if not rows:
            return "no_news_for_target_date"

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        with closing(hook.get_conn()) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(f"CREATE SCHEMA IF NOT EXISTS {NEWS_SCHEMA};")
                cur.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {NEWS_SCHEMA}.{NEWS_TABLE} (
                        news_id TEXT NOT NULL,
                        related_ticker TEXT NOT NULL,
                        author TEXT,
                        friendly_sub_title TEXT,
                        news_source_link TEXT,
                        created_at TIMESTAMPTZ,
                        public_date DATE,
                        updated_at TIMESTAMPTZ,
                        lang_code TEXT,
                        news_short_content TEXT,
                        news_full_content TEXT,
                        price_change_pct NUMERIC,
                        fetched_at TIMESTAMPTZ,
                        ds DATE NOT NULL,
                        inserted_at TIMESTAMPTZ DEFAULT NOW(),
                        PRIMARY KEY (news_id, related_ticker)
                    );
                    """
                )

                insert_sql = f"""
                    INSERT INTO {NEWS_SCHEMA}.{NEWS_TABLE}
                    (news_id, related_ticker, author, friendly_sub_title, news_source_link,
                     created_at, public_date, updated_at, lang_code, news_short_content,
                     news_full_content, price_change_pct, fetched_at, ds)
                    VALUES %s
                    ON CONFLICT (news_id, related_ticker) DO UPDATE SET
                        author = EXCLUDED.author,
                        friendly_sub_title = EXCLUDED.friendly_sub_title,
                        news_source_link = EXCLUDED.news_source_link,
                        created_at = EXCLUDED.created_at,
                        public_date = EXCLUDED.public_date,
                        updated_at = EXCLUDED.updated_at,
                        lang_code = EXCLUDED.lang_code,
                        news_short_content = EXCLUDED.news_short_content,
                        news_full_content = EXCLUDED.news_full_content,
                        price_change_pct = EXCLUDED.price_change_pct,
                        fetched_at = COALESCE(EXCLUDED.fetched_at, {NEWS_SCHEMA}.{NEWS_TABLE}.fetched_at),
                        ds = EXCLUDED.ds,
                        inserted_at = NOW();
                """
                execute_values(cur, insert_sql, rows)

        return f"loaded:{len(rows)}"

    upload_to_database(collect_news())


daily_news_collection()
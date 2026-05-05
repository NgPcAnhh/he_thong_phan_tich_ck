from __future__ import annotations

import concurrent.futures
import json
import random
import re
import threading
import time
from datetime import datetime, timedelta
from html import unescape
from pathlib import Path
from typing import Iterable, Optional

import pandas as pd
import requests
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.models.param import Param
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from bs4 import BeautifulSoup


def _current_quarter() -> int:
    return (datetime.utcnow().month - 1) // 3 + 1


# Shared config
MINIO_BUCKET = Variable.get("minio_bucket", default_var="thongtin-congty-va-bctc")
MINIO_CONN_ID = "minio_finance"
MULTIPLIER = 1_000_000

# Key requested by user for upsert behavior
UPSERT_KEY_COLS = ["ticker", "year", "quarter", "ind_code"]

# mapping file kept inside plugins folder
MAPPING_FILE = Path(__file__).resolve().parents[1] / "plugins" / "logic" / "bctc.md"
TICKERS_FILE = Path(__file__).resolve().parents[1] / "plugins" / "logic" / "tickers_cache.txt"

# Scrape constants copied from script logic
DEFAULT_HTML_PARSER = "lxml"
BASE_URL_TEMPLATE = "https://web.stockbiz.vn/Stocks/{ticker}/FinancialStatements.aspx"
HEADER_PANEL_ID = "ctl00_webPartManager_wp603001723_wp866410259_cbFinanceReport"
REPORT_TABLE_ID = "tblReports"
CALLBACK_CONTROL_ID = "ctl00_webPartManager_wp603001723_wp866410259_cbFinanceReport"
TAB_INDEX_TO_NAME = {
    0: "bang_can_doi_ke_toan",
    1: "ket_qua_kinh_doanh",
    2: "luu_chuyen_tien_te_truc_tiep",
    3: "luu_chuyen_tien_te_gian_tiep",
}

_thread_local = threading.local()


def sanitize_filename(text: str) -> str:
    text = re.sub(r"\s+", "_", str(text).strip())
    text = re.sub(r"[^\w\-.]", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text or "unknown"


def read_tickers(tickers_file: Path, max_tickers: int = 0) -> list[str]:
    if not tickers_file.exists():
        raise FileNotFoundError(f"Ticker file not found: {tickers_file}")

    raw = tickers_file.read_text(encoding="utf-8", errors="ignore").splitlines()
    cleaned = [line.strip().upper() for line in raw if line.strip()]

    seen: set[str] = set()
    unique: list[str] = []
    for ticker in cleaned:
        if ticker not in seen:
            seen.add(ticker)
            unique.append(ticker)

    if max_tickers and max_tickers > 0:
        unique = unique[:max_tickers]

    return unique


def chunked(items: list[str], size: int) -> Iterable[list[str]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


def build_http_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/123.0.0.0 Safari/537.36"
            )
        }
    )
    return session


def get_thread_session() -> requests.Session:
    session = getattr(_thread_local, "session", None)
    if session is None:
        session = build_http_session()
        _thread_local.session = session
    return session


def make_soup(html: str) -> BeautifulSoup:
    try:
        return BeautifulSoup(html, DEFAULT_HTML_PARSER)
    except Exception:
        return BeautifulSoup(html, "html.parser")


def parse_headers(soup: BeautifulSoup) -> list[str]:
    panel = soup.find(id=HEADER_PANEL_ID)
    if panel is not None:
        header_tds = panel.find_all("td")
        header_values: list[str] = []
        for td in header_tds[2:]:
            b = td.find("b")
            text = (b.get_text(strip=True) if b else td.get_text(strip=True)).strip()
            if text:
                header_values.append(text)
        if len(header_values) >= 5:
            return header_values[:5]

    table = soup.find(id=REPORT_TABLE_ID)
    if table is None:
        return []

    for tr in table.find_all("tr"):
        tds = tr.find_all("td", recursive=False)
        if len(tds) < 7:
            continue

        header_values: list[str] = []
        for td in tds[2:7]:
            b = td.find("b")
            text = (b.get_text(strip=True) if b else td.get_text(strip=True)).strip()
            if text:
                header_values.append(text)
        if len(header_values) == 5:
            return header_values

    return []


def parse_indicator_name(first_td) -> str:
    nested_table = first_td.find("table")
    if nested_table is not None:
        nested_tr = nested_table.find("tr")
        if nested_tr is not None:
            nested_tds = nested_tr.find_all("td")
            if len(nested_tds) >= 2:
                return nested_tds[1].get_text(" ", strip=True)
    return first_td.get_text(" ", strip=True)


def parse_table_rows(soup: BeautifulSoup) -> list[list[str]]:
    table = soup.find(id=REPORT_TABLE_ID)
    if table is None:
        return []

    rows = []
    for tr in table.find_all("tr", class_="rowcolor3"):
        tds = tr.find_all("td", recursive=False)
        if len(tds) < 7:
            continue

        indicator = parse_indicator_name(tds[0])
        if not indicator:
            continue

        values = []
        for idx in range(2, 7):
            td = tds[idx]
            b = td.find("b")
            value = b.get_text(" ", strip=True) if b else td.get_text(" ", strip=True)
            values.append(value)

        rows.append([indicator] + values)

    return rows


def _decode_callback_payload(response_text: str) -> str:
    match = re.search(r"<CallbackContent>(.*?)</CallbackContent>", response_text, re.S | re.I)
    if match is not None:
        payload = match.group(1).strip()
        if payload.startswith("<![CDATA[") and payload.endswith("]]>"):
            payload = payload[9:-3]
        payload = payload.replace("$$$CART_CDATA_CLOSE$$$", "]]>")
        return unescape(payload)

    xml = make_soup(response_text)
    node = xml.find("callbackcontent")
    if node is not None:
        payload = node.decode_contents()
        payload = payload.replace("$$$CART_CDATA_CLOSE$$$", "]]>")
        return unescape(payload)
    return response_text


def fetch_tab_soup(
    session: requests.Session,
    ticker: str,
    tab_index: int,
    connect_timeout: int,
    read_timeout: int,
    max_retries: int,
    retry_backoff: float,
) -> BeautifulSoup:
    url = BASE_URL_TEMPLATE.format(ticker=ticker)
    timeout_tuple = (connect_timeout, read_timeout)

    for attempt in range(max_retries + 1):
        try:
            if tab_index == 0:
                resp = session.get(url, timeout=timeout_tuple)
                resp.raise_for_status()
                return make_soup(resp.text)

            payload = [
                (f"Cart_{CALLBACK_CONTROL_ID}_Callback_Param", "0"),
                (f"Cart_{CALLBACK_CONTROL_ID}_Callback_Param", str(tab_index)),
                (f"Cart_{CALLBACK_CONTROL_ID}_Callback_Param", "1"),
            ]
            resp = session.post(url, data=payload, timeout=timeout_tuple)
            resp.raise_for_status()
            decoded = _decode_callback_payload(resp.text)
            return make_soup(decoded)
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
            if attempt >= max_retries:
                raise
            sleep_s = retry_backoff * (2 ** attempt) + random.uniform(0.0, 0.35)
            time.sleep(sleep_s)


def fetch_ticker_tables(
    ticker: str,
    request_sleep: float,
    connect_timeout: int,
    read_timeout: int,
    max_retries: int,
    retry_backoff: float,
) -> list[dict]:
    session = get_thread_session()
    exported_tables: list[dict] = []

    for tab_index, tab_name in TAB_INDEX_TO_NAME.items():
        try:
            soup = fetch_tab_soup(
                session=session,
                ticker=ticker,
                tab_index=tab_index,
                connect_timeout=connect_timeout,
                read_timeout=read_timeout,
                max_retries=max_retries,
                retry_backoff=retry_backoff,
            )

            if soup.find(id=REPORT_TABLE_ID) is None:
                continue

            headers = parse_headers(soup)
            rows = parse_table_rows(soup)
            if len(headers) < 5 or not rows:
                continue

            period = sanitize_filename(headers[4])
            source_file = f"{tab_name}_{ticker}_{period}.csv"
            raw_df = pd.DataFrame(rows, columns=["chi_tieu"] + headers)

            exported_tables.append(
                {
                    "ticker": ticker,
                    "source_file": source_file,
                    "raw_df": raw_df,
                }
            )
        except Exception:
            continue
        finally:
            if request_sleep > 0:
                time.sleep(request_sleep)

    return exported_tables


def _norm_text(text: object) -> str:
    s = "" if text is None else str(text).strip().lower()
    return re.sub(r"\s+", " ", s)


def _clean_indicator(value: object) -> str:
    if pd.isna(value):
        return ""
    text = str(value).strip()
    text = re.sub(r"^[\s=\-\+]+", "", text)
    text = re.sub(r"^(?:(?:[a-zA-Z]+)\.\s*|(?:\d+\.)+\s*|\d+\s*\-\s*)", "", text)
    text = re.sub(r"^[\s=\-\+]+", "", text)
    return text.strip()


def parse_de_vn_number(value: object) -> float:
    if pd.isna(value):
        return float("nan")
    s = str(value).strip()
    if s == "":
        return float("nan")
    if s.lower() in {"nan", "none", "null", "n/a", "na", "-", "--"}:
        return float("nan")

    negative = False
    if s.startswith("(") and s.endswith(")"):
        negative = True
        s = s[1:-1].strip()

    s = s.replace("\u00A0", "").replace(" ", "")
    s = s.replace("%", "")
    s = re.sub(r"[^0-9,.-]", "", s)

    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        s = s.replace(",", ".")
    elif re.fullmatch(r"-?\d{1,3}(?:\.\d{3})+", s):
        s = s.replace(".", "")

    try:
        num = float(s)
        return -num if negative else num
    except ValueError:
        return float("nan")


def normalize_one_df(raw_df: pd.DataFrame, ticker: str, source_file: str) -> pd.DataFrame:
    df = raw_df.copy()
    df.columns = [str(c).strip() for c in df.columns]

    indicator_col = None
    for c in df.columns:
        if str(c).strip().lower() in {"chi_tieu", "chỉ tiêu", "chi tieu"}:
            indicator_col = c
            break
    if indicator_col is None:
        indicator_col = df.columns[0]

    if indicator_col != "chi_tieu":
        df = df.rename(columns={indicator_col: "chi_tieu"})

    df["chi_tieu"] = df["chi_tieu"].map(_clean_indicator)
    df["ticker"] = str(ticker).upper().strip()
    df["source_file"] = source_file

    non_value_cols = {"chi_tieu", "ticker", "source_file"}
    value_cols = [c for c in df.columns if c not in non_value_cols]

    for col in value_cols:
        df[col] = df[col].map(parse_de_vn_number)

    if value_cols:
        df = df.dropna(subset=value_cols, how="all")

    return df


def parse_quarter_year(col_name: object) -> tuple[int | None, int | None]:
    s = str(col_name).strip()
    m = re.search(r"[Qq]\s*([1-4])\D*([12]\d{3})", s)
    if m:
        return int(m.group(1)), int(m.group(2))

    m = re.search(r"([12]\d{3})\D*[Qq]\s*([1-4])", s)
    if m:
        return int(m.group(2)), int(m.group(1))

    return None, None


def infer_report_meta(source_file: object) -> tuple[str | None, str | None]:
    file_name = Path(str(source_file)).name.lower()
    if file_name.startswith("bang_can_doi_ke_toan"):
        return "balance_sheet", "BL"
    if file_name.startswith("ket_qua_kinh_doanh"):
        return "income_statement", "IS"
    if file_name.startswith("luu_chuyen_tien_te"):
        return "cash_flow", "CF"
    return None, None


def deep_clean_indicator(text: object) -> str:
    if pd.isna(text):
        return ""
    s = str(text).strip()
    s = re.sub(r"^[\s=\-\+]+", "", s)
    regex_pattern = (
        r"^("
        r"(?:[a-zA-Z]+)\.\s*"
        r"|"
        r"(?:\d+\.)+\s*"
        r"|"
        r"\d+\s*\-\s*"
        r")"
    )
    s = re.sub(regex_pattern, "", s)
    s = re.sub(r"^[\s=\-\+]+", "", s)
    return s.strip()


def _load_mapping() -> tuple[dict[str, str], dict[str, str]]:
    if not MAPPING_FILE.exists():
        raise FileNotFoundError(f"Mapping file not found: {MAPPING_FILE}")

    raw = json.loads(MAPPING_FILE.read_text(encoding="utf-8"))
    exact: dict[str, str] = {}
    norm: dict[str, str] = {}
    for item in raw:
        name = str(item.get("ind_name", "")).strip()
        code = str(item.get("ind_code", "")).strip()
        if not name or not code:
            continue
        exact[name] = code
        norm[_norm_text(name)] = code
    return exact, norm


def _map_ind_code(ind_name: str, ind_map_exact: dict[str, str], ind_map_norm: dict[str, str]) -> Optional[str]:
    if ind_name in ind_map_exact:
        return ind_map_exact[ind_name]
    return ind_map_norm.get(_norm_text(ind_name))


def _finalize_records(df: pd.DataFrame, year: int, quarter: int) -> pd.DataFrame:
    if df.empty:
        return df

    records = df.copy()

    # keep expected columns only
    required_cols = [
        "ticker",
        "quarter",
        "year",
        "ind_name",
        "ind_code",
        "value",
        "report_name",
        "report_code",
    ]
    missing = [c for c in required_cols if c not in records.columns]
    if missing:
        raise ValueError(f"Missing required columns after extract/transform: {missing}")

    # normalize types
    records["ticker"] = records["ticker"].astype(str).str.strip().str.upper()
    records["year"] = pd.to_numeric(records["year"], errors="coerce").astype("Int64")
    records["quarter"] = pd.to_numeric(records["quarter"], errors="coerce").astype("Int64")
    records["value"] = pd.to_numeric(records["value"], errors="coerce")
    records["ind_name"] = records["ind_name"].map(_clean_indicator)
    records["ind_code"] = records["ind_code"].astype(str).str.strip()

    # strict year/quarter filter for this run
    records = records[(records["year"] == int(year)) & (records["quarter"] == int(quarter))].copy()

    # drop invalid key rows
    records = records.dropna(subset=["ticker", "year", "quarter", "ind_code"]).copy()
    records = records[records["ind_code"] != ""].copy()

    # de-duplicate by requested key, keep latest row in input order
    records = records.drop_duplicates(subset=UPSERT_KEY_COLS, keep="last")

    # quarter in destination table is varchar
    records["quarter"] = records["quarter"].astype(int).astype(str)
    records["year"] = records["year"].astype(int)

    return records[required_cols]


def transform_scraped_tables_to_records(tables: list[dict], year: int, quarter: int) -> pd.DataFrame:
    if not tables:
        return pd.DataFrame()

    dfs = []
    for item in tables:
        raw_df = item["raw_df"]
        ticker = item["ticker"]
        source_file = item["source_file"]
        cleaned = normalize_one_df(raw_df, ticker=ticker, source_file=source_file)
        if not cleaned.empty:
            dfs.append(cleaned)

    if not dfs:
        return pd.DataFrame()

    df_clean = pd.concat(dfs, ignore_index=True, sort=False)

    base_cols = ["ticker", "source_file", "chi_tieu"]
    value_cols = [c for c in df_clean.columns if c not in base_cols]

    df_long = df_clean.melt(
        id_vars=base_cols,
        value_vars=value_cols,
        var_name="period_col",
        value_name="value",
    )
    df_long = df_long.dropna(subset=["value"]).copy()

    qy = df_long["period_col"].map(parse_quarter_year)
    df_long["quarter"] = [x[0] for x in qy]
    df_long["year"] = [x[1] for x in qy]

    df_long = df_long.dropna(subset=["quarter", "year"]).copy()
    df_long["quarter"] = df_long["quarter"].astype("int64")
    df_long["year"] = df_long["year"].astype("int64")

    meta = df_long["source_file"].map(infer_report_meta)
    df_long["report_name"] = [x[0] for x in meta]
    df_long["report_code"] = [x[1] for x in meta]

    df_long["ind_name"] = df_long["chi_tieu"].apply(deep_clean_indicator)
    df_long = df_long[df_long["ind_name"].str.upper() != "#NAME?"].copy()

    ind_map_exact, ind_map_norm = _load_mapping()
    df_long["ind_code"] = df_long["ind_name"].map(ind_map_exact)
    missing_mask = df_long["ind_code"].isna()
    df_long.loc[missing_mask, "ind_code"] = df_long.loc[missing_mask, "ind_name"].map(
        lambda x: ind_map_norm.get(_norm_text(x))
    )

    df_bctc_records = df_long[
        [
            "ticker",
            "quarter",
            "year",
            "ind_name",
            "ind_code",
            "value",
            "report_name",
            "report_code",
        ]
    ].copy()

    # Balance sheet values require one extra x1,000,000 multiplier.
    bl_mask = df_bctc_records["report_code"] == "BL"
    if bl_mask.any():
        df_bctc_records.loc[bl_mask, "value"] = (
            pd.to_numeric(df_bctc_records.loc[bl_mask, "value"], errors="coerce") * MULTIPLIER
        )

    df_bctc_records = df_bctc_records.dropna(
        subset=["value", "ind_code", "report_name", "report_code"]
    ).copy()

    return _finalize_records(df_bctc_records, year=year, quarter=quarter)


def _save_to_minio(df: pd.DataFrame, partition_folder: str, year: int, quarter: int) -> str:
    object_key = f"bctc_luong2/{partition_folder}/bctc_luong2_y{year}_q{quarter}.csv"
    csv_bytes = df.to_csv(index=False).encode("utf-8-sig")

    hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
    if not hook.check_for_bucket(MINIO_BUCKET):
        hook.create_bucket(bucket_name=MINIO_BUCKET)

    hook.load_bytes(
        bytes_data=csv_bytes,
        key=object_key,
        bucket_name=MINIO_BUCKET,
        replace=True,
    )
    return object_key


@dag(
    dag_id="bctc_luong_2",
    start_date=datetime(2026, 1, 1),
    schedule="@monthly",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "airflow",
        "retries": 2,
        "retry_delay": timedelta(minutes=3),
    },
    params={
        "year": Param(
            default=datetime.utcnow().year,
            type="integer",
            minimum=2020,
            maximum=2035,
            description="Report year",
        ),
        "quarter": Param(
            default=_current_quarter(),
            type="integer",
            enum=[1, 2, 3, 4],
            description="Report quarter",
        ),
        "batch_size": Param(
            default=20,
            type="integer",
            minimum=1,
            maximum=100,
            description="Number of tickers in each API batch",
        ),
        "workers": Param(
            default=8,
            type="integer",
            minimum=1,
            maximum=64,
            description="Max worker threads for scraping each batch",
        ),
        "request_sleep": Param(
            default=0.2,
            type="number",
            minimum=0,
            maximum=10,
            description="Sleep seconds after each tab request",
        ),
        "connect_timeout": Param(
            default=20,
            type="integer",
            minimum=1,
            maximum=120,
            description="HTTP connect timeout (seconds)",
        ),
        "read_timeout": Param(
            default=45,
            type="integer",
            minimum=1,
            maximum=300,
            description="HTTP read timeout (seconds)",
        ),
        "http_retries": Param(
            default=3,
            type="integer",
            minimum=0,
            maximum=10,
            description="HTTP retries per request",
        ),
        "retry_backoff": Param(
            default=1.0,
            type="number",
            minimum=0,
            maximum=20,
            description="Exponential backoff base seconds",
        ),
        "max_tickers": Param(
            default=0,
            type="integer",
            minimum=0,
            maximum=10000,
            description="Limit number of tickers for test run (0 = all)",
        ),
    },
    tags=["vnstock", "bctc", "all-in-one", "minio"],
    description="BCTC luong 2 DAG: fetch -> transform -> save MinIO for bctc_luong2",
)
def bctc_luong_2_dag():
    @task
    def get_partition_folder() -> str:
        now = datetime.utcnow()
        return f"{now.strftime('%Y-%m-%d_%H:%M:%S')}:{int(now.microsecond / 1000):03d}"

    @task
    def run_all_in_one(partition_folder: str, **context) -> str:
        params = context["params"]
        year = int(params.get("year", datetime.utcnow().year))
        quarter = int(params.get("quarter", _current_quarter()))
        batch_size = int(params.get("batch_size", 20))
        workers = int(params.get("workers", 8))
        request_sleep = float(params.get("request_sleep", 0.2))
        connect_timeout = int(params.get("connect_timeout", 20))
        read_timeout = int(params.get("read_timeout", 45))
        http_retries = int(params.get("http_retries", 3))
        retry_backoff = float(params.get("retry_backoff", 1.0))
        max_tickers = int(params.get("max_tickers", 0))

        print("=" * 80)
        print("BCTC_LUONG_2 ALL-IN-ONE START")
        print(f"year={year}, quarter={quarter}, batch_size={batch_size}")
        print(f"partition_folder={partition_folder}")
        print("=" * 80)

        # load mapping from plugins/logic/bctc.md (validate upfront)
        ind_map_exact, _ = _load_mapping()
        print(f"Loaded mapping entries: {len(ind_map_exact):,} from {MAPPING_FILE}")

        tickers = read_tickers(TICKERS_FILE, max_tickers=max_tickers)
        if not tickers:
            return "No tickers found"

        batches = list(chunked(tickers, batch_size))
        print(f"Total tickers: {len(tickers):,} | Total batches: {len(batches):,}")

        scraped_tables: list[dict] = []
        for idx, batch in enumerate(batches, start=1):
            max_workers = max(1, min(workers, len(batch)))
            print(f"[Batch {idx}/{len(batches)}] tickers={len(batch)} | workers={max_workers}")

            if max_workers == 1:
                for ticker in batch:
                    tables = fetch_ticker_tables(
                        ticker=ticker,
                        request_sleep=request_sleep,
                        connect_timeout=connect_timeout,
                        read_timeout=read_timeout,
                        max_retries=http_retries,
                        retry_backoff=retry_backoff,
                    )
                    scraped_tables.extend(tables)
            else:
                with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                    futures = {
                        executor.submit(
                            fetch_ticker_tables,
                            ticker=ticker,
                            request_sleep=request_sleep,
                            connect_timeout=connect_timeout,
                            read_timeout=read_timeout,
                            max_retries=http_retries,
                            retry_backoff=retry_backoff,
                        ): ticker
                        for ticker in batch
                    }
                    for future in concurrent.futures.as_completed(futures):
                        tables = future.result()
                        scraped_tables.extend(tables)

        print(f"Scraped table count: {len(scraped_tables):,}")
        records = transform_scraped_tables_to_records(scraped_tables, year=year, quarter=quarter)
        if records.empty:
            return "No rows after transform/finalize"

        print(f"Rows after finalize: {len(records):,}")

        # save to minio, no local writes
        object_key = _save_to_minio(records, partition_folder=partition_folder, year=year, quarter=quarter)
        print(f"Saved to MinIO: s3://{MINIO_BUCKET}/{object_key}")

        print("-" * 80)
        print(f"Rows written to MinIO      : {len(records):,}")
        print("Database sync              : delegated to minio_to_db_sync DAG")
        print("=" * 80)

        return f"OK | minio_key={object_key} | rows={len(records)}"

    partition_folder = get_partition_folder()
    run_all_in_one(partition_folder)


bctc_luong_2_dag()

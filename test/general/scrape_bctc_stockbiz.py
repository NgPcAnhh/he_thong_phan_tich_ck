import argparse
import concurrent.futures
import csv
import logging
import random
import re
import threading
import time
from html import unescape
from pathlib import Path
from typing import Iterable

import requests
from bs4 import BeautifulSoup

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


def default_tickers_file() -> Path:
    root = Path(__file__).resolve().parents[2]
    return root / "etl" / "airflow" / "plugins" / "logic" / "tickers_cache.txt"


def setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def sanitize_filename(text: str) -> str:
    text = re.sub(r"\s+", "_", str(text).strip())
    text = re.sub(r"[^\w\-.]", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text or "unknown"


def try_import_pandas():
    try:
        import pandas as pd  # type: ignore

        return pd
    except Exception as exc:
        logging.warning(
            "pandas unavailable or incompatible, fallback to csv module. Details: %s",
            exc,
        )
        return None


def read_tickers(tickers_file: Path) -> list[str]:
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

    # Callback responses for some report types may only contain tblReports.
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


def write_rows_to_csv(output_file: Path, headers: list[str], rows: list[list[str]]) -> None:
    with output_file.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["chi_tieu"] + headers)
        writer.writerows(rows)


def _decode_callback_payload(response_text: str) -> str:
    # ComponentArt callback often returns XML with HTML inside CallbackContent.
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
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as exc:
            if attempt >= max_retries:
                raise

            # Exponential backoff + small jitter to avoid synchronized retries.
            sleep_s = retry_backoff * (2 ** attempt) + random.uniform(0.0, 0.35)
            logging.warning(
                "Ticker=%s | Tab=%s | attempt=%s/%s | transient error=%s | retry_in=%.2fs",
                ticker,
                tab_index,
                attempt + 1,
                max_retries + 1,
                type(exc).__name__,
                sleep_s,
            )
            time.sleep(sleep_s)


def process_ticker(
    ticker: str,
    output_dir: Path,
    request_sleep: float,
    connect_timeout: int,
    read_timeout: int,
    max_retries: int,
    retry_backoff: float,
    pd_module,
) -> int:
    session = get_thread_session()
    url = BASE_URL_TEMPLATE.format(ticker=ticker)
    logging.info("Ticker=%s | URL=%s", ticker, url)
    ticker_output_dir = output_dir / ticker
    ticker_output_dir.mkdir(parents=True, exist_ok=True)

    exported = 0
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
                logging.info("Ticker=%s | Tab=%s(%s) | no tblReports -> skip tab", ticker, tab_name, tab_index)
                continue

            headers = parse_headers(soup)
            rows = parse_table_rows(soup)
            if len(headers) < 5 or not rows:
                logging.info("Ticker=%s | Tab=%s(%s) | empty or parse-failed -> skip tab", ticker, tab_name, tab_index)
                continue

            file_period = sanitize_filename(headers[4])
            file_name = f"{tab_name}_{ticker}_{file_period}.csv"
            output_file = ticker_output_dir / file_name

            if pd_module is not None:
                df = pd_module.DataFrame(rows, columns=["chi_tieu"] + headers)
                df.to_csv(output_file, index=False, encoding="utf-8-sig")
            else:
                write_rows_to_csv(output_file, headers=headers, rows=rows)

            exported += 1
            logging.info(
                "Ticker=%s | Tab=%s(%s) | rows=%s | file=%s",
                ticker,
                tab_name,
                tab_index,
                len(rows),
                output_file,
            )
        except Exception as exc:
            logging.exception("Ticker=%s | Tab=%s(%s) | error: %s", ticker, tab_name, tab_index, exc)
        finally:
            if request_sleep > 0:
                time.sleep(request_sleep)

    return exported


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Scrape detailed financial statement tables from stockbiz.vn by ticker batches."
    )
    parser.add_argument("--tickers-file", type=Path, default=default_tickers_file())
    parser.add_argument("--output-dir", type=Path, default=Path("./bctc_chi_tiet_csv"))
    parser.add_argument("--batch-size", type=int, default=20)
    parser.add_argument("--request-sleep", type=float, default=0.2)
    parser.add_argument("--connect-timeout", type=int, default=20)
    parser.add_argument("--read-timeout", type=int, default=45)
    parser.add_argument("--http-retries", type=int, default=3)
    parser.add_argument("--retry-backoff", type=float, default=1.0)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--max-tickers", type=int, default=0)
    parser.add_argument("--verbose", action="store_true", default=False)
    args = parser.parse_args()

    setup_logging(args.verbose)

    tickers = read_tickers(args.tickers_file)
    if args.max_tickers and args.max_tickers > 0:
        tickers = tickers[: args.max_tickers]

    if not tickers:
        raise SystemExit("No ticker found in tickers file.")

    args.output_dir.mkdir(parents=True, exist_ok=True)

    logging.info("Start scraping")
    logging.info("Tickers file: %s", args.tickers_file)
    logging.info("Output dir  : %s", args.output_dir)
    logging.info("Total ticker: %s", len(tickers))
    logging.info("Batch size  : %s", args.batch_size)
    logging.info("Workers     : %s", args.workers)
    logging.info("Timeout     : connect=%ss read=%ss", args.connect_timeout, args.read_timeout)
    logging.info("HTTP retries: %s | backoff=%s", args.http_retries, args.retry_backoff)

    total_exported = 0
    total_processed = 0
    pd_module = try_import_pandas()
    all_batches = list(chunked(tickers, args.batch_size))
    for batch_idx, batch in enumerate(all_batches, start=1):
        logging.info("=" * 80)
        logging.info("Batch %s/%s | size=%s | tickers=%s", batch_idx, len(all_batches), len(batch), batch)
        max_workers = max(1, min(args.workers, len(batch)))
        if max_workers == 1:
            for i, ticker in enumerate(batch, start=1):
                logging.info("[%s/%s in batch] Processing %s", i, len(batch), ticker)
                try:
                    exported = process_ticker(
                        ticker=ticker,
                        output_dir=args.output_dir,
                        request_sleep=args.request_sleep,
                        connect_timeout=args.connect_timeout,
                        read_timeout=args.read_timeout,
                        max_retries=args.http_retries,
                        retry_backoff=args.retry_backoff,
                        pd_module=pd_module,
                    )
                    total_exported += exported
                    total_processed += 1
                except Exception as exc:
                    logging.exception("Ticker=%s | fatal error: %s", ticker, exc)
        else:
            logging.info("Batch %s running with %s workers", batch_idx, max_workers)
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_ticker = {
                    executor.submit(
                        process_ticker,
                        ticker=ticker,
                        output_dir=args.output_dir,
                        request_sleep=args.request_sleep,
                        connect_timeout=args.connect_timeout,
                        read_timeout=args.read_timeout,
                        max_retries=args.http_retries,
                        retry_backoff=args.retry_backoff,
                        pd_module=pd_module,
                    ): ticker
                    for ticker in batch
                }
                for future in concurrent.futures.as_completed(future_to_ticker):
                    ticker = future_to_ticker[future]
                    try:
                        exported = future.result()
                        total_exported += exported
                        total_processed += 1
                        logging.info("Ticker=%s | completed | exported=%s", ticker, exported)
                    except Exception as exc:
                        logging.exception("Ticker=%s | fatal error: %s", ticker, exc)

        logging.info("Batch %s done", batch_idx)

    logging.info("=" * 80)
    logging.info("Finished | processed_tickers=%s | exported_csv=%s", total_processed, total_exported)


if __name__ == "__main__":
    main()

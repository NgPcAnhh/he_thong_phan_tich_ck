import re
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Iterable

import pandas as pd
import requests
from bs4 import BeautifulSoup


DEFAULT_TICKERS_PATH = Path(__file__).resolve().parent / "tickers_cache.txt"


def load_tickers(tickers: str | None = None, tickers_file: str | None = None, max_tickers: int = 0) -> list[str]:
    if tickers:
        symbols = [x.strip().upper() for x in tickers.split(",") if x.strip()]
    else:
        path = Path(tickers_file) if tickers_file else DEFAULT_TICKERS_PATH
        if not path.exists():
            raise FileNotFoundError(f"Ticker file not found: {path}")

        raw_lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
        seen = set()
        symbols = []
        for line in raw_lines:
            symbol = line.strip().upper()
            if symbol and symbol not in seen:
                seen.add(symbol)
                symbols.append(symbol)

    if max_tickers > 0:
        symbols = symbols[:max_tickers]

    if not symbols:
        raise ValueError("Ticker list is empty")

    return symbols


def get_raw_financial_data_smoney(ticker: str, retries: int = 3, retry_delay_seconds: float = 1.0) -> pd.DataFrame | str:
    url = f"https://smoney.com.vn/bao-cao-tai-chinh/{ticker}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }

    column_mapping = {
        "Biên lợi nhuận gộp (%)": "gross_margin",
        "Biên lợi nhuận (%)": "net_margin",
        "Đòn bẩy tài chính": "financial_leverage",
        "Vay ngắn và dài hạn trên vốn chủ": "long_short_term_debt_on_equity",
        "Nợ trên vốn chủ": "debt_to_equity",
        "Vòng quay tài sản": "asset_turnover",
        "Vòng quay tài sản cố định": "fixed_asset_turnover",
        "Số ngày thu tiền bình quân": "receivable_days",
        "Số ngày tồn kho bình quân": "inventory_days",
        "Số ngày thanh toán bình quân": "payable_days",
        "Chu kỳ tiền": "cash_conversion_cycle",
        "ROE (%)": "roe",
        "Tỷ suất sinh lời trên vốn đầu tư (%)": "roic",
        "ROA (%)": "roa",
        "Hệ số thanh toán hiện hành (%)": "current_ratio",
        "Hệ số thanh toán tiền mặt (%)": "cash_ratio",
        "Hệ số thanh toán nhanh (%)": "quick_ratio",
        "Vốn hoá (tỷ đồng)": "market_cap",
        "Số cp lưu hành (triệu CP)": "outstanding_shares",
        "PE": "pe",
        "PB": "pb",
        "PS": "ps",
        "Tỷ lệ giá trên dòng tiền": "p_cashflow",
        "EV/EBITDA": "ev_ebitda",
        "Tỷ suất cổ tức (%)": "dividend_yield",
    }

    ordered_cols = [
        "ticker",
        "quarter",
        "year",
        "gross_margin",
        "net_margin",
        "ebit_value",
        "financial_leverage",
        "long_short_term_debt_on_equity",
        "debt_to_equity",
        "asset_turnover",
        "fixed_asset_turnover",
        "receivable_days",
        "inventory_days",
        "payable_days",
        "cash_conversion_cycle",
        "inventory_turnover",
        "roe",
        "roic",
        "roa",
        "ebitda_value",
        "current_ratio",
        "cash_ratio",
        "quick_ratio",
        "interest_coverage_ratio",
        "market_cap",
        "outstanding_shares",
        "pe",
        "pb",
        "ps",
        "p_cashflow",
        "eps",
        "bvps",
        "ev_ebitda",
        "dividend_yield",
    ]

    def normalize_metric_name(text: str) -> str:
        return re.sub(r"\s+", " ", text.strip())

    def parse_period(period_text: str) -> tuple[int | None, int | None]:
        m = re.search(r"Q\s*(\d)\s*[-/]\s*(\d{4})", period_text, flags=re.IGNORECASE)
        if not m:
            return None, None
        return int(m.group(1)), int(m.group(2))

    def get_cell_text(value_text: str | None) -> str | None:
        if value_text is None:
            return None
        cleaned = value_text.strip().replace("\xa0", " ")
        return cleaned if cleaned else None

    last_error: str | None = None

    with requests.Session() as session:
        for attempt in range(1, retries + 1):
            try:
                response = session.get(url, headers=headers, timeout=30)
                soup = BeautifulSoup(response.text, "html.parser")
                table = soup.find("table", class_="table table-hover unified-metrics-table mb-0")

                if table is None and response.status_code != 200:
                    raise RuntimeError(f"HTTP {response.status_code} and table is missing")

                if table is None:
                    raise RuntimeError("Cannot find unified-metrics-table")

                period_texts = []
                seen = set()
                period_nodes = table.select("thead .period-col")

                for node in period_nodes:
                    candidates = node.select(".mb-0") or [node]
                    for cand in candidates:
                        txt = cand.get_text(" ", strip=True)
                        if re.search(r"Q\s*\d\s*[-/]\s*\d{4}", txt, flags=re.IGNORECASE):
                            normalized = re.sub(r"\s+", "", txt).upper().replace("/", "-")
                            if normalized not in seen:
                                seen.add(normalized)
                                period_texts.append(normalized)

                if not period_texts:
                    raise RuntimeError("Cannot parse period headers")

                records_by_period: dict[str, dict] = {}
                for p_text in period_texts:
                    quarter, year = parse_period(p_text)
                    records_by_period[p_text] = {
                        "ticker": ticker,
                        "quarter": quarter,
                        "year": year,
                    }

                metric_rows = table.select("tbody tr.metric-row")

                for row in metric_rows:
                    name_cell = row.select_one("td.metric-name-cell")
                    if not name_cell:
                        continue

                    metric_name = normalize_metric_name(name_cell.get_text(" ", strip=True))
                    metric_key = column_mapping.get(metric_name)
                    if metric_key is None:
                        continue

                    value_cells = row.select("td.text-center.metric-value-cell")
                    for idx, period_text in enumerate(period_texts):
                        if idx >= len(value_cells):
                            records_by_period[period_text][metric_key] = None
                            continue

                        span_tag = value_cells[idx].find("span")
                        raw_value = span_tag.get_text(strip=True) if span_tag else value_cells[idx].get_text(strip=True)
                        records_by_period[period_text][metric_key] = get_cell_text(raw_value)

                out_df = pd.DataFrame(records_by_period.values())

                for col in ordered_cols:
                    if col not in out_df.columns:
                        out_df[col] = None

                out_df = out_df[ordered_cols]
                out_df = out_df.sort_values(by=["year", "quarter"], ascending=[False, False], na_position="last").reset_index(drop=True)
                return out_df
            except Exception as exc:
                last_error = str(exc)
                if attempt < retries:
                    print(f"[SMONEY RETRY] {ticker}: attempt {attempt}/{retries} failed - {exc}")
                    time.sleep(retry_delay_seconds * attempt)

    return f"Cannot fetch SMONEY data after {retries} retries: {last_error}"


def crawl_smoney(
    tickers: Iterable[str],
    workers: int = 12,
    retries: int = 3,
    retry_delay_seconds: float = 1.0,
) -> pd.DataFrame:
    all_frames: list[pd.DataFrame] = []
    errors: list[tuple[str, str]] = []

    def _run_one(symbol: str):
        return symbol, get_raw_financial_data_smoney(symbol, retries=retries, retry_delay_seconds=retry_delay_seconds)

    with ThreadPoolExecutor(max_workers=max(1, workers)) as executor:
        futures = {executor.submit(_run_one, t): t for t in tickers}

        for future in as_completed(futures):
            symbol = futures[future]
            try:
                ticker, result = future.result()
                if isinstance(result, pd.DataFrame) and not result.empty:
                    all_frames.append(result)
                    print(f"[SMONEY OK] {ticker}: {len(result)} rows")
                else:
                    errors.append((ticker, str(result)))
                    print(f"[SMONEY SKIP] {ticker}: {result}")
            except Exception as exc:
                errors.append((symbol, str(exc)))
                print(f"[SMONEY ERR] {symbol}: {exc}")

    if errors:
        print(f"[SMONEY] Total tickers failed/no-data: {len(errors)}")

    if not all_frames:
        return pd.DataFrame()

    return pd.concat(all_frames, ignore_index=True)


def _to_float_safe(value: str | None) -> float | None:
    if value is None:
        return None

    if isinstance(value, (int, float)):
        if pd.isna(value):
            return None
        return float(value)

    text = value.strip()
    if text == "" or text.upper() in {"NA", "N/A", "--", "-"}:
        return None

    text = text.replace(",", "")
    text = re.sub(r"[^0-9.\-]", "", text)
    if text in {"", ".", "-", "-."}:
        return None

    try:
        return float(text)
    except ValueError:
        return None


def _quarter_from_report_term(term_id: int | str | None) -> int | None:
    mapping = {2: 1, 3: 2, 4: 3, 5: 4}
    try:
        return mapping.get(int(term_id))
    except (TypeError, ValueError):
        return None


def _pick_norm_id(norm_rows: list[dict], needle: str) -> int | None:
    needle_up = needle.upper()
    for row in norm_rows:
        name = str(row.get("ReportNormName") or "").upper()
        if needle_up in name:
            norm_id = row.get("ReportNormId")
            try:
                return int(norm_id)
            except (TypeError, ValueError):
                return None
    return None


def _build_detail_payload(ticker: str, periods: list[dict], total_count: int, token: str) -> str:
    parts: list[tuple[str, str]] = [("StockCode", ticker), ("Unit", "1000000000"), ("TypeCompare", "1")]
    for i, item in enumerate(periods):
        parts.extend(
            [
                (f"listReportDataIds[{i}][Index]", str(i)),
                (f"listReportDataIds[{i}][ReportDataId]", str(item.get("ReportDataID"))),
                (f"listReportDataIds[{i}][IsShowData]", "true" if item.get("IsShowData_Permission") else "false"),
                (f"listReportDataIds[{i}][RowNumber]", str(item.get("RowNumber"))),
                (f"listReportDataIds[{i}][YearPeriod]", str(item.get("YearPeriod"))),
                (f"listReportDataIds[{i}][TotalCount]", str(total_count)),
                (f"listReportDataIds[{i}][SortTimeType]", "Time_ASC"),
            ]
        )
    parts.append(("__RequestVerificationToken", token))
    return urllib.parse.urlencode(parts)


def init_vietstock_session(ticker: str, timeout: int = 30) -> tuple[requests.Session, str]:
    session = requests.Session()
    page_url = f"https://finance.vietstock.vn/{ticker}/tai-chinh.htm?tab=BCTT"
    page = session.get(page_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=timeout)
    page.raise_for_status()

    soup = BeautifulSoup(page.text, "html.parser")
    token_tag = soup.select_one('input[name="__RequestVerificationToken"]')
    if token_tag is None or not token_tag.get("value"):
        session.close()
        raise RuntimeError("Cannot get Vietstock request token")

    return session, token_tag.get("value")


def get_eps_bvps_vietstock_api(ticker: str, wait_seconds: int = 5, retries: int = 2) -> pd.DataFrame | str:
    del wait_seconds  # kept for backward compatibility with test script signature
    ajax_headers = {"User-Agent": "Mozilla/5.0", "X-Requested-With": "XMLHttpRequest"}

    last_error = None
    for attempt in range(1, retries + 1):
        session: requests.Session | None = None
        try:
            session, token = init_vietstock_session(ticker=ticker, timeout=30)

            periods_resp = session.post(
                "https://finance.vietstock.vn/data/BCTT_GetListReportData",
                data={
                    "StockCode": ticker,
                    "UnitedId": "-1",
                    "AuditedStatusId": "-1",
                    "Unit": "1000000000",
                    "IsNamDuongLich": "false",
                    "PeriodType": "QUY",
                    "SortTimeType": "Time_ASC",
                    "__RequestVerificationToken": token,
                },
                headers=ajax_headers,
                timeout=30,
            )
            periods_resp.raise_for_status()
            all_periods = periods_resp.json().get("data", [])
            if not all_periods:
                return "Cannot get report periods"

            selected_periods = list(reversed(all_periods[-9:]))

            norm_resp = session.post(
                "https://finance.vietstock.vn/data/GetListReportNorm_BCTT_ByStockCode",
                data={"stockCode": ticker, "__RequestVerificationToken": token},
                headers=ajax_headers,
                timeout=30,
            )
            norm_resp.raise_for_status()
            norm_rows = norm_resp.json().get("data", [])

            eps_norm_id = _pick_norm_id(norm_rows, "(EPS)")
            bvps_norm_id = _pick_norm_id(norm_rows, "(BVPS)")
            if eps_norm_id is None and bvps_norm_id is None:
                return "Cannot find EPS/BVPS norm id"

            detail_payload = _build_detail_payload(
                ticker=ticker,
                periods=selected_periods,
                total_count=len(all_periods),
                token=token,
            )

            detail_resp = session.post(
                "https://finance.vietstock.vn/data/GetReportDataDetailValue_BCTT_ByReportDataIds",
                data=detail_payload,
                headers={**ajax_headers, "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"},
                timeout=30,
            )
            detail_resp.raise_for_status()
            detail_rows = detail_resp.json().get("data", [])

            by_norm: dict[int, dict] = {}
            for row in detail_rows:
                try:
                    by_norm[int(row.get("ReportNormId"))] = row
                except (TypeError, ValueError):
                    continue

            eps_row = by_norm.get(eps_norm_id, {}) if eps_norm_id is not None else {}
            bvps_row = by_norm.get(bvps_norm_id, {}) if bvps_norm_id is not None else {}

            rows: list[dict] = []
            for idx, period in enumerate(selected_periods, start=1):
                quarter = _quarter_from_report_term(period.get("ReportTermID"))
                year = pd.to_numeric(period.get("YearPeriod"), errors="coerce")
                if pd.isna(year) or quarter is None:
                    continue

                eps_val = _to_float_safe(eps_row.get(f"Value{idx}")) if eps_row else None
                bvps_val = _to_float_safe(bvps_row.get(f"Value{idx}")) if bvps_row else None

                if eps_val is None and bvps_val is None:
                    continue

                rows.append(
                    {
                        "ticker": ticker,
                        "quarter": int(quarter),
                        "year": int(year),
                        "eps": eps_val,
                        "bvps": bvps_val,
                    }
                )

            if not rows:
                return "No valid EPS/BVPS data"

            return pd.DataFrame(rows)
        except Exception as exc:
            last_error = exc
            print(f"[VIETSTOCK RETRY] {ticker}: attempt {attempt}/{retries} failed - {exc}")
        finally:
            if session is not None:
                session.close()

    return f"Cannot fetch Vietstock data after {retries} retries: {last_error}"


def crawl_vietstock_eps_bvps(tickers: Iterable[str], workers: int = 8, wait_seconds: int = 5, retries: int = 2) -> pd.DataFrame:
    all_frames: list[pd.DataFrame] = []
    errors: list[tuple[str, str]] = []

    def _run_one(symbol: str):
        return symbol, get_eps_bvps_vietstock_api(symbol, wait_seconds=wait_seconds, retries=retries)

    with ThreadPoolExecutor(max_workers=max(1, workers)) as executor:
        futures = {executor.submit(_run_one, t): t for t in tickers}

        for future in as_completed(futures):
            symbol = futures[future]
            try:
                ticker, result = future.result()
                if isinstance(result, pd.DataFrame) and not result.empty:
                    all_frames.append(result)
                    print(f"[VIETSTOCK OK] {ticker}: {len(result)} rows")
                else:
                    errors.append((ticker, str(result)))
                    print(f"[VIETSTOCK SKIP] {ticker}: {result}")
            except Exception as exc:
                errors.append((symbol, str(exc)))
                print(f"[VIETSTOCK ERR] {symbol}: {exc}")

    if errors:
        print(f"[VIETSTOCK] Total tickers failed/no-data: {len(errors)}")

    if not all_frames:
        return pd.DataFrame(columns=["ticker", "quarter", "year", "eps", "bvps"])

    out = pd.concat(all_frames, ignore_index=True)
    out = out.sort_values(by=["ticker", "year", "quarter"], ascending=[True, False, False]).reset_index(drop=True)
    return out


def _normalize_key_types(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["ticker"] = out["ticker"].astype(str).str.upper().str.strip()
    out["quarter"] = pd.to_numeric(out["quarter"], errors="coerce").astype("Int64")
    out["year"] = pd.to_numeric(out["year"], errors="coerce").astype("Int64")
    return out


def merge_fill_eps_bvps(smoney_df: pd.DataFrame, vietstock_df: pd.DataFrame) -> pd.DataFrame:
    if smoney_df.empty:
        return smoney_df

    smoney_df = _normalize_key_types(smoney_df)
    vietstock_df = _normalize_key_types(vietstock_df)

    vs_small = (
        vietstock_df[["ticker", "quarter", "year", "eps", "bvps"]]
        .dropna(subset=["quarter", "year"], how="any")
        .drop_duplicates(subset=["ticker", "quarter", "year"], keep="first")
    )

    merged = smoney_df.merge(
        vs_small,
        on=["ticker", "quarter", "year"],
        how="left",
        suffixes=("", "_vs"),
    )

    if "eps" not in merged.columns:
        merged["eps"] = None
    if "bvps" not in merged.columns:
        merged["bvps"] = None

    merged["eps"] = merged["eps"].where(merged["eps"].notna(), merged.get("eps_vs"))
    merged["bvps"] = merged["bvps"].where(merged["bvps"].notna(), merged.get("bvps_vs"))

    drop_cols = [c for c in ["eps_vs", "bvps_vs"] if c in merged.columns]
    merged = merged.drop(columns=drop_cols)

    merged = merged.sort_values(by=["ticker", "year", "quarter"], ascending=[True, False, False], na_position="last")
    merged = merged.reset_index(drop=True)
    return merged


def run_pipeline_to_dataframe(
    tickers: list[str],
    smoney_workers: int,
    smoney_retries: int,
    smoney_retry_delay_seconds: float,
    vietstock_workers: int,
    vietstock_wait_seconds: int,
    vietstock_retries: int,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    print(f"[PIPELINE] Total tickers: {len(tickers)}")

    print("[PIPELINE] Start flow 1: SMONEY")
    smoney_df = crawl_smoney(
        tickers=tickers,
        workers=smoney_workers,
        retries=smoney_retries,
        retry_delay_seconds=smoney_retry_delay_seconds,
    )
    print(f"[PIPELINE] SMONEY done: {len(smoney_df)} rows")

    print("[PIPELINE] Start flow 2: VIETSTOCK")
    vietstock_df = crawl_vietstock_eps_bvps(
        tickers=tickers,
        workers=vietstock_workers,
        wait_seconds=vietstock_wait_seconds,
        retries=vietstock_retries,
    )
    print(f"[PIPELINE] VIETSTOCK done: {len(vietstock_df)} rows")

    final_df = merge_fill_eps_bvps(smoney_df=smoney_df, vietstock_df=vietstock_df)
    print(f"[PIPELINE] MERGE done: {len(final_df)} rows")

    return smoney_df, vietstock_df, final_df

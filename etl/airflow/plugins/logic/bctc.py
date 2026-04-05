import json
import re
import time
from pathlib import Path

import pandas as pd
from vnstock import Finance

# ---------------------------------------------------------------------------
# Nguồn duy nhất — không fallback, không retry để tránh tăng số request
# ---------------------------------------------------------------------------
SOURCE = "VCI"

# Interval giữa mỗi API call (giây). Target: ~19 req/phút (< 20 limit)
# Tính: 60 / 3.2 ≈ 18.75 req/phút
CALL_INTERVAL = 3.2


# ---------------------------------------------------------------------------
# ind_name → ind_code mapping (loaded from bctc.md)
# ---------------------------------------------------------------------------
_MAPPING_FILE = Path(__file__).resolve().parent.parent.parent.parent / "web_ptich_ck" / "md" / "bctc.md"

def _load_ind_code_mapping() -> dict:
    """Load ind_name → ind_code mapping from bctc.md JSON file."""
    mapping = {}
    try:
        with open(_MAPPING_FILE, "r", encoding="utf-8") as f:
            entries = json.load(f)
        for entry in entries:
            name = str(entry.get("ind_name", "")).strip()
            code = str(entry.get("ind_code", "")).strip()
            if name and code:
                mapping[name] = code
    except Exception as exc:
        print(f"⚠ Could not load bctc.md mapping: {exc}")
    return mapping

IND_NAME_TO_CODE = _load_ind_code_mapping()


def _slugify_fallback(text: str) -> str:
    """Fallback: generate ind_code from ind_name if not in mapping."""
    text = re.sub(r"[^A-Za-z0-9]+", "_", str(text)).strip("_")
    return text.lower() or "unknown"


def get_ind_code(ind_name: str) -> str:
    """Lookup ind_code from mapping, fallback to slugify."""
    name = str(ind_name).strip()
    return IND_NAME_TO_CODE.get(name, _slugify_fallback(name))


# ---------------------------------------------------------------------------
# Fetch 1 báo cáo cho 1 mã — GỌI ĐÚNG 1 LẦN, lỗi → bỏ qua (trả rỗng)
# ---------------------------------------------------------------------------
def _fetch_one_report(symbol: str, method: str) -> pd.DataFrame:
    """Gọi Finance(symbol).{method}() đúng 1 lần duy nhất, source=VCI.

    - Không retry, không fallback source → chỉ 1 request/report.
    - Bắt mọi exception kể cả SystemExit (vnstock gọi sys.exit khi 429).
    - Lỗi → print cảnh báo và trả DataFrame rỗng, bỏ qua mã đó.
    """
    try:
        finance = Finance(symbol=symbol, source=SOURCE, period="quarter")
        fetcher = getattr(finance, method, None)
        if fetcher is None:
            print(f"⚠️ {symbol}: Không tìm thấy hàm {method}")
            return pd.DataFrame()
        df = fetcher(period="quarter", lang="vi", dropna=True)
        if df is not None and isinstance(df, pd.DataFrame) and not df.empty:
            return df
        return pd.DataFrame()
    except TypeError:
        # Fallback nếu hàm không hỗ trợ params lang/dropna
        try:
            df = fetcher()
            if df is not None and isinstance(df, pd.DataFrame) and not df.empty:
                return df
        except Exception:
            pass
        return pd.DataFrame()
    except SystemExit as exc:
        print(f"⚠ {symbol}.{method}: SystemExit (rate-limit?): {str(exc)[:100]} → bỏ qua")
        return pd.DataFrame()
    except Exception as exc:
        print(f"❌ {symbol}.{method}: {type(exc).__name__}: {exc} → bỏ qua")
        return pd.DataFrame()



def transform_to_db_format(df: pd.DataFrame, report_name: str, statement_type: str, current_symbol: str) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    df = df.reset_index(drop=True)
    
    # VNStock v3 thường trả về: [ticker, year, quarter, ...chỉ tiêu...]
    col_ticker = df.columns[0] if len(df.columns) > 0 else None
    col_year = df.columns[1] if len(df.columns) > 1 else None
    col_quarter = df.columns[2] if len(df.columns) > 2 else None

    # Lọc cột chỉ tiêu (bỏ 3 cột đầu)
    indicator_cols = [c for c in df.columns if c not in [col_ticker, col_year, col_quarter]]
    
    if not indicator_cols:
        return pd.DataFrame()

    id_cols = [c for c in [col_ticker, col_year, col_quarter] if c is not None]
    
    # Melt dữ liệu: Chuyển cột ngang thành dòng dọc
    long_df = pd.melt(df, id_vars=id_cols, value_vars=indicator_cols, var_name="ind_name", value_name="value")

    # Đổi tên cột chuẩn
    long_df.rename(columns={
        col_ticker: "ticker",
        col_year: "year",
        col_quarter: "quarter",
    }, inplace=True)

    long_df["year"] = pd.to_numeric(long_df.get("year"), errors="coerce").astype("Int64")
    long_df["quarter"] = pd.to_numeric(long_df.get("quarter"), errors="coerce").astype("Int64")
    long_df["value"] = pd.to_numeric(long_df.get("value"), errors="coerce")

    # Bổ sung các cột meta đúng với schema lưu trữ
    long_df["report_name"] = report_name
    long_df["report_code"] = statement_type
    long_df["ind_code"] = long_df["ind_name"].apply(get_ind_code)
    long_df["import_time"] = pd.Timestamp.utcnow()
    
    # Đảm bảo cột ticker luôn đúng với mã đang request (quan trọng cho Batch)
    if "ticker" not in long_df.columns or long_df["ticker"].isnull().all():
        long_df["ticker"] = current_symbol
    else:
        long_df["ticker"] = long_df["ticker"].fillna(current_symbol)

    # Chọn và sắp xếp cột cuối cùng
    final_cols = [
        "ticker",
        "quarter",
        "year",
        "ind_name",
        "ind_code",
        "value",
        "import_time",
        "report_name",
        "report_code",
    ]

    return long_df[[c for c in final_cols if c in long_df.columns]]


# ---------------------------------------------------------------------------
# Main Logic — Sequential, 1 batch tại 1 thời điểm
# ---------------------------------------------------------------------------
REPORT_PLAN = [
    ("income_statement", "income_statement", "IS"),
    ("balance_sheet", "balance_sheet", "BL"),
    ("cash_flow", "cash_flow", "CF"),
]


def get_financial_reports(
    symbols: list,
    current_year: int | str | None = None,
    current_quarter: int | str | None = None,
) -> pd.DataFrame:
    """Fetch BCTC cho 1 batch symbols, lọc đúng 1 quý + 1 năm.

    Rate-limit: sleep CALL_INTERVAL (3.2s) sau mỗi API call.
    Chỉ 1 batch chạy tại 1 thời điểm (max_active_tis=1 ở DAG).
    → ~19 req/phút, an toàn dưới giới hạn 20.
    Không retry, lỗi → bỏ qua mã.
    """
    # Parse filters
    try:
        year_filter = int(current_year) if current_year is not None else None
    except Exception:
        year_filter = None

    try:
        quarter_filter = int(current_quarter) if current_quarter is not None else None
        if quarter_filter is not None and quarter_filter not in (1, 2, 3, 4):
            print(f"⚠ quarter={quarter_filter} không hợp lệ, bỏ lọc quý.")
            quarter_filter = None
    except Exception:
        quarter_filter = None

    print(f"[BCTC] Batch {len(symbols)} mã: {symbols}")
    print(f"  Filter: year={year_filter}, quarter={quarter_filter}")
    print(f"  Call interval: {CALL_INTERVAL}s (~{60/CALL_INTERVAL:.0f} req/phút)")

    frames: list[pd.DataFrame] = []
    skipped: list[str] = []

    for idx, symbol in enumerate(symbols):
        symbol = str(symbol).upper().strip()
        print(f" >> [{idx + 1}/{len(symbols)}] {symbol}")

        symbol_ok = True
        for method, report_name, stype in REPORT_PLAN:
            # Sleep TRƯỚC mỗi call (trừ call đầu tiên của batch)
            if idx > 0 or method != REPORT_PLAN[0][0]:
                time.sleep(CALL_INTERVAL)

            raw_df = _fetch_one_report(symbol, method)

            # Nếu trả về rỗng → bỏ qua mã này
            if raw_df is None or raw_df.empty:
                print(f"  ⚠ {symbol}.{method}: không có dữ liệu → bỏ qua mã này")
                symbol_ok = False
                skipped.append(symbol)
                break

            normalized = transform_to_db_format(
                raw_df, report_name, stype, current_symbol=symbol,
            )
            # Lọc đúng 1 quý + 1 năm (check cột tồn tại tránh KeyError)
            if year_filter is not None and "year" in normalized.columns:
                normalized = normalized[normalized["year"] == year_filter]
            if quarter_filter is not None and "quarter" in normalized.columns:
                normalized = normalized[normalized["quarter"] == quarter_filter]

            if not normalized.empty:
                frames.append(normalized)

    if skipped:
        print(f"⚠ Bỏ qua {len(skipped)} mã: {skipped}")

    if not frames:
        print("⚠ Batch này không thu được dữ liệu nào.")
        return pd.DataFrame()

    df_final = pd.concat(frames, ignore_index=True)
    print(f"✓ Hoàn thành batch. Tổng số dòng: {len(df_final)}")
    return df_final

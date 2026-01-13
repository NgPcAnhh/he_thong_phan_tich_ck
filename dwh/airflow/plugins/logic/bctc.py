import re
import time
from typing import Callable

import pandas as pd
from vnstock import Finance

"""Chuyển đổi tên chỉ tiêu thành slug (viết hoa, gạch dưới)."""
def slugify(text: str) -> str:
    text = re.sub(r"[^A-Za-z0-9]+", "_", str(text)).strip("_")
    return text.upper() or "UNKNOWN"

"""Gọi hàm từ thư viện vnstock an toàn."""
def _retry_call(callable_fn: Callable[[], pd.DataFrame], method: str, retries: int = 3, base_delay: float = 2.5) -> pd.DataFrame:
    for attempt in range(retries):
        try:
            return callable_fn()
        except Exception as exc:
            msg = str(exc)
            if "429" in msg or "Too Many Requests" in msg:
                wait = base_delay * (attempt + 1)
                print(f"❗ {method}: 429 Too Many Requests, retry {attempt + 1}/{retries} sau {wait:.1f}s")
                time.sleep(wait)
                continue
            print(f"❌ Lỗi {method}: {exc}")
            return pd.DataFrame()
    return pd.DataFrame()


def fetch_report(finance: Finance, method: str) -> pd.DataFrame:
    fetcher = getattr(finance, method, None)
    if fetcher is None:
        print(f"⚠️ Không tìm thấy hàm {method} trong Finance")
        return pd.DataFrame()

    try:
        return _retry_call(lambda: fetcher(period="quarter"), method)
    except TypeError:
        return _retry_call(lambda: fetcher(), method)



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
    long_df["ind_code"] = long_df["ind_name"].apply(slugify)
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


# --- Main Logic Function for Airflow ---
def get_financial_reports(symbols: list, current_year: int | str | None = None) -> pd.DataFrame:
    print(f"Bắt đầu xử lý batch {len(symbols)} mã: {symbols}")
    # Format: (tên_hàm_vnstock, tên_báo_cáo_db, loại_báo_cáo_viết_tắt)
    plan = [
        ("income_statement", "income_statement", "IS"),
        ("balance_sheet", "balance_sheet", "BL"),
        ("cash_flow", "cash_flow", "CF"),
    ]

    try:
        year_filter = int(current_year) if current_year is not None else None
    except Exception:
        year_filter = None

    all_normalized_frames = []

    for symbol in symbols:
        # Clean symbol input
        symbol = str(symbol).upper().strip()
        print(f" >> Đang lấy dữ liệu: {symbol}")
        
        try:
            # Khởi tạo client cho từng mã
            finance = Finance(symbol=symbol, source="vci", period="quarter")
            
            for method, report_name, stype in plan:
                # 1. Fetch
                raw_df = fetch_report(finance, method)
                
                # 2. Transform
                if raw_df is not None and not raw_df.empty:
                    normalized = transform_to_db_format(raw_df, report_name, stype, current_symbol=symbol)

                    if year_filter is not None:
                        normalized = normalized[normalized["year"] == year_filter]
                    
                    if not normalized.empty:
                        all_normalized_frames.append(normalized)

                # Throttle giữa các lời gọi phương pháp để tránh rate-limit
                time.sleep(1.5)
        
        except Exception as e:
            print(f" Lỗi xử lý mã {symbol}: {e}")
            continue # Bỏ qua mã lỗi, tiếp tục mã tiếp theo

        # Giảm tốc giữa các mã để tránh rate limit dồn dập
        time.sleep(2)

    # 3. Kết hợp dữ liệu
    if not all_normalized_frames:
        print(" ⚠ Batch này không thu được dữ liệu nào.")
        return pd.DataFrame()

    df_final = pd.concat(all_normalized_frames, ignore_index=True)
    
    print(f" ✓ Hoàn thành batch. Tổng số dòng: {len(df_final)}")
    return df_final
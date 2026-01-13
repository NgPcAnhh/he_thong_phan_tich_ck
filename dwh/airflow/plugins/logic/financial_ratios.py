"""
Logic module to fetch financial ratios for Vietnamese stocks using vnstock.
Uses the Finance.ratio() method to get key financial metrics.
"""
import time
from datetime import datetime
from typing import Callable

import pandas as pd
from vnstock import Finance


def _retry_call(callable_fn: Callable[[], pd.DataFrame], symbol: str, retries: int = 3, base_delay: float = 2.5) -> pd.DataFrame:
    """Gọi hàm từ thư viện vnstock an toàn với retry logic."""
    for attempt in range(retries):
        try:
            return callable_fn()
        except Exception as exc:
            msg = str(exc)
            if "429" in msg or "Too Many Requests" in msg:
                wait = base_delay * (attempt + 1)
                print(f"❗ {symbol}: 429 Too Many Requests, retry {attempt + 1}/{retries} sau {wait:.1f}s")
                time.sleep(wait)
                continue
            print(f"❌ Lỗi {symbol}: {exc}")
            return pd.DataFrame()
    return pd.DataFrame()


def _normalize_ratio_df(df: pd.DataFrame, symbol: str, period: str) -> pd.DataFrame:
    """Chuẩn hóa DataFrame và thêm metadata."""
    if df is None or df.empty:
        return pd.DataFrame()

    df = df.copy()
    df = df.reset_index(drop=True)
    
    # Đảm bảo có cột ticker (nếu không có thì thêm)
    if "ticker" not in df.columns:
        df.insert(0, "ticker", symbol)
    else:
        df["ticker"] = df["ticker"].fillna(symbol)
    
    # Thêm metadata
    df["period_type"] = period  # 'quarter' hoặc 'year'
    df["extracted_at"] = pd.Timestamp.utcnow()
    
    return df


def get_financial_ratios_batch(symbols: list, period: str = 'quarter') -> pd.DataFrame:
    """
    Fetch financial ratios for a batch of symbols.
    
    Args:
        symbols: List of stock ticker symbols
        period: 'quarter' for quarterly ratios, 'year' for annual ratios
        
    Returns:
        pd.DataFrame with financial ratios for all symbols
    """
    print(f"[RATIO] Bắt đầu xử lý batch {len(symbols)} mã: {symbols}")
    print(f"[RATIO] Kỳ dữ liệu: {period}")
    
    all_frames: list[pd.DataFrame] = []

    for symbol in symbols:
        # Clean symbol input
        symbol = str(symbol).upper().strip()
        print(f" >> [RATIO] Đang lấy dữ liệu: {symbol}")
        
        try:
            # Khởi tạo Finance client cho từng mã
            finance = Finance(symbol=symbol, source="VCI")
            
            # Fetch ratios với retry logic
            raw_df = _retry_call(
                lambda: finance.ratio(period=period, lang='vi', dropna=True),
                symbol
            )
            
            # Normalize data
            if raw_df is not None and not raw_df.empty:
                normalized = _normalize_ratio_df(raw_df, symbol, period)
                
                if not normalized.empty:
                    all_frames.append(normalized)
                    print(f" ✓ [RATIO] {symbol}: {len(normalized)} rows")
                else:
                    print(f" ⚠️ [RATIO] {symbol}: No data after normalization")
            else:
                print(f" ⚠️ [RATIO] {symbol}: No data from API")
            
            # Throttle giữa các mã để tránh rate-limit
            time.sleep(2)
            
        except Exception as e:
            print(f" ❌ [RATIO] Lỗi xử lý mã {symbol}: {e}")
            continue  # Bỏ qua mã lỗi, tiếp tục mã tiếp theo

    # Kết hợp dữ liệu
    if not all_frames:
        print(" ⚠ [RATIO] Batch này không thu được dữ liệu nào.")
        return pd.DataFrame()

    df_final = pd.concat(all_frames, ignore_index=True)
    print(f" ✅ [RATIO] Hoàn thành batch. Tổng số dòng: {len(df_final)}")
    
    return df_final

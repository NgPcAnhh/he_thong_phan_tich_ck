from pathlib import Path

from airflow.exceptions import AirflowException
from vnstock import Listing
import numpy as np

"""
Trả về list các list mã. VD: [['HPG', 'VIC'], ['VNM', 'FPT']] để tiện chạy song song theo batch
"""

# Sàn chính — dùng khi muốn loại UPCOM
MAIN_EXCHANGES = {"HOSE", "HNX"}


def _fetch_tickers(exclude_upcom: bool = False) -> list[str]:
    """Lấy danh sách mã từ vnstock API, có cache.

    Args:
        exclude_upcom: True để chỉ lấy HOSE + HNX, bỏ UPCOM.
    """
    cache_path = Path(__file__).resolve().parent / "tickers_cache.txt"

    tickers: list[str] = []

    try:
        df = Listing().all_symbols()
        print(f"[list_macp] Tổng mã từ API: {len(df)}")

        # Filter UPCOM nếu được yêu cầu
        if exclude_upcom:
            exchange_col = None
            for col in ["exchange", "organ", "comGroupCode"]:
                if col in df.columns:
                    exchange_col = col
                    break

            if exchange_col:
                before = len(df)
                df = df[df[exchange_col].str.upper().isin(MAIN_EXCHANGES)]
                print(f"[list_macp] Loại UPCOM ({exchange_col}): {before} → {len(df)}")
            else:
                print(f"[list_macp] ⚠ Không tìm thấy cột exchange, lấy tất cả. Columns: {list(df.columns)}")

        tickers = df["symbol"].astype(str).str.upper().str.strip().unique().tolist()
        if tickers:
            cache_path.write_text("\n".join(tickers), encoding="utf-8")
    except Exception as e:
        print(f"Lỗi lấy danh sách mã: {e}. Dùng cache nếu có.")
        if cache_path.exists():
            tickers = [line.strip().upper() for line in cache_path.read_text(encoding="utf-8").splitlines() if line.strip()]

    if not tickers:
        raise AirflowException("Danh sách mã rỗng: API thất bại và không có cache tickers_cache.txt")

    return tickers


def get_all_tickers(exclude_upcom: bool = False) -> list[str]:
    """Trả về flat list mã cổ phiếu."""
    return _fetch_tickers(exclude_upcom=exclude_upcom)


def get_ticker_batches(batch_size=20, exclude_upcom: bool = False):
    """Trả về list các batch mã. VD: [['HPG', 'VIC'], ['VNM', 'FPT']]"""
    tickers = _fetch_tickers(exclude_upcom=exclude_upcom)
    batches = [tickers[i:i + batch_size] for i in range(0, len(tickers), batch_size)]
    return batches
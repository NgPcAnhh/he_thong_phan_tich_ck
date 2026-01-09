from pathlib import Path

from airflow.exceptions import AirflowException
from vnstock import Listing
import numpy as np

"""
Trả về list các list mã. VD: [['HPG', 'VIC'], ['VNM', 'FPT']] để tện chạy song song theo batch
"""

def get_ticker_batches(batch_size=20):
    cache_path = Path(__file__).resolve().parent / "tickers_cache.txt"

    tickers: list[str] = []

    try:
        df = Listing().all_symbols()
        tickers = df["symbol"].astype(str).str.upper().str.strip().unique().tolist()
        if tickers:
            cache_path.write_text("\n".join(tickers), encoding="utf-8")
    except Exception as e:
        print(f"Lỗi lấy danh sách mã: {e}. Dùng cache nếu có.")
        if cache_path.exists():
            tickers = [line.strip().upper() for line in cache_path.read_text(encoding="utf-8").splitlines() if line.strip()]

    if not tickers:
        raise AirflowException("Danh sách mã rỗng: API thất bại và không có cache tickers_cache.txt")

    batches = [tickers[i:i + batch_size] for i in range(0, len(tickers), batch_size)]
    return batches
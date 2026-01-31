from datetime import datetime

import pandas as pd

from logic.history_price import get_history_price_batch


def get_daily_price(
    target_date: str | None = None,
    **kwargs  # Bỏ qua các params không dùng để tương thích với DAG cũ
) -> pd.DataFrame:
    """
    Lấy giá giao dịch của ngày target_date cho tất cả mã CP.
    Sử dụng lại logic từ history_price nhưng chỉ lấy 1 ngày.
    """
    from logic.list_macp import get_ticker_batches
    
    date_str = pd.to_datetime(target_date or datetime.utcnow().date()).strftime("%Y-%m-%d")
    
    # Lấy tất cả tickers và gọi history_price với start = end = target_date
    all_tickers = []
    for batch in get_ticker_batches(batch_size=999999):  # Lấy tất cả
        all_tickers.extend(batch)
    
    print(f"[daily_price] Collecting {len(all_tickers)} tickers for {date_str} using history_price logic")
    
    # Gọi hàm đã test tốt từ history_price
    df = get_history_price_batch(
        symbols=all_tickers,
        start_date=date_str,
        end_date=date_str
    )
    
    if df.empty:
        raise RuntimeError(f"No price data collected on {date_str}; market likely closed or API unavailable.")
    
    print(f"[daily_price] Collected {len(df)} records for {date_str}")
    return df

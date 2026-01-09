import time
from datetime import datetime

import pandas as pd
from vnstock import Quote


def _retry_price(symbol: str, start_date: str, end_date: str, retries: int = 3, base_delay: float = 1.5) -> pd.DataFrame:
    """Call vnstock Quote.history with simple retry for rate-limit errors."""
    for attempt in range(retries):
        try:
            quote = Quote(symbol=symbol, source="VCI")
            df = quote.history(start=start_date, end=end_date)
            return df if isinstance(df, pd.DataFrame) else pd.DataFrame()
        except Exception as exc:
            msg = str(exc)
            if "429" in msg or "Too Many Requests" in msg:
                wait = base_delay * (attempt + 1)
                print(f"{symbol}: 429 Too Many Requests, retry {attempt + 1}/{retries} in {wait:.1f}s")
                time.sleep(wait)
                continue
            print(f"{symbol}: {exc}")
            return pd.DataFrame()
    return pd.DataFrame()


def _normalize_price_df(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    """Ensure consistent columns for downstream storage."""
    if df is None or df.empty:
        return pd.DataFrame()

    df = df.copy()
    rename_map = {"time": "date", "Time": "date"}
    df.rename(columns=rename_map, inplace=True)

    if "date" in df.columns:
        df["trading_date"] = pd.to_datetime(df["date"], errors="coerce").dt.date.astype(str)
    elif "trading_date" not in df.columns:
        return pd.DataFrame()

    # VNStock trả ra open, high, close, low, volume
    for col in ["open", "high", "low", "close", "volume"]:
        if col not in df.columns:
            df[col] = pd.NA

    df["ticker"] = symbol

    cols = ["ticker", "trading_date", "open", "high", "low", "close", "volume"]
    return df[cols].dropna(subset=["trading_date"])


def get_history_price_batch(symbols: list, start_date: str | None = None, end_date: str | None = None) -> pd.DataFrame:
    """Fetch daily historical prices for a batch of symbols."""
    start = start_date or "2008-01-01"  # theo yêu cầu: lấy từ 2008 trở đi
    end = end_date or datetime.utcnow().strftime("%Y-%m-%d")

    frames: list[pd.DataFrame] = []

    for symbol in symbols:
        sym = str(symbol).upper().strip()
        raw_df = _retry_price(sym, start, end)
        norm_df = _normalize_price_df(raw_df, sym)
        if not norm_df.empty:
            frames.append(norm_df)
        time.sleep(0.5)

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
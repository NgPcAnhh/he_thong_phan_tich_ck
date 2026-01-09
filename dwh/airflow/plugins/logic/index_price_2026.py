import time
from datetime import datetime
from typing import Iterable

import pandas as pd

from logic.history_price import _normalize_price_df, _retry_price

INDEX_SYMBOLS: Iterable[str] = [
    "VNINDEX",
    "HNXINDEX",
    "UPCOMINDEX",
    "VN30",
    "HNX30",
]

START_DATE = datetime(2026, 1, 1).date()


def get_index_price_2026(end_date: str | None = None, sleep_time: float = 1.0) -> pd.DataFrame:
    end_dt = pd.to_datetime(end_date or datetime.utcnow().date()).date()
    start_str = START_DATE.strftime("%Y-%m-%d")
    end_str = end_dt.strftime("%Y-%m-%d")

    frames: list[pd.DataFrame] = []

    for symbol in INDEX_SYMBOLS:
        sym_clean = symbol.upper().strip()
        raw_df = _retry_price(sym_clean, start_str, end_str, retries=3, base_delay=sleep_time)
        norm_df = _normalize_price_df(raw_df, sym_clean)

        if norm_df is None or norm_df.empty:
            time.sleep(sleep_time)
            continue

        norm_df = norm_df.copy()
        norm_df["trading_date"] = pd.to_datetime(norm_df["trading_date"], errors="coerce").dt.date
        norm_df.dropna(subset=["trading_date"], inplace=True)
        norm_df = norm_df[(norm_df["trading_date"] >= START_DATE) & (norm_df["trading_date"] <= end_dt)]

        if not norm_df.empty:
            norm_df["trading_date"] = norm_df["trading_date"].astype(str)
            frames.append(norm_df)

        time.sleep(sleep_time)

    if not frames:
        raise RuntimeError(f"No index prices collected between {start_str} and {end_str}.")

    out = pd.concat(frames, ignore_index=True)
    out.sort_values(["ticker", "trading_date"], inplace=True)
    out.drop_duplicates(subset=["ticker", "trading_date"], keep="last", inplace=True)
    return out

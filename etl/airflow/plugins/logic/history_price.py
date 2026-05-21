import re
import time
from datetime import datetime

import pandas as pd
from vnstock import Quote


# === RATE LIMIT CONFIG ===
# vnstock Guest: 20 req/min → mỗi request cách nhau ít nhất 3s
# Với sleep 3.5s: 20 symbols × 3.5s = 70s/batch → ~17 req/min (an toàn)
RATE_LIMIT_DELAY = 1.0  # Giây chờ giữa mỗi symbol
RATE_LIMIT_RETRY_DEFAULT = 35  # Giây chờ mặc định khi bị rate limit


def _parse_wait_seconds(error_msg: str) -> int:
    """Parse 'Chờ X giây' or 'Wait X seconds' from rate limit error message."""
    patterns = [
        r"[Cc]hờ\s+(\d+)\s+giây",
        r"[Ww]ait\s+(\d+)\s+(?:giây|second)",
    ]
    for pattern in patterns:
        match = re.search(pattern, error_msg)
        if match:
            return int(match.group(1))
    return RATE_LIMIT_RETRY_DEFAULT


def _is_rate_limit_error(exc_msg: str) -> bool:
    """Check if error is specifically a rate limit (429) error."""
    rate_limit_keywords = [
        "429", "rate limit", "giới hạn api", "rate_limit",
        "too many requests", "request limit",
    ]
    msg_lower = exc_msg.lower()
    return any(kw in msg_lower for kw in rate_limit_keywords)


def _retry_price(symbol: str, start_date: str, end_date: str, retries: int = 3, base_delay: float = 5.0, sources: list = None) -> pd.DataFrame:
    """Call vnstock Quote.history with retry for rate-limit and connection errors.
    
    Args:
        symbol: Stock/index symbol
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        retries: Number of retry attempts per source
        base_delay: Base delay between retries (exponential backoff)
        sources: List of data sources to try (default: ["VCI", "TCBS"])
    
    Returns:
        DataFrame with price data or empty DataFrame on failure
    """
    if sources is None:
        sources = ["VCI", "TCBS"]
    
    # Retryable error keywords
    RETRYABLE_ERRORS = [
        "429", "Too Many Requests",
        "ConnectionError", "Connection",
        "RetryError", "Retry",
        "Timeout", "TimeoutError",
        "SSLError", "SSL",
        "HTTPError", "HTTP",
        "NameResolutionError",
        "NewConnectionError",
        "MaxRetryError",
        "ImportError",
        "rate limit", "Rate Limit",
    ]
    
    def is_retryable(exc_msg: str) -> bool:
        return any(err.lower() in exc_msg.lower() for err in RETRYABLE_ERRORS)
    
    for source in sources:
        for attempt in range(retries):
            try:
                quote = Quote(symbol=symbol, source=source)
                df = quote.history(start=start_date, end=end_date)
                if df is not None and isinstance(df, pd.DataFrame) and not df.empty:
                    return df
                # Empty result, try next attempt
                if attempt < retries - 1:
                    time.sleep(base_delay)
                    continue
            except Exception as exc:
                msg = str(exc)
                exc_type = type(exc).__name__
                full_msg = f"{exc_type}: {msg}"
                
                if _is_rate_limit_error(full_msg):
                    # Rate limit: parse wait time from error message
                    wait = _parse_wait_seconds(full_msg)
                    print(f"{symbol}: ⚠️ Rate limit hit (source={source}), waiting {wait}s before retry {attempt + 1}/{retries}")
                    time.sleep(wait)
                    continue
                elif is_retryable(full_msg):
                    wait = base_delay * (2 ** attempt)  # Exponential backoff
                    print(f"{symbol}: {exc_type} (source={source}), retry {attempt + 1}/{retries} in {wait:.1f}s")
                    time.sleep(wait)
                    continue
                else:
                    # Non-retryable error, try next source
                    print(f"{symbol}: Non-retryable error (source={source}): {full_msg}")
                    break
        
        # If we exhausted retries for current source, log and try next source
        print(f"{symbol}: Exhausted retries for source={source}, trying next source...")
    
    print(f"{symbol}: All sources failed after retries")
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
    """Fetch daily historical prices for a batch of symbols.
    
    Rate limit: vnstock Guest tier = 20 requests/minute.
    With RATE_LIMIT_DELAY=3.5s: 20 symbols × 3.5s = 70s → ~17 req/min (safe).
    """
    start = start_date or "2026-01-01"  # theo yêu cầu: lấy từ 2008 trở đi
    end = end_date or datetime.utcnow().strftime("%Y-%m-%d")

    frames: list[pd.DataFrame] = []
    total = len(symbols)

    for idx, symbol in enumerate(symbols, 1):
        sym = str(symbol).upper().strip()
        print(f"[{idx}/{total}] Fetching {sym}...")
        
        raw_df = _retry_price(sym, start, end)
        norm_df = _normalize_price_df(raw_df, sym)
        if not norm_df.empty:
            frames.append(norm_df)
            print(f"  ✓ {sym}: {len(norm_df)} rows")
        else:
            print(f"  ⚠ {sym}: no data")
        
        # Rate limit: chờ giữa mỗi symbol để tránh vượt 20 req/phút
        if idx < total:
            time.sleep(RATE_LIMIT_DELAY)

    print(f"\n📊 Batch complete: {len(frames)}/{total} symbols successful")
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
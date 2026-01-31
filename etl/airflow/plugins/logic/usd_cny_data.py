"""
Logic module to fetch USD/CNY exchange rate data using yfinance.
Data is fetched from Yahoo Finance using the USDCNY=X ticker.
"""
from datetime import datetime
import pandas as pd
import yfinance as yf


def get_usd_cny_rate(start_date: str | None = None, end_date: str | None = None) -> pd.DataFrame:
    """
    Fetch historical USD/CNY exchange rate data from Yahoo Finance.
    
    Args:
        start_date: Start date in 'YYYY-MM-DD' format. Defaults to '2000-01-01'
        end_date: End date in 'YYYY-MM-DD' format. Defaults to today
        
    Returns:
        pd.DataFrame with columns: date, open, high, low, close, volume, indicator_type
    """
    # Default date range
    start = start_date or "2000-01-01"
    end = end_date or datetime.now().strftime("%Y-%m-%d")
    
    try:
        # Fetch USD/CNY exchange rate (USDCNY=X)
        ticker = yf.Ticker("USDCNY=X")
        df = ticker.history(start=start, end=end)
        
        if df.empty:
            print(f"⚠️ No USD/CNY data found for period {start} to {end}")
            return pd.DataFrame()
        
        # Reset index to make date a column
        df = df.reset_index()
        
        # Rename columns to match expected format
        df = df.rename(columns={
            "Date": "date",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume"
        })
        
        # Convert date to string format
        df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
        
        # Add indicator type identifier
        df["indicator_type"] = "USD_CNY"
        
        # Select only required columns
        df = df[["date", "open", "high", "low", "close", "volume", "indicator_type"]]
        
        print(f"✅ Fetched {len(df)} rows of USD/CNY data from {start} to {end}")
        return df
        
    except Exception as e:
        print(f"❌ Error fetching USD/CNY data: {e}")
        return pd.DataFrame()

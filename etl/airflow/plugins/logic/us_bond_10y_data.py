"""
Logic module to fetch US 10-Year Treasury Bond Yield data using yfinance.
Data is fetched from Yahoo Finance using the ^TNX ticker.
"""
from datetime import datetime
import pandas as pd
import yfinance as yf


def get_us_bond_10y(start_date: str | None = None, end_date: str | None = None) -> pd.DataFrame:
    """
    Fetch historical US 10-Year Treasury Bond Yield data from Yahoo Finance.
    
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
        # Fetch US 10-Year Treasury Yield (^TNX)
        ticker = yf.Ticker("^TNX")
        df = ticker.history(start=start, end=end)
        
        if df.empty:
            print(f"⚠️ No US 10Y Bond data found for period {start} to {end}")
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
        df["indicator_type"] = "US_BOND_10Y"
        
        # Select only required columns
        df = df[["date", "open", "high", "low", "close", "volume", "indicator_type"]]
        
        print(f"✅ Fetched {len(df)} rows of US 10Y Bond yield data from {start} to {end}")
        return df
        
    except Exception as e:
        print(f"❌ Error fetching US 10Y Bond data: {e}")
        return pd.DataFrame()

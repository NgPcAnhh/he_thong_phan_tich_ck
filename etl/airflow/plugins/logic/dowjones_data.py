"""
Logic module to fetch Dow Jones Industrial Average (DJIA) index data using yfinance.
Data is fetched from Yahoo Finance using the ^DJI ticker.
"""
from datetime import datetime
import pandas as pd
import yfinance as yf


def get_dowjones_index(start_date: str | None = None, end_date: str | None = None) -> pd.DataFrame:
    """
    Fetch historical Dow Jones index data from Yahoo Finance.
    
    Args:
        start_date: Start date in 'YYYY-MM-DD' format. Defaults to '2000-01-01'
        end_date: End date in 'YYYY-MM-DD' format. Defaults to today
        
    Returns:
        pd.DataFrame with columns: date, open, high, low, close, volume, asset_type
    """
    # Default date range
    start = start_date or "2000-01-01"
    end = end_date or datetime.now().strftime("%Y-%m-%d")
    
    try:
        # Fetch Dow Jones Industrial Average data (^DJI)
        ticker = yf.Ticker("^DJI")
        df = ticker.history(start=start, end=end)
        
        if df.empty:
            print(f"⚠️ No Dow Jones index data found for period {start} to {end}")
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
        
        # Add asset type identifier
        df["asset_type"] = "DOWJONES"
        
        # Select only required columns
        df = df[["date", "open", "high", "low", "close", "volume", "asset_type"]]
        
        print(f"✅ Fetched {len(df)} rows of Dow Jones index data from {start} to {end}")
        return df
        
    except Exception as e:
        print(f"❌ Error fetching Dow Jones index data: {e}")
        return pd.DataFrame()

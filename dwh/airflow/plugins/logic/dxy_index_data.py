"""
Logic module to fetch US Dollar Index (DXY) data using yfinance.
Data is fetched from Yahoo Finance using the DX-Y.NYB ticker.
"""
from datetime import datetime
import pandas as pd
import yfinance as yf


def get_dxy_index(start_date: str | None = None, end_date: str | None = None) -> pd.DataFrame:
    """
    Fetch historical DXY (US Dollar Index) data from Yahoo Finance.
    
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
        # Fetch DXY Index (DX-Y.NYB)
        ticker = yf.Ticker("DX-Y.NYB")
        df = ticker.history(start=start, end=end)
        
        if df.empty:
            print(f"⚠️ No DXY Index data found for period {start} to {end}")
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
        df["indicator_type"] = "DXY_INDEX"
        
        # Select only required columns
        df = df[["date", "open", "high", "low", "close", "volume", "indicator_type"]]
        
        print(f"✅ Fetched {len(df)} rows of DXY Index data from {start} to {end}")
        return df
        
    except Exception as e:
        print(f"❌ Error fetching DXY Index data: {e}")
        return pd.DataFrame()

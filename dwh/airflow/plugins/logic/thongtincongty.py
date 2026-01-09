import time

import pandas as pd
from vnstock import Company


def _retry_df(fn, symbol: str, label: str, retries: int = 3, base_delay: float = 1.5) -> pd.DataFrame:
    for attempt in range(retries):
        try:
            return fn()
        except Exception as exc:
            msg = str(exc)
            if "429" in msg or "Too Many Requests" in msg:
                wait = base_delay * (attempt + 1)
                print(f"{label} {symbol}: 429, retry {attempt + 1}/{retries} sau {wait:.1f}s")
                time.sleep(wait)
                continue
            print(f"{label} {symbol}: {exc}")
            return pd.DataFrame()
    return pd.DataFrame()

def get_overview_batch(symbols: list) -> pd.DataFrame:
    frames = []
    cols = ["symbol", "company_profile", "icb_name2", "icb_name3", "icb_name4"]
    
    for symbol in symbols:
        try:
            company = Company(symbol=symbol, source="vci")
            overview = _retry_df(lambda: company.overview(), symbol, "Err overview")
            
            # Chuẩn hóa cột
            for c in cols:
                if c not in overview.columns: overview[c] = ""
            
            clean_df = overview[cols].drop_duplicates(subset=["symbol"])
            frames.append(clean_df)
        except Exception as e:
            print(f"Err overview {symbol}: {e}")

        time.sleep(1)
            
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def get_people_batch(symbols: list) -> pd.DataFrame:
    frames = []
    for symbol in symbols:
        try:
            company = Company(symbol=symbol, source="vci")
            
            # 1. Shareholders
            sh = _retry_df(lambda: company.shareholders(), symbol, "shareholders")
            if not sh.empty:
                sh["symbol"] = symbol
                sh["name"] = sh.get("share_holder", "")
                sh["position"] = "Cổ đông"
                sh["percent"] = sh.get("share_own_percent", 0)
                sh["type"] = "shareholder"
            
            # 2. Officers
            of = _retry_df(lambda: company.officers(filter_by="working"), symbol, "officers")
            if not of.empty:
                of["symbol"] = symbol
                of["name"] = of.get("officer_name", "")
                of["position"] = of.get("officer_position", "")
                of["percent"] = of.get("officer_own_percent", 0)
                of["type"] = "officer"

            # Merge
            cols = ["symbol", "name", "position", "percent", "type"]
            # Reindex để tránh lỗi thiếu cột
            df_sh = sh.reindex(columns=cols) if not sh.empty else pd.DataFrame()
            df_of = of.reindex(columns=cols) if not of.empty else pd.DataFrame()
            
            combined = pd.concat([df_sh, df_of], ignore_index=True)
            frames.append(combined)
            
        except Exception:
            continue

        time.sleep(1)
            
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
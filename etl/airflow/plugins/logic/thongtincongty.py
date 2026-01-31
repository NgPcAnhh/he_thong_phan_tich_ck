import time

import pandas as pd
from vnstock import Company, Listing


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


def _normalize_symbol(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    df = df.copy()
    symbol_col = None
    for candidate in ["symbol", "ticker", "code"]:
        if candidate in df.columns:
            symbol_col = candidate
            break

    if symbol_col is None:
        return pd.DataFrame()

    df["symbol"] = df[symbol_col].astype(str).str.upper().str.strip()
    return df


def _load_listing_meta() -> pd.DataFrame:
    try:
        listing = Listing()
        industries = _normalize_symbol(listing.symbols_by_industries())
        exchanges = _normalize_symbol(listing.symbols_by_exchange())
    except Exception as exc:
        print(f"Listing metadata error: {exc}")
        industries = pd.DataFrame()
        exchanges = pd.DataFrame()

    base_cols = ["symbol", "icb_name2", "icb_name3", "icb_name4"]
    if industries.empty:
        industries = pd.DataFrame(columns=base_cols)
    for col in base_cols:
        if col not in industries.columns:
            industries[col] = ""
    industries = industries[base_cols].drop_duplicates(subset=["symbol"])

    exch_cols = ["symbol", "exchange", "type_info", "organ_short_name", "organ_name"]
    if exchanges.empty:
        exchanges = pd.DataFrame(columns=exch_cols)
    for col in exch_cols:
        if col not in exchanges.columns:
            exchanges[col] = ""
    exchanges = exchanges[exch_cols].drop_duplicates(subset=["symbol"])

    return industries.merge(exchanges, on="symbol", how="left")

def get_overview_batch(symbols: list) -> pd.DataFrame:
    frames = []
    cols = [
        "symbol",
        "company_profile",
        "icb_name2",
        "icb_name3",
        "icb_name4",
        "exchange",
        "type_info",
        "organ_short_name",
        "organ_name",
    ]

    listing_meta = _load_listing_meta()
    
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
            
    combined = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if combined.empty:
        return combined

    combined["symbol"] = combined["symbol"].astype(str).str.upper().str.strip()

    if not listing_meta.empty:
        combined = combined.merge(listing_meta, on="symbol", how="left", suffixes=("", "_listing"))

        for col in ["icb_name2", "icb_name3", "icb_name4"]:
            listing_col = f"{col}_listing"
            if listing_col in combined.columns:
                combined[col] = combined[col].where(
                    combined[col].notna() & (combined[col] != ""), combined[listing_col]
                )

        for col in ["exchange", "type_info", "organ_short_name", "organ_name"]:
            listing_col = f"{col}_listing"
            if listing_col in combined.columns and col in combined.columns:
                combined[col] = combined[col].where(
                    combined[col].notna() & (combined[col] != ""), combined[listing_col]
                )
            elif listing_col in combined.columns:
                combined[col] = combined[listing_col]

        listing_cols = [c for c in combined.columns if c.endswith("_listing")]
        combined.drop(columns=listing_cols, inplace=True, errors="ignore")

    for col in cols:
        if col not in combined.columns:
            combined[col] = ""

    return combined[cols]


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
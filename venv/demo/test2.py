import time
import pandas as pd
from vnstock import Quote
from datetime import datetime

ETF_LIST = [
    "E1VN30",
    "FUEVFVND",
    "FUESSVFL",
    "FUEMAV30",
    "FUESSV50"
]

START_YEAR = 2014   # ETF VN ra đời muộn
END_YEAR = datetime.today().year


def fetch_etf(symbol):
    quote = Quote(symbol=symbol, source="VCI")
    frames = []

    for year in range(START_YEAR, END_YEAR + 1):
        start = f"{year}-01-01"
        end = f"{year}-12-31"

        try:
            df = quote.history(
                start=start,
                end=end,
                interval="1D"
            )

            if df is not None and not df.empty:
                frames.append(df)

            time.sleep(1.2)  # bắt buộc để tránh bị block

        except Exception as e:
            print(f"[WARN] {symbol} {year}: {e}")
            time.sleep(3)

    if not frames:
        return None

    out = pd.concat(frames, ignore_index=True)
    out["fund_code"] = symbol
    out["fund_type"] = "ETF"
    return out


def main():
    all_data = []

    for etf in ETF_LIST:
        print(f"Fetching {etf} ...")
        df = fetch_etf(etf)
        if df is not None:
            all_data.append(df)

    if not all_data:
        raise RuntimeError("Không lấy được dữ liệu ETF nào")

    df_all = pd.concat(all_data, ignore_index=True)
    df_all.sort_values(["fund_code", "time"], inplace=True)

    df_all.to_csv("vn_etf_price_history.csv", index=False)
    print("Saved: vn_etf_price_history.csv")


if __name__ == "__main__":
    main()

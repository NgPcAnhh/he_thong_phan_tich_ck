import time
import pandas as pd
from vnstock import Quote
from datetime import datetime

INDEX_LIST = [
    "VNINDEX",
    "HNXINDEX",
    "UPCOMINDEX",
    "VN30",
    "HNX30"
]

START_YEAR = 2000
END_YEAR = datetime.today().year


def fetch_index(symbol):
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

            time.sleep(1.2)  # tránh bị block

        except Exception as e:
            print(f"[WARN] {symbol} {year}: {e}")
            time.sleep(3)

    if not frames:
        return None

    out = pd.concat(frames, ignore_index=True)
    out["index_code"] = symbol
    return out


def main():
    all_data = []

    for idx in INDEX_LIST:
        print(f"Fetching {idx} ...")
        df = fetch_index(idx)
        if df is not None:
            all_data.append(df)

    if not all_data:
        raise RuntimeError("Không lấy được dữ liệu index nào")

    df_all = pd.concat(all_data, ignore_index=True)
    df_all.sort_values(["index_code", "time"], inplace=True)

    df_all.to_csv("vn_index_history.csv", index=False)
    print("Saved: vn_index_history.csv")


if __name__ == "__main__":
    main()

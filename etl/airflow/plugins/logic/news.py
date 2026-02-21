import feedparser
import pandas as pd
from datetime import datetime

RSS_SOURCES = {
    "CafeF_ThiTruong": "https://cafef.vn/rss/thi-truong-chung-khoan.rss",
    "VnExpress_KinhDoanh": "https://vnexpress.net/rss/kinh-doanh.rss",
    "Vietstock_ViMo": "https://vietstock.vn/761/kinh-te/vi-mo.rss",
    "Vietstock_KinhTeDauTu": "https://vietstock.vn/768/kinh-te/kinh-te-dau-tu.rss",
    "Vietstock_CKTheGioi": "https://vietstock.vn/773/the-gioi/chung-khoan-the-gioi.rss",
    "Vietstock_GiaoDichNoiBo": "https://vietstock.vn/739/chung-khoan/giao-dich-noi-bo.rss",
    "Vietstock_ChinhSach": "https://vietstock.vn/143/chung-khoan/chinh-sach.rss",
    "Vietstock_HDKD": "https://vietstock.vn/737/doanh-nghiep/hoat-dong-kinh-doanh.rss",
    "Vietstock_Vang": "https://vietstock.vn/759/hang-hoa/vang-va-kim-loai-quy.rss",
    "Vietstock_NganHang": "https://vietstock.vn/757/tai-chinh/ngan-hang.rss",
    "Vietstock_ThueNganSach": "https://vietstock.vn/758/tai-chinh/thue-va-ngan-sach.rss"
}

def clean_summary(text):
    if not text:
        return ""
    idx = text.find("/>")
    if idx != -1:
        return text[:idx + 2].strip()
    return text.strip()

def parse_rss_today(source_name, url, today):
    try:
        feed = feedparser.parse(url)
        records = []
        for entry in feed.entries:
            if hasattr(entry, "published_parsed") and entry.published_parsed:
                published_dt = datetime(*entry.published_parsed[:6])
                if published_dt.date() == today:
                    records.append({
                        "source": source_name,
                        "title": entry.get("title"),
                        "link": entry.get("link"),
                        "published": published_dt,
                        "summary": clean_summary(entry.get("summary", ""))
                    })
        return records
    except Exception as e:
        print(f"⚠️ Lỗi khi lấy dữ liệu từ {source_name}: {e}")
        return []

def get_financial_news_today() -> pd.DataFrame:
    try:
        print("📥 Đang lấy tin tức tài chính trong ngày từ các nguồn RSS...")
        
        today = datetime.now().date()
        all_news = []

        for name, url in RSS_SOURCES.items():
            print(f"🔎 Đang lấy: {name}")
            all_news.extend(parse_rss_today(name, url, today))

        df = pd.DataFrame(all_news)

        if df.empty:
            print("⚠️ Không có bài báo nào xuất bản trong hôm nay (hoặc lỗi kết nối).")
            return pd.DataFrame()

        # Deduplicate by link and sort by published time
        df = (
            df
            .drop_duplicates(subset=["link"])
            .sort_values("published", ascending=False)
            .reset_index(drop=True)
        )

        print(f"✅ Lấy thành công {len(df)} bài báo tài chính hôm nay.")
        print(f"   Các cột: {list(df.columns)}")
        return df

    except Exception as e:
        print(f"❌ Lỗi tổng thể khi lấy dữ liệu tin tức RSS: {e}")
        return pd.DataFrame()

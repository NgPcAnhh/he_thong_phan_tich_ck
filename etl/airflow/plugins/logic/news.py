import feedparser
import pandas as pd
from datetime import datetime

RSS_SOURCES = {
    "CafeF_ThiTruong": "https://cafef.vn/rss/thi-truong-chung-khoan.rss",
    "CafeBiz_xa-hoi": "https://cafebiz.vn/rss/xa-hoi.rss",
    "CafeBiz_ViMo": "https://cafebiz.vn/rss/vi-mo.rss",
    "CafeBiz_CauChuyenKinhDoanh": "https://cafebiz.vn/rss/cau-chuyen-kinh-doanh.rss",
    "CafeBiz_CongNghe": "https://cafebiz.vn/rss/cong-nghe.rss",
    "CafeBiz_SucBatTuNhan": "https://cafebiz.vn/rss/suc-bat-tu-nhan.rss",
    "CafeBiz_BizMoney": "https://cafebiz.vn/rss/bizmoney.rss",
    "VnEconomy_ChungKhoan": "https://vneconomy.vn/chung-khoan.rss",
    "VnEconomy_DauTuHaTang": "https://vneconomy.vn/dau-tu-ha-tang.rss",
    "VnEconomy_ThiTruong": "https://vneconomy.vn/thi-truong.rss",
    "VnEconomy_NhipCauDoanhNghiep": "https://vneconomy.vn/nhip-cau-doanh-nghiep.rss",
    "VnEconomy_TieuDung": "https://vneconomy.vn/tieu-dung.rss",
    "VnEconomy_KinhTeXanh": "https://vneconomy.vn/kinh-te-xanh.rss",
    "VnEconomy_TaiChinh": "https://vneconomy.vn/tai-chinh.rss",
    "VnEconomy_KinhTeSo": "https://vneconomy.vn/kinh-te-so.rss",
    "VnEconomy_DiaOc": "https://vneconomy.vn/dia-oc.rss",
    "VnEconomy_KinhTeTheGioi": "https://vneconomy.vn/kinh-te-the-gioi.rss",
    "VnEconomy_DauTu": "https://vneconomy.vn/dau-tu.rss",
    "VnEconomy_CongNgheStartup": "https://vneconomy.vn/cong-nghe-startup.rss",
    "VnExpress_KinhDoanh": "https://vnexpress.net/rss/kinh-doanh.rss",
    "VnExpress_TaiChinh": "https://vnexpress.net/rss/tai-chinh.rss",
    "VnExpress_KHCN": "https://vnexpress.net/rss/khoa-hoc-cong-nghe.rss",
    "VnExpress_BatDongSan": "https://vnexpress.net/rss/bat-dong-san.rss",
    "VnExpress_ThoiSu": "https://vnexpress.net/rss/thoi-su.rss",
    "VnExpress_GiaiTri": "https://vnexpress.net/rss/giai-tri.rss",
    "VnExpress_TheGioi": "https://vnexpress.net/rss/the-gioi.rss",
    "Vietstock_ViMo": "https://vietstock.vn/761/kinh-te/vi-mo.rss",
    "Vietstock_KinhTeDauTu": "https://vietstock.vn/768/kinh-te/kinh-te-dau-tu.rss",
    "Vietstock_CKTheGioi": "https://vietstock.vn/773/the-gioi/chung-khoan-the-gioi.rss",
    "Vietstock_GiaoDichNoiBo": "https://vietstock.vn/739/chung-khoan/giao-dich-noi-bo.rss",
    "Vietstock_ChinhSach": "https://vietstock.vn/143/chung-khoan/chinh-sach.rss",
    "Vietstock_HDKD": "https://vietstock.vn/737/doanh-nghiep/hoat-dong-kinh-doanh.rss",
    "Vietstock_Vang": "https://vietstock.vn/759/hang-hoa/vang-va-kim-loai-quy.rss",
    "Vietstock_NganHang": "https://vietstock.vn/757/tai-chinh/ngan-hang.rss",
    "Vietstock_ThueNganSach": "https://vietstock.vn/758/tai-chinh/thue-va-ngan-sach.rss",
    "Vietstock_ETFVaCacQuy": "https://vietstock.vn/3358/chung-khoan/etf-va-cac-quy.rss",
    "Vietstock_ChungKhoanPhaiSinh": "https://vietstock.vn/4186/chung-khoan/chung-khoan-phai-sinh.rss",
    "Vietstock_YKienChuyenGia": "https://vietstock.vn/145/chung-khoan/y-kien-chuyen-gia.rss",
    "Vietstock_CoTuc": "https://vietstock.vn/738/doanh-nghiep/co-tuc.rss",
    "Vietstock_ThiTruongNhaDat": "https://vietstock.vn/4220/bat-dong-san/thi-truong-nha-dat.rss",
    "Vietstock_NhienLieu": "https://vietstock.vn/34/hang-hoa/nhien-lieu.rss",
    "Vietstock_TaiSanSo": "https://vietstock.vn/16312/tai-chinh/tai-san-so.rss",
    "Vietstock_DongDuong_ThiTruongChungKhoan": "https://vietstock.vn/1328/dong-duong/thi-truong-chung-khoan.rss",
    "Vietstock_TaiChinhQuocTe": "https://vietstock.vn/772/the-gioi/tai-chinh-quoc-te.rss",
    "Vietstock_NhanDinhThiTruong": "https://vietstock.vn/1636/nhan-dinh-phan-tich/nhan-dinh-thi-truong.rss",
    "Vietstock_PhanTichCoBan": "https://vietstock.vn/582/nhan-dinh-phan-tich/phan-tich-co-ban.rss",
    "Vietstock_PhanTichKyThuat": "https://vietstock.vn/585/nhan-dinh-phan-tich/phan-tich-ky-thuat.rss",
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

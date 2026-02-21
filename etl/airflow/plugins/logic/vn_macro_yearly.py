import pandas as pd
import wbgapi as wb


INDICATORS = {
    # --- 1. KINH TẾ VĨ MÔ ---
    'NY.GDP.MKTP.KD.ZG': 'Tăng trưởng GDP (% năm)',
    'FP.CPI.TOTL.ZG': 'Lạm phát (CPI) (% năm)',
    'NV.IND.TOTL.KD.ZG': 'Tăng trưởng Công nghiệp & Xây dựng (% năm)',
    'NV.IND.MANF.KD.ZG': 'Tăng trưởng ngành Chế biến chế tạo (% năm)',
    'NE.CON.PRVT.KD.ZG': 'Tăng trưởng Tiêu dùng hộ gia đình (% năm)',

    # --- 2. LÃI SUẤT & TIỀN TỆ ---
    'PA.NUS.FCRF': 'Tỷ giá USD/VND (Trung bình năm)',
    'FR.INR.DPST': 'Lãi suất tiền gửi (%/năm)',
    'FR.INR.LEND': 'Lãi suất cho vay (%/năm)',

    # --- 3. THƯƠNG MẠI & ĐẦU TƯ ---
    'NE.EXP.GNFS.KD.ZG': 'Tăng trưởng Xuất khẩu (% năm)',
    'NE.IMP.GNFS.KD.ZG': 'Tăng trưởng Nhập khẩu (% năm)',
    'BN.GSR.GNFS.CD': 'Cán cân thương mại (USD)',
    'BX.KLT.DINV.CD.WD': 'FDI thực hiện (Dòng vốn ròng - USD)',

    # --- 4. TÀI CHÍNH ---
    'FI.RES.TOTL.CD': 'Dự trữ ngoại hối (USD)',
    'FM.LBL.BMNY.ZG': 'Tăng trưởng Cung tiền M2 (% năm)',
    'FB.AST.NPER.ZS': 'Nợ xấu ngân hàng (% tổng dư nợ)',
}


def get_vn_macro_data() -> pd.DataFrame:
    try:
        print("📥 Đang lấy dữ liệu vĩ mô Việt Nam từ World Bank API...")

        # 1. Fetch data from World Bank for Vietnam (VNM) - 2014 to 2024
        df = wb.data.DataFrame(
            list(INDICATORS.keys()),
            economy=['VNM'],
            time=range(2014, 2025),
            labels=True,
        )

        if df is None or df.empty:
            print("⚠️ Không có dữ liệu trả về từ World Bank API")
            return pd.DataFrame()

        # 2. Drop economy column (we only query VNM)
        df = df.drop(columns=['economy'], errors='ignore')

        # 3. Map indicator codes to Vietnamese names
        df['chi_so'] = df.index.map(INDICATORS)
        df = df.reset_index(drop=True)

        # 4. Reorder: chi_so first, then year columns
        cols = ['chi_so'] + [c for c in df.columns if c != 'chi_so']
        df = df[cols]

        print(f"✅ Lấy thành công {len(df)} chỉ số vĩ mô Việt Nam")
        print(f"   Các cột: {list(df.columns)}")
        return df

    except Exception as e:
        print(f"❌ Lỗi khi lấy dữ liệu vĩ mô: {e}")
        return pd.DataFrame()

import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
import concurrent.futures
import multiprocessing
import time

# --- CẤU HÌNH ---
DB_CONN_INFO = {"dbname": "postgres", "user": "admin", "password": "123456", "host": "localhost", "port": "5432"}
SCHEMA_TABLE = "hethong_phantich_chungkhoan.bctc"
FOLDER_PATH = "2026-01-01"

# --- BỘ MAPPING Tên Tiếng Anh -> {Tiếng Việt, ind_code} ---
FULL_IND_MAP = {
    "Revenue (Bn. VND)": {"vi": "Doanh thu", "code": "REV"},
    "Attribute to parent company (Bn. VND)": {"vi": "LNST của cổ đông công ty mẹ", "code": "NET_PROFIT_PARENT"},
    "Financial Income": {"vi": "Doanh thu tài chính", "code": "FIN_INC"},
    "Sales": {"vi": "Doanh thu bán hàng", "code": "SALES"},
    "Sales deductions": {"vi": "Các khoản giảm trừ doanh thu", "code": "SALES_DED"},
    "Net Sales": {"vi": "Doanh thu thuần", "code": "NET_SALES"},
    "Cost of Sales": {"vi": "Giá vốn hàng bán", "code": "COGS"},
    "Gross Profit": {"vi": "Lợi nhuận gộp", "code": "GROSS_PROFIT"},
    "Financial Expenses": {"vi": "Chi phí tài chính", "code": "FIN_EXP"},
    "Selling Expenses": {"vi": "Chi phí bán hàng", "code": "SELL_EXP"},
    "General & Admin Expenses": {"vi": "Chi phí quản lý doanh nghiệp", "code": "ADMIN_EXP"},
    "Operating Profit/Loss": {"vi": "Lợi nhuận từ HĐKD", "code": "OPER_PROFIT"},
    "Other income": {"vi": "Thu nhập khác", "code": "OTH_INC"},
    "Other Income/Expenses": {"vi": "Thu nhập/Chi phí khác", "code": "OTH_INC_EXP"},
    "Net other income/expenses": {"vi": "Lợi nhuận khác", "code": "NET_OTH_PROFIT"},
    "Profit before tax": {"vi": "Lợi nhuận trước thuế", "code": "PBT"},
    "Net Profit For the Year": {"vi": "Lợi nhuận sau thuế thu nhập DN", "code": "PAT"},
    "Attributable to parent company": {"vi": "LNST phân bổ cho CĐ công ty mẹ", "code": "PAT_PARENT"},
    "CURRENT ASSETS (Bn. VND)": {"vi": "Tài sản ngắn hạn", "code": "CUR_ASSET"},
    "Cash and cash equivalents (Bn. VND)": {"vi": "Tiền và tương đương tiền", "code": "CASH"},
    "Accounts receivable (Bn. VND)": {"vi": "Các khoản phải thu", "code": "RECEIVABLES"},
    "Net Inventories": {"vi": "Hàng tồn kho ròng", "code": "INV_NET"},
    "Other current assets": {"vi": "Tài sản ngắn hạn khác", "code": "OTH_CUR_ASSET"},
    "LONG-TERM ASSETS (Bn. VND)": {"vi": "Tài sản dài hạn", "code": "LT_ASSET"},
    "Fixed assets (Bn. VND)": {"vi": "Tài sản cố định", "code": "FIXED_ASSET"},
    "Long-term investments (Bn. VND)": {"vi": "Đầu tư tài chính dài hạn", "code": "LT_INVEST"},
    "Other non-current assets": {"vi": "Tài sản dài hạn khác", "code": "OTH_LT_ASSET"},
    "TOTAL ASSETS (Bn. VND)": {"vi": "Tổng cộng tài sản", "code": "TOTAL_ASSET"},
    "LIABILITIES (Bn. VND)": {"vi": "Nợ phải trả", "code": "LIABILITIES"},
    "Current liabilities (Bn. VND)": {"vi": "Nợ ngắn hạn", "code": "CUR_LIAB"},
    "Long-term liabilities (Bn. VND)": {"vi": "Nợ dài hạn", "code": "LT_LIAB"},
    "OWNER'S EQUITY(Bn.VND)": {"vi": "Vốn chủ sở hữu", "code": "EQUITY"},
    "Capital and reserves (Bn. VND)": {"vi": "Vốn và các quỹ", "code": "CAPITAL_RESERVE"},
    "Undistributed earnings (Bn. VND)": {"vi": "LNST chưa phân phối", "code": "RETAIN_EARN"},
    "TOTAL RESOURCES (Bn. VND)": {"vi": "Tổng cộng nguồn vốn", "code": "TOTAL_RESOURCES"},
    "Prepayments to suppliers (Bn. VND)": {"vi": "Trả trước cho người bán", "code": "PREPAY_SUPP"},
    "Inventories, Net (Bn. VND)": {"vi": "Hàng tồn kho", "code": "INV"},
    "Other current assets (Bn. VND)": {"vi": "Tài sản ngắn hạn khác (Bn)", "code": "OTH_CUR_ASSET_BN"},
    "Investment and development funds (Bn. VND)": {"vi": "Quỹ đầu tư phát triển", "code": "DEV_FUND"},
    "Common shares (Bn. VND)": {"vi": "Cổ phiếu phổ thông", "code": "COMMON_SHARE"},
    "Paid-in capital (Bn. VND)": {"vi": "Vốn góp của chủ sở hữu", "code": "PAID_IN_CAP"},
    "Advances from customers (Bn. VND)": {"vi": "Người mua trả tiền trước", "code": "ADV_CUST"},
    "Short-term borrowings (Bn. VND)": {"vi": "Vay và nợ ngắn hạn", "code": "ST_DEBT"},
    "Long-term prepayments (Bn. VND)": {"vi": "Chi phí trả trước dài hạn", "code": "LT_PREPAY"},
    "Revenue YoY (%)": {"vi": "Tăng trưởng doanh thu YoY", "code": "REV_YOY"},
    "Attribute to parent company YoY (%)": {"vi": "Tăng trưởng LNST mẹ YoY", "code": "NET_PROFIT_PARENT_YOY"},
    "Interest Expenses": {"vi": "Chi phí lãi vay", "code": "INT_EXP"},
    "Gain/(loss) from joint ventures": {"vi": "Lãi/lỗ từ công ty liên doanh liên kết", "code": "JOINT_VENTURE_GAIN"},
    "Business income tax - current": {"vi": "Thuế TNDN hiện hành", "code": "TAX_CURRENT"},
    "Business income tax - deferred": {"vi": "Thuế TNDN hoãn lại", "code": "TAX_DEFERRED"},
    "Minority Interest": {"vi": "Lợi ích cổ đông thiểu số", "code": "MINORITY_INT"},
    "Short-term investments (Bn. VND)": {"vi": "Đầu tư tài chính ngắn hạn", "code": "ST_INVEST"},
    "Long-term loans receivables (Bn. VND)": {"vi": "Phải thu về cho vay dài hạn", "code": "LT_LOAN_RECEIV"},
    "MINORITY INTERESTS": {"vi": "Lợi ích cổ đông thiểu số (Nguồn vốn)", "code": "MINORITY_INTERESTS"},
    "Short-term loans receivables (Bn. VND)": {"vi": "Phải thu về cho vay ngắn hạn", "code": "ST_LOAN_RECEIV"},
    "Long-term borrowings (Bn. VND)": {"vi": "Vay và nợ dài hạn", "code": "LT_DEBT"},
    "Good will (Bn. VND)": {"vi": "Lợi thế thương mại", "code": "GOODWILL"},
    "Other long-term assets (Bn. VND)": {"vi": "Tài sản dài hạn khác (Bn)", "code": "OTH_LT_ASSET_BN"},
    "Other long-term receivables (Bn. VND)": {"vi": "Phải thu dài hạn khác", "code": "OTH_LT_RECEIV"},
    "Long-term trade receivables (Bn. VND)": {"vi": "Phải thu dài hạn của khách hàng", "code": "LT_TRADE_RECEIV"},
    "Net Profit/Loss before tax": {"vi": "Lợi nhuận/Lỗ thuần trước thuế", "code": "NET_PBT"},
    "Depreciation and Amortisation": {"vi": "Khấu hao và hao mòn", "code": "DEPRECIATION"},
    "Provision for credit losses": {"vi": "Dự phòng tổn thất tín dụng", "code": "PROVISION_CREDIT"},
    "Unrealized foreign exchange gain/loss": {"vi": "Lãi/lỗ chênh lệch tỷ giá chưa thực hiện", "code": "UNREALIZED_FX"},
    "Profit/Loss from investing activities": {"vi": "Lãi/lỗ từ hoạt động đầu tư", "code": "INVEST_GAIN"},
    "Interest Expense": {"vi": "Chi phí lãi vay (Lưu chuyển tiền tệ)", "code": "INT_EXP_CF"},
    "Operating profit before changes in working capital": {"vi": "Lợi nhuận từ HĐKD trước thay đổi vốn lưu động", "code": "OPER_PROFIT_BEFORE_WC"},
    "Increase/Decrease in receivables": {"vi": "Tăng/Giảm các khoản phải thu", "code": "CF_RECEIVABLES"},
    "Increase/Decrease in inventories": {"vi": "Tăng/Giảm hàng tồn kho", "code": "CF_INV"},
    "Increase/Decrease in payables": {"vi": "Tăng/Giảm các khoản phải trả", "code": "CF_PAYABLES"},
    "Increase/Decrease in prepaid expenses": {"vi": "Tăng/Giảm chi phí trả trước", "code": "CF_PREPAY"},
    "Interest paid": {"vi": "Tiền lãi vay đã trả", "code": "CF_INT_PAID"},
    "Business Income Tax paid": {"vi": "Thuế TNDN đã nộp", "code": "CF_TAX_PAID"},
    "Net cash inflows/outflows from operating activities": {"vi": "Lưu chuyển tiền thuần từ HĐKD", "code": "CFO"},
    "Purchase of fixed assets": {"vi": "Tiền chi mua sắm TSCĐ", "code": "CF_BUY_FIXED_ASSET"},
    "Proceeds from disposal of fixed assets": {"vi": "Tiền thu thanh lý TSCĐ", "code": "CF_SELL_FIXED_ASSET"},
    "Loans granted, purchases of debt instruments (Bn. VND)": {"vi": "Tiền cho vay, mua công cụ nợ", "code": "CF_LOAN_GRANTED"},
    "Collection of loans, proceeds from sales of debts instruments (Bn. VND)": {"vi": "Tiền thu hồi cho vay, bán công cụ nợ", "code": "CF_LOAN_COLLECT"},
    "Investment in other entities": {"vi": "Tiền chi đầu tư góp vốn vào đơn vị khác", "code": "CF_INVEST_OTH"},
    "Proceeds from divestment in other entities": {"vi": "Tiền thu hồi đầu tư góp vốn vào đơn vị khác", "code": "CF_DIVEST_OTH"},
    "Gain on Dividend": {"vi": "Tiền thu lãi cho vay, cổ tức và lợi nhuận được chia", "code": "CF_DIV_RECV"},
    "Net Cash Flows from Investing Activities": {"vi": "Lưu chuyển tiền thuần từ HĐ đầu tư", "code": "CFI"},
    "Increase in charter captial": {"vi": "Tiền thu từ phát hành cổ phiếu, nhận vốn góp", "code": "CF_ISSUE_SHARE"},
    "Payments for share repurchases": {"vi": "Tiền chi trả vốn góp, mua lại cổ phiếu", "code": "CF_REBUY_SHARE"},
    "Proceeds from borrowings": {"vi": "Tiền thu từ đi vay", "code": "CF_BORROW"},
    "Repayment of borrowings": {"vi": "Tiền trả nợ gốc vay", "code": "CF_REPAY_DEBT"},
    "Finance lease principal payments": {"vi": "Tiền trả nợ gốc thuê tài chính", "code": "CF_LEASE_PAY"},
    "Dividends paid": {"vi": "Cổ tức, lợi nhuận đã trả cho chủ sở hữu", "code": "CF_DIV_PAID"},
    "Cash flows from financial activities": {"vi": "Lưu chuyển tiền thuần từ HĐ tài chính", "code": "CFF"},
    "Net increase/decrease in cash and cash equivalents": {"vi": "Lưu chuyển tiền thuần trong kỳ", "code": "NET_CF"},
    "Cash and cash equivalents": {"vi": "Tiền và tương đương tiền đầu kỳ", "code": "CASH_BEGIN"},
    "Foreign exchange differences Adjustment": {"vi": "Ảnh hưởng của thay đổi tỷ giá", "code": "FX_ADJ"},
    "Cash and Cash Equivalents at the end of period": {"vi": "Tiền và tương đương tiền cuối kỳ", "code": "CASH_END"},
    "Interest income and dividends": {"vi": "Lãi tiền gửi và cổ tức", "code": "INT_DIV_INC"}
}

def process_bctc_csv(file_path):
    # Mỗi Process sẽ chạy hàm này độc lập
    print(f"🚀 [PID {os.getpid()}] Đang xử lý: {os.path.basename(file_path)}")
    try:
        df = pd.read_csv(file_path, encoding='utf-8-sig')
        
        # --- FIX LỖI TÊN CỘT: Chuẩn hóa về chữ thường và bỏ khoảng trắng ---
        # Ví dụ: "Quarter " -> "quarter", "YEAR" -> "year", "Report Name" -> "report name"
        df.columns = df.columns.str.strip().str.lower()

        # 1. Bỏ cột company nếu tồn tại
        if 'company' in df.columns:
            df = df.drop(columns=['company'])
            
        rows = []
        for _, row in df.iterrows():
            eng_name = str(row.get('ind_name', '')).strip()
            
            # 2. Thực hiện mapping Tiếng Việt và lấy code ngắn
            mapping = FULL_IND_MAP.get(eng_name)
            if mapping:
                vi_name = mapping['vi']
                ind_code = mapping['code']
            else:
                vi_name = eng_name
                ind_code = eng_name.upper().replace(" ", "_")[:50]

            # 3. Xử lý giá trị số (Chặn e-notation)
            val = row.get('value', 0)
            try:
                numeric_val = float(val)
                formatted_val = '{:.4f}'.format(numeric_val).rstrip('0').rstrip('.')
            except:
                formatted_val = '0'
            
            # --- FIX LỖI DATA TYPE cho Quarter và Year ---
            try:
                raw_q = row.get('report_quarter')
                quarter = int(float(raw_q)) if pd.notna(raw_q) and raw_q != '' else None
            except:
                quarter = None

            try:
                raw_y = row.get('report_year')
                year = int(float(raw_y)) if pd.notna(raw_y) and raw_y != '' else None
            except:
                year = None
            
            # 4. Lấy thêm report_name và report_code
            # Ưu tiên lấy theo tên cột chuẩn, fallback về None nếu không có
            report_name = row.get('report_name') or row.get('report name')
            report_code = row.get('statement_type') or row.get('statement_type')

            rows.append((
                row.get('ticker'), 
                quarter,
                year,
                report_name,
                report_code,
                vi_name,
                ind_code,
                formatted_val if formatted_val != '' else '0',
                datetime.now()
            ))

        if rows:
            # TẠO KẾT NỐI RIÊNG CHO MỖI FILE (Tiến trình)
            conn = psycopg2.connect(**DB_CONN_INFO)
            conn.set_client_encoding('UTF8')
            cursor = conn.cursor()
            
            # Cập nhật INSERT query thêm report_name và report_code
            query = f"INSERT INTO {SCHEMA_TABLE} (ticker, quarter, year, report_name, report_code, ind_name, ind_code, value, import_time) VALUES %s;"
            
            # page_size=1000 giúp tối ưu hóa gói tin gửi đi DB
            execute_values(cursor, query, rows, page_size=1000)
            
            conn.commit()
            cursor.close()
            conn.close()
            return f"✅ [PID {os.getpid()}] Thành công: {os.path.basename(file_path)} ({len(rows)} dòng)"
        else:
            return f"⚠️ [PID {os.getpid()}] File rỗng hoặc không có dữ liệu hợp lệ: {os.path.basename(file_path)}"
            
    except Exception as e:
        return f"❌ [PID {os.getpid()}] Lỗi file {os.path.basename(file_path)}: {e}"

def main():
    if not os.path.exists(FOLDER_PATH):
        print(f"❌ Thư mục {FOLDER_PATH} không thấy.")
        return

    csv_files = [os.path.join(FOLDER_PATH, f) for f in os.listdir(FOLDER_PATH) if f.endswith('.csv')]
    total_files = len(csv_files)
    
    if total_files == 0:
        print("Không tìm thấy file CSV nào.")
        return

    print(f"🔥 Bắt đầu xử lý {total_files} files với ĐA TIẾN TRÌNH...")
    start_time = time.time()

    # Tự động lấy số lượng CPU cores của máy tính
    max_workers = os.cpu_count() 
    
    results = []
    
    # Sử dụng ProcessPoolExecutor để chạy song song
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(process_bctc_csv, f) for f in csv_files]
        
        for i, future in enumerate(concurrent.futures.as_completed(futures)):
            result = future.result()
            print(result) 

    end_time = time.time()
    print(f"\n🎉 HOÀN THÀNH TOÀN BỘ SAU {end_time - start_time:.2f} GIÂY")

if __name__ == "__main__":
    multiprocessing.freeze_support() 
    main()
from contextlib import closing
import pandas as pd
from psycopg2.extras import execute_values
from lake_to_dwh.utils import (
    get_latest_partition,
    read_all_csvs_from_folder,
    get_postgres_connection,
    ensure_schema,
    clean_dataframe,
    standardize_ticker
)


# Indicator mapping from bctc_mapping.py (158 mappings)
INDICATOR_MAPPING = {
    # BẢNG CÂN ĐỐI KẾ TOÁN
    'Tiền và tương đương tiền': 'Tiền và tương đương tiền',
    'Tiền và tương đương tiền đầu kỳ': 'Tiền và tương đương tiền đầu kỳ',
    'Tiền và tương đương tiền cuối kỳ': 'Tiền và tương đương tiền cuối kỳ',
    'Đầu tư tài chính ngắn hạn': 'Đầu tư tài chính ngắn hạn',
    'Trading Securities': 'Chứng khoán kinh doanh',
    'Trading Securities, net': 'Chứng khoán kinh doanh (ròng)',
    'Provision for diminution in value of Trading Securities': 'Dự phòng giảm giá chứng khoán kinh doanh',
    'Các khoản phải thu': 'Các khoản phải thu',
    'Phải thu về cho vay ngắn hạn': 'Phải thu về cho vay ngắn hạn',
    'Người mua trả tiền trước': 'Người mua trả tiền trước',
    'Trả trước cho người bán': 'Trả trước cho người bán',
    'Hàng tồn kho': 'Hàng tồn kho',
    'Hàng tồn kho ròng': 'Hàng tồn kho ròng',
    'Tài sản ngắn hạn khác': 'Tài sản ngắn hạn khác',
    'Tài sản ngắn hạn khác (Bn)': 'Tài sản ngắn hạn khác',
    'Tài sản ngắn hạn': 'Tổng tài sản ngắn hạn',
    'Phải thu dài hạn của khách hàng': 'Phải thu dài hạn của khách hàng',
    'Phải thu dài hạn khác': 'Phải thu dài hạn khác',
    'Phải thu về cho vay dài hạn': 'Phải thu về cho vay dài hạn',
    'Tài sản cố định': 'Tài sản cố định',
    'Tangible fixed assets': 'Tài sản cố định hữu hình',
    'Intagible fixed assets': 'Tài sản cố định vô hình',
    'Leased assets': 'Tài sản thuê tài chính',
    'Chi phí trả trước dài hạn': 'Chi phí trả trước dài hạn',
    'Đầu tư tài chính dài hạn': 'Đầu tư tài chính dài hạn',
    'Investment Securities': 'Chứng khoán đầu tư',
    'Held-to-Maturity Securities': 'Chứng khoán nắm giữ đến ngày đáo hạn',
    'Available-for Sales Securities': 'Chứng khoán sẵn sàng để bán',
    'Balances with the SBV': 'Tiền gửi tại Ngân hàng Nhà nước',
    'Less: Provision for diminution in value of investment securities': 'Dự phòng giảm giá chứng khoán đầu tư',
    'Less: Provision for diminuation in value of long term investments': 'Dự phòng giảm giá đầu tư dài hạn',
    'Investment in properties': 'Đầu tư bất động sản',
    'Investment in joint ventures': 'Đầu tư vào công ty liên doanh',
    'Investments in associate companies': 'Đầu tư vào công ty liên kết',
    'Goodwill': 'Lợi thế thương mại',
    'Lợi thế thương mại': 'Lợi thế thương mại',
    'Tài sản dài hạn khác': 'Tài sản dài hạn khác',
    'Tài sản dài hạn khác (Bn)': 'Tài sản dài hạn khác',
    'Other Assets': 'Tài sản khác',
    'Tài sản dài hạn': 'Tổng tài sản dài hạn',
    'Tổng cộng tài sản': 'Tổng cộng tài sản',
    # NỢ PHẢI TRẢ
    'Nợ ngắn hạn': 'Nợ ngắn hạn',
    'Vay và nợ ngắn hạn': 'Vay và nợ ngắn hạn',
    'Deposits from customers': 'Tiền gửi khách hàng',
    'Deposits and borrowings from other credit institutions': 'Tiền gửi và vay từ các tổ chức tín dụng khác',
    'Placements with and loans to other credit institutions': 'Tiền gửi tại và cho vay các tổ chức tín dụng khác',
    'Due to Gov and borrowings from SBV': 'Nợ Chính phủ và vay NHNN',
    'Derivatives and other financial liabilities': 'Các công cụ phái sinh và nợ tài chính khác',
    '_Derivatives and other financial liabilities': 'Các công cụ phái sinh và nợ tài chính khác',
    'Nợ dài hạn': 'Nợ dài hạn',
    'Vay và nợ dài hạn': 'Vay và nợ dài hạn',
    'Convertible bonds (Bn. VND)': 'Trái phiếu chuyển đổi',
    'Convertible bonds/CDs and other valuable papers issued': 'Trái phiếu chuyển đổi và giấy tờ có giá đã phát hành',
    'Other liabilities': 'Nợ khác',
    'Nợ phải trả': 'Tổng nợ phải trả',
    'Dự phòng tổn thất tín dụng': 'Dự phòng tổn thất tín dụng',
    'Loans and advances to customers': 'Cho vay khách hàng',
    'Loans and advances to customers, net': 'Cho vay khách hàng (ròng)',
    'Less: Provision for losses on loans and advances to customers': 'Dự phòng tổn thất cho vay khách hàng',
    # VỐN CHỦ SỞ HỮU
    'Vốn chủ sở hữu': 'Vốn chủ sở hữu',
    'Capital': 'Vốn góp',
    'Vốn góp của chủ sở hữu': 'Vốn góp của chủ sở hữu',
    'Cổ phiếu phổ thông': 'Cổ phiếu phổ thông',
    'Vốn và các quỹ': 'Vốn và các quỹ',
    'Reserves': 'Các quỹ dự trữ',
    'Other Reserves': 'Quỹ khác',
    '_Other Reserves': 'Quỹ khác',
    'Quỹ đầu tư phát triển': 'Quỹ đầu tư phát triển',
    'Foreign Currency Difference reserve': 'Quỹ dự trữ chênh lệch tỷ giá',
    'Difference upon Assets Revaluation': 'Chênh lệch đánh giá lại tài sản',
    'LNST chưa phân phối': 'Lợi nhuận sau thuế chưa phân phối',
    'LNST của cổ đông công ty mẹ': 'Lợi nhuận sau thuế thuộc cổ đông công ty mẹ',
    'Lợi ích cổ đông thiểu số': 'Lợi ích cổ đông thiểu số',
    'Lợi ích cổ đông thiểu số (Nguồn vốn)': 'Lợi ích cổ đông thiểu số',
    'Budget sources and other funds': 'Nguồn kinh phí và quỹ khác',
    'Funds received from Gov, international and other institutions': 'Nguồn kinh phí đã hình thành tài sản cố định',
    'Tổng cộng nguồn vốn': 'Tổng cộng nguồn vốn',
    # BÁO CÁO KẾT QUẢ KINH DOANH
    'Doanh thu': 'Doanh thu',
    'Doanh thu bán hàng': 'Doanh thu bán hàng và cung cấp dịch vụ',
    'Total operating revenue': 'Tổng doanh thu hoạt động',
    'Các khoản giảm trừ doanh thu': 'Các khoản giảm trừ doanh thu',
    'Doanh thu thuần': 'Doanh thu thuần',
    'Giá vốn hàng bán': 'Giá vốn hàng bán',
    'Lợi nhuận gộp': 'Lợi nhuận gộp về bán hàng và cung cấp dịch vụ',
    'Chi phí bán hàng': 'Chi phí bán hàng',
    'Chi phí quản lý doanh nghiệp': 'Chi phí quản lý doanh nghiệp',
    'Khấu hao và hao mòn': 'Khấu hao và hao mòn',
    'Doanh thu tài chính': 'Doanh thu hoạt động tài chính',
    'Interest and Similar Income': 'Thu nhập lãi và thu nhập tương tự',
    'Fees and Comission Income': 'Thu nhập phí và hoa hồng',
    'Lãi tiền gửi và cổ tức': 'Lãi tiền gửi và cổ tức nhận được',
    'Dividends received': 'Cổ tức nhận được',
    'Chi phí tài chính': 'Chi phí hoạt động tài chính',
    'Interest and Similar Expenses': 'Chi phí lãi và chi phí tương tự',
    'Fees and Comission Expenses': 'Chi phí phí và hoa hồng',
    'Chi phí lãi vay': 'Chi phí lãi vay',
    'Chi phí lãi vay (Lưu chuyển tiền tệ)': 'Chi phí lãi vay',
    'Net Interest Income': 'Thu nhập lãi thuần',
    'Net Fee and Commission Income': 'Thu nhập phí và hoa hồng thuần',
    'Lãi/lỗ từ hoạt động đầu tư': 'Lãi lỗ từ hoạt động đầu tư',
    'Lãi/lỗ từ công ty liên doanh liên kết': 'Lãi lỗ từ công ty liên doanh, liên kết',
    'Net income from associated companies': 'Thu nhập từ công ty liên kết',
    'Net gain (loss) from disposal of investment securities': 'Lãi lỗ thuần từ thanh lý chứng khoán đầu tư',
    'Net gain (loss) from foreign currency and gold dealings': 'Lãi lỗ thuần từ hoạt động kinh doanh ngoại tệ và vàng',
    'Net gain (loss) from trading of trading securities': 'Lãi lỗ thuần từ mua bán chứng khoán kinh doanh',
    'Profit/Loss from disposal of fixed assets': 'Lãi lỗ từ thanh lý tài sản cố định',
    'Thu nhập khác': 'Thu nhập khác',
    'Other expenses': 'Chi phí khác',
    'Thu nhập/Chi phí khác': 'Thu nhập chi phí khác',
    'Lợi nhuận khác': 'Lợi nhuận khác',
    'Profits from other activities': 'Lợi nhuận từ hoạt động khác',
    'Net Other income/(expenses)': 'Thu nhập khác thuần',
    'Net Other income/expenses': 'Thu nhập khác thuần',
    'Lợi nhuận từ HĐKD': 'Lợi nhuận từ hoạt động kinh doanh',
    'Lợi nhuận từ HĐKD trước thay đổi vốn lưu động': 'Lợi nhuận từ hoạt động kinh doanh trước thay đổi vốn lưu động',
    'Operating Profit before Provision': 'Lợi nhuận hoạt động trước trích lập dự phòng',
    'Lãi/lỗ chênh lệch tỷ giá chưa thực hiện': 'Lãi lỗ chênh lệch tỷ giá chưa thực hiện',
    'Ảnh hưởng của thay đổi tỷ giá': 'Ảnh hưởng của thay đổi tỷ giá hối đoái',
    'Lợi nhuận trước thuế': 'Tổng lợi nhuận kế toán trước thuế',
    'Lợi nhuận/Lỗ thuần trước thuế': 'Lợi nhuận thuần trước thuế',
    'Tax For the Year': 'Chi phí thuế thu nhập doanh nghiệp',
    'Thuế TNDN hiện hành': 'Chi phí thuế TNDN hiện hành',
    'Thuế TNDN hoãn lại': 'Chi phí thuế TNDN hoãn lại',
    'Lợi nhuận sau thuế thu nhập DN': 'Lợi nhuận sau thuế thu nhập doanh nghiệp',
    'LNST phân bổ cho CĐ công ty mẹ': 'Lợi nhuận sau thuế thuộc cổ đông công ty mẹ',
    'EPS_basis': 'Lãi cơ bản trên cổ phiếu',
    'Tăng trưởng doanh thu YoY': 'Tăng trưởng doanh thu so với cùng kỳ',
    'Tăng trưởng LNST mẹ YoY': 'Tăng trưởng lợi nhuận sau thuế công ty mẹ so với cùng kỳ',
    # BÁO CÁO LƯU CHUYỂN TIỀN TỆ (continued)
    'Lưu chuyển tiền thuần trong kỳ': 'Lưu chuyển tiền thuần trong kỳ',
    
    # ENGLISH MAPPINGS - Balance Sheet
    'Accounts receivable (Bn. VND)': 'Các khoản phải thu',
    'Advances from customers (Bn. VND)': 'Người mua trả tiền trước',
    'Cash and cash equivalents (Bn. VND)': 'Tiền và tương đương tiền',
    'Cash and Cash Equivalents at the end of period': 'Tiền và tương đương tiền cuối kỳ',
    'CURRENT ASSETS (Bn. VND)': 'Tổng tài sản ngắn hạn',
    'Current liabilities (Bn. VND)': 'Nợ ngắn hạn',
    'Fixed assets (Bn. VND)': 'Tài sản cố định',
    'Good will (Bn. VND)': 'Lợi thế thương mại',
    'Inventories, Net (Bn. VND)': 'Hàng tồn kho ròng',
    'Investment and development funds (Bn. VND)': 'Quỹ đầu tư phát triển',
    'LIABILITIES (Bn. VND)': 'Tổng nợ phải trả',
    'LONG-TERM ASSETS (Bn. VND)': 'Tổng tài sản dài hạn',
    'Long-term borrowings (Bn. VND)': 'Vay và nợ dài hạn',
    'Long-term investments (Bn. VND)': 'Đầu tư tài chính dài hạn',
    'Long-term liabilities (Bn. VND)': 'Nợ dài hạn',
    'Long-term loans receivables (Bn. VND)': 'Phải thu về cho vay dài hạn',
    'Long-term prepayments (Bn. VND)': 'Chi phí trả trước dài hạn',
    'Long-term trade receivables (Bn. VND)': 'Phải thu dài hạn của khách hàng',
    'Other current assets (Bn. VND)': 'Tài sản ngắn hạn khác',
    'Other long-term assets (Bn. VND)': 'Tài sản dài hạn khác',
    'Other long-term receivables (Bn. VND)': 'Phải thu dài hạn khác',
    'Other current assets': 'Tài sản ngắn hạn khác',
    'Other non-current assets': 'Tài sản dài hạn khác',
    'OWNER\'S EQUITY(Bn.VND)': 'Vốn chủ sở hữu',
    'Paid-in capital (Bn. VND)': 'Vốn góp',
    'Prepayments to suppliers (Bn. VND)': 'Trả trước cho người bán',
    'Short-term borrowings (Bn. VND)': 'Vay và nợ ngắn hạn',
    'Short-term investments (Bn. VND)': 'Đầu tư tài chính ngắn hạn',
    'Short-term loans receivables (Bn. VND)': 'Phải thu về cho vay ngắn hạn',
    'TOTAL ASSETS (Bn. VND)': 'Tổng cộng tài sản',
    'TOTAL RESOURCES (Bn. VND)': 'Tổng cộng nguồn vốn',
    'Undistributed earnings (Bn. VND)': 'Lợi nhuận sau thuế chưa phân phối',
    'Common shares (Bn. VND)': 'Cổ phiếu phổ thông',
    'Capital and reserves (Bn. VND)': 'Vốn và các quỹ',
    
    # ENGLISH MAPPINGS - Income Statement
    'Attributable to parent company': 'Lợi nhuận sau thuế thuộc cổ đông công ty mẹ',
    'Attribute to parent company (Bn. VND)': 'Lợi nhuận sau thuế thuộc cổ đông công ty mẹ',
    'Attribute to parent company YoY (%)': 'Tăng trưởng lợi nhuận sau thuế công ty mẹ so với cùng kỳ',
    'Revenue (Bn. VND)': 'Doanh thu',
    'Revenue YoY (%)': 'Tăng trưởng doanh thu so với cùng kỳ',
    'Sales': 'Doanh thu bán hàng và cung cấp dịch vụ',
    'Net Sales': 'Doanh thu thuần',
    'Sales deductions': 'Các khoản giảm trừ doanh thu',
    'Cost of Sales': 'Giá vốn hàng bán',
    'Gross Profit': 'Lợi nhuận gộp về bán hàng và cung cấp dịch vụ',
    'Selling Expenses': 'Chi phí bán hàng',
    'General & Admin Expenses': 'Chi phí quản lý doanh nghiệp',
    'Depreciation and Amortisation': 'Khấu hao và hao mòn',
    'Financial Income': 'Doanh thu hoạt động tài chính',
    'Financial Expenses': 'Chi phí hoạt động tài chính',
    'Interest income and dividends': 'Lãi tiền gửi và cổ tức nhận được',
    'Gain on Dividend': 'Cổ tức nhận được',
    'Interest Expense': 'Chi phí lãi vay',
    'Interest Expenses': 'Chi phí lãi vay',
    'Profit/Loss from investing activities': 'Lãi lỗ từ hoạt động đầu tư',
    'Gain/(loss) from joint ventures': 'Lãi lỗ từ công ty liên doanh, liên kết',
    'Other income': 'Thu nhập khác',
    'Other Income/Expenses': 'Thu nhập chi phí khác',
    'Operating Profit/Loss': 'Lợi nhuận từ hoạt động kinh doanh',
    'Profit before tax': 'Tổng lợi nhuận kế toán trước thuế',
    'Net Profit/Loss before tax': 'Lợi nhuận thuần trước thuế',
    'Net Profit For the Year': 'Lợi nhuận sau thuế thu nhập doanh nghiệp',
    'Minority Interest': 'Lợi ích cổ đông thiểu số',
    'MINORITY INTERESTS': 'Lợi ích cổ đông thiểu số',
    
    # ENGLISH MAPPINGS - Cash Flow
    'Net cash inflows/outflows from operating activities': 'Lưu chuyển tiền thuần từ hoạt động kinh doanh',
    'Operating profit before changes in working capital': 'Lợi nhuận từ hoạt động kinh doanh trước thay đổi vốn lưu động',
    'Net Cash Flows from Investing Activities': 'Lưu chuyển tiền thuần từ hoạt động đầu tư',
    'Cash flows from financial activities': 'Lưu chuyển tiền thuần từ hoạt động tài chính',
    'Net increase/decrease in cash and cash equivalents': 'Lưu chuyển tiền thuần trong kỳ',
    'Increase/Decrease in receivables': 'Tăng giảm các khoản phải thu',
    'Increase/Decrease in inventories': 'Tăng giảm hàng tồn kho',
    'Increase/Decrease in prepaid expenses': 'Tăng giảm chi phí trả trước',
    'Increase/Decrease in payables': 'Tăng giảm các khoản phải trả',
    'Interest paid': 'Tiền lãi vay đã trả',
    'Business Income Tax paid': 'Thuế thu nhập doanh nghiệp đã nộp',
    'Purchase of fixed assets': 'Tiền chi để mua sắm xây dựng tài sản cố định',
    'Proceeds from disposal of fixed assets': 'Tiền thu từ thanh lý nhượng bán tài sản cố định',
    'Investment in other entities': 'Tiền chi đầu tư góp vốn vào đơn vị khác',
    'Proceeds from divestment in other entities': 'Tiền thu hồi đầu tư góp vốn vào đơn vị khác',
    'Loans granted, purchases of debt instruments (Bn. VND)': 'Tiền chi cho vay mua các công cụ nợ của đơn vị khác',
    'Collection of loans, proceeds from sales of debts instruments (Bn. VND)': 'Tiền thu hồi cho vay bán lại các công cụ nợ của đơn vị khác',
    'Proceeds from borrowings': 'Tiền thu từ đi vay',
    'Repayment of borrowings': 'Tiền trả nợ gốc vay',
    'Finance lease principal payments': 'Tiền trả nợ gốc thuê tài chính',
    'Dividends paid': 'Cổ tức lợi nhuận đã trả cho chủ sở hữu',
    'Increase in charter captial': 'Tiền thu từ phát hành cổ phiếu nhận vốn góp của chủ sở hữu',
    'Payments for share repurchases': 'Tiền chi trả vốn góp cho các chủ sở hữu mua lại cổ phiếu',
    'Payment from reserves': 'Chi từ các quỹ',
    'Foreign exchange differences Adjustment': 'Ảnh hưởng của thay đổi tỷ giá hối đoái',
    'Unrealized foreign exchange gain/loss': 'Lãi lỗ chênh lệch tỷ giá chưa thực hiện',
    'Business income tax - current': 'Chi phí thuế TNDN hiện hành',
    'Business income tax - deferred': 'Chi phí thuế TNDN hoãn lại',
    'Net other income/expenses': 'Thu nhập khác thuần',
    'Net Inventories': 'Hàng tồn kho ròng',
    'Provision for credit losses': 'Dự phòng tổn thất tín dụng',

    # other
    'Other payments on operating activities': 'Các khoản chi khác từ hoạt động kinh doanh',
    '_Increase/Decrease in receivables': 'Tăng/giảm các khoản phải thu',
    'Cash and cash equivalents': 'Tiền mặt và tiền mặt tương đương',
    'Net Cash Flows from Operating Activities before BIT': 'Doanh thu từ hoạt động kinh doanh trước BIT',
    '_Increase/Decrease in payables': 'Tăng/giảm các khoản phải trả',
    'Other receipts from operating activities': 'Thu khác từ hoạt động kinh doanh'
}


def apply_indicator_mapping(df: pd.DataFrame) -> pd.DataFrame:
    """Apply indicator name normalization using the mapping."""
    if 'ind_name' in df.columns:
        df['ind_name'] = df['ind_name'].replace(INDICATOR_MAPPING)
    return df


def sync_bctc_to_db(
    db_url: str,
    schema: str,
    bucket: str,
    minio_conn_id: str = "minio_finance",
    folder_prefix: str = "bctc/",
    table: str = "bctc"
) -> str:

    print("=" * 70)
    print("📊 SYNC BCTC TO DATABASE (WITH INDICATOR MAPPING)")
    print("=" * 70)
    
    # Step 1: Find latest partition
    print("\n[1/5] Finding latest partition...")
    latest_partition = get_latest_partition(bucket, folder_prefix, minio_conn_id)
    
    if not latest_partition:
        return "❌ No partition found"
    
    # Step 2: Read all CSV files
    print("\n[2/5] Reading CSV files...")
    df = read_all_csvs_from_folder(bucket, latest_partition, minio_conn_id)
    
    if df.empty:
        return "⚠️ No data found"
    
    print(f"Loaded {len(df)} rows")
    
    # Step 3: Clean and transform data
    print("\n[3/5] Cleaning and transforming data...")
    
    # Normalize column names
    df.columns = df.columns.str.lower().str.strip()
    
    # Ensure required columns exist
    required_cols = ['ticker', 'quarter', 'year', 'ind_name', 'value']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        return f"❌ Missing columns: {missing_cols}"
    
    # Clean data
    df = clean_dataframe(df, required_columns=required_cols)
    df = standardize_ticker(df, 'ticker')
    
    # Parse year and quarter  
    # Note: quarter is VARCHAR(10) in database, year is INTEGER
    df['year'] = pd.to_numeric(df['year'], errors='coerce').astype('Int64')
    df['quarter'] = df['quarter'].astype(str).str.strip()  # Keep as string
    df['value'] = pd.to_numeric(df['value'], errors='coerce')
    
    # Remove rows with null year or quarter
    df = df.dropna(subset=['year', 'quarter'])
    
    print(f"After cleaning: {len(df)} rows")
    
    # Step 4: Apply indicator mapping
    print("\n[4/5] Applying indicator name normalization...")
    df = apply_indicator_mapping(df)
    mapped_count = df['ind_name'].isin(INDICATOR_MAPPING.values()).sum()
    print(f"✓ Normalized {mapped_count}/{len(df)} indicators using mapping")
    
    # Create ind_code from ind_name
    if 'ind_code' not in df.columns:
        df['ind_code'] = df['ind_name'].str.upper().str.replace(' ', '_').str.replace('/', '_')
    
    # Truncate ind_code to match database VARCHAR(50) constraint
    df['ind_code'] = df['ind_code'].astype(str).str[:50]
    
    # Select final columns
    final_cols = ['ticker', 'quarter', 'year', 'ind_name', 'ind_code', 'value', 'report_name', 'report_code']
    df = df[[col for col in final_cols if col in df.columns]].copy()
    
    if df.empty:
        return "⚠️ No data after transformation"
    
    # Step 5: Insert into database
    print("\n[5/5] Inserting into database...")
    
    with closing(get_postgres_connection(db_url)) as conn:
        conn.autocommit = False
        
        try:
            # Ensure schema exists
            ensure_schema(conn, schema)
            
            # Prepare data for insertion
            rows = []
            for _, row in df.iterrows():
                rows.append((
                    row.get('ticker'),
                    row.get('quarter'),
                    row.get('year'),
                    row.get('ind_name'),
                    row.get('ind_code'),
                    row.get('value'),
                    row.get('report_name'),
                    row.get('report_code'),
                ))
            
            # Note: bctc table doesn't have a primary key in the schema
            # We'll use a temporary table to delete duplicates then insert
            
            with conn.cursor() as cur:
                # Create temporary table for delete keys
                cur.execute(f"""
                    CREATE TEMP TABLE IF NOT EXISTS temp_bctc_delete_keys (
                        ticker VARCHAR(10),
                        quarter VARCHAR(10),
                        year INTEGER,
                        ind_code VARCHAR(50)
                    ) ON COMMIT DROP;
                """)
                
                # Prepare unique keys for deletion
                delete_keys = set()
                for row in rows:
                    ticker = row[0]
                    quarter = str(row[1]) if pd.notna(row[1]) and row[1] else None
                    year = int(row[2]) if pd.notna(row[2]) else None
                    ind_code = row[4]
                    if ticker and quarter and year and ind_code:
                        delete_keys.add((ticker, quarter, year, ind_code))
                
                # Insert keys into temp table
                if delete_keys:
                    delete_keys_list = list(delete_keys)
                    execute_values(cur, """
                        INSERT INTO temp_bctc_delete_keys (ticker, quarter, year, ind_code)
                        VALUES %s
                    """, delete_keys_list)
                    print(f"Prepared {len(delete_keys)} unique keys for deletion")
                    
                    # Delete existing records using temp table
                    cur.execute(f"""
                        DELETE FROM {schema}.{table} t
                        USING temp_bctc_delete_keys tmp
                        WHERE t.ticker = tmp.ticker 
                          AND t.quarter = tmp.quarter 
                          AND t.year = tmp.year 
                          AND t.ind_code = tmp.ind_code;
                    """)
                    deleted = cur.rowcount
                    print(f"Deleted {deleted} existing rows")
                
                # Insert new data
                insert_sql = f"""
                    INSERT INTO {schema}.{table}
                    (ticker, quarter, year, ind_name, ind_code, value, report_name, report_code)
                    VALUES %s;
                """
                execute_values(cur, insert_sql, rows)
            
            conn.commit()
            print(f"✅ Inserted {len(rows)} rows")
            
            return f"✅ Success: {len(rows)} rows"
            
        except Exception as e:
            conn.rollback()
            print(f"❌ Error: {str(e)}")
            raise
    
    print("=" * 70)

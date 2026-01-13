
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import logging

# Cấu hình DAG
default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'email': ['data@company.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'standardize_bctc_indicators',
    default_args=default_args,
    description='Chuẩn hóa tên chỉ tiêu báo cáo tài chính',
    schedule_interval='0 2 * * *',  # Chạy lúc 2h sáng hàng ngày
    start_date=days_ago(1),
    catchup=False,
    tags=['finance', 'data_quality', 'standardization'],
)

# Task 1: Kiểm tra kết nối database
def check_database_connection(**context):
    """Kiểm tra kết nối đến database"""
    try:
        hook = PostgresHook(postgres_conn_id='postgres_bctc')
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        db_version = cursor.fetchone()
        logging.info(f"✅ Kết nối database thành công: {db_version}")
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        logging.error(f"❌ Lỗi kết nối database: {str(e)}")
        raise

check_db = PythonOperator(
    task_id='check_database_connection',
    python_callable=check_database_connection,
    dag=dag,
)

# Task 2: Backup bảng bctc trước khi chuẩn hóa
backup_bctc = PostgresOperator(
    task_id='backup_bctc_table',
    postgres_conn_id='postgres_bctc',
    sql="""
        -- Drop backup cũ nếu có
        DROP TABLE IF EXISTS hethong_phantich_chungkhoan.bctc_backup_{{ ds_nodash }};
        
        -- Tạo backup mới với timestamp
        CREATE TABLE hethong_phantich_chungkhoan.bctc_backup_{{ ds_nodash }} AS 
        SELECT * FROM hethong_phantich_chungkhoan.bctc;
        
        -- Log số lượng records đã backup
        DO $$ 
        DECLARE 
            row_count INTEGER;
        BEGIN
            SELECT COUNT(*) INTO row_count 
            FROM hethong_phantich_chungkhoan.bctc_backup_{{ ds_nodash }};
            RAISE NOTICE 'Đã backup % records', row_count;
        END $$;
    """,
    dag=dag,
)

# Task 3: Kiểm tra và tạo bảng mapping nếu chưa có
create_mapping_table = PostgresOperator(
    task_id='create_mapping_table',
    postgres_conn_id='postgres_bctc',
    sql="""
        -- Tạo bảng mapping nếu chưa tồn tại
        CREATE TABLE IF NOT EXISTS hethong_phantich_chungkhoan.indicator_mapping_4bctc (
            raw_ind_name TEXT PRIMARY KEY,
            std_ind_name TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Tạo index để tăng tốc độ tra cứu
        CREATE INDEX IF NOT EXISTS idx_mapping_std_name 
        ON hethong_phantich_chungkhoan.indicator_mapping_4bctc(std_ind_name);
    """,
    dag=dag,
)

# Task 4: Truncate và insert mapping data
truncate_and_insert_mapping = PostgresOperator(
    task_id='truncate_and_insert_mapping',
    postgres_conn_id='postgres_bctc',
    sql="""
        -- Xóa dữ liệu cũ
        TRUNCATE TABLE hethong_phantich_chungkhoan.indicator_mapping_4bctc;
        
        -- Insert 158 chỉ tiêu mapping
        INSERT INTO hethong_phantich_chungkhoan.indicator_mapping_4bctc (raw_ind_name, std_ind_name) VALUES
        
        -- BẢNG CÂN ĐỐI KẾ TOÁN
        ('Tiền và tương đương tiền', 'Tiền và tương đương tiền'),
        ('Tiền và tương đương tiền đầu kỳ', 'Tiền và tương đương tiền đầu kỳ'),
        ('Tiền và tương đương tiền cuối kỳ', 'Tiền và tương đương tiền cuối kỳ'),
        ('Đầu tư tài chính ngắn hạn', 'Đầu tư tài chính ngắn hạn'),
        ('Trading Securities', 'Chứng khoán kinh doanh'),
        ('Trading Securities, net', 'Chứng khoán kinh doanh (ròng)'),
        ('Provision for diminution in value of Trading Securities', 'Dự phòng giảm giá chứng khoán kinh doanh'),
        ('Các khoản phải thu', 'Các khoản phải thu'),
        ('Phải thu về cho vay ngắn hạn', 'Phải thu về cho vay ngắn hạn'),
        ('Người mua trả tiền trước', 'Người mua trả tiền trước'),
        ('Trả trước cho người bán', 'Trả trước cho người bán'),
        ('Hàng tồn kho', 'Hàng tồn kho'),
        ('Hàng tồn kho ròng', 'Hàng tồn kho ròng'),
        ('Tài sản ngắn hạn khác', 'Tài sản ngắn hạn khác'),
        ('Tài sản ngắn hạn khác (Bn)', 'Tài sản ngắn hạn khác'),
        ('Tài sản ngắn hạn', 'Tổng tài sản ngắn hạn'),
        ('Phải thu dài hạn của khách hàng', 'Phải thu dài hạn của khách hàng'),
        ('Phải thu dài hạn khác', 'Phải thu dài hạn khác'),
        ('Phải thu về cho vay dài hạn', 'Phải thu về cho vay dài hạn'),
        ('Tài sản cố định', 'Tài sản cố định'),
        ('Tangible fixed assets', 'Tài sản cố định hữu hình'),
        ('Intagible fixed assets', 'Tài sản cố định vô hình'),
        ('Leased assets', 'Tài sản thuê tài chính'),
        ('Chi phí trả trước dài hạn', 'Chi phí trả trước dài hạn'),
        ('Đầu tư tài chính dài hạn', 'Đầu tư tài chính dài hạn'),
        ('Investment Securities', 'Chứng khoán đầu tư'),
        ('Held-to-Maturity Securities', 'Chứng khoán nắm giữ đến ngày đáo hạn'),
        ('Available-for Sales Securities', 'Chứng khoán sẵn sàng để bán'),
        ('Balances with the SBV', 'Tiền gửi tại Ngân hàng Nhà nước'),
        ('Less: Provision for diminution in value of investment securities', 'Dự phòng giảm giá chứng khoán đầu tư'),
        ('Less: Provision for diminuation in value of long term investments', 'Dự phòng giảm giá đầu tư dài hạn'),
        ('Investment in properties', 'Đầu tư bất động sản'),
        ('Investment in joint ventures', 'Đầu tư vào công ty liên doanh'),
        ('Investments in associate companies', 'Đầu tư vào công ty liên kết'),
        ('Goodwill', 'Lợi thế thương mại'),
        ('Lợi thế thương mại', 'Lợi thế thương mại'),
        ('Tài sản dài hạn khác', 'Tài sản dài hạn khác'),
        ('Tài sản dài hạn khác (Bn)', 'Tài sản dài hạn khác'),
        ('Other Assets', 'Tài sản khác'),
        ('Tài sản dài hạn', 'Tổng tài sản dài hạn'),
        ('Tổng cộng tài sản', 'Tổng cộng tài sản'),
        
        -- NỢ PHẢI TRẢ
        ('Nợ ngắn hạn', 'Nợ ngắn hạn'),
        ('Vay và nợ ngắn hạn', 'Vay và nợ ngắn hạn'),
        ('Deposits from customers', 'Tiền gửi khách hàng'),
        ('Deposits and borrowings from other credit institutions', 'Tiền gửi và vay từ các tổ chức tín dụng khác'),
        ('Placements with and loans to other credit institutions', 'Tiền gửi tại và cho vay các tổ chức tín dụng khác'),
        ('Due to Gov and borrowings from SBV', 'Nợ Chính phủ và vay NHNN'),
        ('Derivatives and other financial liabilities', 'Các công cụ phái sinh và nợ tài chính khác'),
        ('_Derivatives and other financial liabilities', 'Các công cụ phái sinh và nợ tài chính khác'),
        ('Nợ dài hạn', 'Nợ dài hạn'),
        ('Vay và nợ dài hạn', 'Vay và nợ dài hạn'),
        ('Convertible bonds (Bn. VND)', 'Trái phiếu chuyển đổi'),
        ('Convertible bonds/CDs and other valuable papers issued', 'Trái phiếu chuyển đổi và giấy tờ có giá đã phát hành'),
        ('Other liabilities', 'Nợ khác'),
        ('Nợ phải trả', 'Tổng nợ phải trả'),
        ('Dự phòng tổn thất tín dụng', 'Dự phòng tổn thất tín dụng'),
        ('Loans and advances to customers', 'Cho vay khách hàng'),
        ('Loans and advances to customers, net', 'Cho vay khách hàng (ròng)'),
        ('Less: Provision for losses on loans and advances to customers', 'Dự phòng tổn thất cho vay khách hàng'),
        
        -- VỐN CHỦ SỞ HỮU
        ('Vốn chủ sở hữu', 'Vốn chủ sở hữu'),
        ('Capital', 'Vốn góp'),
        ('Vốn góp của chủ sở hữu', 'Vốn góp của chủ sở hữu'),
        ('Cổ phiếu phổ thông', 'Cổ phiếu phổ thông'),
        ('Vốn và các quỹ', 'Vốn và các quỹ'),
        ('Reserves', 'Các quỹ dự trữ'),
        ('Other Reserves', 'Quỹ khác'),
        ('_Other Reserves', 'Quỹ khác'),
        ('Quỹ đầu tư phát triển', 'Quỹ đầu tư phát triển'),
        ('Foreign Currency Difference reserve', 'Quỹ dự trữ chênh lệch tỷ giá'),
        ('Difference upon Assets Revaluation', 'Chênh lệch đánh giá lại tài sản'),
        ('LNST chưa phân phối', 'Lợi nhuận sau thuế chưa phân phối'),
        ('LNST của cổ đông công ty mẹ', 'Lợi nhuận sau thuế thuộc cổ đông công ty mẹ'),
        ('Lợi ích cổ đông thiểu số', 'Lợi ích cổ đông thiểu số'),
        ('Lợi ích cổ đông thiểu số (Nguồn vốn)', 'Lợi ích cổ đông thiểu số'),
        ('Budget sources and other funds', 'Nguồn kinh phí và quỹ khác'),
        ('Funds received from Gov, international and other institutions', 'Nguồn kinh phí đã hình thành tài sản cố định'),
        ('Tổng cộng nguồn vốn', 'Tổng cộng nguồn vốn'),
        
        -- BÁO CÁO KẾT QUẢ KINH DOANH
        ('Doanh thu', 'Doanh thu'),
        ('Doanh thu bán hàng', 'Doanh thu bán hàng và cung cấp dịch vụ'),
        ('Total operating revenue', 'Tổng doanh thu hoạt động'),
        ('Các khoản giảm trừ doanh thu', 'Các khoản giảm trừ doanh thu'),
        ('Doanh thu thuần', 'Doanh thu thuần'),
        ('Giá vốn hàng bán', 'Giá vốn hàng bán'),
        ('Lợi nhuận gộp', 'Lợi nhuận gộp về bán hàng và cung cấp dịch vụ'),
        ('Chi phí bán hàng', 'Chi phí bán hàng'),
        ('Chi phí quản lý doanh nghiệp', 'Chi phí quản lý doanh nghiệp'),
        ('Khấu hao và hao mòn', 'Khấu hao và hao mòn'),
        ('Doanh thu tài chính', 'Doanh thu hoạt động tài chính'),
        ('Interest and Similar Income', 'Thu nhập lãi và thu nhập tương tự'),
        ('Fees and Comission Income', 'Thu nhập phí và hoa hồng'),
        ('Lãi tiền gửi và cổ tức', 'Lãi tiền gửi và cổ tức nhận được'),
        ('Dividends received', 'Cổ tức nhận được'),
        ('Chi phí tài chính', 'Chi phí hoạt động tài chính'),
        ('Interest and Similar Expenses', 'Chi phí lãi và chi phí tương tự'),
        ('Fees and Comission Expenses', 'Chi phí phí và hoa hồng'),
        ('Chi phí lãi vay', 'Chi phí lãi vay'),
        ('Chi phí lãi vay (Lưu chuyển tiền tệ)', 'Chi phí lãi vay'),
        ('Net Interest Income', 'Thu nhập lãi thuần'),
        ('Net Fee and Commission Income', 'Thu nhập phí và hoa hồng thuần'),
        ('Lãi/lỗ từ hoạt động đầu tư', 'Lãi lỗ từ hoạt động đầu tư'),
        ('Lãi/lỗ từ công ty liên doanh liên kết', 'Lãi lỗ từ công ty liên doanh, liên kết'),
        ('Net income from associated companies', 'Thu nhập từ công ty liên kết'),
        ('Net gain (loss) from disposal of investment securities', 'Lãi lỗ thuần từ thanh lý chứng khoán đầu tư'),
        ('Net gain (loss) from foreign currency and gold dealings', 'Lãi lỗ thuần từ hoạt động kinh doanh ngoại tệ và vàng'),
        ('Net gain (loss) from trading of trading securities', 'Lãi lỗ thuần từ mua bán chứng khoán kinh doanh'),
        ('Profit/Loss from disposal of fixed assets', 'Lãi lỗ từ thanh lý tài sản cố định'),
        ('Thu nhập khác', 'Thu nhập khác'),
        ('Other expenses', 'Chi phí khác'),
        ('Thu nhập/Chi phí khác', 'Thu nhập chi phí khác'),
        ('Lợi nhuận khác', 'Lợi nhuận khác'),
        ('Profits from other activities', 'Lợi nhuận từ hoạt động khác'),
        ('Net Other income/(expenses)', 'Thu nhập khác thuần'),
        ('Net Other income/expenses', 'Thu nhập khác thuần'),
        ('Lợi nhuận từ HĐKD', 'Lợi nhuận từ hoạt động kinh doanh'),
        ('Lợi nhuận từ HĐKD trước thay đổi vốn lưu động', 'Lợi nhuận từ hoạt động kinh doanh trước thay đổi vốn lưu động'),
        ('Operating Profit before Provision', 'Lợi nhuận hoạt động trước trích lập dự phòng'),
        ('Lãi/lỗ chênh lệch tỷ giá chưa thực hiện', 'Lãi lỗ chênh lệch tỷ giá chưa thực hiện'),
        ('Ảnh hưởng của thay đổi tỷ giá', 'Ảnh hưởng của thay đổi tỷ giá hối đoái'),
        ('Lợi nhuận trước thuế', 'Tổng lợi nhuận kế toán trước thuế'),
        ('Lợi nhuận/Lỗ thuần trước thuế', 'Lợi nhuận thuần trước thuế'),
        ('Tax For the Year', 'Chi phí thuế thu nhập doanh nghiệp'),
        ('Thuế TNDN hiện hành', 'Chi phí thuế TNDN hiện hành'),
        ('Thuế TNDN hoãn lại', 'Chi phí thuế TNDN hoãn lại'),
        ('Lợi nhuận sau thuế thu nhập DN', 'Lợi nhuận sau thuế thu nhập doanh nghiệp'),
        ('LNST phân bổ cho CĐ công ty mẹ', 'Lợi nhuận sau thuế thuộc cổ đông công ty mẹ'),
        ('EPS_basis', 'Lãi cơ bản trên cổ phiếu'),
        ('Tăng trưởng doanh thu YoY', 'Tăng trưởng doanh thu so với cùng kỳ'),
        ('Tăng trưởng LNST mẹ YoY', 'Tăng trưởng lợi nhuận sau thuế công ty mẹ so với cùng kỳ'),
        
        -- BÁO CÁO LƯU CHUYỂN TIỀN TỆ
        ('Net Cash Flows from Operating Activities before BIT', 'Lưu chuyển tiền thuần từ hoạt động kinh doanh trước thuế'),
        ('Tăng/Giảm các khoản phải thu', 'Tăng giảm các khoản phải thu'),
        ('_Increase/Decrease in receivables', 'Tăng giảm các khoản phải thu'),
        ('Tăng/Giảm hàng tồn kho', 'Tăng giảm hàng tồn kho'),
        ('Tăng/Giảm chi phí trả trước', 'Tăng giảm chi phí trả trước'),
        ('Tăng/Giảm các khoản phải trả', 'Tăng giảm các khoản phải trả'),
        ('_Increase/Decrease in payables', 'Tăng giảm các khoản phải trả'),
        ('Other receipts from operating activities', 'Tiền thu khác từ hoạt động kinh doanh'),
        ('Other payments on operating activities', 'Tiền chi khác từ hoạt động kinh doanh'),
        ('Tiền lãi vay đã trả', 'Tiền lãi vay đã trả'),
        ('Thuế TNDN đã nộp', 'Thuế thu nhập doanh nghiệp đã nộp'),
        ('Lưu chuyển tiền thuần từ HĐKD', 'Lưu chuyển tiền thuần từ hoạt động kinh doanh'),
        ('Tiền chi mua sắm TSCĐ', 'Tiền chi để mua sắm xây dựng tài sản cố định'),
        ('Tiền thu thanh lý TSCĐ', 'Tiền thu từ thanh lý nhượng bán tài sản cố định'),
        ('Tiền chi đầu tư góp vốn vào đơn vị khác', 'Tiền chi đầu tư góp vốn vào đơn vị khác'),
        ('Tiền thu hồi đầu tư góp vốn vào đơn vị khác', 'Tiền thu hồi đầu tư góp vốn vào đơn vị khác'),
        ('Tiền cho vay, mua công cụ nợ', 'Tiền chi cho vay mua các công cụ nợ của đơn vị khác'),
        ('Tiền thu hồi cho vay, bán công cụ nợ', 'Tiền thu hồi cho vay bán lại các công cụ nợ của đơn vị khác'),
        ('Tiền thu lãi cho vay, cổ tức và lợi nhuận được chia', 'Tiền thu lãi cho vay cổ tức và lợi nhuận được chia'),
        ('Lưu chuyển tiền thuần từ HĐ đầu tư', 'Lưu chuyển tiền thuần từ hoạt động đầu tư'),
        ('Tiền thu từ phát hành cổ phiếu, nhận vốn góp', 'Tiền thu từ phát hành cổ phiếu nhận vốn góp của chủ sở hữu'),
        ('Tiền chi trả vốn góp, mua lại cổ phiếu', 'Tiền chi trả vốn góp cho các chủ sở hữu mua lại cổ phiếu'),
        ('Tiền thu từ đi vay', 'Tiền thu từ đi vay'),
        ('Tiền trả nợ gốc vay', 'Tiền trả nợ gốc vay'),
        ('Tiền trả nợ gốc thuê tài chính', 'Tiền trả nợ gốc thuê tài chính'),
        ('Cổ tức, lợi nhuận đã trả cho chủ sở hữu', 'Cổ tức lợi nhuận đã trả cho chủ sở hữu'),
        ('Payment from reserves', 'Chi từ các quỹ'),
        ('Lưu chuyển tiền thuần từ HĐ tài chính', 'Lưu chuyển tiền thuần từ hoạt động tài chính'),
        ('Lưu chuyển tiền thuần trong kỳ', 'Lưu chuyển tiền thuần trong kỳ');
        
        -- Log số lượng mapping đã insert
        DO $$ 
        DECLARE 
            mapping_count INTEGER;
        BEGIN
            SELECT COUNT(*) INTO mapping_count 
            FROM hethong_phantich_chungkhoan.indicator_mapping_4bctc;
            RAISE NOTICE '✅ Đã insert % chỉ tiêu mapping', mapping_count;
        END $$;
    """,
    dag=dag,
)

# Task 5: Kiểm tra tính toàn vẹn của mapping
def validate_mapping_data(**context):
    """Kiểm tra số lượng và tính hợp lệ của mapping"""
    hook = PostgresHook(postgres_conn_id='postgres_bctc')
    
    # Đếm số lượng mapping
    count_query = "SELECT COUNT(*) FROM hethong_phantich_chungkhoan.indicator_mapping_4bctc"
    mapping_count = hook.get_first(count_query)[0]
    
    logging.info(f"Số lượng mapping: {mapping_count}")
    
    # Kiểm tra có duplicate không
    duplicate_query = """
        SELECT raw_ind_name, COUNT(*) as cnt 
        FROM hethong_phantich_chungkhoan.indicator_mapping_4bctc 
        GROUP BY raw_ind_name 
        HAVING COUNT(*) > 1
    """
    duplicates = hook.get_records(duplicate_query)
    
    if duplicates:
        logging.error(f"❌ Phát hiện {len(duplicates)} chỉ tiêu trùng lặp!")
        for dup in duplicates:
            logging.error(f"  - {dup[0]}: {dup[1]} lần")
        raise ValueError("Mapping data có chỉ tiêu trùng lặp!")
    
    # Kiểm tra số lượng tối thiểu (158 chỉ tiêu)
    if mapping_count < 158:
        logging.error(f"❌ Thiếu mapping! Cần 158 chỉ tiêu, chỉ có {mapping_count}")
        raise ValueError(f"Mapping không đủ số lượng: {mapping_count}/158")
    
    logging.info(f"✅ Validation thành công: {mapping_count} chỉ tiêu")
    return mapping_count

validate_mapping = PythonOperator(
    task_id='validate_mapping_data',
    python_callable=validate_mapping_data,
    dag=dag,
)

# Task 6: Thống kê trước khi chuẩn hóa
def analyze_before_standardization(**context):
    """Phân tích dữ liệu trước khi chuẩn hóa"""
    hook = PostgresHook(postgres_conn_id='postgres_bctc')
    
    # Đếm tổng số records
    total_query = "SELECT COUNT(*) FROM hethong_phantich_chungkhoan.bctc"
    total_records = hook.get_first(total_query)[0]
    
    # Đếm số chỉ tiêu unique
    unique_query = "SELECT COUNT(DISTINCT ind_name) FROM hethong_phantich_chungkhoan.bctc"
    unique_indicators = hook.get_first(unique_query)[0]
    
    # Đếm số chỉ tiêu sẽ được mapping
    mappable_query = """
        SELECT COUNT(DISTINCT b.ind_name)
        FROM hethong_phantich_chungkhoan.bctc b
        INNER JOIN hethong_phantich_chungkhoan.indicator_mapping_4bctc m
            ON b.ind_name = m.raw_ind_name
    """
    mappable_indicators = hook.get_first(mappable_query)[0]
    
    # Liệt kê các chỉ tiêu KHÔNG có trong mapping
    unmapped_query = """
        SELECT DISTINCT b.ind_name
        FROM hethong_phantich_chungkhoan.bctc b
        LEFT JOIN hethong_phantich_chungkhoan.indicator_mapping_4bctc m
            ON b.ind_name = m.raw_ind_name
        WHERE m.raw_ind_name IS NULL
        ORDER BY b.ind_name
        LIMIT 20
    """
    unmapped_indicators = hook.get_records(unmapped_query)
    
    logging.info("=" * 70)
    logging.info("📊 PHÂN TÍCH TRƯỚC KHI CHUẨN HÓA")
    logging.info("=" * 70)
    logging.info(f"Tổng số records: {total_records:,}")
    logging.info(f"Số chỉ tiêu unique: {unique_indicators}")
    logging.info(f"Số chỉ tiêu có mapping: {mappable_indicators}")
    logging.info(f"Số chỉ tiêu KHÔNG có mapping: {unique_indicators - mappable_indicators}")
    
    if unmapped_indicators:
        logging.warning("⚠️ Các chỉ tiêu KHÔNG có mapping (top 20):")
        for ind in unmapped_indicators:
            logging.warning(f"  - {ind[0]}")
    
    logging.info("=" * 70)
    
    # Push kết quả sang XCom để task khác sử dụng
    context['task_instance'].xcom_push(key='total_records', value=total_records)
    context['task_instance'].xcom_push(key='unique_indicators', value=unique_indicators)
    context['task_instance'].xcom_push(key='mappable_indicators', value=mappable_indicators)
    
    return {
        'total_records': total_records,
        'unique_indicators': unique_indicators,
        'mappable_indicators': mappable_indicators
    }

analyze_before = PythonOperator(
    task_id='analyze_before_standardization',
    python_callable=analyze_before_standardization,
    dag=dag,
)

# Task 7: Thực hiện chuẩn hóa (UPDATE)
standardize_bctc = PostgresOperator(
    task_id='standardize_bctc_indicators',
    postgres_conn_id='postgres_bctc',
    sql="""
        -- Update ind_name theo mapping
        UPDATE hethong_phantich_chungkhoan.bctc b
        SET ind_name = m.std_ind_name
        FROM hethong_phantich_chungkhoan.indicator_mapping_4bctc m
        WHERE b.ind_name = m.raw_ind_name;
        
        -- Log kết quả
        DO $ 
        DECLARE 
            updated_count INTEGER;
        BEGIN
            GET DIAGNOSTICS updated_count = ROW_COUNT;
            RAISE NOTICE '✅ Đã chuẩn hóa % records', updated_count;
        END $;
    """,
    dag=dag,
)

# Task 8: Thống kê sau khi chuẩn hóa
def analyze_after_standardization(**context):
    """Phân tích kết quả sau khi chuẩn hóa"""
    hook = PostgresHook(postgres_conn_id='postgres_bctc')
    
    # Lấy dữ liệu trước đó từ XCom
    ti = context['task_instance']
    total_records = ti.xcom_pull(key='total_records', task_ids='analyze_before_standardization')
    unique_before = ti.xcom_pull(key='unique_indicators', task_ids='analyze_before_standardization')
    
    # Đếm số chỉ tiêu unique sau khi chuẩn hóa
    unique_query = "SELECT COUNT(DISTINCT ind_name) FROM hethong_phantich_chungkhoan.bctc"
    unique_after = hook.get_first(unique_query)[0]
    
    # Thống kê phân bố chỉ tiêu
    distribution_query = """
        SELECT ind_name, COUNT(*) as record_count
        FROM hethong_phantich_chungkhoan.bctc
        GROUP BY ind_name
        ORDER BY record_count DESC
        LIMIT 10
    """
    top_indicators = hook.get_records(distribution_query)
    
    # Kiểm tra các chỉ tiêu vẫn chưa được mapping
    unmapped_query = """
        SELECT DISTINCT b.ind_name, COUNT(*) as record_count
        FROM hethong_phantich_chungkhoan.bctc b
        LEFT JOIN hethong_phantich_chungkhoan.indicator_mapping_4bctc m
            ON b.ind_name = m.std_ind_name
        WHERE m.std_ind_name IS NULL
        GROUP BY b.ind_name
        ORDER BY record_count DESC
        LIMIT 10
    """
    still_unmapped = hook.get_records(unmapped_query)
    
    logging.info("=" * 70)
    logging.info("📊 KẾT QUẢ SAU KHI CHUẨN HÓA")
    logging.info("=" * 70)
    logging.info(f"Tổng số records: {total_records:,}")
    logging.info(f"Số chỉ tiêu TRƯỚC chuẩn hóa: {unique_before}")
    logging.info(f"Số chỉ tiêu SAU chuẩn hóa: {unique_after}")
    logging.info(f"Đã giảm: {unique_before - unique_after} chỉ tiêu trùng lặp")
    logging.info("")
    logging.info("📈 TOP 10 chỉ tiêu có nhiều records nhất:")
    for ind, count in top_indicators:
        logging.info(f"  - {ind}: {count:,} records")
    
    if still_unmapped:
        logging.warning("")
        logging.warning("⚠️ Các chỉ tiêu vẫn CHƯA được chuẩn hóa:")
        for ind, count in still_unmapped:
            logging.warning(f"  - {ind}: {count:,} records")
    
    logging.info("=" * 70)
    
    return {
        'total_records': total_records,
        'unique_before': unique_before,
        'unique_after': unique_after,
        'reduced': unique_before - unique_after
    }

analyze_after = PythonOperator(
    task_id='analyze_after_standardization',
    python_callable=analyze_after_standardization,
    dag=dag,
)

# Task 9: Tạo báo cáo quality check
def generate_quality_report(**context):
    """Tạo báo cáo chất lượng sau chuẩn hóa"""
    hook = PostgresHook(postgres_conn_id='postgres_bctc')
    
    # Kiểm tra NULL values
    null_check_query = """
        SELECT 
            COUNT(*) as total_rows,
            SUM(CASE WHEN ind_name IS NULL THEN 1 ELSE 0 END) as null_ind_name,
            SUM(CASE WHEN ind_name = '' THEN 1 ELSE 0 END) as empty_ind_name
        FROM hethong_phantich_chungkhoan.bctc
    """
    null_stats = hook.get_first(null_check_query)
    
    # Kiểm tra data consistency
    consistency_query = """
        SELECT 
            CASE 
                WHEN ind_name LIKE '%  %' THEN 'double_space'
                WHEN ind_name LIKE ' %' OR ind_name LIKE '% ' THEN 'leading_trailing_space'
                WHEN ind_name ~ '[^a-zA-Z0-9ÀÁÂÃÈÉÊÌÍÒÓÔÕÙÚĂĐĨŨƠàáâãèéêìíòóôõùúăđĩũơƯĂẠẢẤẦẨẪẬẮẰẲẴẶẸẺẼỀỀỂưăạảấầẩẫậắằẳẵặẹẻẽềềểỄỆỈỊỌỎỐỒỔỖỘỚỜỞỠỢỤỦỨỪễệỉịọỏốồổỗộớờởỡợụủứừỬỮỰỲỴÝỶỸửữựỳỵýỷỹ (),-/]' THEN 'special_characters'
                ELSE 'clean'
            END as issue_type,
            COUNT(*) as count
        FROM hethong_phantich_chungkhoan.bctc
        GROUP BY 1
        ORDER BY 2 DESC
    """
    consistency_stats = hook.get_records(consistency_query)
    
    logging.info("=" * 70)
    logging.info("🔍 BÁO CÁO CHẤT LƯỢNG DỮ LIỆU")
    logging.info("=" * 70)
    logging.info(f"Tổng records: {null_stats[0]:,}")
    logging.info(f"NULL ind_name: {null_stats[1]:,}")
    logging.info(f"Empty ind_name: {null_stats[2]:,}")
    logging.info("")
    logging.info("Consistency Check:")
    for issue_type, count in consistency_stats:
        logging.info(f"  - {issue_type}: {count:,} records")
    logging.info("=" * 70)
    
    # Raise alert nếu có vấn đề nghiêm trọng
    if null_stats[1] > 0 or null_stats[2] > 0:
        logging.error("❌ Phát hiện dữ liệu NULL hoặc rỗng!")
        raise ValueError("Data quality check failed: NULL/Empty values detected")
    
    return {
        'total_rows': null_stats[0],
        'quality_passed': True
    }

quality_report = PythonOperator(
    task_id='generate_quality_report',
    python_callable=generate_quality_report,
    dag=dag,
)

# Task 10: Gửi thông báo kết quả
def send_completion_notification(**context):
    """Gửi thông báo hoàn thành"""
    ti = context['task_instance']
    
    # Lấy thông tin từ các task trước
    total_records = ti.xcom_pull(key='total_records', task_ids='analyze_before_standardization')
    unique_before = ti.xcom_pull(key='unique_indicators', task_ids='analyze_before_standardization')
    
    result = ti.xcom_pull(task_ids='analyze_after_standardization')
    unique_after = result['unique_after']
    reduced = result['reduced']
    
    execution_date = context['execution_date'].strftime('%Y-%m-%d %H:%M:%S')
    
    message = f"""
    ✅ CHUẨN HÓA BÁO CÁO TÀI CHÍNH HOÀN TẤT
    
    📅 Thời gian: {execution_date}
    📊 Tổng records: {total_records:,}
    📉 Giảm từ {unique_before} xuống {unique_after} chỉ tiêu ({reduced} trùng lặp)
    ✨ Tỷ lệ chuẩn hóa: {(reduced/unique_before*100):.1f}%
    
    Database: hethong_phantich_chungkhoan.bctc
    Backup: bctc_backup_{context['ds_nodash']}
    """
    
    logging.info(message)
    
    # Ở đây bạn có thể thêm code gửi email, Slack, etc.
    # send_slack_message(message)
    # send_email(message)
    
    return message

send_notification = PythonOperator(
    task_id='send_completion_notification',
    python_callable=send_completion_notification,
    dag=dag,
)

# Task 11: Cleanup old backups (giữ 7 ngày gần nhất)
cleanup_old_backups = PostgresOperator(
    task_id='cleanup_old_backups',
    postgres_conn_id='postgres_bctc',
    sql="""
        DO $
        DECLARE
            backup_table TEXT;
        BEGIN
            -- Tìm và xóa các bảng backup cũ hơn 7 ngày
            FOR backup_table IN 
                SELECT tablename 
                FROM pg_tables 
                WHERE schemaname = 'hethong_phantich_chungkhoan' 
                  AND tablename LIKE 'bctc_backup_%'
                  AND tablename < 'bctc_backup_' || TO_CHAR(CURRENT_DATE - INTERVAL '7 days', 'YYYYMMDD')
            LOOP
                EXECUTE 'DROP TABLE IF EXISTS hethong_phantich_chungkhoan.' || backup_table;
                RAISE NOTICE 'Đã xóa backup cũ: %', backup_table;
            END LOOP;
        END $;
    """,
    dag=dag,
)

# ===== ĐỊNH NGHĨA WORKFLOW =====
# Luồng xử lý tuần tự
check_db >> backup_bctc >> create_mapping_table >> truncate_and_insert_mapping
truncate_and_insert_mapping >> validate_mapping >> analyze_before
analyze_before >> standardize_bctc >> analyze_after
analyze_after >> quality_report >> send_notification >> cleanup_old_backups
import os
import io
import time
import pandas as pd
from datetime import datetime
import psycopg2
from concurrent.futures import ProcessPoolExecutor

# --- 1. CẤU HÌNH HỆ THỐNG ---
DB_CONFIG = {
    "dbname": "postgres",
    "user": "admin",
    "pass": "123456",
    "host": "localhost",
    "port": "5432",
    "schema": "hethong_phantich_chungkhoan",
    "table": "owner"
}

# Thư mục chứa các file CSV
FOLDER_PATH = "2026-01-01"

# --- 2. KIỂM TRA KẾT NỐI VÀ CẤU TRÚC DB ---
def check_db_setup():
    print(f"[{datetime.now().strftime('%H:%M:%S')}] 🔍 Đang khởi tạo và kiểm tra kết nối...")
    try:
        conn = psycopg2.connect(
            dbname=DB_CONFIG['dbname'], user=DB_CONFIG['user'], 
            password=DB_CONFIG['pass'], host=DB_CONFIG['host'], port=DB_CONFIG['port'],
            connect_timeout=5
        )
        conn.set_client_encoding('UTF8')
        cursor = conn.cursor()
        
        # Kiểm tra bảng và cột import_time
        check_query = f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = '{DB_CONFIG['schema']}' 
            AND table_name = '{DB_CONFIG['table']}' 
            AND column_name = 'import_time';
        """
        cursor.execute(check_query)
        if not cursor.fetchone():
            print(f"❌ Lỗi: Bảng '{DB_CONFIG['table']}' thiếu cột 'import_time'.")
            print("Vui lòng chạy lệnh: ALTER TABLE hethong_phantich_chungkhoan.owner ADD COLUMN import_time TIMESTAMP;")
            return False
            
        print("✅ Kết nối Database thành công. Cấu trúc bảng hợp lệ.")
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        print(f"❌ Không thể kết nối tới Database: {e}")
        return False

# --- 3. WORKER XỬ LÝ DỮ LIỆU ---
def process_file_worker(file_info):
    file_path, current_idx, total_files = file_info
    file_name = os.path.basename(file_path)
    start_time = time.time()
    prefix = f"[{current_idx}/{total_files}]"

    try:
        # Đọc CSV với hỗ trợ Tiếng Việt (utf-8-sig xử lý file từ Excel có BOM)
        # Ép kiểu 'percent' là object/string ngay từ đầu để tránh lỗi làm tròn của Pandas
        df = pd.read_csv(file_path, encoding='utf-8-sig', dtype={'percent': str})
        
        # Đổi tên cột: symbol -> ticker
        if 'symbol' in df.columns:
            df = df.rename(columns={'symbol': 'ticker'})
        
        # XỬ LÝ SỐ (Xóa bỏ Scientific Notation e-05)
        if 'percent' in df.columns:
            # Chuyển về dạng float rồi format lại thành chuỗi thập phân cố định
            df['percent'] = pd.to_numeric(df['percent'], errors='coerce').fillna(0)
            # Dùng định dạng .10f để lấy 10 chữ số thập phân, rstrip để xóa số 0 thừa
            df['percent'] = df['percent'].apply(lambda x: '{:.10f}'.format(x).rstrip('0').rstrip('.'))
        
        # Thêm dấu thời gian import
        df['import_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Sắp xếp đúng thứ tự cột trong DB
        cols_order = ['ticker', 'name', 'position', 'percent', 'type', 'import_time']
        df = df[cols_order]
        
        # Làm sạch khoảng trắng thừa cho các cột văn bản (Tiếng Việt)
        for col in ['name', 'position', 'type']:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()

        # Chuyển DataFrame thành luồng dữ liệu CSV ảo trong RAM (Buffer)
        output = io.StringIO()
        df.to_csv(output, index=False, header=False, encoding='utf-8')
        output.seek(0)

        # Kết nối và đẩy dữ liệu dùng lệnh COPY (tốc độ cao nhất)
        conn = psycopg2.connect(
            dbname=DB_CONFIG['dbname'], user=DB_CONFIG['user'], 
            password=DB_CONFIG['pass'], host=DB_CONFIG['host'], port=DB_CONFIG['port']
        )
        conn.set_client_encoding('UTF8')
        cursor = conn.cursor()
        
        copy_sql = f"COPY {DB_CONFIG['schema']}.{DB_CONFIG['table']} FROM STDIN WITH CSV"
        cursor.copy_expert(sql=copy_sql, file=output)
        
        conn.commit()
        cursor.close()
        conn.close()

        elapsed = round(time.time() - start_time, 2)
        return f"{prefix} ✅ Xong: {file_name} ({len(df)} dòng) - {elapsed}s"

    except Exception as e:
        return f"{prefix} ❌ Lỗi tại file {file_name}: {str(e)}"

# --- 4. HÀM CHÍNH ĐIỀU PHỐI ---
def main():
    # Bước 1: Check DB
    if not check_db_setup():
        return

    # Bước 2: Quét thư mục lấy danh sách file
    if not os.path.exists(FOLDER_PATH):
        print(f"❌ Thư mục '{FOLDER_PATH}' không tồn tại.")
        return

    csv_files = [os.path.join(FOLDER_PATH, f) for f in os.listdir(FOLDER_PATH) if f.endswith('.csv')]
    total = len(csv_files)
    
    if total == 0:
        print("⚠️ Không tìm thấy file CSV nào trong thư mục.")
        return

    print(f"🚀 Tìm thấy {total} file. Bắt đầu đẩy dữ liệu (Multiprocessing)...")
    print("-" * 80)

    # Chuẩn bị dữ liệu cho các tiến trình con
    tasks = [(csv_files[i], i + 1, total) for i in range(total)]

    # Bước 3: Chạy song song (4 workers là con số an toàn cho CPU và DB)
    with ProcessPoolExecutor(max_workers=4) as executor:
        results = list(executor.map(process_file_worker, tasks))

    # Bước 4: In kết quả tổng hợp
    for res in results:
        print(res)

    print("-" * 80)
    print(f"🏁 Hoàn thành lúc: {datetime.now().strftime('%H:%M:%S')}")

if __name__ == "__main__":
    main()
    
import os
import io
import time
import pandas as pd
from datetime import datetime
import psycopg2
from sqlalchemy import create_engine
from concurrent.futures import ProcessPoolExecutor

# --- CẤU HÌNH (Dựa trên ảnh của bạn) ---
DB_CONFIG = {
    "dbname": "postgres",
    "user": "admin",
    "pass": "123456",
    "host": "localhost",
    "port": "5432",
    "schema": "hethong_phantich_chungkhoan",
    "table": "history_price"
}
FOLDER_PATH = "2026-01-01"

def check_connection():
    """Kiểm tra kết nối DB trước khi chạy"""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] 🔍 Đang kiểm tra kết nối tới Database...")
    try:
        conn = psycopg2.connect(
            dbname=DB_CONFIG['dbname'], 
            user=DB_CONFIG['user'], 
            password=DB_CONFIG['pass'], 
            host=DB_CONFIG['host'], 
            port=DB_CONFIG['port'],
            connect_timeout=5
        )
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        db_version = cursor.fetchone()
        print(f"✅ Kết nối thành công! Phiên bản DB: {db_version[0]}")
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        print(f"❌ Kết nối thất bại: {e}")
        return False

def fast_copy_worker(file_info):
    """Worker xử lý từng file"""
    file_path, current_idx, total_files = file_info
    file_name = os.path.basename(file_path)
    start_time = time.time()
    
    prefix = f"[{current_idx}/{total_files}]"
    print(f"{prefix} 🚀 Đang bắt đầu: {file_name}")

    try:
        # Đọc dữ liệu
        df = pd.read_csv(file_path)
        row_count = len(df)
        df['import_time'] = datetime.now()
        
        # Chuyển sang buffer để dùng lệnh COPY (tốc độ cao nhất)
        output = io.StringIO()
        df.to_csv(output, index=False, header=False)
        output.seek(0)
        
        # Kết nối và thực thi
        conn = psycopg2.connect(
            dbname=DB_CONFIG['dbname'], user=DB_CONFIG['user'], 
            password=DB_CONFIG['pass'], host=DB_CONFIG['host'], port=DB_CONFIG['port']
        )
        cursor = conn.cursor()
        
        copy_sql = f"COPY {DB_CONFIG['schema']}.{DB_CONFIG['table']} FROM STDIN WITH CSV"
        cursor.copy_expert(sql=copy_sql, file=output)
        
        conn.commit()
        cursor.close()
        conn.close()
        
        duration = round(time.time() - start_time, 2)
        return f"{prefix} ✅ Xong: {file_name} | {row_count} dòng | {duration}s"
    
    except Exception as e:
        return f"{prefix} ❌ Lỗi tại {file_name}: {e}"

def main():
    # 1. Kiểm tra kết nối trước
    if not check_connection():
        return

    # 2. Quét thư mục
    if not os.path.exists(FOLDER_PATH):
        print(f"❌ Thư mục '{FOLDER_PATH}' không tồn tại.")
        return

    csv_files = [os.path.join(FOLDER_PATH, f) for f in os.listdir(FOLDER_PATH) if f.endswith('.csv')]
    total = len(csv_files)
    
    if total == 0:
        print("⚠️ Không tìm thấy file CSV nào để xử lý.")
        return

    print(f"[{datetime.now().strftime('%H:%M:%S')}] 📂 Tìm thấy {total} files. Bắt đầu đẩy dữ liệu dùng đa nhân...")
    print("-" * 60)

    # Chuẩn bị dữ liệu cho worker (đính kèm index để log tiến trình)
    file_tasks = [(csv_files[i], i + 1, total) for i in range(total)]

    # 3. Chạy đa tiến trình
    # max_workers: nên để khoảng 2-4 tùy cấu hình máy để tránh khóa bảng (deadlock)
    with ProcessPoolExecutor(max_workers=4) as executor:
        results = list(executor.map(fast_copy_worker, file_tasks))

    print("-" * 60)
    print(f"[{datetime.now().strftime('%H:%M:%S')}] 🏁 HOÀN THÀNH TẤT CẢ.")
    for res in results:
        print(res)

if __name__ == "__main__":
    main()
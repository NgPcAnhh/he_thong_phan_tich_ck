import os
import io
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
import psycopg2
from concurrent.futures import ProcessPoolExecutor

# --- CẤU HÌNH DỰA TRÊN ẢNH ---
DB_NAME = "postgres"  # Tên database chính
DB_USER = "admin"
DB_PASS = "123456"
DB_HOST = "localhost"
DB_PORT = "5432"
SCHEMA_NAME = "hethong_phantich_chungkhoan"
TABLE_NAME = "market_index"
FOLDER_PATH = "daily_price"

def fast_copy_csv(file_path):
    """
    Sử dụng lệnh COPY của Postgres để đẩy dữ liệu cực nhanh
    """
    try:
        # 1. Đọc và xử lý thêm cột import_time bằng Pandas
        df = pd.read_csv(file_path)
        df['import_time'] = datetime.now()
        
        # 2. Chuyển DataFrame thành buffer (như một file ảo trong RAM)
        output = io.StringIO()
        df.to_csv(output, index=False, header=False) # Không ghi header vì dùng COPY
        output.seek(0)
        
        # 3. Kết nối trực tiếp bằng psycopg2
        conn = psycopg2.connect(
            dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT
        )
        cursor = conn.cursor()
        
        # Lệnh COPY chuyên dụng của Postgres
        copy_sql = f"COPY {SCHEMA_NAME}.{TABLE_NAME} FROM STDIN WITH CSV"
        cursor.copy_expert(sql=copy_sql, file=output)
        
        conn.commit()
        cursor.close()
        conn.close()
        return f"✅ Thành công: {os.path.basename(file_path)}"
    except Exception as e:
        return f"❌ Lỗi tại file {os.path.basename(file_path)}: {e}"

def main():
    # Quét danh sách file
    if not os.path.exists(FOLDER_PATH):
        print(f"Thư mục '{FOLDER_PATH}' không tồn tại!")
        return

    csv_files = [os.path.join(FOLDER_PATH, f) for f in os.listdir(FOLDER_PATH) if f.endswith('.csv')]
    
    if not csv_files:
        print("Không tìm thấy file CSV nào.")
        return

    print(f"Đang xử lý {len(csv_files)} files bằng đa nhân CPU...")

    # Sử dụng ProcessPoolExecutor để chạy song song (Workers)
    # Tăng số lượng worker nếu bạn có nhiều nhân CPU
    with ProcessPoolExecutor(max_workers=4) as executor:
        results = list(executor.map(fast_copy_csv, csv_files))

    for res in results:
        print(res)

if __name__ == "__main__":
    main()
import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import hashlib
from datetime import datetime

# --- CẤU HÌNH ---
DB_CONN_INFO = {"dbname": "postgres", "user": "admin", "password": "123456", "host": "localhost", "port": "5432"}
SCHEMA_TABLE = "hethong_phantich_chungkhoan.news"
FOLDER_PATH = "2026-01-01"

def process_single_csv(file_path):
    print(f"📄 Đang xử lý file: {os.path.basename(file_path)}")
    try:
        df = pd.read_csv(file_path, encoding='utf-8-sig')
        rows = []
        for _, row in df.iterrows():
            title = str(row.get('title', ''))
            if not title: continue
            
            # Tạo news_id duy nhất từ tiêu đề nếu CSV không có ID
            news_id = hashlib.md5(title.encode()).hexdigest()
            
            rows.append((
                news_id,
                row.get('ticker'),
                title,
                row.get('description'), 
                row.get('link'),        
                row.get('image'),       
                row.get('source'),      
                pd.to_datetime(row.get('date')) if pd.notnull(row.get('date')) else None,
                datetime.now() # import_time
            ))

        if rows:
            conn = psycopg2.connect(**DB_CONN_INFO)
            cursor = conn.cursor()
            query = f"""
                INSERT INTO {SCHEMA_TABLE} 
                (news_id, ticker, title, short_content, source_link, image_url, source_name, public_date, import_time)
                VALUES %s ON CONFLICT (news_id) DO NOTHING;
            """
            execute_values(cursor, query, rows)
            conn.commit()
            cursor.close()
            conn.close()
            return f"✅ Thành công: {os.path.basename(file_path)} ({len(rows)} tin)"
    except Exception as e:
        return f"❌ Lỗi tại {os.path.basename(file_path)}: {e}"

def main():
    if not os.path.exists(FOLDER_PATH):
        print(f"Thư mục {FOLDER_PATH} không tồn tại.")
        return

    files = [os.path.join(FOLDER_PATH, f) for f in os.listdir(FOLDER_PATH) if f.endswith('.csv')]
    print(f"🚀 Tìm thấy {len(files)} file CSV. Bắt đầu import...")
    
    for f in files:
        result = process_single_csv(f)
        print(result)

if __name__ == "__main__":
    main()
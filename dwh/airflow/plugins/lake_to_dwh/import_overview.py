import os
import io
import time
import pandas as pd
from datetime import datetime
import psycopg2
from concurrent.futures import ProcessPoolExecutor

# --- CONFIGURATION ---
DB_CONFIG = {
    "dbname": "postgres",
    "user": "admin",
    "pass": "123456",
    "host": "localhost",
    "port": "5432",
    "schema": "hethong_phantich_chungkhoan",
    "table": "company_overview"
}
FOLDER_PATH = "2026-01-01"

def check_db_connection():
    """Kiểm tra kết nối và bảng trước khi chạy"""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] 🔍 Đang kiểm tra Database...")
    try:
        conn = psycopg2.connect(
            dbname=DB_CONFIG['dbname'], user=DB_CONFIG['user'], 
            password=DB_CONFIG['pass'], host=DB_CONFIG['host'], port=DB_CONFIG['port']
        )
        print("✅ Kết nối thành công!")
        conn.close()
        return True
    except Exception as e:
        print(f"❌ Lỗi kết nối: {e}")
        return False

def process_company_worker(file_info):
    file_path, current_idx, total_files = file_info
    file_name = os.path.basename(file_path)
    start_time = time.time()
    prefix = f"[{current_idx}/{total_files}]"

    try:
        # 1. Đọc CSV với mã hóa UTF-8 Tiếng Việt
        # Lưu ý: CSV của bạn có các cột trùng tên 'icb_name', Pandas sẽ tự đổi thành icb_name, icb_name.1, icb_name.2
        df = pd.read_csv(file_path, encoding='utf-8-sig')
        
        # 2. Ánh xạ cột (Mapping)
        mapping = {
            df.columns[0]: 'ticker',    # symbol
            df.columns[1]: 'overview',  # company_
            df.columns[2]: 'icb_name1', # icb_name thứ 1
            df.columns[3]: 'icb_name2', # icb_name thứ 2
            df.columns[4]: 'icb_name3'  # icb_name thứ 3
        }
        df = df.rename(columns=mapping)
        
        # 3. Thêm import_time
        df['import_time'] = datetime.now()

        # 4. Giữ lại đúng các cột trong Database
        cols_in_db = ['ticker', 'overview', 'icb_name1', 'icb_name2', 'icb_name3', 'import_time']
        df = df[cols_in_db]

        # 5. Xử lý định dạng dữ liệu
        # Đảm bảo không bị Scientific Notation (e-05) và xóa khoảng trắng thừa cho Tiếng Việt
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].astype(str).str.strip()
            elif df[col].dtype == 'float':
                df[col] = df[col].apply(lambda x: '{:.10f}'.format(x).rstrip('0').rstrip('.') if pd.notnull(x) else '')

        # 6. Chuyển vào Buffer dùng lệnh COPY
        output = io.StringIO()
        df.to_csv(output, index=False, header=False, encoding='utf-8')
        output.seek(0)

        # 7. Thực thi đẩy vào Postgres
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

        duration = round(time.time() - start_time, 2)
        return f"{prefix} ✅ {file_name} | {len(df)} dòng | {duration}s"

    except Exception as e:
        return f"{prefix} ❌ Lỗi tại {file_name}: {str(e)}"

def main():
    if not check_db_connection(): return

    csv_files = [os.path.join(FOLDER_PATH, f) for f in os.listdir(FOLDER_PATH) if f.endswith('.csv')]
    if not csv_files:
        print("⚠️ Không tìm thấy file CSV.")
        return

    print(f"🚀 Bắt đầu Import {len(csv_files)} file vào bảng 'company_overview'...")
    print("-" * 75)

    tasks = [(csv_files[i], i + 1, len(csv_files)) for i in range(len(csv_files))]

    with ProcessPoolExecutor(max_workers=4) as executor:
        results = list(executor.map(process_company_worker, tasks))

    for res in results:
        print(res)

    print("-" * 75)
    print(f"🏁 Hoàn thành: {datetime.now().strftime('%H:%M:%S')}")

if __name__ == "__main__":
    main()
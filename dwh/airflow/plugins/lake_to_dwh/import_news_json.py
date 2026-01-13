import json
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime

DB_CONN = {"dbname": "postgres", "user": "admin", "password": "123456", "host": "localhost", "port": "5432"}
TABLE_FULL = "hethong_phantich_chungkhoan.news"

def import_fiin_json(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # Nếu data là một list các tin nhắn
    rows = []
    for item in data:
        # Convert Unix Timestamp (ms) to datetime
        pub_date = datetime.fromtimestamp(item.get('public_date') / 1000.0) if item.get('public_date') else None
        
        rows.append((
            item.get('news_id'),
            item.get('related_ticker'),
            item.get('news_title'),
            item.get('news_sub_title'),
            item.get('news_short_content'),
            item.get('news_full_content'),
            item.get('news_image_url'),
            item.get('news_source_link'),
            'FiinGroup',
            item.get('lang_code', 'vi'),
            pub_date,
            item.get('close_price'),
            item.get('ref_price'),
            item.get('floor'),
            item.get('ceiling'),
            item.get('price_change_pct')
        ))

    conn = psycopg2.connect(**DB_CONN)
    cursor = conn.cursor()
    query = f"""
        INSERT INTO {TABLE_FULL} 
        (news_id, ticker, title, sub_title, short_content, full_content, image_url, source_link, source_name, lang_code, public_date, close_price, ref_price, floor_price, ceiling_price, price_change_pct)
        VALUES %s ON CONFLICT (news_id) DO NOTHING;
    """
    execute_values(cursor, query, rows)
    conn.commit()
    print(f"✅ Đã import {len(rows)} tin từ FiinGroup JSON")
    conn.close()

if __name__ == "__main__":
    import_fiin_json("news_2025.json")
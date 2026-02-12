
from contextlib import closing
import pandas as pd
import hashlib
from datetime import datetime
from psycopg2.extras import execute_values

from lake_to_dwh.utils import (
    get_latest_partition,
    get_minio_hook,
    read_csv_from_minio,
    get_postgres_connection,
    ensure_schema,
    clean_dataframe
)


def sync_news_to_db(
    db_url: str,
    schema: str,
    bucket: str,
    minio_conn_id: str = "minio_finance",
    folder_prefix: str = "news/site/",
    table: str = "news"
) -> str:
    print("=" * 70)
    print("📊 SYNC NEWS TO DATABASE")
    print("=" * 70)
    
    # Step 1: Find latest partition
    print("\n[1/4] Finding latest partition...")
    latest_partition = get_latest_partition(bucket, folder_prefix, minio_conn_id)
    
    if not latest_partition:
        return "❌ No partition found"
    
    # Step 2: Read all files (CSV and JSON) from latest partition
    print("\n[2/4] Reading files...")
    hook = get_minio_hook(minio_conn_id)
    keys = hook.list_keys(bucket_name=bucket, prefix=latest_partition)
    
    csv_files = [k for k in keys if k.endswith('.csv')]
    json_files = [k for k in keys if k.endswith('.json')]
    
    all_dfs = []
    
    # Process CSV files
    for csv_file in csv_files:
        print(f"Processing CSV: {csv_file}")
        df = read_csv_from_minio(bucket, csv_file, minio_conn_id)
        
        if not df.empty:
            # Process CSV format (similar to import_news_csv.py)
            df.columns = df.columns.str.lower().str.strip()
            
            # Create news_id from title if not exists
            if 'news_id' not in df.columns and 'title' in df.columns:
                df['news_id'] = df['title'].apply(
                    lambda x: hashlib.md5(str(x).encode()).hexdigest() if pd.notnull(x) else None
                )
            
            # Map columns
            column_mappings = {
                'description': 'short_content',
                'link': 'source_link',
                'image': 'image_url',
                'source': 'source_name',
                'date': 'public_date'
            }
            
            for old_col, new_col in column_mappings.items():
                if old_col in df.columns and new_col not in df.columns:
                    df.rename(columns={old_col: new_col}, inplace=True)
            
            all_dfs.append(df)
    
    # Process JSON files
    for json_file in json_files:
        print(f"Processing JSON: {json_file}")
        try:
            import json
            import io
            
            content = hook.read_key(key=json_file, bucket_name=bucket)
            if content:
                data = json.loads(content)
                
                # Process JSON format (similar to import_news_json.py)
                if isinstance(data, list):
                    records = []
                    for item in data:
                        # Convert Unix timestamp to datetime
                        pub_date = None
                        if item.get('public_date'):
                            try:
                                pub_date = datetime.fromtimestamp(item['public_date'] / 1000.0)
                            except:
                                pass
                        
                        record = {
                            'news_id': item.get('news_id'),
                            'ticker': item.get('related_ticker'),
                            'title': item.get('news_title'),
                            'sub_title': item.get('news_sub_title'),
                            'short_content': item.get('news_short_content'),
                            'full_content': item.get('news_full_content'),
                            'image_url': item.get('news_image_url'),
                            'source_link': item.get('news_source_link'),
                            'source_name': 'FiinGroup',
                            'lang_code': item.get('lang_code', 'vi'),
                            'public_date': pub_date,
                            'close_price': item.get('close_price'),
                            'ref_price': item.get('ref_price'),
                            'floor_price': item.get('floor'),
                            'ceiling_price': item.get('ceiling'),
                            'price_change_pct': item.get('price_change_pct')
                        }
                        records.append(record)
                    
                    if records:
                        df_json = pd.DataFrame(records)
                        all_dfs.append(df_json)
        except Exception as e:
            print(f"⚠️ Error processing JSON file {json_file}: {str(e)}")
    
    if not all_dfs:
        return "⚠️ No data found"
    
    # Concatenate all dataframes
    df = pd.concat(all_dfs, ignore_index=True)
    print(f"\nTotal loaded: {len(df)} rows")
    
    # Step 3: Clean and transform data
    print("\n[3/4] Cleaning and transforming data...")
    
    # Ensure required columns
    if 'news_id' not in df.columns or 'title' not in df.columns:
        return "❌ Missing required columns (news_id or title)"
    
    # Clean data
    df = df.dropna(subset=['news_id', 'title'])
    df = df.drop_duplicates(subset=['news_id'])
    
    # Parse public_date
    if 'public_date' in df.columns:
        df['public_date'] = pd.to_datetime(df['public_date'], errors='coerce')
    
    print(f"After cleaning: {len(df)} rows")
    
    if df.empty:
        return "⚠️ No data after cleaning"
    
    # Step 4: Insert into database
    print("\n[4/4] Inserting into database...")
    
    with closing(get_postgres_connection(db_url)) as conn:
        conn.autocommit = False
        
        try:
            # Ensure schema exists
            ensure_schema(conn, schema)
            
            # Prepare data for insertion
            # Schema columns: news_id, ticker, title, sub_title, short_content, full_content,
            #                 image_url, source_link, source_name, lang_code, public_date,
            #                 close_price, ref_price, floor_price, ceiling_price, price_change_pct
            
            rows = []
            for _, row in df.iterrows():
                rows.append((
                    row.get('news_id'),
                    row.get('ticker'),
                    row.get('title'),
                    row.get('sub_title'),
                    row.get('short_content'),
                    row.get('full_content'),
                    row.get('image_url'),
                    row.get('source_link'),
                    row.get('source_name'),
                    row.get('lang_code', 'vi'),
                    row.get('public_date'),
                    row.get('close_price'),
                    row.get('ref_price'),
                    row.get('floor_price'),
                    row.get('ceiling_price'),
                    row.get('price_change_pct'),
                ))
            
            # DELETE+INSERT pattern (table doesn't have unique constraint)
            news_ids = [row[0] for row in rows]
            
            with conn.cursor() as cur:
                # Delete existing records with same news_ids
                delete_sql = f"DELETE FROM {schema}.{table} WHERE news_id = ANY(%s);"
                cur.execute(delete_sql, (news_ids,))
                deleted = cur.rowcount
                print(f"Deleted {deleted} existing rows")
                
                # Insert new data
                insert_sql = f"""
                    INSERT INTO {schema}.{table}
                    (news_id, ticker, title, sub_title, short_content, full_content,
                     image_url, source_link, source_name, lang_code, public_date,
                     close_price, ref_price, floor_price, ceiling_price, price_change_pct)
                    VALUES %s;
                """
                execute_values(cur, insert_sql, rows)
                inserted = cur.rowcount
            
            conn.commit()
            print(f"✅ Inserted {inserted} new rows")
            
            return f"✅ Success: {inserted} new rows"
            
        except Exception as e:
            conn.rollback()
            print(f"❌ Error: {str(e)}")
            raise
    
    print("=" * 70)

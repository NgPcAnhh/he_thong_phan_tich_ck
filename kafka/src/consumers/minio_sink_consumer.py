import json
import logging
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io
import time
import os
from datetime import datetime, timezone, timedelta
from kafka import KafkaConsumer
from boto3 import client

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("TOPIC_STOCK_QUOTES", "market.quotes.raw")
KAFKA_GROUP_ID = os.getenv("CONSUMER_GROUP_ID", "minio-sink-group")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "12345678")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "thongtin-congty-va-bctc")

# Các hằng số điều kiện
TARGET_BATCH_SIZE = 20000
FLUSH_INTERVAL_SECONDS = 600  # 10 phút

# MinIO Client
s3_client = client(
    's3',
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    region_name='us-east-1'
)

def ensure_bucket_exists():
    try:
        s3_client.head_bucket(Bucket=MINIO_BUCKET)
    except:
        s3_client.create_bucket(Bucket=MINIO_BUCKET)
        logger.info(f"Created bucket: {MINIO_BUCKET}")

def upload_to_minio(buffer_data):
    if not buffer_data:
        return
    
    try:
        # Gom dữ liệu thông minh, xử lý cả 2 loại format
        processed_data = []
        for item in buffer_data:
            if isinstance(item, dict) and 'data' in item:
                processed_data.append(item['data']) # Dữ liệu format cũ
            elif isinstance(item, dict):
                processed_data.append(item)         # Dữ liệu phẳng mới
                
        if not processed_data:
            logger.warning("Không có dữ liệu hợp lệ để upload.")
            return

        df = pd.DataFrame(processed_data)
        
        table = pa.Table.from_pandas(df)
        parquet_buffer = io.BytesIO()
        pq.write_table(table, parquet_buffer)
        
        now = datetime.now()
        date_str = now.strftime("%Y-%m-%d") # Format yyyy-mm-dd
        
        key = f"realtime/{date_str}/quotes_{int(time.time()*1000)}.parquet"
        
        s3_client.put_object(
            Bucket=MINIO_BUCKET,
            Key=key,
            Body=parquet_buffer.getvalue()
        )
        logger.info(f"✅ ĐÃ UPLOAD THÀNH CÔNG {len(processed_data)} records lên MinIO: {key}")
    except Exception as e:
        logger.error(f"❌ Error uploading to MinIO: {e}")

def run_consumer():
    ensure_bucket_exists()
    
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset='earliest',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    
    buffer = []
    last_flush_time = time.time()
    
    # Thiết lập Múi giờ Việt Nam (UTC+7)
    vn_tz = timezone(timedelta(hours=7))
    
    logger.info(f"Starting MinIO Sink Consumer for topic: {KAFKA_TOPIC}")
    logger.info(f"Chế độ ghi file: Đợi đủ {TARGET_BATCH_SIZE} bản ghi HOẶC quá 10 phút.")
    
    try:
        while True:
            # 1. Kiểm tra giờ đóng cửa (15:00 UTC+7)
            now_vn = datetime.now(vn_tz)
            if now_vn.hour >= 15:
                logger.info("🕒 Đã 15:00 chiều (Giờ VN). Thị trường đã đóng cửa. Tự động dừng consumer...")
                break
                
            # 2. Lấy dữ liệu mỗi 1 giây (để vòng lặp không bị kẹt cứng)
            records = consumer.poll(timeout_ms=1000)
            
            for tp, messages in records.items():
                for message in messages:
                    buffer.append(message.value)
                    
                    if len(buffer) % 5000 == 0:
                        logger.info(f"Đang gom dữ liệu... ({len(buffer)} / {TARGET_BATCH_SIZE})")

            # 3. Kiểm tra 2 điều kiện tạo file Parquet
            current_time = time.time()
            time_elapsed = current_time - last_flush_time
            
            if len(buffer) >= TARGET_BATCH_SIZE or (buffer and time_elapsed >= FLUSH_INTERVAL_SECONDS):
                if len(buffer) >= TARGET_BATCH_SIZE:
                    logger.info(f"📦 Đã đạt đủ {len(buffer)} bản ghi. Đang đóng gói file...")
                else:
                    logger.info(f"⏳ Đã qua 10 phút. Đóng gói {len(buffer)} bản ghi còn lại...")
                    
                upload_to_minio(buffer)
                buffer = []
                last_flush_time = time.time()
                
    except KeyboardInterrupt:
        logger.info("Consumer stopped by user")
    finally:
        # Nếu tắt (do hết 15:00 hoặc tắt thủ công), gom nốt dữ liệu còn thừa
        if buffer:
            logger.info(f"Lưu vớt {len(buffer)} bản ghi cuối cùng vào MinIO...")
            upload_to_minio(buffer)
        consumer.close()

if __name__ == "__main__":
    run_consumer()
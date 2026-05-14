import os
from kafka import KafkaConsumer
from boto3 import client

def check_kafka():
    try:
        consumer = KafkaConsumer(bootstrap_servers='localhost:9092')
        topics = consumer.topics()
        print(f"[KAFKA] Connected successfully! Topics available: {topics}")
        if 'market.quotes.raw' in topics:
            print("[KAFKA] Topic 'market.quotes.raw' exists. OK.")
        else:
            print("[KAFKA] Topic 'market.quotes.raw' DOES NOT EXIST.")
        consumer.close()
    except Exception as e:
        print(f"[KAFKA] Failed to connect: {e}")

def check_minio():
    MINIO_ENDPOINT = "http://localhost:9000"
    MINIO_ACCESS_KEY = "admin"
    MINIO_SECRET_KEY = "12345678"
    MINIO_BUCKET = "thongtin-congty-va-bctc"

    try:
        s3_client = client(
            's3',
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
            region_name='us-east-1'
        )
        
        buckets = s3_client.list_buckets()
        bucket_names = [b['Name'] for b in buckets['Buckets']]
        print(f"[MINIO] Connected successfully! Buckets: {bucket_names}")
        
        if MINIO_BUCKET in bucket_names:
            objects = s3_client.list_objects_v2(Bucket=MINIO_BUCKET, Prefix='realtime/')
            if 'Contents' in objects:
                files = [obj['Key'] for obj in objects['Contents']]
                print(f"[MINIO] Found {len(files)} files in 'realtime/'. Data flow to MinIO is working!")
                for f in files[:5]: # print first 5
                    print(f"  -> {f}")
            else:
                print("[MINIO] Bucket exists but NO data found in 'realtime/'.")
        else:
            print(f"[MINIO] Bucket '{MINIO_BUCKET}' DOES NOT EXIST.")
    except Exception as e:
        print(f"[MINIO] Failed to connect: {e}")

if __name__ == '__main__':
    print("--- STARTING CHECK ---")
    check_kafka()
    check_minio()
    print("--- CHECK COMPLETE ---")

"""
Kafka Configuration Module
Centralized configuration for Kafka producer and consumer
"""
import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env from project root (2 levels up from this file)
env_path = Path(__file__).parent.parent.parent / ".env"
load_dotenv(env_path)

# Kafka Broker Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# Topic Names
TOPIC_STOCK_QUOTES = os.getenv("TOPIC_STOCK_QUOTES", "stock-quotes")
TOPIC_STOCK_CANDLES = os.getenv("TOPIC_STOCK_CANDLES", "stock-candles")

# Topic Configuration
TOPIC_PARTITIONS = int(os.getenv("TOPIC_PARTITIONS", "3"))
TOPIC_REPLICATION_FACTOR = int(os.getenv("TOPIC_REPLICATION_FACTOR", "1"))

# Producer Configuration
PRODUCER_CONFIG = {
    'bootstrap_servers': KAFKA_BOOTSTRAP_SERVERS,
    'value_serializer': lambda v: v.encode('utf-8') if isinstance(v, str) else v,
    'key_serializer': lambda k: k.encode('utf-8') if isinstance(k, str) else None,
    'acks': 'all',  # Wait for all replicas to acknowledge
    'retries': 3,
    'batch_size': int(os.getenv("PRODUCER_BATCH_SIZE", "16384")),
    'linger_ms': int(os.getenv("PRODUCER_LINGER_MS", "10")),
    'compression_type': 'gzip',  # Changed from snappy to gzip (built-in)
    'max_in_flight_requests_per_connection': int(os.getenv("PRODUCER_MAX_IN_FLIGHT_REQUESTS", "5")),
}

# Consumer Configuration
CONSUMER_CONFIG = {
    'bootstrap_servers': KAFKA_BOOTSTRAP_SERVERS,
    'group_id': os.getenv("CONSUMER_GROUP_ID", "stock-data-consumer-group"),
    'auto_offset_reset': os.getenv("CONSUMER_AUTO_OFFSET_RESET", "earliest"),
    'enable_auto_commit': os.getenv("CONSUMER_ENABLE_AUTO_COMMIT", "true").lower() == "true",
    'max_poll_records': int(os.getenv("CONSUMER_MAX_POLL_RECORDS", "500")),
    'value_deserializer': lambda m: m.decode('utf-8') if m else None,
}

# WebSocket Configuration
WEBSOCKET_URL = os.getenv("WEBSOCKET_URL", "wss://stream2.simplize.vn/ws")
SYMBOL_LIMIT = int(os.getenv("SYMBOL_LIMIT", "2000"))
SUB_BATCH_SIZE = int(os.getenv("SUB_BATCH_SIZE", "500"))

# Database Configuration
# Use dwh-postgres for Docker deployment, localhost for local development
DB_HOST = os.getenv("DB_HOST", "dwh-postgres")
DB_DSN = os.getenv("DB_DSN", f"postgresql://admin:123456@{DB_HOST}:5432/postgres")
DB_SCHEMA = os.getenv("DB_SCHEMA", "hethong_phantich_chungkhoan")
DB_BATCH_SIZE = 2000

# WebSocket Headers
WEBSOCKET_HEADERS = {
    "Origin": "https://simplize.vn",
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"
    ),
}

# Index Codes
INDEX_CODES = [
    "HNX30", "HNXINDEX", "HNXUPCOMINDEX", "VN100", "VN30", "VNALL", "VNCOND", 
    "VNCONS", "VNDIAMOND", "VNENE", "VNFIN", "VNFINLEAD", "VNFINSELECT", 
    "VNHEAL", "VNIND", "VNINDEX", "VNIT", "VNMAT", "VNMID", "VNREAL", 
    "VNSI", "VNSML", "VNUTI", "VNX50", "VNXALL"
]

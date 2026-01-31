# Kafka Real-time Stock Data Streaming System

Hệ thống streaming dữ liệu chứng khoán real-time sử dụng Apache Kafka để thu thập và xử lý dữ liệu từ WebSocket Simplize.

## 📋 Mục lục

- [Kiến trúc hệ thống](#kiến-trúc-hệ-thống)
- [Cài đặt](#cài-đặt)
- [Cấu hình](#cấu-hình)
- [Sử dụng](#sử-dụng)
- [Monitoring](#monitoring)
- [Troubleshooting](#troubleshooting)

## 🏗️ Kiến trúc hệ thống

```
┌─────────────────┐
│ Simplize        │
│ WebSocket       │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Kafka Producer  │
│ (Python)        │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Kafka Broker    │
│ (Docker)        │
└────────┬────────┘
         │
    ┌────┴────┐
    ▼         ▼
┌────────┐ ┌────────┐
│ Topic: │ │ Topic: │
│ quotes │ │candles │
└───┬────┘ └───┬────┘
    │          │
    └────┬─────┘
         ▼
┌─────────────────┐
│ Kafka Consumer  │
│ (Python)        │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ PostgreSQL      │
│ Database        │
└─────────────────┘
```

## 📦 Cài đặt

### 1. Cài đặt Docker Desktop

Tải và cài đặt Docker Desktop từ: https://www.docker.com/products/docker-desktop

### 2. Clone hoặc tạo thư mục dự án

```bash
cd d:\project\phantich_chungkhoan\kafka
```

### 3. Cài đặt Python dependencies

```bash
pip install -r requirements.txt
```

## ⚙️ Cấu hình

### 1. Cấu hình môi trường

Chỉnh sửa file `.env` để cấu hình các thông số:

```env
# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=localhost:9092

# Topics
TOPIC_STOCK_QUOTES=stock-quotes
TOPIC_STOCK_CANDLES=stock-candles

# Database
DB_DSN=postgresql://user:password@localhost:5432/database_name
```

### 2. Kafka Topics

Hệ thống sử dụng 2 topics chính:

#### **stock-quotes**
- Dữ liệu real-time về giá, khối lượng, bid/ask
- Partition: 3
- Retention: 7 ngày
- Key: Symbol (mã chứng khoán)

#### **stock-candles**
- Dữ liệu nến 1 phút (OHLCV)
- Partition: 3
- Retention: 7 ngày
- Key: Symbol (mã chứng khoán)

## 🚀 Sử dụng

### Bước 1: Khởi động Kafka Cluster

```bash
# Di chuyển vào thư mục kafka
cd d:\project\phantich_chungkhoan\kafka

# Khởi động Docker containers
docker-compose up -d

# Kiểm tra trạng thái
docker-compose ps
```

Kết quả mong đợi:
```
NAME         IMAGE                              STATUS
kafka        confluentinc/cp-kafka:7.5.0       Up
kafka-ui     provectuslabs/kafka-ui:latest     Up
zookeeper    confluentinc/cp-zookeeper:7.5.0   Up
```

### Bước 2: Kiểm tra Kafka hoạt động

Truy cập Kafka UI tại: **http://localhost:8081**

Bạn sẽ thấy:
- Kafka cluster "local"
- Brokers đang chạy
- Topics (nếu đã được tạo)

### Bước 3: Tạo Topics (Tùy chọn)

Topics sẽ được tự động tạo khi producer gửi message lần đầu. Nếu muốn tạo thủ công:

```bash
# Tạo topic stock-quotes
docker-compose exec kafka kafka-topics --create \
  --topic stock-quotes \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1

# Tạo topic stock-candles
docker-compose exec kafka kafka-topics --create \
  --topic stock-candles \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1

# Liệt kê tất cả topics
docker-compose exec kafka kafka-topics --list \
  --bootstrap-server localhost:9092
```

### Bước 4: Chạy Kafka Producer

Producer sẽ kết nối WebSocket và gửi dữ liệu vào Kafka:

```bash
# Chạy producer
python kafka_producer.py
```

Bạn sẽ thấy log:
```
🔎 Loaded 2000 symbols (examples: ['AAA', 'AAM', 'AAS', ...])
🔧 Initializing Kafka producers...
✅ Kafka producers initialized
✅ Connected to Simplize WebSocket
📦 Subscribing to 2000 symbols (batch size = 500)
📤 Sending subscription batch 1 (500 symbols)
...
📊 QUOTES (received 150 symbols)
```

### Bước 5: Kiểm tra dữ liệu trong Kafka

#### Cách 1: Sử dụng Kafka UI
1. Mở http://localhost:8080
2. Click vào topic `stock-quotes` hoặc `stock-candles`
3. Xem Messages tab

#### Cách 2: Sử dụng Console Consumer

```bash
# Xem messages từ topic stock-quotes
docker-compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic stock-quotes \
  --from-beginning \
  --max-messages 10

# Xem messages từ topic stock-candles
docker-compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic stock-candles \
  --from-beginning \
  --max-messages 10
```

### Bước 6: Chạy Kafka Consumer (Tùy chọn)

Consumer sẽ đọc dữ liệu từ Kafka và ghi vào PostgreSQL:

```bash
# Chạy consumer
python kafka_consumer_db.py
```

Bạn sẽ thấy log:
```
🔧 Initializing database connection pool...
✅ Database pool initialized
🔧 Initializing Kafka consumers...
✅ Kafka consumers initialized
📥 Consuming from topics: stock-quotes, stock-candles
💾 realtime_quotes: inserted/updated 500 records
💾 candles_1m: inserted/updated 200 candles
```

## 📊 Monitoring

### 1. Kafka UI Dashboard

Truy cập: **http://localhost:8080**

Theo dõi:
- **Brokers**: Trạng thái broker
- **Topics**: Số lượng messages, partitions
- **Consumers**: Consumer groups, lag
- **Messages**: Xem nội dung messages

### 2. Docker Logs

```bash
# Xem logs của Kafka broker
docker-compose logs -f kafka

# Xem logs của Zookeeper
docker-compose logs -f zookeeper

# Xem logs của Kafka UI
docker-compose logs -f kafka-ui
```

### 3. Kafka Metrics

```bash
# Kiểm tra consumer group lag
docker-compose exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe \
  --group stock-data-consumer-group
```

## 🔧 Troubleshooting

### Lỗi: "Connection refused" khi chạy producer

**Nguyên nhân**: Kafka chưa khởi động hoặc chưa sẵn sàng

**Giải pháp**:
```bash
# Kiểm tra trạng thái containers
docker-compose ps

# Khởi động lại nếu cần
docker-compose restart kafka

# Đợi Kafka sẵn sàng (khoảng 30 giây)
docker-compose logs -f kafka | grep "started"
```

### Lỗi: "Topic not found"

**Nguyên nhân**: Topic chưa được tạo

**Giải pháp**:
```bash
# Tạo topic thủ công
docker-compose exec kafka kafka-topics --create \
  --topic stock-quotes \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1
```

### Lỗi: Producer gửi message chậm

**Nguyên nhân**: Cấu hình batch size hoặc linger.ms không tối ưu

**Giải pháp**: Chỉnh sửa trong `.env`:
```env
PRODUCER_BATCH_SIZE=32768  # Tăng batch size
PRODUCER_LINGER_MS=50      # Tăng linger time
```

### Lỗi: Consumer lag cao

**Nguyên nhân**: Consumer xử lý chậm hơn producer

**Giải pháp**:
1. Tăng số lượng partitions
2. Chạy nhiều consumer instances
3. Tối ưu hóa database writes (batch size lớn hơn)

### Dừng và xóa toàn bộ hệ thống

```bash
# Dừng containers
docker-compose down

# Dừng và xóa volumes (XÓA DỮ LIỆU)
docker-compose down -v
```

## 📝 Cấu trúc dữ liệu

### Quote Message Format

```json
{
  "symbol": "VNM",
  "ts": 1705324800000,
  "timestamp_iso": "2024-01-15T14:00:00",
  "is_index": false,
  "last_price": 85500,
  "avg_price": 85300,
  "total_volume": 1250000,
  "bid1_price": 85400,
  "bid1_qty": 5000,
  "ask1_price": 85500,
  "ask1_qty": 3000,
  "change_percent": 2.5,
  "high_price": 86000,
  "low_price": 84500
}
```

### Candle Message Format

```json
{
  "symbol": "VNM",
  "bucket_time": "2024-01-15T14:00:00",
  "open_price": 85000,
  "high_price": 86000,
  "low_price": 84500,
  "close_price": 85500,
  "volume": 125000
}
```

## 🎯 Lợi ích của kiến trúc Kafka

1. **Decoupling**: Producer và Consumer độc lập
2. **Scalability**: Dễ dàng scale horizontal
3. **Reliability**: Message persistence, không mất dữ liệu
4. **Flexibility**: Thêm consumer mới không ảnh hưởng producer
5. **Replay**: Có thể replay dữ liệu lịch sử
6. **Performance**: Throughput cao, latency thấp

## 📞 Hỗ trợ

Nếu gặp vấn đề, kiểm tra:
1. Docker Desktop đang chạy
2. Ports 2181, 9092, 8080 không bị chiếm
3. File `.env` cấu hình đúng
4. Database connection string chính xác

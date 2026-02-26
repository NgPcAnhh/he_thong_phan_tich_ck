# Real-time Stock Data Streaming với Apache Kafka

Hệ thống streaming dữ liệu chứng khoán real-time sử dụng Apache Kafka để thu thập và xử lý dữ liệu từ WebSocket Simplize, sau đó lưu trữ kết quả thẳng vào PostgreSQL.

## 📋 Mục lục

- [Kiến trúc và Luồng dữ liệu](#kiến-trúc-và-luồng-dữ-liệu)
- [Cấu trúc thư mục](#cấu-trúc-thư-mục)
- [Cài đặt và Cấu hình](#cài-đặt-và-cấu-hình)
- [Hướng dẫn sử dụng (Quick Start)](#hướng-dẫn-sử-dụng-quick-start)
- [Cấu trúc Message](#cấu-trúc-message)
- [Monitoring & Troubleshooting](#monitoring--troubleshooting)

---

## 🏗️ Kiến trúc và Luồng dữ liệu (Data Flow)

Hệ thống được thiết kế theo mô hình **Producer - Message Broker - Consumer**, chi tiết hoạt động với 2 chế độ (Modes):

### Sơ đồ luồng dữ liệu

```text
┌─────────────────────┐
│  DEMO MODE          │
│  kafka_producer_    │
│  fake.py            │──┐
└─────────────────────┘  │
                         │
┌─────────────────────┐  │      ┌──────────────┐
│  PRODUCTION MODE    │  │      │  Kafka       │
│  kafka_producer.py  │──┼─────▶│  Topic:      │
│  (Simplize WS)      │  │      │  stock-      │
└─────────────────────┘  │      │  quotes      │
                         │      └──────┬───────┘
                         │             │
                                       ▼
                              ┌────────────────┐
                              │ Kafka Consumer │
                              │ kafka_         │
                              │ consumer_db.py │
                              └────────┬───────┘
                                       │
                                       ▼
                              ┌────────────────┐
                              │  PostgreSQL    │
                              │  realtime_     │
                              │  quotes        │
                              └────────────────┘
```

### Giải thích chi tiết luồng xử lý:

1. **Nguồn cấp dữ liệu (Producers)**
   - **Production Mode (`kafka_producer.py`)**: Giao tiếp trực tiếp với Simplize WebSocket để lắng nghe sự thay đổi giá cổ phiếu (quotes), khối lượng, bid/ask theo thời gian thực. Sau đó, ứng dụng sẽ format dữ liệu thành chuẩn JSON và publish (đẩy) liên tục vào topic Kafka. (Lưu ý mode này chỉ có dữ liệu trong giờ giao dịch 9:00 - 15:00, T2-T6).
   - **Demo Mode (`kafka_producer_fake.py`)**: Sinh ra dữ liệu ngẫu nhiên (fake quotes) để phục vụ cho việc test logic hệ thống và development mà không cần đợi giờ giao dịch thực tế. Dữ liệu giả lập cũng được đẩy vào topic Kafka tương tự như hàng thật.

2. **Message Broker (Apache Kafka cluster)**
   - Đóng vai trò là ống dẫn và bộ đệm (buffer) ở giữa. Nhận message từ cả 2 loại Producer phía trên.
   - Dữ liệu được tổ chức theo các **Topics**:
     - `stock-quotes`: Chứa dữ liệu giá chứng khoán realtime (Tick data).
     - `stock-candles`: Chứa dữ liệu nến 1 phút (ví dụ OHLCV).
   - Kafka đảm bảo tính bền vững (persistence), khả năng chống lỗi, chia partition để tăng tốc độ xử lý song song, cũng như cho phép nhiều Consumers khác nhau lấy cùng một luồng dữ liệu mà không ảnh hưởng tới luồng gửi của Producer (Decoupling). Dữ liệu sau khi xử lý hay lưu trữ vẫn có thể xem lại trong hạn mức lưu trữ (retention policy).

3. **Tiêu thụ dữ liệu (Consumer)**
   - **Kafka Consumer (`kafka_consumer_db.py`)**: Liên tục subscribe (hiện diện/lắng nghe) vào topic `stock-quotes` và `stock-candles` thông qua một consumer group (`stock-data-consumer-group`).
   - Ngay khi có message mới đến, consumer sẽ liên tục thực hiện:
     - Đọc data thô, Deserialize file JSON ra Dictionary/Object.
     - Tập hợp các objects lại tạo thành một batch dữ liệu (tối ưu hóa lượt I/O gọi xuống Database).
     - Kết nối tới PostgreSQL bằng Database Connection Pool.
     - Đẩy nguyên lô (batch insert/upsert) để lưu trữ hiệu quả dữ liệu lịch sử vào bảng `realtime_quotes`.

---

## 📁 Cấu trúc thư mục

```text
kafka/
├── src/                          # Source code
│   ├── producers/                # Đầu vào - Kafka producers (Data Ingestion)
│   │   ├── kafka_producer.py     # Real WebSocket producer
│   │   └── kafka_producer_fake.py# Fake data producer  
│   ├── consumers/                # Đầu ra - Kafka consumers 
│   │   └── kafka_consumer_db.py  # Consumer ghi vào database PostgreSQL
│   └── common/                   # Shared utilities
│       └── config.py             # File Configuration system
├── db/                           # Database scripts
│   ├── migrations/               # SQL schema/migrations
│   │   └── init_realtime_quotes.sql
│   └── scripts/                  
├── tests/                        # Source tests 
├── .env                          # Biến môi trường
├── docker-compose.yml            # Docker Config cho Kafka broker & UI tool
└── requirements.txt              # Thư viện Python yêu cầu
```

---

## ⚙️ Cài đặt và Cấu hình

### 1. Cài đặt cơ bản
Yêu cầu đã cài đặt **Docker Desktop** và **Python 3.8+**.

```bash
# Di chuyển vào thư mục dự án
cd d:\project\lakehouse_ptich_ck\kafka

# Cài đặt Python dependencies
pip install -r requirements.txt
```

### 2. Cấu hình môi trường (.env)
Chỉnh sửa file `.env` nếu cần thiết (host, tài khoản DB, tên topics):
```env
# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=localhost:9092

# Topics
TOPIC_STOCK_QUOTES=stock-quotes
TOPIC_STOCK_CANDLES=stock-candles

# Database Connection String
DB_DSN=postgresql://user:password@localhost:5432/database_name
```

---

## 🚀 Hướng dẫn sử dụng (Quick Start)

### Bước 1: Setup Cơ sở dữ liệu
Khởi tạo bảng `realtime_quotes` trên DB PostgreSQL:
```bash
psql -U postgres -d stock_database_vn -f db/migrations/init_realtime_quotes.sql
```

### Bước 2: Khởi động hệ thống Kafka (Cluster)
Kafka và Zookeeper sẽ được chạy bằng Docker Compose:
```bash
cd d:\project\lakehouse_ptich_ck\kafka
docker-compose up -d

# Xác minh các container đã chạy (Status "Up")
docker-compose ps
```
Quản lý trực quan Kafka qua UI tại: **http://localhost:8081** (đôi khi port là 8080 tùy config compose).

### Bước 3: Chạy Kafka Consumer
Mở một terminal chuyên dụng, chạy script consumer liên tục để ghi vào Data Warehouse:
```bash
cd d:\project\lakehouse_ptich_ck\kafka
python -m src.consumers.kafka_consumer_db
```
*(Nếu thành công, log sẽ báo: Lắng nghe topic thành công, khởi tạo pool thành công...)*

### Bước 4: Chạy Kafka Producer

**Tùy chọn A: DEMO MODE (Kết nối Test) - Phù hợp debug, code**
Mở terminal tiếp theo và sinh dữ liệu giả:
```bash
cd d:\project\lakehouse_ptich_ck\kafka
python -m src.producers.kafka_producer_fake
```

**Tùy chọn B: PRODUCTION MODE (Kết nối Thật)**
Nếu trong phiên giao dịch (sau 9h sáng), dừng producer cũ, gõ lệnh:
```bash
cd d:\project\lakehouse_ptich_ck\kafka
python -m src.producers.kafka_producer
```

### Bước 5: Kiểm chứng luồng

Vào PgAdmin / tool DBeaver / terminal DB và chạy:
```sql
-- Xem record mới
SELECT symbol, ts, last_price, total_volume 
FROM realtime_quotes 
ORDER BY ts DESC 
LIMIT 10;

-- Theo dõi độ trễ của Pipeline (Freshness)
SELECT symbol, ts, last_price, NOW() - ts AS age
FROM realtime_quotes 
ORDER BY ts DESC 
LIMIT 5;
```

---

## 📝 Cấu trúc Message tiêu chuẩn

**Ví dụ Message tại `stock-quotes` topic:**
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

---

## 🔍 Monitoring & Troubleshooting

### 1. Monitoring
Truy cập giao diện Kafka trên **http://localhost:8081**. Bạn có thể nhìn thấy message đi vào Kafka Cluster.

### 2. Kiểm tra độ lag dữ liệu
Đảm bảo Consumer đọc nhanh hơn Produce bằng cách phân tích độ trễ.
```bash
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe \
  --group stock-data-consumer-group
```
**Bình thường:** Cột `LAG` phải ở con số rất nhỏ (< 1000). Nếu tăng vọt, hãy nghĩ tới việc tăng memory cho Python consumer hoặc tăng chunk DB_BATCH_SIZE cấu hình.

### 3. Giải quyết sự cố thường gặp
- **❌ Error "Connection refused" ở màn Consumer**: Kafka Broker chưa boot lên hẳn (startup mất khoảng 15~30s). Hãy `docker-compose restart kafka` và đợi.
- **❌ Error "relation realtime_quotes does not exist"**: Consumer gọi xuống DB lỗi schema. Bạn đã quên thực thi script init ở Bước 1. 

### Shutdown sạch sẽ hệ thống
Khi code xong, giải phóng Port và RAM:
```bash
cd d:\project\lakehouse_ptich_ck\kafka
docker-compose down

# 🔥 THẬN TRỌNG: Câu lệnh này XÓA HOÀN TOÀN TOPICS VÀ DỮ LIỆU BÊN TRONG KAFKA.
docker-compose down -v
```

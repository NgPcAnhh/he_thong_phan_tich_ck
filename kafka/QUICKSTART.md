# Real-time Stock Data Streaming - Quick Start Guide

## 📋 Tổng quan

Hệ thống streaming dữ liệu chứng khoán real-time với **2 modes**:
- **DEMO MODE**: Fake producer tạo dữ liệu test
- **PRODUCTION MODE**: Real socket từ Simplize WebSocket

## 🚀 Cách sử dụng

### 1️⃣ Setup Database

Tạo bảng `realtime_quotes`:

```bash
psql -U postgres -d stock_database_vn -f init_realtime_quotes.sql
```

**Verify**:
```sql
\d realtime_quotes
```

---

### 2️⃣ Start Kafka Cluster

```bash
cd d:\project\lakehouse_ptich_ck\kafka
docker-compose up -d

# Verify Kafka running
docker-compose ps
```

**Kafka UI**: http://localhost:8081

---

### 3️⃣ DEMO MODE - Test với Fake Data

#### Terminal 1: Consumer (luôn chạy)
```bash
cd d:\project\lakehouse_ptich_ck\kafka
python kafka_consumer_db.py
```

Logs mong đợi:
```
🔧 Initializing database connection pool...
✅ Database pool initialized
📥 Consuming from topics: stock-quotes, stock-candles
```

#### Terminal 2: Fake Producer
```bash
cd d:\project\lakehouse_ptich_ck\kafka
python kafka_producer_fake.py
```

Logs mong đợi:
```
🔎 Loaded 500 symbols
🔧 Initializing Kafka producer...
✅ Kafka producer initialized
📤 Iteration 1: Sent 100 fake quotes to Kafka
```

#### Verify Data

**Kafka UI** (http://localhost:8081):
- Click topic: `stock-quotes`
- Tab: Messages
- Should see JSON messages với fake data

**Database**:
```sql
-- Latest 10 records
SELECT symbol, ts, last_price, total_volume 
FROM realtime_quotes 
ORDER BY ts DESC 
LIMIT 10;

-- Count records
SELECT COUNT(*) FROM realtime_quotes;

-- Check data freshness
SELECT 
    symbol, 
    ts,
    last_price,
    NOW() - ts AS age
FROM realtime_quotes 
ORDER BY ts DESC 
LIMIT 5;
```

---

### 4️⃣ PRODUCTION MODE - Real Socket

#### Stop fake producer
```bash
# Press Ctrl+C in Terminal 2
```

#### Start real producer
```bash
cd d:\project\lakehouse_ptich_ck\kafka
python kafka_producer.py
```

Logs mong đợi:
```
✅ Connected to Simplize WebSocket
📊 QUOTES (received 150 symbols)
```

**Lưu ý**: Real producer chỉ hoạt động trong **giờ giao dịch** (9:00-15:00, T2-T6).

---

## 🔍 Monitoring & Troubleshooting

### Check Kafka Consumer Lag

```bash
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe \
  --group stock-data-consumer-group
```

**Expected**: LAG < 1000

### Check Database Performance

```sql
-- Table size
SELECT pg_size_pretty(pg_total_relation_size('realtime_quotes'));

-- Records per symbol
SELECT symbol, COUNT(*) 
FROM realtime_quotes 
GROUP BY symbol 
ORDER BY COUNT(*) DESC 
LIMIT 10;

-- Time range
SELECT 
    MIN(ts) AS earliest,
    MAX(ts) AS latest,
    MAX(ts) - MIN(ts) AS time_span
FROM realtime_quotes;
```

### Common Issues

#### ❌ Error: "Connection refused" (Kafka)
**Fix**: Kafka chưa khởi động
```bash
docker-compose restart kafka
# Đợi 30 giây
```

#### ❌ Error: "relation realtime_quotes does not exist"
**Fix**: Chưa chạy init script
```bash
psql -U postgres -d stock_database_vn -f init_realtime_quotes.sql
```

#### ❌ Consumer không nhận messages
**Fix**: Check Kafka topic exists
```bash
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092
```

---

## 📊 Data Flow Architecture

```
┌─────────────────────┐
│  DEMO MODE          │
│  kafka_producer_    │  
│  fake.py           │──┐
└─────────────────────┘  │
                         │
┌─────────────────────┐  │      ┌──────────────┐
│  PRODUCTION MODE    │  │      │  Kafka       │
│  kafka_producer.py  │──┼─────▶│  Topic:      │
│  (Simplize WS)     │  │      │  stock-      │
└─────────────────────┘  │      │  quotes      │
                         │      └──────┬───────┘
                         │             │
                                       ▼
                              ┌────────────────┐
                              │  kafka_        │
                              │  consumer_     │
                              │  db.py         │
                              └────────┬───────┘
                                       │
                                       ▼
                              ┌────────────────┐
                              │  PostgreSQL    │
                              │  realtime_     │
                              │  quotes        │
                              └────────────────┘
```

---

## ⚙️ Configuration

### Fake Producer Settings

Edit `kafka_producer_fake.py`:

```python
# Line ~180: Adjust parameters
await fake_streaming_loop(
    syms, 
    interval=2.0,      # Seconds between batches
    batch_size=100     # Symbols per batch
)
```

### Database Batch Size

Edit `config.py`:

```python
DB_BATCH_SIZE = 2000  # Increase for better performance
```

---

## 🧪 Testing Checklist

- [ ] Kafka cluster running (docker-compose ps)
- [ ] Database table exists (\d realtime_quotes)
- [ ] Consumer started and waiting
- [ ] Fake producer sending data
- [ ] Kafka UI shows messages
- [ ] Database có records mới
- [ ] No errors in consumer logs

---

## 🛑 Shutdown

```bash
# Stop producers (Ctrl+C)
# Stop consumer (Ctrl+C)

# Stop Kafka
cd d:\project\lakehouse_ptich_ck\kafka
docker-compose down

# Optional: Delete Kafka data (⚠️ XÓA DỮ LIỆU)
docker-compose down -v
```

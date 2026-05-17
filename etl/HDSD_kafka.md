# Tài Liệu Hướng Dẫn & Phân Tích Kiến Trúc Pipeline Realtime (Socket -> Kafka -> MinIO)

Tài liệu này không chỉ hướng dẫn cách vận hành mà còn giải thích chi tiết về thiết kế, luồng dữ liệu và vai trò của từng thành phần trong hệ thống Data Pipeline realtime của dự án.

---

## 1. Kiến Trúc Tổng Quan Hệ Thống (System Architecture)

Hệ thống được thiết kế theo mô hình **Event-driven Architecture** kết hợp với **Data Lakehouse**, chuyên trị việc xử lý dữ liệu streaming với độ trễ thấp (low-latency) và lưu trữ mở rộng (scalable storage).

```mermaid
graph LR
    subgraph External Source
        WS[WebSocket API\nstream2.simplize.vn]
    end

    subgraph Docker Container: realtime-producer
        P[Python Producer\nsubscribe & fetch]
    end

    subgraph Kafka Cluster
        Z[Zookeeper] --- K[Kafka Broker\nTopic: market.quotes.raw]
        UI[Kafka UI\nQuản lý] -.-> K
    end

    subgraph Docker Container: minio-sink-consumer
        C[Python Consumer\nBuffer & Convert]
    end

    subgraph Data Lake (Local)
        M[(MinIO Storage\nbucket: thongtin-...)]
    end

    WS -- JSON Stream --> P
    P -- JSON Message --> K
    K -- Pull Data --> C
    C -- Parquet Files --> M
```

---

## 2. Phân Tích Chi Tiết Các Thành Phần Thiết Kế

### 2.1. Nguồn Dữ Liệu (WebSocket)
- **Thiết kế:** Dữ liệu chứng khoán thay đổi liên tục, do đó việc sử dụng WebSocket (thay vì REST API polling) giúp nhận dữ liệu đẩy (push) ngay lập tức khi có giao dịch mới mà không làm nghẽn mạng.
- **Payload:** Dữ liệu trả về ở định dạng JSON thô (raw).

### 2.2. Realtime Producer (`src/producers/realtime_producer.py`)
- **Vai trò:** Đóng vai trò là cầu nối (Adapter) giữa External WebSocket và Hệ thống nội bộ.
- **Thiết kế:** 
  - Khởi tạo danh sách các mã chứng khoán (Symbols) từ thư viện `vnstock` và gửi bản tin Subscribe lên WebSocket.
  - Sử dụng bất đồng bộ (`asyncio` & `websockets`) để lắng nghe luồng stream liên tục mà không bị blocking.
  - Mỗi bản ghi nhận được sẽ được thêm trường `ingested_at` (thời gian hệ thống nhận dữ liệu) để phục vụ cho việc tracking/auditing sau này.
  - Dữ liệu được serialize thành chuỗi byte (UTF-8 JSON) và đẩy vào Kafka topic `market.quotes.raw` với `key` là mã cổ phiếu (giúp đảm bảo thứ tự message của cùng một mã cổ phiếu nếu cần thiết).

### 2.3. Hệ Sinh Thái Kafka (Message Broker)
- **Zookeeper:** Quản lý cấu hình, trạng thái cluster và bầu chọn leader cho Kafka.
- **Kafka Broker:** Lưu trữ tạm thời (buffer) dữ liệu streaming. 
  - **Topic `market.quotes.raw`:** Thiết kế để chứa dữ liệu thô (Bronze Layer trong mô hình Medallion).
  - Giúp **Decoupling** (Tách rời) giữa hệ thống sinh dữ liệu (Producer) và hệ thống xử lý/lưu trữ (Consumer). Nếu MinIO chết, dữ liệu vẫn được lưu trữ an toàn trong Kafka (cấu hình hiện tại giữ log trong 7 ngày) và không bị mất đi.
- **Kafka-UI:** Giao diện trực quan để monitor sức khỏe broker, xem cấu trúc topic và dữ liệu thực tế đang chạy qua.

### 2.4. MinIO Sink Consumer (`src/consumers/minio_sink_consumer.py`)
- **Vai trò:** Hút dữ liệu từ Kafka và đưa vào kho lưu trữ vĩnh viễn (Data Lake).
- **Thiết kế (Micro-batching):**
  - Không ghi từng dòng (row) vào MinIO vì việc ghi quá nhiều file nhỏ (small files problem) sẽ làm hỏng hiệu năng của Data Lake/HDFS.
  - Sử dụng cơ chế buffer trong bộ nhớ: Gộp dữ liệu lại cho đến khi đạt `BATCH_SIZE` (ví dụ: 50 tin nhắn) HOẶC đạt giới hạn thời gian `FLUSH_INTERVAL` (ví dụ: 30 giây).
  - **Chuyển đổi định dạng (Format Conversion):** Khi xả (flush) buffer, hệ thống chuyển đổi JSON sang định dạng **Apache Parquet** thông qua `Pandas` và `PyArrow`.
    - *Tại sao là Parquet?* Định dạng cột (Columnar format) này nén rất tốt, tiết kiệm dung lượng và cực kỳ tối ưu cho các Engine truy vấn phân tích (như Spark, Trino) để đọc sau này.
  - **Phân vùng dữ liệu (Data Partitioning):** File parquet được đẩy lên MinIO theo cấu trúc đường dẫn phân vùng thời gian thực: `realtime/YYYY/MM/DD/HH/quotes_YYYYMMDD_HHMMSS.parquet`. Thiết kế này giúp các truy vấn trong tương lai chỉ cần đọc đúng thư mục của giờ/ngày cần thiết thay vì scan toàn bộ hệ thống.

---

## 3. Hướng Dẫn Vận Hành Tự Động

Bạn không cần thao tác tay từng bước, toàn bộ kiến trúc trên đã được "đóng gói" hoàn chỉnh trong `docker-compose.yml`. Chỉ cần bật Docker, luồng sẽ tự động kết nối và chảy dữ liệu.

### Bước 1: Yêu cầu bắt buộc
Đảm bảo **MinIO server** trên máy host của bạn (`localhost:9000`) ĐÃ ĐƯỢC BẬT, vì Consumer sẽ kết nối thẳng vào đó thông qua `host.docker.internal`.

### Bước 2: Khởi động hệ thống
Mở terminal, di chuyển vào thư mục dự án và chạy:
```bash
cd d:\project\lakehouse_ptich_ck\kafka
docker-compose up -d
```
Lệnh này kích hoạt cả Zookeeper, Kafka, Producer và Consumer cùng lúc chạy ngầm.

### Bước 3: Kiểm tra luồng dữ liệu

**Cách 1: Trực quan qua Giao diện Web**
- **Kafka (Broker):** Vào [http://localhost:8081](http://localhost:8081). Chọn Topics -> `market.quotes.raw` -> Messages. Bạn sẽ thấy dòng chảy JSON nhảy liên tục.
- **MinIO (Lake):** Mở trình duyệt vào MinIO Console, đăng nhập và xem bucket `thongtin-congty-va-bctc`. Kiểm tra thư mục `realtime/` sẽ thấy các file `.parquet` dần xuất hiện (sau mỗi 30s hoặc 50 tin nhắn).

**Cách 2: Script tự động (Python)**
Chạy script kiểm tra trong môi trường ảo:
```bash
..\.venv\Scripts\python.exe check_status.py
```
Script sẽ tự động báo cáo kết nối và đếm số file đã được sinh ra.

**Cách 3: Debug Log**
Nếu thấy lỗi, kiểm tra log của các script chạy ngầm:
```bash
docker logs -f realtime-producer
docker logs -f minio-sink-consumer
```

### Bước 4: Tắt hệ thống
Khi kết thúc phiên làm việc:
```bash
docker-compose down
```
Kafka và Zookeeper sẽ tắt, nhưng dữ liệu trong MinIO và dữ liệu buffer của Kafka vẫn được giữ lại trong volume.

# 📈 Stock Analysis System - Data Pipeline

Hệ thống Data Pipeline tự động phục vụ cho Dự án Phân tích Chứng khoán Việt Nam. Hệ thống kết hợp kiến trúc **Lambda Architecture** xử lý dữ liệu lớn (Realtime Streaming & Batch Processing) và mô hình **Data Lakehouse** hiện đại để lưu trữ, chuẩn hóa dữ liệu tài chính trước khi đưa vào kho dữ liệu Data Warehouse (DWH).

---

## 🏛️ 1. Kiến Trúc Tổng Quan (System Architecture)

Hệ thống được tổ chức thành 3 tầng dữ liệu chính theo mô hình Medallion Lakehouse (Bronze -> Silver -> Gold) kết hợp hai cơ chế xử lý dữ liệu lớn (Batch & Real-time Streaming):

![Kiến trúc hệ thống](assets/system_architecture.png)

---

## 🌊 2. Luồng Dữ Liệu Chi Tiết (Detailed Sub-pipelines)

### 🌊 2.1. Luồng Dữ Liệu Real-time qua Apache Kafka (Bronze Layer)

Luồng streaming được thiết kế theo mô hình **Event-driven** kết hợp **Micro-batching** để tối ưu hóa hiệu năng ghi đĩa và giải quyết triệt để bài toán file nhỏ (Small Files Problem) trên hồ dữ liệu MinIO:

![Luồng xử lý Kafka Real-time](assets/kafka_flow.png)

#### Các Bước Vận Hành Luồng Real-time:
1.  **Simplize WebSocket Client:** Container `realtime-producer` lắng nghe sự kiện luồng dữ liệu chứng khoán thời gian thực từ API `stream2.simplize.vn`.
2.  **Làm giàu dữ liệu & Đẩy vào Topic:** Python Producer bổ sung trường thời gian ghi nhận `ingested_at` để phục vụ đối soát, sau đó serialize dữ liệu JSON thành bytes và đẩy vào topic `market.quotes.raw` trong cụm **Apache Kafka Broker** (được quản lý bởi **Zookeeper**). Stock Symbol được sử dụng làm Partition Key để đảm bảo tính thứ tự của dữ liệu giao dịch trên từng mã cổ phiếu.
3.  **Hút & Gom Cụm dữ liệu (Micro-batching):** Container `minio-sink-consumer` sử dụng cơ chế kéo (pull) không đồng bộ từ Kafka. Thay vì ghi ngay lập tức lên MinIO (gây quá tải request I/O), Consumer duy trì một bộ nhớ đệm (in-memory buffer) để gộp dữ liệu theo số lượng (`BATCH_SIZE` - vd: 50 bản ghi) hoặc theo thời gian định kỳ (`FLUSH_INTERVAL` - vd: 120 giây).
4.  **Chuyển đổi định dạng Parquet & Lưu trữ:** Khi điều kiện xả (flush) đệm được thỏa mãn, Consumer sử dụng `Pandas` và `PyArrow` để chuyển đổi danh sách JSON sang định dạng cột **Apache Parquet** (tối ưu nén và tốc độ đọc phân tích sau này) và đẩy trực tiếp lên MinIO theo cấu trúc phân vùng thời gian thực: `realtime/YYYY/MM/DD/HH/quotes_YYYYMMDD_HHMMSS.parquet`.

---

### 📅 2.2. Luồng Định Kỳ Batch ETL qua Apache Airflow (Silver -> Gold Layer)

Luồng Batch được lập lịch tự động bằng **Apache Airflow** giúp khai thác, làm sạch và làm giàu dữ liệu tài chính trước khi nạp vào kho dữ liệu Data Warehouse (PostgreSQL):

![Luồng xử lý Airflow Batch ETL](assets/airflow_flow.png)

#### Các Bước Vận Hành Luồng Batch:
1.  **Lập lịch & Thu thập (Extraction - Bronze):** Airflow Scheduler kích hoạt các DAGs định kỳ để cào/tải dữ liệu thô (Báo cáo tài chính BCTC, giá lịch sử EOD, chỉ số vĩ mô, tin tức doanh nghiệp) từ các API/Web nguồn và lưu dưới dạng các tệp CSV thô tại thư mục phân vùng ngày trên MinIO.
2.  **Đọc & Làm sạch dữ liệu (Transformation - Silver):** Airflow Workers tải các tệp thô từ MinIO và thực thi các bước làm sạch dữ liệu: chuẩn hóa tên cột, loại bỏ dòng trùng lặp, xử lý các giá trị rỗng/lỗi định dạng.
3.  **Làm giàu dữ liệu với AI & Smart Mapping (Gold):**
    *   **Đối với Báo cáo tài chính (BCTC):** Áp dụng giải thuật Smart Mapping (đọc từ cấu hình ánh xạ [bctc.md](file:///d:/project/web_ptich_ck/md/bctc.md)) để chuẩn hóa tên chỉ tiêu tiếng Việt thành mã chỉ tiêu thống nhất (`ind_code`), tự động tạo slug thay thế nếu không tìm thấy cấu hình khớp.
    *   **Đối với Tin tức thị trường:** Gửi tiêu đề bài viết qua mô hình học máy **PhoBERT Sentiment Analysis** (chạy cục bộ trong môi trường Docker thông qua thư viện PyTorch) để phân loại và chấm điểm sắc thái cảm xúc tích cực/trung lập/tiêu cực của tin tức.
4.  **Nạp Dữ Liệu Tối Ưu (Loading - Gold):** Thực thi các câu lệnh UPSERT/APPEND bằng các câu lệnh SQL tối ưu (`INSERT ... ON CONFLICT DO UPDATE` hoặc `TRUNCATE + INSERT`) vào kho dữ liệu `dwh-postgres` trong schema `hethong_phantich_chungkhoan` để đảm bảo dữ liệu được cập nhật mới nhất mà không bị trùng lặp khóa chính.

---

## 🛠️ 3. Công Nghệ Sử Dụng (Tech Stack)

*   **Orchestrator:** Apache Airflow (Celery Executor + Redis + PostgreSQL).
*   **Message Broker:** Apache Kafka & Zookeeper (Confluent Platform).
*   **Object Storage:** MinIO (Tương thích chuẩn AWS S3 API).
*   **Database (DWH):** PostgreSQL 16 tích hợp `pgvector` phục vụ lưu trữ Vector embeddings chatbot.
*   **AI/NLP Model:** HuggingFace `wonrax/phobert-base-vietnamese-sentiment` chạy trên môi trường CPU PyTorch.
*   **Data Processing:** Pandas, PyArrow, Vnstock, yfinance.

---

## 📂 4. Cấu Trúc Thư Mục Dự Án (Directory Structure)

Cấu trúc chi tiết của repository `data_pipeline`:

```text
data_pipeline/
├── etl/                                # Thư mục chính chứa mã nguồn ETL
│   ├── .env                            # Tệp cấu hình biến môi trường
│   ├── docker-compose.yaml             # Cấu hình khởi chạy các container hệ thống
│   ├── HDSD_kafka.md                   # Hướng dẫn chi tiết luồng Kafka realtime
│   ├── HDSD_postgres_docker.md         # Hướng dẫn câu lệnh thao tác nhanh trên PostgreSQL
│   │
│   ├── airflow/                        # Môi trường chạy lập lịch Apache Airflow
│   │   ├── Dockerfile                  # Xây dựng Custom Airflow Image (tải trước PhoBERT & PyTorch CPU)
│   │   ├── requirements.txt            # Thư viện Python phụ trợ trong các DAGs
│   │   ├── dags/                       # Các kịch bản định kỳ (Directed Acyclic Graphs)
│   │   │   ├── minio_to_db_sync.py     # DAG đồng bộ chính MinIO -> PostgreSQL DWH
│   │   │   ├── dl_bctc_luong_2.py      # DAG tải BCTC luồng 2
│   │   │   ├── dl_dailly_price.py      # DAG tải giá cuối ngày EOD
│   │   │   └── ...
│   │   └── plugins/                    # Module xử lý mở rộng của Airflow
│   │       └── lake_to_dwh/            # Thư viện xử lý chuyển đổi dữ liệu
│   │           ├── sync_generic.py     # Trình đồng bộ hóa dữ liệu tổng quát
│   │           ├── sync_bctc.py        # Chuẩn hóa mã chỉ tiêu BCTC dựa trên bctc.md
│   │           ├── sentiment_processor.py # Phân tích cảm xúc tin tức qua PhoBERT
│   │           └── utils.py            # Hàm bổ trợ MinIO và Database
│   │
│   ├── kafka/                          # Hệ thống xử lý dữ liệu realtime
│   │   ├── Dockerfile                  # Xây dựng Image chạy Kafka producer/consumer
│   │   ├── requirements.txt            # Dependencies của luồng streaming
│   │   ├── check_status.py             # Script kiểm tra nhanh trạng thái dữ liệu
│   │   └── src/
│   │       ├── producers/
│   │       │   └── kafka_producer.py   # WebSocket Client -> Đẩy tin lên Kafka
│   │       └── consumers/
│   │           └── minio_sink_consumer.py # Gom message -> Parquet -> Đẩy lên MinIO
│   │
│   └── minio/                          # Nơi gắn kết dữ liệu hồ lưu trữ MinIO
│       └── data/                       # Dữ liệu vật lý dạng Parquet/CSV lưu cục bộ
└── .gitignore                          # Cấu hình bỏ qua các tệp không cần commit lên Git
```

---

## 🚀 5. Hướng Dẫn Cài Đặt Chi Tiết (Installation Guide)

### 💻 5.1. Yêu Cầu Hệ Thống (Prerequisites)

Trước khi bắt đầu, hãy đảm bảo máy tính của bạn đã được cài đặt và cấu hình các công cụ sau:

*   **Docker & Docker Compose** (Khuyên dùng Docker Desktop bản mới nhất).
*   **WSL2 (Windows Subsystem for Linux)** nếu bạn chạy trên hệ điều hành Windows.
*   **Cấu hình tài nguyên cho Docker Desktop:**
    > [!IMPORTANT]
    > Hệ sinh thái Airflow & Kafka khá nặng, bạn bắt buộc phải cấp tối thiểu **4GB RAM** và **2 Core CPU** cho Docker Desktop để tránh lỗi tràn bộ nhớ (OOM) hoặc treo container.

---

### 🚀 5.2. Các Bước Cài Đặt (Step-by-Step Installation)

### Bước 1: Tải mã nguồn dự án
Tải toàn bộ dự án về máy tính mới của bạn:
```bash
git clone <repository_url>
cd data_pipeline/etl
```

---

### Bước 2: Khởi tạo các Docker Volume (Tự động)
Các Volume lưu trữ dữ liệu cho PostgreSQL, Kafka, Zookeeper trong tệp cấu hình [docker-compose.yaml](file:///d:/project/data_pipeline/etl/docker-compose.yaml) đã được cấu hình với thuộc tính `name` mà không có `external: true`.

> [!NOTE]
> Hệ thống sẽ tự động tạo các volume này nếu chúng chưa tồn tại khi bạn chạy lệnh `docker-compose up`. Nếu các volume đã tồn tại sẵn (như trên máy cũ của bạn), Docker Compose sẽ tự động nhận diện và tái sử dụng dữ liệu cũ của bạn mà không làm mất mát dữ liệu. Bạn **không cần** chạy các lệnh `docker volume create` thủ công nữa.

---

### Bước 3: Chuẩn bị tệp tin cấu hình môi trường `.env`
Đảm bảo bạn đã có tệp tin `.env` nằm tại đường dẫn `data_pipeline/etl/.env`. Dưới đây là nội dung mẫu tiêu chuẩn phục vụ môi trường Local:

```env
# ====== AIRFLOW ======
AIRFLOW_UID=50000
AIRFLOW_IMAGE_NAME=apache/airflow:2.9.3
AIRFLOW_PROJ_DIR=./airflow
_PIP_ADDITIONAL_REQUIREMENTS=
_AIRFLOW_WWW_USER_USERNAME=airflow
_AIRFLOW_WWW_USER_PASSWORD=airflow

# ====== KAFKA ======
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_INTERNAL_BOOTSTRAP_SERVERS=kafka:9093

# Topics Configuration
TOPIC_STOCK_QUOTES=market.quotes.raw
TOPIC_STOCK_CANDLES=stock-candles
TOPIC_PARTITIONS=3
TOPIC_REPLICATION_FACTOR=1

# MinIO Configuration
MINIO_ENDPOINT=http://localhost:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
MINIO_BUCKET=thongtin-congty-va-bctc

# Database Configuration (for consumer)
DB_HOST=dwh-postgres
DB_DSN=postgresql://admin:123456@dwh-postgres:5432/postgres
DB_SCHEMA=hethong_phantich_chungkhoan
```

---

### Bước 4: Khởi động hệ thống Docker Compose
Mở Terminal tại thư mục `data_pipeline/etl` và chạy câu lệnh sau để xây dựng và khởi chạy các container dưới nền:
```bash
docker-compose up -d --build
```

> [!NOTE]
> Quá trình build lần đầu tiên sẽ mất khoảng 5-10 phút do Docker cần xây dựng Airflow Custom Image:
> 1. Tải hệ điều hành nền và cài đặt các thư viện hệ thống (`build-essential`, `libpq-dev`).
> 2. Tải và cài đặt PyTorch phiên bản CPU để chạy các mô hình AI.
> 3. Tải trước mô hình học máy tiếng Việt (`wonrax/phobert-base-vietnamese-sentiment`) phục vụ cho việc phân tích sắc thái tin tức.

---

### Bước 5: Cấu hình Cơ sở dữ liệu Đích (dwh-postgres)
Container `dwh-postgres` khởi động lần đầu là một database trống rỗng. Bạn cần tạo Schema và cấu trúc bảng để Airflow đồng bộ dữ liệu vào.

1.  **Kết nối Database:** Dùng các công cụ quản trị như DBeaver hoặc pgAdmin kết nối tới thông số:
    *   **Host:** `localhost` hoặc `127.0.0.1`
    *   **Port:** `5432`
    *   **Username:** `admin`
    *   **Password:** `123456`
    *   **Database:** `postgres`
2.  **Khởi tạo Schema & Search Path:** Thực thi các truy vấn SQL sau để tạo không gian tên:
    ```sql
    CREATE SCHEMA IF NOT EXISTS hethong_phantich_chungkhoan;
    CREATE SCHEMA IF NOT EXISTS system;
    ALTER DATABASE postgres SET search_path TO hethong_phantich_chungkhoan, public, system;
    ```
3.  **Khởi tạo cấu trúc bảng:** Tìm đến các file SQL trong thư mục dự án Backend của bạn (`web_ptich_ck/BE/app/database/`) và chạy lần lượt theo thứ tự:
    *   [schema_v1.sql](file:///d:/project/web_ptich_ck/BE/app/database/schema_v1.sql) (Tạo các bảng DWH cốt lõi: BCTC, giá EOD, commodity,...).
    *   [indexes.sql](file:///d:/project/web_ptich_ck/BE/app/database/indexes.sql) (Tạo các Index tối ưu hóa hiệu năng truy vấn).
    *   [auth_tables.sql](file:///d:/project/web_ptich_ck/BE/app/database/auth_tables.sql) (Khởi tạo các bảng đăng nhập, tài khoản).
    *   [schema_system.sql](file:///d:/project/web_ptich_ck/BE/app/database/schema_system.sql) (Tạo các bảng lưu vết người dùng, log tìm kiếm,...).
    *   [materialized_views_stock_list.sql](file:///d:/project/web_ptich_ck/BE/app/database/materialized_views_stock_list.sql) (Khởi tạo Materialized View thống kê danh sách mã).

*(Mẹo: Bạn có thể chạy nhanh qua Command Prompt trên host)*:
```bash
docker exec -i dwh-postgres psql -U admin -d postgres < d:/project/web_ptich_ck/BE/app/database/schema_v1.sql
```

---

### Bước 6: Khởi tạo MinIO & Airflow S3 Connection
Để các tiến trình truyền dữ liệu từ MinIO về PostgreSQL chạy chính xác, bạn cần khởi tạo thủ công hai thành phần:

#### 1. Tạo Bucket lưu trữ trên MinIO:
1.  Truy cập vào giao diện quản lý MinIO: [http://localhost:9001](http://localhost:9001) (Tài khoản: `minioadmin` / `minioadmin`).
2.  Vào mục **Buckets** ở danh mục bên trái -> chọn **Create Bucket**.
3.  Đặt tên bucket chính xác là: `thongtin-congty-va-bctc` rồi nhấn **Create Bucket**.

#### 2. Cấu hình kết nối S3 trên Airflow UI:
1.  Truy cập vào trang quản trị Airflow: [http://localhost:8080](http://localhost:8080) (Tài khoản: `airflow` / `airflow`).
2.  Trên thanh Menu đầu trang, chọn **Admin** -> **Connections**.
3.  Nhấp vào biểu tượng dấu cộng (`+`) để tạo mới Connection và cấu hình:
    *   **Connection Id:** `minio_finance`
    *   **Connection Type:** `Amazon S3` (hoặc `S3`)
    *   **Extra:**
        ```json
        {
          "aws_access_key_id": "minioadmin",
          "aws_secret_access_key": "minioadmin",
          "endpoint_url": "http://minio:9000"
        }
        ```
4.  Nhấp **Save** để lưu lại.

---

## 🔍 6. Kiểm Tra Hoạt Động (Verification)

Sau khi cài đặt xong, bạn có thể kiểm tra xem hệ thống đã chạy ổn định chưa bằng các đường dẫn sau:

0. **dumbdb tạm thời :** Truy cập [https://drive.google.com/drive/folders/1y2zmunS2DlMVXL3D5H_eCmiJ89DnjttY?usp=drive_link](https://drive.google.com/drive/folders/1y2zmunS2DlMVXL3D5H_eCmiJ89DnjttY?usp=drive_link)
1.  **Airflow Webserver:** Truy cập [http://localhost:8080](http://localhost:8080) để bật (Unpause) các DAG và bấm **Trigger** chạy thử luồng.
2.  **Kafka UI:** Truy cập [http://localhost:8081](http://localhost:8081), chọn mục **Topics** -> `market.quotes.raw` -> **Messages** để kiểm tra dữ liệu chứng khoán thời gian thực có đang đổ về liên tục hay không.
3.  **Flower (Celery Monitor):** Truy cập [http://localhost:5555](http://localhost:5555) để kiểm tra trạng thái hoạt động và hiệu năng xử lý của các Worker chạy ngầm.

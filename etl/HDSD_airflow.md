# Các lệnh Docker để kiểm tra trạng thái Apache Airflow

Tài liệu này tổng hợp các lệnh thường dùng để kiểm tra tình trạng Apache Airflow đang chạy trên Docker. Nội dung bao gồm:

- Lệnh Docker / Docker Compose để kiểm tra container và log hệ thống.
- Lệnh Airflow CLI để kiểm tra DAG, task và debug task.
- Cách truy cập trực tiếp vào container để kiểm tra file log hoặc cấu trúc thư mục.

> **Lưu ý**
>
> Các lệnh bên dưới giả định bạn đang sử dụng `docker-compose`.
>
> Nếu bạn dùng Docker Compose phiên bản mới, có thể cần dùng:
>
> ```bash
> docker compose ...
> ```
>
> thay cho:
>
> ```bash
> docker-compose ...
> ```
>
> Nếu dùng Docker thuần, hãy thay `<tên_container>` bằng ID hoặc tên container thực tế. Có thể lấy tên container bằng lệnh:
>
> ```bash
> docker ps
> ```

---

## 1. Kiểm tra trạng thái các container

Dùng để kiểm tra các thành phần của Airflow như Webserver, Scheduler, Worker, Database có đang chạy bình thường hay không.

### Kiểm tra tổng quan bằng Docker Compose

```bash
docker-compose ps
```

Lệnh này hiển thị trạng thái của các service, ví dụ:

- `Up`
- `Exit`
- `Restarting`

### Kiểm tra bằng Docker thuần

```bash
docker ps | grep airflow
```

Lệnh này lọc ra các container có liên quan đến Airflow.

---

## 2. Kiểm tra log hệ thống

Khi Airflow không chạy DAG, bị treo, hoặc task không được thực thi, lỗi thường nằm ở `Scheduler`, `Worker` hoặc `Webserver`.

### Xem log của Scheduler

Scheduler chịu trách nhiệm lập lịch và kích hoạt DAG/task.

```bash
docker-compose logs -f airflow-scheduler
```

### Xem log của Worker

Worker là nơi thực thi các task.

```bash
docker-compose logs -f airflow-worker
```

### Xem log của Webserver

Dùng khi giao diện Airflow UI bị lỗi hoặc không truy cập được.

```bash
docker-compose logs -f airflow-webserver
```

> Nhấn `Ctrl + C` để thoát chế độ xem log real-time.

---

## 3. Kiểm tra DAGs và Tasks bằng Airflow CLI

Sử dụng `docker exec` để chạy lệnh Airflow CLI bên trong một container đang hoạt động, thường là `airflow-webserver` hoặc `airflow-scheduler`.

Ví dụ tên container:

```bash
airflow-webserver-1
```

Cú pháp chung:

```bash
docker exec -it <tên_container> airflow <lệnh_airflow>
```

---

## 4. Làm việc với DAGs

### Liệt kê tất cả DAG

```bash
docker exec -it <tên_container> airflow dags list
```

### Kiểm tra lỗi import DAG

```bash
docker exec -it <tên_container> airflow dags report
```

Lệnh này hữu ích khi DAG không xuất hiện trên Airflow UI hoặc bị lỗi import Python.

### Kiểm tra trạng thái của một DAG

```bash
docker exec -it <tên_container> airflow dags state <dag_id> <execution_date>
```

Ví dụ:

```bash
docker exec -it airflow-webserver-1 airflow dags state my_daily_job 2023-10-25T00:00:00+00:00
```

---

## 5. Làm việc với Tasks

### Liệt kê tất cả task trong một DAG

```bash
docker exec -it <tên_container> airflow tasks list <dag_id>
```

### Xem cấu trúc cây của task trong DAG

```bash
docker exec -it <tên_container> airflow tasks list <dag_id> --tree
```

### Kiểm tra trạng thái của một task cụ thể

```bash
docker exec -it <tên_container> airflow tasks state <dag_id> <task_id> <execution_date>
```

Ví dụ:

```bash
docker exec -it airflow-webserver-1 airflow tasks state my_daily_job extract_data 2023-10-25T00:00:00+00:00
```

---

## 6. Test và debug task

Dùng để chạy thử một task trực tiếp trên terminal và xem log chi tiết.

```bash
docker exec -it <tên_container> airflow tasks test <dag_id> <task_id> <execution_date>
```

Ví dụ:

```bash
docker exec -it airflow-webserver-1 airflow tasks test my_daily_job extract_data 2023-10-25T00:00:00+00:00
```

Lệnh này rất hữu ích để debug:

- Lỗi code Python.
- Lỗi import module.
- Lỗi cấu hình connection.
- Lỗi biến môi trường.
- Lỗi quyền truy cập file hoặc thư mục.

> **Lưu ý**
>
> `airflow tasks test` chạy task ở chế độ test và không ghi kết quả chạy task vào metadata database như một lần chạy DAG thông thường.

---

## 7. Truy cập trực tiếp vào container

Dùng khi cần kiểm tra file log vật lý, cấu trúc thư mục DAG, plugin hoặc cấu hình bên trong container.

### Truy cập terminal của container

```bash
docker exec -it <tên_container> /bin/bash
```

Nếu container không có `bash`, dùng `sh`:

```bash
docker exec -it <tên_container> /bin/sh
```

### Kiểm tra thư mục log Airflow

Sau khi đã vào bên trong container:

```bash
cd /opt/airflow/logs
ls -la
```

### Kiểm tra thư mục DAG

```bash
cd /opt/airflow/dags
ls -la
```

---

## 8. Các lệnh nhanh thường dùng

### Xem tất cả container đang chạy

```bash
docker ps
```

### Xem tất cả container, bao gồm container đã dừng

```bash
docker ps -a
```

### Restart một service Airflow

```bash
docker-compose restart airflow-scheduler
```

Ví dụ khác:

```bash
docker-compose restart airflow-webserver
docker-compose restart airflow-worker
```

### Restart toàn bộ stack Airflow

```bash
docker-compose restart
```

### Dừng toàn bộ stack Airflow

```bash
docker-compose down
```

### Khởi động lại toàn bộ stack Airflow

```bash
docker-compose up -d
```

---

## 9. Quy trình kiểm tra nhanh khi Airflow gặp lỗi

Khi DAG không chạy hoặc Airflow hoạt động bất thường, có thể kiểm tra theo thứ tự sau:

1. Kiểm tra container có đang chạy không:

   ```bash
   docker-compose ps
   ```

2. Xem log Scheduler:

   ```bash
   docker-compose logs -f airflow-scheduler
   ```

3. Xem log Worker:

   ```bash
   docker-compose logs -f airflow-worker
   ```

4. Kiểm tra lỗi import DAG:

   ```bash
   docker exec -it <tên_container> airflow dags report
   ```

5. Kiểm tra danh sách DAG:

   ```bash
   docker exec -it <tên_container> airflow dags list
   ```

6. Kiểm tra danh sách task trong DAG:

   ```bash
   docker exec -it <tên_container> airflow tasks list <dag_id> --tree
   ```

7. Test task trực tiếp:

   ```bash
   docker exec -it <tên_container> airflow tasks test <dag_id> <task_id> <execution_date>
   ```

---

## 10. Bảng tóm tắt lệnh

| Mục đích | Lệnh |
|---|---|
| Kiểm tra service Docker Compose | `docker-compose ps` |
| Kiểm tra container Airflow | `docker ps \| grep airflow` |
| Xem log Scheduler | `docker-compose logs -f airflow-scheduler` |
| Xem log Worker | `docker-compose logs -f airflow-worker` |
| Xem log Webserver | `docker-compose logs -f airflow-webserver` |
| Liệt kê DAG | `docker exec -it <tên_container> airflow dags list` |
| Kiểm tra lỗi import DAG | `docker exec -it <tên_container> airflow dags report` |
| Kiểm tra trạng thái DAG | `docker exec -it <tên_container> airflow dags state <dag_id> <execution_date>` |
| Liệt kê task | `docker exec -it <tên_container> airflow tasks list <dag_id>` |
| Xem cây task | `docker exec -it <tên_container> airflow tasks list <dag_id> --tree` |
| Kiểm tra trạng thái task | `docker exec -it <tên_container> airflow tasks state <dag_id> <task_id> <execution_date>` |
| Test task | `docker exec -it <tên_container> airflow tasks test <dag_id> <task_id> <execution_date>` |
| Vào container | `docker exec -it <tên_container> /bin/bash` |
| Vào thư mục log | `cd /opt/airflow/logs` |

---

## 11. Ghi chú thay thế biến

Trong các lệnh trên, cần thay các giá trị sau bằng thông tin thực tế:

| Biến | Ý nghĩa | Ví dụ |
|---|---|---|
| `<tên_container>` | Tên hoặc ID container Airflow | `airflow-webserver-1` |
| `<dag_id>` | ID của DAG | `my_daily_job` |
| `<task_id>` | ID của task trong DAG | `extract_data` |
| `<execution_date>` | Ngày chạy DAG/task | `2023-10-25T00:00:00+00:00` |

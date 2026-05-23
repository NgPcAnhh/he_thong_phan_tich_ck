truy cập database qua docker: 
docker exec -it dwh-postgres psql -U admin -d postgres

các lệnh thông dụng:
    - \l: Liệt kê tất cả các Databases hiện có trên server.
    - \c <tên_db>: Kết nối (chuyển sang) một database khác.
    - \dt: Liệt kê các Bảng (Tables) trong database hiện tại.
    - \dt+: Giống lệnh trên nhưng hiển thị thêm kích thước (size) và mô tả của bảng.
    - \d <tên_bảng>: Xem chi tiết cấu trúc bảng (các cột, kiểu dữ liệu, khóa chính, khóa ngoại).
    - \dn: Liệt kê các Schemas (ví dụ: public, staging, gold).
    - \du: Liệt kê các User (Roles) và quyền hạn của họ.
    - \q: Thoát khỏi psql.

dump db: docker exec dwh-postgres pg_dump -U admin -d postgres > D:/backup_db.sql
dump + nén: docker exec dwh-postgres pg_dump -U admin -d postgres | gzip > D:/backup_db.sql.gz
restore: docker exec -i dwh-postgres psql -U admin -d postgres < D:/backup_db.sql
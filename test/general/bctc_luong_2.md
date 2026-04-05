luồng lấy dữ liệu bctc chi tiết:


===== lưu ý: =====
- thực hiện lấy lần lượt, chia theo batch 20 mã 1 batch, xong batch 1 mới đến batch 2
- hiển thị log chi tiết 


===== mô tả chi tiết quá trình lấy =====

url  = https://web.stockbiz.vn/Stocks/{ma_cp}/FinancialStatements.aspx
ma_cp = {} <-- lấy ở D:\project\lakehouse_ptich_ck\etl\airflow\plugins\logic\tickers_cache.txt

- tổng quan về cấu trúc trang web: 
trong thẻ div id = yatabs2, có thẻ <ul> thì có các thẻ <li> có các hàm này bên trong:
onclick="return changeReportType(0);"  <-- tương ứng với bảng cân đối kế toán 
onclick="return changeReportType(1);"  <-- tương ứng với kết quả kinh doanh
onclick="return changeReportType(2);"  <-- tương ứng với lưu chuyển tiền tệ trực tiếp
onclick="return changeReportType(3);"  <-- tương ứng với lưu chuyển tiền tệ gián tiếp


với mỗi bên trong mỗi tab này sẽ có table và tôi cần lấy dữ liệu từ đây về ưu tiên lưu vào dataframe sau đó xuất csv:
tạo 6 cột: 
- cột 1 là chỉ tiêu
- cột 23456: trong thẻ id = "ctl00_webPartManager_wp603001723_wp866410259_cbFinanceReport", trong các thẻ <td> bên trong thì loại bỏ 2 thẻ đầu tiên, lấy lần lượt các nội dung bên trong thẻ td từ thẻ thứ 3 trở đi (thường nằm bên trong thẻ <b></b> bên trong thẻ <td> đó ) lấy nội dung làm tên của cột của cột 23456. 

"""
*** lưu ý ***
giá trị của cột thứ 6 lấy ra để làm tên file, tên file = mã cổ phiếu_nội dung của cột thứ 6
"""

nội dung của bảng như sau: 
tìm tabble có id = "tblReports" --> tìm tất cả các thẻ <tr> có class = "rowcolor3" --> tìm các thẻ <td> bên trong 
| --> với thẻ <td> đầu tiên tìm bên trong đó có các thẻ <table> --> <tbody> --> <tr> --> lấy thẻ <td> thứ 2 và lấy nội dung bên trong đó 
| --> với thẻ <td> thứ 34567 tương ứng cho vào cột 23456 (bỏ qua thẻ thứ 2), lấy giá trị của thẻ <b> bên trong thẻ <td> đó, lần lượt từ trên xuống dưới tham chiếu sang bảng là từ trái qua phải
--> làm tương tự với các bản ghi còn lại của bảng 
--> làm tương tự với các tab sau đó 
--> nếu trong tab nào mà tìm thẻ id="divNoReports" thì bỏ qua tab đó luôn

===== luồng xử lý dữ liệu =====

trường dữ liệu bctc. cấu trúc của bảng như sau:
    create table bctc
    (
        ticker      varchar(10) not null,
        quarter     varchar(10) not null,
        year        integer     not null,
        ind_name    text,
        ind_code    text        not null,
        value       numeric(25, 4),
        import_time timestamp default CURRENT_TIMESTAMP,
        report_name varchar(255),
        report_code varchar(100),
        constraint pk_bctc
            primary key (ticker, year, quarter, ind_code)
    );

đảo bảo sau khi thực hiện lấy dữ liệu về hãy chuyển đổi thành theo dạng bảng giống như logic xử lý của bảng D:\project\lakehouse_ptich_ck\etl\airflow\plugins\logic\bctc.py

sau đó lưu thực hiện lưu vào đồng bộ vào bảng bctc. luồng này viết riêng từ lấy dữ liệu --> xử lý dữ liệu --> lưu vào minio đặt tên là bctc_luong2 --> đồng bộ vào bảng bctc
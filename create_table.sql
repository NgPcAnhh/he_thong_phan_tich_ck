CREATE DATABASE IF NOT EXISTS stock_db;
USE stock_db; 

-- 1. Tạo bảng Company (Bảng trung tâm)
CREATE TABLE company (
    company_id INT PRIMARY KEY,
    company_name VARCHAR(255),
    industry VARCHAR(255),
    description TEXT
);

-- 2. Tạo bảng Market Index
CREATE TABLE market_index (
    index_id INT PRIMARY KEY,
    index_code VARCHAR(50),
    name VARCHAR(255)
);

-- 3. Tạo bảng Stock (Liên kết với Company)
CREATE TABLE stock (
    stock_id INT PRIMARY KEY,
    ticker VARCHAR(20),
    exchange VARCHAR(50),
    company_id INT,
    listing_date DATE,
    status VARCHAR(50),
    FOREIGN KEY (company_id) REFERENCES company(company_id)
);

-- 4. Tạo bảng News (Liên kết với Company)
CREATE TABLE news (
    news_id INT PRIMARY KEY,
    company_id INT,
    title VARCHAR(255),
    content text,
    author VARCHAR(255),
    publish_time TEXT,
    FOREIGN KEY (company_id) REFERENCES company(company_id)
);

-- 5. Tạo bảng Income Statement (Báo cáo kết quả kinh doanh)
CREATE TABLE income_statement (
    ind_code INT,
    ind_name varchar(255), -- Đã sửa thành varchar (Hợp lý)
    time_stamp TEXT,
    company_id INT,
    value DOUBLE, -- Đã sửa thành DOUBLE (Hợp lý)
    update_time TEXT,
    PRIMARY KEY (ind_code, company_id),
    FOREIGN KEY (company_id) REFERENCES company(company_id)
);

-- 6. Tạo bảng Balance Sheet (Bảng cân đối kế toán)
CREATE TABLE balance_sheet (
    ind_code INT,
    ind_name INT, -- Lưu ý: Bảng này bạn vẫn để là INT, có muốn sửa thành varchar giống bảng trên không?
    time_stamp TEXT,
    company_id INT,
    value DOUBLE,
    update_time TEXT,
    PRIMARY KEY (ind_code, company_id),
    FOREIGN KEY (company_id) REFERENCES company(company_id)
);

-- 7. Tạo bảng Intraday Flow
CREATE TABLE intraday_flow (
    int_code INT,
    int_name INT,
    time_stamp TEXT,
    company_id INT,
    value DOUBLE,
    update_time TEXT,
    PRIMARY KEY (int_code, company_id),
    FOREIGN KEY (company_id) REFERENCES company(company_id)
);

-- 8. Tạo bảng Market Index History (Liên kết với Market Index)
CREATE TABLE market_index_history (
    idx_hist_id BIGINT PRIMARY KEY,
    index_id INT,
    trade_date DATE,
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    volume BIGINT,
    value DOUBLE,
    FOREIGN KEY (index_id) REFERENCES market_index(index_id)
);

-- 9. Tạo bảng Price History (Liên kết với Stock)
CREATE TABLE price_history (
    price_id BIGINT PRIMARY KEY,
    stock_id INT,
    trade_date DATE,
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    volume BIGINT,
    value DOUBLE,
    FOREIGN KEY (stock_id) REFERENCES stock(stock_id)
);

-- 10. Tạo bảng Order Book (Sổ lệnh - Liên kết với Stock)
CREATE TABLE order_book (
    orderbook_id BIGINT PRIMARY KEY,
    stock_id INT,
    timestamp DATETIME,
    bid_price_lv1 DOUBLE,
    bid_volume_lv1 BIGINT,
    ask_price_lv1 DOUBLE,
    ask_volume_lv1 BIGINT,
    bid_price_lv2 DOUBLE,
    bid_volume_lv2 BIGINT,
    ask_price_lv2 DOUBLE,
    ask_volume_lv2 BIGINT,
    bid_price_lv3 DOUBLE,
    bid_volume_lv3 BIGINT,
    ask_price_lv3 DOUBLE,
    ask_volume_lv3 BIGINT,
    FOREIGN KEY (stock_id) REFERENCES stock(stock_id)
);

-- 11. Tạo bảng Price In Day (Giá trong ngày - Liên kết với Stock)
CREATE TABLE price_in_day (
    intraday_id BIGINT PRIMARY KEY,
    stock_id INT,
    timestamp TEXT,
    last_price DOUBLE,
    best_bid DOUBLE,
    best_ask DOUBLE,
    volume_accumulation BIGINT,
    FOREIGN KEY (stock_id) REFERENCES stock(stock_id)
);

-- 12. Tạo bảng Trade Tick (Khớp lệnh từng tick - Liên kết với Stock)
CREATE TABLE trade_tick (
    trade_id BIGINT PRIMARY KEY,
    stock_id INT,
    timestamp TEXT,
    price DOUBLE,
    volume BIGINT,
    buyer_type VARCHAR(50),
    seller_type VARCHAR(50),
    FOREIGN KEY (stock_id) REFERENCES stock(stock_id)
);
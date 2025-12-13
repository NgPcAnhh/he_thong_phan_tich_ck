import random
from itertools import count
from datetime import datetime, timedelta
from db_config import DatabaseConfig

class StockDataGenerator: 
    def __init__(self):        
        # Dữ liệu công ty Việt Nam
        self.companies = [
            (1, "Vingroup JSC", "Conglomerate", "Tập đoàn đa ngành hàng đầu Việt Nam"),
            (2, "Vietcombank", "Banking", "Ngân hàng thương mại cổ phần Ngoại thương Việt Nam"),
            (3, "Vinhomes JSC", "Real Estate", "Công ty phát triển bất động sản"),
            (4, "Masan Group", "Consumer Goods", "Tập đoàn hàng tiêu dùng"),
            (5, "Hoa Phat Group", "Steel", "Tập đoàn thép hàng đầu Việt Nam"),
            (6, "FPT Corporation", "Technology", "Công ty công nghệ lớn nhất Việt Nam"),
            (7, "Mobile World", "Retail", "Chuỗi bán lẻ điện thoại và điện máy"),
            (8, "Vinamilk", "Dairy", "Công ty sữa lớn nhất Việt Nam"),
            (9, "PetroVietnam Gas", "Oil & Gas", "Công ty khí dầu khí Việt Nam"),
            (10, "Vietjet Aviation", "Aviation", "Hãng hàng không giá rẻ"),
            (11, "Techcombank", "Banking", "Ngân hàng thương mại cổ phần Kỹ thương"),
            (12, "SSI Securities", "Securities", "Công ty chứng khoán"),
            (13, "Vietnam Dairy", "Dairy", "Công ty sữa Việt Nam"),
            (14, "Petrovietnam Power", "Power", "Tổng công ty Điện lực Dầu khí Việt Nam"),
            (15, "Sabeco", "Beverage", "Tổng công ty cổ phần Bia - Rượu - Nước giải khát Sài Gòn")
        ]
        
        self.stocks = [
            (1, 1, "VIC", "HOSE", "2007-01-26", "active"),
            (2, 2, "VCB", "HOSE", "2009-07-14", "active"),
            (3, 3, "VHM", "HOSE", "2018-05-17", "active"),
            (4, 4, "MSN", "HOSE", "2009-12-08", "active"),
            (5, 5, "HPG", "HOSE", "2007-11-20", "active"),
            (6, 6, "FPT", "HOSE", "2006-12-08", "active"),
            (7, 7, "MWG", "HOSE", "2014-11-27", "active"),
            (8, 8, "VNM", "HOSE", "2006-01-19", "active"),
            (9, 9, "GAS", "HOSE", "2012-12-28", "active"),
            (10, 10, "VJC", "HOSE", "2017-02-28", "active"),
            (11, 11, "TCB", "HOSE", "2018-06-14", "active"),
            (12, 12, "SSI", "HOSE", "2007-08-31", "active"),
            (13, 13, "VNL", "HNX", "2020-03-10", "active"),
            (14, 14, "POW", "HOSE", "2011-10-13", "active"),
            (15, 15, "SAB", "HOSE", "2017-12-22", "active")
        ]
        
        # Giá khởi điểm cho mỗi mã (VNĐ)
        self.initial_prices = {
            "VIC": 95000, "VCB": 85000, "VHM": 75000, "MSN": 120000, "HPG": 45000,
            "FPT": 110000, "MWG": 135000, "VNM": 80000, "GAS": 95000, "VJC": 125000,
            "TCB": 42000, "SSI": 38000, "VNL": 55000, "POW": 11500, "SAB": 180000
        }
        
        self.stock_latest_prices = {}
        # Simple in-memory ID generators to satisfy NOT NULL primary keys
        self.price_id = count(1)
        self.orderbook_id = count(1)
        self.trade_id = count(1)
        self.intraday_id = count(1)
        self.idx_hist_id = count(1)
        self.news_id = count(1)
    
    @staticmethod
    def get_price_limits(ref_price):
        ceiling = round(ref_price * 1.07, -2)  # Giá trần +7%
        floor = round(ref_price * 0.93, -2)    # Giá sàn -7%
        return floor, ceiling
    
    def generate_daily_prices(self, prev_close, ticker):
        floor, ceiling = self.get_price_limits(prev_close)
        
        # Random xu hướng: 50% tăng, 30% giảm, 20% sideway
        trend = random.choices(['up', 'down', 'side'], weights=[0.5, 0.3, 0.2])[0]
        
        if trend == 'up':
            close = random.uniform(prev_close * 1.01, min(prev_close * 1.065, ceiling))
        elif trend == 'down':
            close = random.uniform(max(prev_close * 0.935, floor), prev_close * 0.99)
        else:
            close = random.uniform(prev_close * 0.98, prev_close * 1.02)
        
        close = round(close, -2)
        close = max(floor, min(ceiling, close))
        
        # Open thường gần với giá đóng cửa hôm trước
        open_price = round(random.uniform(prev_close * 0.985, prev_close * 1.015), -2)
        open_price = max(floor, min(ceiling, open_price))
        
        # High và Low
        high = max(open_price, close) * random.uniform(1.005, 1.02)
        high = round(high, -2)
        high = max(floor, min(ceiling, high))
        
        low = min(open_price, close) * random.uniform(0.98, 0.995)
        low = round(low, -2)
        low = max(floor, min(ceiling, low))
        
        # Volume: Random từ 1-10 triệu cổ phiếu
        volume = random.randint(1000000, 10000000)
        
        # Value = volume * average_price
        avg_price = (open_price + high + low + close) / 4
        value = volume * avg_price
        
        return open_price, high, low, close, volume, value
    
    def insert_companies_and_stocks(self, connection):
        cursor = connection.cursor()
        
        try:
            print("\n" + "="*80)
            print("BƯỚC 1: INSERT COMPANIES & STOCKS")
            print("="*80)
            
            # Insert companies
            cursor.executemany(
                'INSERT IGNORE INTO company (company_id, company_name, industry, description) VALUES (%s,%s,%s,%s)',
                self.companies
            )
            
            # Insert stocks
            cursor.executemany(
                'INSERT IGNORE INTO stock (stock_id, company_id, ticker, exchange, listing_date, status) VALUES (%s,%s,%s,%s,%s,%s)',
                self.stocks
            )
            
            connection.commit()
            print(f"Đã insert {len(self.companies)} công ty và {len(self.stocks)} mã chứng khoán")
            
        except Exception as e:
            print(f"Lỗi insert companies/stocks: {e}")
            connection.rollback()
        finally:
            cursor.close()
    
    def insert_price_history(self, connection, days=730):
        cursor = connection.cursor()
        start_date = datetime.now() - timedelta(days=days)
        
        print("\n" + "="*80)
        print(f"BƯỚC 2: INSERT PRICE HISTORY ({days} ngày)")
        print("="*80)
        
        batch_size = 5000  # larger batch to speed up bulk insert
        price_data = []
        total_inserted = 0
        
        for stock_id, company_id, ticker, exchange, listing_date, status in self.stocks:
            current_price = self.initial_prices[ticker]
            current_date = start_date
            
            print(f"Đang tạo dữ liệu cho {ticker}...", end=" ")
            
            for day in range(days):
                # Bỏ qua cuối tuần
                if current_date.weekday() >= 5:
                    current_date += timedelta(days=1)
                    continue
                
                open_p, high, low, close, volume, value = self.generate_daily_prices(current_price, ticker)
                
                price_data.append((
                    next(self.price_id),
                    stock_id,
                    current_date.strftime('%Y-%m-%d'),
                    open_p, high, low, close, volume, value
                ))
                
                # Insert theo batch
                if len(price_data) >= batch_size:
                    try:
                        cursor.executemany('''
                            INSERT INTO price_history (price_id, stock_id, trade_date, open, high, low, close, volume, value)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ''', price_data)
                        connection.commit()
                        total_inserted += len(price_data)
                        price_data = []
                    except Exception as e:
                        print(f"\nLỗi insert batch: {e}")
                        connection.rollback()
                
                current_price = close
                current_date += timedelta(days=1)
            
            self.stock_latest_prices[stock_id] = current_price
            print("✓")
        
        # Insert records còn lại
        if price_data:
            try:
                cursor.executemany('''
                    INSERT INTO price_history (price_id, stock_id, trade_date, open, high, low, close, volume, value)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ''', price_data)
                connection.commit()
                total_inserted += len(price_data)
            except Exception as e:
                print(f"Lỗi insert batch cuối: {e}")
        
        cursor.close()
        print(f"Đã insert tổng cộng {total_inserted:,} price history records")
    
    def insert_order_book(self, connection, days=30, snapshots_per_day=500):
        cursor = connection.cursor()
        
        print("\n" + "="*80)
        print(f"BƯỚC 3: INSERT ORDER BOOK ({days} ngày × {snapshots_per_day} snapshots/ngày)")
        print("="*80)
        
        batch_size = 2000
        orderbook_data = []
        total_inserted = 0
        
        for stock_id, ticker_info in enumerate(self.stocks, 1):
            ticker = ticker_info[2]
            ref_price = self.stock_latest_prices[stock_id]
            
            print(f"Đang tạo order book cho {ticker}...", end=" ")
            
            for day_offset in range(days):
                trade_date = datetime.now() - timedelta(days=day_offset)
                
                if trade_date.weekday() >= 5:
                    continue
                
                for i in range(snapshots_per_day):
                    hour = 9 + (i // 83)
                    minute = int((i % 83) * 0.72)
                    second = random.randint(0, 59)
                    timestamp = trade_date.replace(hour=hour, minute=minute, second=second)
                    
                    # Bid prices (giá mua) thấp hơn ref_price
                    bid1 = round(ref_price * random.uniform(0.995, 0.9995), -2)
                    bid2 = round(bid1 * random.uniform(0.997, 0.9999), -2)
                    bid3 = round(bid2 * random.uniform(0.997, 0.9999), -2)
                    
                    # Ask prices (giá bán) cao hơn ref_price
                    ask1 = round(ref_price * random.uniform(1.0005, 1.005), -2)
                    ask2 = round(ask1 * random.uniform(1.0001, 1.003), -2)
                    ask3 = round(ask2 * random.uniform(1.0001, 1.003), -2)
                    
                    orderbook_data.append((
                        next(self.orderbook_id),
                        stock_id,
                        timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                        bid1, random.randint(10000, 500000),
                        ask1, random.randint(10000, 500000),
                        bid2, random.randint(5000, 300000),
                        ask2, random.randint(5000, 300000),
                        bid3, random.randint(1000, 200000),
                        ask3, random.randint(1000, 200000)
                    ))
                    
                    if len(orderbook_data) >= batch_size:
                        try:
                            cursor.executemany('''
                                INSERT INTO order_book (orderbook_id, stock_id, timestamp,
                                    bid_price_lv1, bid_volume_lv1, ask_price_lv1, ask_volume_lv1,
                                    bid_price_lv2, bid_volume_lv2, ask_price_lv2, ask_volume_lv2,
                                    bid_price_lv3, bid_volume_lv3, ask_price_lv3, ask_volume_lv3)
                                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                            ''', orderbook_data)
                            connection.commit()
                            total_inserted += len(orderbook_data)
                            orderbook_data = []
                        except Exception as e:
                            print(f"\n Lỗi insert batch: {e}")
                            connection.rollback()
            
            print("✓")
        
        # Insert records còn lại
        if orderbook_data:
            try:
                cursor.executemany('''
                    INSERT INTO order_book (orderbook_id, stock_id, timestamp,
                        bid_price_lv1, bid_volume_lv1, ask_price_lv1, ask_volume_lv1,
                        bid_price_lv2, bid_volume_lv2, ask_price_lv2, ask_volume_lv2,
                        bid_price_lv3, bid_volume_lv3, ask_price_lv3, ask_volume_lv3)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ''', orderbook_data)
                connection.commit()
                total_inserted += len(orderbook_data)
            except Exception as e:
                print(f"Lỗi insert batch cuối: {e}")
        
        cursor.close()
        print(f"✅ Đã insert tổng cộng {total_inserted:,} order book records")
    
    def insert_trade_tick(self, connection, days=30):
        cursor = connection.cursor()
        
        print("\n" + "="*80)
        print(f"💱 BƯỚC 4: INSERT TRADE TICK ({days} ngày)")
        print("="*80)
        
        batch_size = 2000
        trade_tick_data = []
        total_inserted = 0
        buyer_types = ['Individual', 'Institution', 'Foreign']
        seller_types = ['Individual', 'Institution', 'Foreign']
        
        for stock_id in range(1, 16):
            ticker = self.stocks[stock_id - 1][2]
            ref_price = self.stock_latest_prices[stock_id]
            floor, ceiling = self.get_price_limits(ref_price)
            
            print(f"Đang tạo trade tick cho {ticker}...", end=" ")
            
            for day_offset in range(days):
                trade_date = datetime.now() - timedelta(days=day_offset)
                
                if trade_date.weekday() >= 5:
                    continue
                
                num_trades = random.randint(200, 300)
                
                for i in range(num_trades):
                    hour = 9 + (i // 43)
                    minute = random.randint(0, 59)
                    second = random.randint(0, 59)
                    timestamp = trade_date.replace(hour=hour, minute=minute, second=second)
                    
                    price = round(ref_price * random.uniform(0.97, 1.03), -2)
                    price = max(floor, min(ceiling, price))
                    
                    volume = random.randint(100, 50000)
                    
                    trade_tick_data.append((
                        next(self.trade_id),
                        stock_id,
                        timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                        price, volume,
                        random.choice(buyer_types),
                        random.choice(seller_types)
                    ))
                    
                    if len(trade_tick_data) >= batch_size:
                        try:
                            cursor.executemany('''
                                INSERT INTO trade_tick (trade_id, stock_id, timestamp, price, volume, buyer_type, seller_type)
                                VALUES (%s,%s,%s,%s,%s,%s,%s)
                            ''', trade_tick_data)
                            connection.commit()
                            total_inserted += len(trade_tick_data)
                            trade_tick_data = []
                        except Exception as e:
                            print(f"\n Lỗi insert batch: {e}")
                            connection.rollback()
            
            print("✓")
        
        # Insert records còn lại
        if trade_tick_data:
            try:
                cursor.executemany('''
                    INSERT INTO trade_tick (trade_id, stock_id, timestamp, price, volume, buyer_type, seller_type)
                    VALUES (%s,%s,%s,%s,%s,%s,%s)
                ''', trade_tick_data)
                connection.commit()
                total_inserted += len(trade_tick_data)
            except Exception as e:
                print(f"Lỗi insert batch cuối: {e}")
        
        cursor.close()
        print(f"Đã insert tổng cộng {total_inserted:,} trade tick records")
    
    def insert_price_in_day(self, connection, days=30):
        """Insert Price in Day"""
        cursor = connection.cursor()
        
        print("\n" + "="*80)
        print(f"BƯỚC 5: INSERT PRICE IN DAY ({days} ngày)")
        print("="*80)
        
        batch_size = 2000
        price_inday_data = []
        total_inserted = 0
        
        for stock_id in range(1, 16):
            ticker = self.stocks[stock_id - 1][2]
            ref_price = self.stock_latest_prices[stock_id]
            floor, ceiling = self.get_price_limits(ref_price)
            
            print(f"Đang tạo price in day cho {ticker}...", end=" ")
            
            for day_offset in range(days):
                trade_date = datetime.now() - timedelta(days=day_offset)
                
                if trade_date.weekday() >= 5:
                    continue
                
                cumulative_vol = 0
                
                for i in range(100):
                    hour = 9 + (i // 15)
                    minute = (i % 15) * 4
                    timestamp = trade_date.replace(hour=hour, minute=minute, second=0)
                    
                    last_price = round(ref_price * random.uniform(0.98, 1.02), -2)
                    last_price = max(floor, min(ceiling, last_price))
                    
                    best_bid = round(last_price * random.uniform(0.998, 0.9999), -2)
                    best_ask = round(last_price * random.uniform(1.0001, 1.002), -2)
                    
                    cumulative_vol += random.randint(5000, 50000)
                    
                    price_inday_data.append((
                        next(self.intraday_id),
                        stock_id,
                        timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                        last_price, best_bid, best_ask, cumulative_vol
                    ))
                    
                    if len(price_inday_data) >= batch_size:
                        try:
                            cursor.executemany('''
                                INSERT INTO price_in_day (intraday_id, stock_id, timestamp, last_price, best_bid, best_ask, volume_accumulation)
                                VALUES (%s,%s,%s,%s,%s,%s,%s)
                            ''', price_inday_data)
                            connection.commit()
                            total_inserted += len(price_inday_data)
                            price_inday_data = []
                        except Exception as e:
                            print(f"\n Lỗi insert batch: {e}")
                            connection.rollback()
            
            print("✓")
        
        # Insert records còn lại
        if price_inday_data:
            try:
                cursor.executemany('''
                    INSERT INTO price_in_day (intraday_id, stock_id, timestamp, last_price, best_bid, best_ask, volume_accumulation)
                    VALUES (%s,%s,%s,%s,%s,%s,%s)
                ''', price_inday_data)
                connection.commit()
                total_inserted += len(price_inday_data)
            except Exception as e:
                print(f"Lỗi insert batch cuối: {e}")
        
        cursor.close()
        print(f"Đã insert tổng cộng {total_inserted:,} price in day records")
    
    def insert_market_index(self, connection, days=730):
        """Insert Market Index và lịch sử"""
        cursor = connection.cursor()
        
        print("\n" + "="*80)
        print(f"BƯỚC 6: INSERT MARKET INDEX & HISTORY ({days} ngày)")
        print("="*80)
        
        try:
            # Insert market indices
            cursor.executemany(
                'INSERT IGNORE INTO market_index (index_id, index_code, name) VALUES (%s,%s,%s)',
                [(1, "VNINDEX", "VN-Index"), (2, "VN30", "VN30-Index"), (3, "HNX", "HNX-Index")]
            )
            connection.commit()
            print("Đã insert 3 market indices")
            
            # Generate index history
            print("Đang tạo market index history...")
            index_data = []
            index_value = 1200.0
            current_date = datetime.now() - timedelta(days=days)
            
            for day in range(days):
                if current_date.weekday() >= 5:
                    current_date += timedelta(days=1)
                    continue
                
                change = random.uniform(-0.02, 0.025)
                open_idx = round(index_value * random.uniform(0.998, 1.002), 2)
                close_idx = round(index_value * (1 + change), 2)
                high_idx = round(max(open_idx, close_idx) * random.uniform(1.002, 1.008), 2)
                low_idx = round(min(open_idx, close_idx) * random.uniform(0.992, 0.998), 2)
                
                volume_total = random.randint(500000000, 800000000)
                value_total = random.uniform(15000, 25000) * 1000000000
                
                for idx_id in [1, 2, 3]:
                    multiplier = 0.95 if idx_id == 3 else 1.0
                    index_data.append((
                        next(self.idx_hist_id),
                        idx_id,
                        current_date.strftime('%Y-%m-%d'),
                        open_idx * multiplier, high_idx * multiplier,
                        low_idx * multiplier, close_idx * multiplier,
                        volume_total, value_total
                    ))
                
                index_value = close_idx
                current_date += timedelta(days=1)
            
            cursor.executemany('''
                INSERT INTO market_index_history (idx_hist_id, index_id, trade_date, open, high, low, close, volume, value)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ''', index_data)
            connection.commit()
            print(f"Đã insert {len(index_data):,} market index history records")
            
        except Exception as e:
            print(f"Lỗi insert market index: {e}")
            connection.rollback()
        finally:
            cursor.close()
    
    def insert_news(self, connection, num_news=500):
        """Insert tin tức"""
        cursor = connection.cursor()
        
        print("\n" + "="*80)
        print(f"BƯỚC 7: INSERT NEWS ({num_news} records)")
        print("="*80)
        
        news_titles = [
            "Kết quả kinh doanh quý {} vượt kỳ vọng",
            "Công bố kế hoạch mở rộng sản xuất",
            "Ký kết hợp đồng lớn với đối tác nước ngoài",
            "Ra mắt sản phẩm mới trong quý tới",
            "Họp đại hội cổ đông thường niên",
            "Thông báo trả cổ tức năm {}",
            "Đầu tư vào công nghệ mới",
            "Mở rộng thị trường xuất khẩu",
        ]
        
        news_data = []
        start_date = datetime.now() - timedelta(days=730)
        
        print("Đang tạo news data...", end=" ")
        
        for i in range(num_news):
            company_id = random.randint(1, 15)
            title = random.choice(news_titles).format(random.randint(1, 4))
            content = f"Nội dung chi tiết về {title.lower()} của công ty..."
            author = random.choice(["VnExpress", "CafeF", "Đầu tư", "Bloomberg Vietnam"])
            pub_date = start_date + timedelta(days=random.randint(0, 729))
            
            news_data.append((
                next(self.news_id),
                company_id, title, content, author,
                pub_date.strftime('%Y-%m-%d %H:%M:%S')
            ))
        
        try:
            cursor.executemany('''
                INSERT INTO news (news_id, company_id, title, content, author, publish_time)
                VALUES (%s,%s,%s,%s,%s,%s)
            ''', news_data)
            connection.commit()
            print("✓")
            print(f"Đã insert {len(news_data):,} news records")
        except Exception as e:
            print(f"\nLỗi insert news: {e}")
            connection.rollback()
        finally:
            cursor.close()
    
    def insert_financial_statements(self, connection):
        """Insert báo cáo tài chính"""
        
        print("\n" + "="*80)
        print("BƯỚC 8: INSERT FINANCIAL STATEMENTS")
        print("="*80)
        
        cursor = connection.cursor()
        start_date = datetime.now() - timedelta(days=730)
        
        for table in ['balance_sheet', 'income_statement', 'intraday_flow']:
            print(f"Đang tạo {table}...", end=" ")
            data = []
            
            for i in range(60):
                company_id = random.randint(1, 15)
                timestamp = (start_date + timedelta(days=random.randint(0, 729))).strftime('%Y-%m-%d %H:%M:%S')
                metric_value = random.uniform(1e8, 1e12)
                
                data.append((
                    i + 1, f"{table}_{i+1}", timestamp,
                    company_id, metric_value, timestamp
                ))
            
            try:
                if table == 'balance_sheet':
                    cursor.executemany('''
                        INSERT INTO balance_sheet (ind_code, ind_name, time_stamp, company_id, value, update_time)
                        VALUES (%s,%s,%s,%s,%s,%s)
                    ''', data)
                elif table == 'intraday_flow':
                    cursor.executemany(f'''
                        INSERT INTO {table} (int_code, int_name, time_stamp, company_id, value, update_time)
                        VALUES (%s,%s,%s,%s,%s,%s)
                    ''', data)
                else:
                    cursor.executemany('''
                        INSERT INTO income_statement (ind_code, ind_name, time_stamp, company_id, value, update_time)
                        VALUES (%s,%s,%s,%s,%s,%s)
                    ''', data)
                
                connection.commit()
                print("✓")
                print(f"  Đã insert {len(data)} {table} records")
            except Exception as e:
                print(f"\n Lỗi insert {table}: {e}")
                connection.rollback()
        
        cursor.close()
    
    def count_all_records(self, connection):
        """Đếm và hiển thị số lượng records của tất cả bảng"""
        cursor = connection.cursor()
        
        print("\n" + "="*80)
        print("THỐNG KÊ SỐ LƯỢNG BẢN GHI TỪNG BẢNG")
        print("="*80 + "\n")
        
        tables_info = [
            ("company", "Công ty"),
            ("stock", "Mã chứng khoán"),
            ("market_index", "Chỉ số thị trường"),
            ("price_history", "Lịch sử giá"),
            ("market_index_history", "Lịch sử chỉ số"),
            ("order_book", "Sổ lệnh"),
            ("trade_tick", "Khớp lệnh"),
            ("price_in_day", "Giá trong ngày"),
            ("news", "Tin tức"),
            ("balance_sheet", "Bảng cân đối kế toán"),
            ("income_statement", "Báo cáo thu nhập"),
            ("intraday_flow", "Dòng tiền trong ngày")
        ]
        
        total_records = 0
        for table_name, description in tables_info:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                count = cursor.fetchone()[0]
                total_records += count
                print(f"  {description:30} ({table_name:25}): {count:>10,} bản ghi")
            except Exception as e:
                print(f"  {description:30} ({table_name:25}): ❌ Lỗi: {e}")
        
        print("\n" + "="*80)
        print(f"TỔNG CỘNG: {total_records:,} bản ghi")
        print("="*80)
        
        cursor.close()


def main():
    """
    Hàm main để chạy toàn bộ quá trình tạo fake data
    """
    print("="*80)
    print("BẮT ĐẦU TẠO FAKE DATA CHO HỆ THỐNG CHỨNG KHOÁN")
    print("="*80)
    
    # Kết nối database
    connection = DatabaseConfig.get_connection()
    
    if not connection:
        print("Không thể kết nối database. Vui lòng kiểm tra cấu hình!")
        return
    
    # Khởi tạo generator
    generator = StockDataGenerator()
    
    try:
        # Bước 1: Insert companies & stocks
        generator.insert_companies_and_stocks(connection)
        
        # Bước 2: Insert price history (2 năm)
        generator.insert_price_history(connection, days=730)
        
        # Bước 3: Insert order book (30 ngày gần nhất)
        generator.insert_order_book(connection, days=30, snapshots_per_day=500)
        
        # Bước 4: Insert trade tick (30 ngày gần nhất)
        generator.insert_trade_tick(connection, days=30)
        
        # Bước 5: Insert price in day (30 ngày gần nhất)
        generator.insert_price_in_day(connection, days=30)
        
        # Bước 6: Insert market index & history
        generator.insert_market_index(connection, days=730)
        
        # Bước 7: Insert news
        generator.insert_news(connection, num_news=500)
        
        # Bước 8: Insert financial statements
        generator.insert_financial_statements(connection)
        
        # Hiển thị thống kê
        generator.count_all_records(connection)
        
        print("\n" + "="*80)
        print("🎉 HOÀN THÀNH TẠO FAKE DATA!")
        print("="*80)
        
    except Exception as e:
        print(f"\n❌ Lỗi trong quá trình tạo data: {e}")
        
    finally:
        # Đóng kết nối
        DatabaseConfig.close_connection(connection)


if __name__ == "__main__":
    main()
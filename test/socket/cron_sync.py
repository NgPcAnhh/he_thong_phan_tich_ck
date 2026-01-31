import asyncio
import aiohttp
import asyncpg
import json
from datetime import datetime

# --- 1. CẤU HÌNH DATABASE ---
DB_DSN = "postgresql://postgres:151104@localhost:5432/stock_database_vn"

# --- 2. CẤU HÌNH API ENDPOINTS ---
URL_EXCHANGE = "https://iboard-query.ssi.com.vn/stock/exchange/{}?boardId=MAIN"
URL_GROUP = "https://iboard-query.ssi.com.vn/stock/group/{}"

# --- 3. DANH SÁCH CẦN QUÉT ---
# SỬA: Bỏ "hosebond" và "hnxbond" để không tính trái phiếu vào Index
EXCHANGES = ["hose", "hnx", "upcom"]

GROUPS = [
    "VN30", "HNX30", "VN100", "VNX50", "VNDIAMOND", "VNFINLEAD",
    "VNFIN", "VNREAL", "VNIND", "VNMAT", "VNCONS", "VNENE",
    "VNUTI", "VNIT", "VNHEAL", "VNSML", "VNMID", "VNFINSELECT",
    "VNCOND", "VNSI", "VNXALL", "VNALL",
]

# --- 4. CẤU HÌNH ĐỘ ƯU TIÊN ---
PRIORITY_MAP = {
    "VN30": 1, "HNX30": 1,
    "VNDIAMOND": 2, "VNFINLEAD": 2,
    "VN100": 3, "VNX50": 3,
    "VNFIN": 4, "VNREAL": 4, "VNIND": 4, "VNMAT": 4,
}

HEADERS = {
    "Authority": "iboard-query.ssi.com.vn",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

async def fetch_base_data_from_exchanges(session):
    """GIAI ĐOẠN 1: Lấy dữ liệu nền từ các sàn"""
    print("\n--- 🏁 GIAI ĐOẠN 1: LẤY DỮ LIỆU NỀN TỪ SÀN ---")
    stock_dict = {}

    for exchange in EXCHANGES:
        url = URL_EXCHANGE.format(exchange)
        print(f"📡 Đang tải sàn: {exchange.upper()}...")

        try:
            async with session.get(url, headers=HEADERS, timeout=20) as resp:
                if resp.status == 200:
                    res_json = await resp.json()
                    data = res_json.get("data", [])
                    print(f"   -> ✅ Tìm thấy {len(data)} mã trên sàn {exchange.upper()}")

                    count_added = 0
                    for item in data:
                        symbol = item.get("stockSymbol")
                        # SỬA: Lọc loại chứng khoán. 
                        # SSI thường dùng: 1=Stock, 2=ETF, 3=CW. Hoặc check ssType/stockType
                        # Để an toàn, ta lọc cơ bản: Mã dài <= 3 ký tự thường là Stock (trừ ETF như E1VFVN30)
                        # Hoặc kiểm tra trường 'stockType' nếu API trả về. 
                        # Ở đây tôi dùng độ dài symbol làm bộ lọc sơ bộ hiệu quả nhất cho Index.
                        
                        # Logic lọc: 
                        # - Mã <= 3 ký tự: Chắc chắn là Cổ phiếu (VIC, FPT...) -> Lấy
                        # - Mã > 3 ký tự: Có thể là ETF (E1VFVN30), CW (CVIC...), Bond -> Tạm bỏ nếu muốn khớp VNINDEX
                        if symbol and len(symbol) <= 3: 
                            name = item.get("companyNameVi") or item.get("stockName") or ""
                            
                            stock_dict[symbol] = {
                                "symbol": symbol,
                                "company_name": name,
                                "exchange": exchange.upper(),
                                "groups": set(), 
                            }
                            count_added += 1
                    
                    print(f"   -> 🟢 Đã lọc lấy {count_added} Cổ phiếu (bỏ ETF/CW/Bond).")
                        
                else:
                    print(f"   -> ⚠️ Lỗi HTTP {resp.status}")
        except Exception as e:
            print(f"   -> ❌ Lỗi kết nối: {e}")

    return stock_dict


async def enrich_group_info(session, stock_dict):
    """GIAI ĐOẠN 2: Cộng dồn thông tin nhóm chỉ số"""
    print("\n--- 🚀 GIAI ĐOẠN 2: QUÉT VÀ CỘNG DỒN CHỈ SỐ (GROUPS) ---")

    for group in GROUPS:
        url = URL_GROUP.format(group)
        
        try:
            async with session.get(url, headers=HEADERS, timeout=15) as resp:
                if resp.status == 200:
                    res_json = await resp.json()
                    data = res_json.get("data", [])
                    
                    count_update = 0
                    for item in data:
                        symbol = item.get("stockSymbol")
                        # Chỉ cập nhật group cho những mã đã có trong danh sách Cổ phiếu (Giai đoạn 1)
                        if symbol and symbol in stock_dict:
                            stock_dict[symbol]["groups"].add(group)
                            count_update += 1
                            
                    if count_update > 0:
                        print(f"   -> 🏷️  Nhóm {group}: Đã gán thêm cho {count_update} mã.")

        except Exception as e:
            print(f"   -> ❌ Lỗi quét nhóm {group}: {e}")

    return list(stock_dict.values())


def format_group_string(group_set):
    """Hàm phụ trợ: Chuyển Set nhóm thành String được sắp xếp theo độ ưu tiên"""
    if not group_set:
        return None
    sorted_groups = sorted(
        list(group_set), 
        key=lambda x: (PRIORITY_MAP.get(x, 100), x)
    )
    return ",".join(sorted_groups)


async def sync_to_db():
    print(f"[{datetime.now()}] KHỞI ĐỘNG TIẾN TRÌNH ĐỒNG BỘ DỮ LIỆU ĐA CHỈ SỐ...")

    # Thời điểm bắt đầu chạy sync - dùng để dọn dẹp mã rác sau này
    sync_start_time = datetime.now()

    connector = aiohttp.TCPConnector(ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        stock_dict = await fetch_base_data_from_exchanges(session)
        if not stock_dict:
            return

        final_list = await enrich_group_info(session, stock_dict)

    print(f"\n📊 TỔNG HỢP: Thu thập được {len(final_list)} mã cổ phiếu thực tế.")

    try:
        print("🔌 Đang kết nối Database...")
        conn = await asyncpg.connect(DB_DSN)

        # Đảm bảo cột group_code là TEXT
        await conn.execute("ALTER TABLE stock_infos ALTER COLUMN group_code TYPE TEXT;")

        records = []
        vn30_check = 0
        
        for s in final_list:
            group_str = format_group_string(s["groups"])
            if group_str and "VN30" in group_str: 
                vn30_check += 1
            
            records.append((
                s["symbol"],
                s["company_name"],
                s["exchange"],
                group_str,
                True,            # active = True
                sync_start_time  # update_at = Thời điểm chạy lệnh này
            ))

        print(f"   -> Kiểm tra: Có {vn30_check} mã chứa nhãn VN30.")
        print(f"📥 Đang upsert {len(records)} bản ghi vào Database...")

        await conn.executemany("""
            INSERT INTO stock_infos (symbol, company_name, exchange, group_code, active, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (symbol) DO UPDATE SET
                company_name = EXCLUDED.company_name,
                exchange = EXCLUDED.exchange,
                group_code = EXCLUDED.group_code,
                active = EXCLUDED.active,
                updated_at = EXCLUDED.updated_at;
        """, records)

        # --- SỬA QUAN TRỌNG: HUỶ KÍCH HOẠT MÃ CŨ ---
        # Những mã nào KHÔNG được cập nhật trong lần chạy này (updated_at < sync_start_time)
        # nghĩa là không còn tìm thấy trên bảng giá -> Set active = FALSE
        print("🧹 Đang dọn dẹp các mã hủy niêm yết hoặc không còn giao dịch...")
        result = await conn.execute("""
            UPDATE stock_infos 
            SET active = FALSE 
            WHERE updated_at < $1 AND active = TRUE
        """, sync_start_time)
        
        print(f"   -> 🗑️  Đã vô hiệu hóa: {result}")

        print("✅ ĐỒNG BỘ HOÀN TẤT THÀNH CÔNG!")

    except Exception as e:
        print(f"❌ Lỗi SQL: {e}")
    finally:
        if 'conn' in locals():
            await conn.close()


if __name__ == "__main__":
    try:
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    except:
        pass
    asyncio.run(sync_to_db())
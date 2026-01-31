import asyncio
import os
import signal
import sys
from datetime import datetime
from typing import List, Dict, Any, Optional
import asyncpg
import orjson
import redis.asyncio as redis
from websockets.asyncio.client import connect
from dotenv import load_dotenv
from loguru import logger

# uvloop chỉ hoạt động trên Linux/Mac
if sys.platform != 'win32':
    try:
        import uvloop
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        logger.info("✅ Using uvloop")
    except ImportError:
        logger.warning("⚠️ uvloop not available, using default event loop")
else:
    logger.info("ℹ️ Running on Windows, using default event loop")

try:
    from zoneinfo import ZoneInfo
    VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")
except ImportError:
    from datetime import timezone, timedelta
    VN_TZ = timezone(timedelta(hours=7))
    logger.warning("⚠️ zoneinfo not available, using UTC+7 offset")

load_dotenv()

WS_URL = os.getenv("WS_URL", "wss://stream2.simplize.vn/ws")
DB_DSN = os.getenv("DB_DSN", "postgresql://postgres:123456789@localhost:5432/toc_database")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
TEST_MODE = os.getenv("TEST_MODE", "false").lower() == "true"

HEADERS = {
    "Origin": "https://simplize.vn",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
}

SUB_BATCH_SIZE = 500
DB_BATCH_SIZE = 2000
RETENTION_DAYS = 7

INDEX_CODES = [
    "VNINDEX", "VN30", "VN100", "VNALL", "VNXALL",
    "HNXINDEX", "HNX30", "HNXUPCOMINDEX"
]


class StockStreamer:
    def __init__(self):
        self.running = True
        self.db_pool: Optional[asyncpg.Pool] = None
        self.redis_client: Optional[redis.Redis] = None
        self.quote_queue = asyncio.Queue(maxsize=20000)
        self.candle_queue = asyncio.Queue(maxsize=20000)
        self.index_candle_queue = asyncio.Queue(maxsize=5000)
        self.quote_state: Dict[str, dict] = {}
        self.current_candles: Dict[str, Dict[int, Dict[str, Any]]] = {}

    async def initialize(self):
        """Initialize connections to Redis and PostgreSQL"""
        
        # Kiểm tra TEST_MODE
        if TEST_MODE:
            logger.warning("🧪 TEST MODE ENABLED - Running without DB/Redis")
            logger.info("📊 Will only print quote updates to console")
            return
        
        # Redis là optional, không bắt buộc
        try:
            self.redis_client = redis.Redis.from_url(REDIS_URL, decode_responses=True)
            await self.redis_client.ping()
            logger.info("✅ Redis Connected")
        except Exception as e:
            logger.warning(f"⚠️ Redis Failed: {e}")
            logger.info(f"ℹ️ Tiếp tục chạy mà không có Redis (chỉ lưu DB, không publish realtime)")
            self.redis_client = None

        # Database connection
        try:
            self.db_pool = await asyncpg.create_pool(DB_DSN, min_size=5, max_size=20)
            logger.info("✅ Database Pool Created")
        except Exception as e:
            logger.error(f"❌ Database connection failed: {e}")
            logger.error(f"💡 Tip: Set TEST_MODE=true in .env to run without database")
            sys.exit(1)

    async def shutdown(self):
        logger.warning("🛑 Shutting down...")
        self.running = False
        if not self.quote_queue.empty() or not self.candle_queue.empty():
            logger.info("⏳ Waiting for queues to drain...")
            await asyncio.sleep(2)
        if self.db_pool: await self.db_pool.close()
        if self.redis_client: await self.redis_client.aclose()
        logger.success("👋 Goodbye.")

    async def load_symbols(self) -> List[str]:
        fallback = ["VIC", "HPG", "FPT", "SSI", "VNM", "TCB"]
        if not self.db_pool: return fallback
        try:
            async with self.db_pool.acquire() as conn:
                rows = await conn.fetch("SELECT symbol FROM stock_infos WHERE active = TRUE")
                symbols = [r["symbol"] for r in rows]
                if symbols:
                    logger.info(f"✅ Loaded {len(symbols)} symbols from DB.")
                    return symbols
                return fallback
        except Exception as e:
            logger.error(f"❌ DB Load Error: {e}")
            return fallback

    def _safe_float(self, v):
        try:
            return float(v) if v is not None else None
        except:
            return None

    def _safe_int(self, v):
        try:
            return int(v) if v is not None else None
        except:
            return None

    def _minute_bucket(self, ts_ms: int):
        minute_key = ts_ms // 60000
        dt_vn = datetime.fromtimestamp(ts_ms / 1000.0, VN_TZ)
        bucket_time = dt_vn.replace(second=0, microsecond=0, tzinfo=None)
        return minute_key, bucket_time

    async def process_quote_payload(self, item: dict):
        symbol = item.get("s")
        if not symbol: return

        state = self.quote_state.setdefault(symbol, {})
        state.update(item)

        raw_ts = state.get("t")
        ts = self._safe_int(raw_ts)
        last_price = state.get("p") or state.get("c") or state.get("a")

        # ĐÃ XÓA: dòng check last_price is None return
        
        price_f = self._safe_float(last_price)
        ref_f = self._safe_float(state.get("r"))

        change_val = self._safe_float(state.get("pn")) or self._safe_float(state.get("cv"))
        change_pct = self._safe_float(state.get("pc")) or self._safe_float(state.get("cp"))

        if change_val is None and price_f is not None and ref_f is not None and ref_f > 0:
            change_val = price_f - ref_f
            change_pct = (change_val / ref_f) * 100

        # --- 1. GỬI REDIS (Luôn gửi để cập nhật Trần/Sàn/TC) ---
        if self.redis_client:
            update_msg = {
                "s": symbol,
                "p": price_f, "c": price_f,
                "v": self._safe_int(state.get("v")), "tv": self._safe_int(state.get("tv")),
                "tva": self._safe_float(state.get("tva")),
                "cp": change_pct, "cv": change_val,
                "r": ref_f, "ce": self._safe_float(state.get("ce")),
                "f": self._safe_float(state.get("f")),
                "h": self._safe_float(state.get("h")), "l": self._safe_float(state.get("l")),
                "bp1": self._safe_float(state.get("pb1")), "bq1": self._safe_int(state.get("qb1")),
                "ap1": self._safe_float(state.get("pa1")), "aq1": self._safe_int(state.get("qa1")),
                "fb": self._safe_int(state.get("bfq")), "fs": self._safe_int(state.get("sfq")),
                # Bổ sung các mức giá 2, 3 cho đầy đủ
                "bp2": self._safe_float(state.get("pb2")), "bq2": self._safe_int(state.get("qb2")),
                "bp3": self._safe_float(state.get("pb3")), "bq3": self._safe_int(state.get("qb3")),
                "ap2": self._safe_float(state.get("pa2")), "aq2": self._safe_int(state.get("qa2")),
                "ap3": self._safe_float(state.get("pa3")), "aq3": self._safe_int(state.get("qa3")),
                "fr": self._safe_float(state.get("fr"))
            }
            try:
                await self.redis_client.publish("stock_updates", orjson.dumps(update_msg))
            except:
                pass

        # --- 2. LƯU DB (Chỉ lưu khi có giá khớp để tránh dữ liệu rác) ---
        if last_price is not None:
            # TEST MODE: Chỉ in ra console
            if TEST_MODE:
                color = "🟢" if change_val and change_val > 0 else "🔴" if change_val and change_val < 0 else "🟡"
                logger.info(f"{color} {symbol}: {price_f:,.1f} ({change_pct:+.2f}%) | Vol: {self._safe_int(state.get('tv')):,}" if price_f else f"📊 {symbol}: State update")
                return  # Không lưu DB trong TEST_MODE
            
            record = {
                "symbol": symbol, "ts": ts,
                "last_price": last_price, "avg_price": state.get("a"),
                "last_volume": state.get("v"), "total_volume": state.get("tv"),
                "total_value": state.get("tva"),
                "foreign_buy_qty": state.get("bfq"), "foreign_sell_qty": state.get("sfq"),
                "foreign_buy_val": state.get("bfv"), "foreign_sell_val": state.get("sfv"),
                "foreign_room": state.get("fr"),
                "bid1_price": state.get("pb1"), "bid1_qty": state.get("qb1"),
                "bid2_price": state.get("pb2"), "bid2_qty": state.get("qb2"),
                "bid3_price": state.get("pb3"), "bid3_qty": state.get("qb3"),
                "ask1_price": state.get("pa1"), "ask1_qty": state.get("qa1"),
                "ask2_price": state.get("pa2"), "ask2_qty": state.get("qa2"),
                "ask3_price": state.get("pa3"), "ask3_qty": state.get("qa3"),
                "ref_price": state.get("r"), "ceil_price": state.get("ce"), "floor_price": state.get("f"),
                "change_percent": change_pct, "change_value": change_val,
                "high_price": state.get("h"), "low_price": state.get("l")
            }

            try:
                self.quote_queue.put_nowait(record)
            except asyncio.QueueFull:
                pass

            if ts is not None and price_f is not None:
                self.process_candle_aggregation(symbol, ts, price_f, self._safe_float(state.get("tv")))

    def process_candle_aggregation(self, symbol, ts, price_val, total_vol_val):
        minute_key, bucket_time = self._minute_bucket(ts)
        if not minute_key: return

        symbol_candles = self.current_candles.setdefault(symbol, {})
        candle = symbol_candles.get(minute_key)

        # Logic đẩy nến cũ vào queue khi sang phút mới
        if candle is None:
            for old_key, old_candle in list(symbol_candles.items()):
                if old_key != minute_key:
                    try:
                        if old_candle["symbol"] in INDEX_CODES:
                            self.index_candle_queue.put_nowait(old_candle)
                        else:
                            self.candle_queue.put_nowait(old_candle)
                    except:
                        pass
                    del symbol_candles[old_key]

            symbol_candles[minute_key] = {
                "symbol": symbol, "minute_key": minute_key, "bucket_time": bucket_time,
                "open_price": price_val, "high_price": price_val, "low_price": price_val, "close_price": price_val,
                "volume": 0.0, "last_total_volume": total_vol_val,
            }
        else:
            c = candle
            c["close_price"] = price_val
            c["high_price"] = max(c["high_price"], price_val)
            c["low_price"] = min(c["low_price"], price_val)
            if total_vol_val is not None:
                last_tv = c.get("last_total_volume")
                if last_tv is not None and total_vol_val >= last_tv:
                    c["volume"] += total_vol_val - last_tv
                c["last_total_volume"] = total_vol_val

    async def worker_db_index_candles(self):
        sql = """
        INSERT INTO market_index_minutes (symbol, bucket_time, open_price, high_price, low_price, close_price, volume)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (symbol, bucket_time) DO UPDATE SET
            high_price=GREATEST(market_index_minutes.high_price, EXCLUDED.high_price),
            low_price=LEAST(market_index_minutes.low_price, EXCLUDED.low_price),
            close_price=EXCLUDED.close_price, 
            volume=market_index_minutes.volume + EXCLUDED.volume;
        """
        logger.info("📊 Index Candle Worker Started")
        while self.running or not self.index_candle_queue.empty():
            try:
                batch = []
                try:
                    item = await self.index_candle_queue.get() if self.running else self.index_candle_queue.get_nowait()
                    batch.append(item)
                    for _ in range(50):  # Batch size nhỏ hơn cho index
                        batch.append(self.index_candle_queue.get_nowait())
                except asyncio.QueueEmpty:
                    pass

                if batch and self.db_pool:
                    values = [
                        (c["symbol"], c["bucket_time"], c["open_price"], c["high_price"], c["low_price"],
                         c["close_price"], c["volume"]) for c in batch
                    ]
                    async with self.db_pool.acquire() as conn:
                        await conn.executemany(sql, values)
            except asyncio.QueueEmpty:
                if not self.running: break
            except Exception as e:
                logger.error(f"Index DB Error: {e}")
                await asyncio.sleep(1)

    async def worker_db_quotes(self):
        sql = """
        INSERT INTO realtime_quotes (
            symbol, ts, last_price, avg_price, last_volume, total_volume, total_value,
            foreign_buy_qty, foreign_sell_qty, foreign_buy_val, foreign_sell_val, foreign_room,
            bid1_price, bid1_qty, bid2_price, bid2_qty, bid3_price, bid3_qty,
            ask1_price, ask1_qty, ask2_price, ask2_qty, ask3_price, ask3_qty,
            ref_price, ceil_price, floor_price, change_percent, change_value,
            high_price, low_price
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31)
        ON CONFLICT (symbol) DO UPDATE SET
            ts=EXCLUDED.ts, last_price=EXCLUDED.last_price, avg_price=EXCLUDED.avg_price,
            last_volume=EXCLUDED.last_volume, total_volume=EXCLUDED.total_volume, total_value=EXCLUDED.total_value,
            foreign_buy_qty=EXCLUDED.foreign_buy_qty, foreign_sell_qty=EXCLUDED.foreign_sell_qty,
            bid1_price=EXCLUDED.bid1_price, bid1_qty=EXCLUDED.bid1_qty, bid2_price=EXCLUDED.bid2_price, bid2_qty=EXCLUDED.bid2_qty,
            bid3_price=EXCLUDED.bid3_price, bid3_qty=EXCLUDED.bid3_qty,
            ask1_price=EXCLUDED.ask1_price, ask1_qty=EXCLUDED.ask1_qty, ask2_price=EXCLUDED.ask2_price, ask2_qty=EXCLUDED.ask2_qty,
            ask3_price=EXCLUDED.ask3_price, ask3_qty=EXCLUDED.ask3_qty,
            change_percent=EXCLUDED.change_percent, change_value=EXCLUDED.change_value,
            high_price=EXCLUDED.high_price, low_price=EXCLUDED.low_price;
        """
        logger.info("👷 Quote DB Worker Started")
        while self.running or not self.quote_queue.empty():
            try:
                batch = []
                try:
                    item = await self.quote_queue.get() if self.running else self.quote_queue.get_nowait()
                    batch.append(item)
                    for _ in range(DB_BATCH_SIZE - 1):
                        batch.append(self.quote_queue.get_nowait())
                except asyncio.QueueEmpty:
                    pass

                if batch and self.db_pool:
                    values = [
                        (r.get("symbol"), r.get("ts"), r.get("last_price"), r.get("avg_price"), r.get("last_volume"),
                         r.get("total_volume"), r.get("total_value"),
                         r.get("foreign_buy_qty"), r.get("foreign_sell_qty"), r.get("foreign_buy_val"),
                         r.get("foreign_sell_val"),
                         r.get("foreign_room"),
                         r.get("bid1_price"), r.get("bid1_qty"), r.get("bid2_price"), r.get("bid2_qty"),
                         r.get("bid3_price"), r.get("bid3_qty"),
                         r.get("ask1_price"), r.get("ask1_qty"), r.get("ask2_price"), r.get("ask2_qty"),
                         r.get("ask3_price"), r.get("ask3_qty"),
                         r.get("ref_price"), r.get("ceil_price"), r.get("floor_price"), r.get("change_percent"),
                         r.get("change_value"),
                         r.get("high_price"), r.get("low_price")) for r in batch
                    ]
                    async with self.db_pool.acquire() as conn:
                        await conn.executemany(sql, values)
            except asyncio.QueueEmpty:
                if not self.running: break
            except Exception as e:
                logger.error(f"DB Quote Error: {e}")
                await asyncio.sleep(1)

    async def worker_db_candles(self):
        sql = """
        INSERT INTO candles_1m (symbol, bucket_time, open_price, high_price, low_price, close_price, volume)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (symbol, bucket_time) DO UPDATE SET
            high_price=GREATEST(candles_1m.high_price, EXCLUDED.high_price),
            low_price=LEAST(candles_1m.low_price, EXCLUDED.low_price),
            close_price=EXCLUDED.close_price, 
            volume=candles_1m.volume + EXCLUDED.volume;
        """
        logger.info("👷 Candle DB Worker Started")
        while self.running or not self.candle_queue.empty():
            try:
                batch = []
                try:
                    item = await self.candle_queue.get() if self.running else self.candle_queue.get_nowait()
                    batch.append(item)
                    for _ in range(DB_BATCH_SIZE - 1):
                        batch.append(self.candle_queue.get_nowait())
                except asyncio.QueueEmpty:
                    pass

                if batch and self.db_pool:
                    values = [
                        (c["symbol"], c["bucket_time"], c["open_price"], c["high_price"], c["low_price"],
                         c["close_price"], c["volume"]) for c in batch
                    ]
                    async with self.db_pool.acquire() as conn:
                        await conn.executemany(sql, values)
            except asyncio.QueueEmpty:
                if not self.running: break
            except Exception:
                await asyncio.sleep(1)

    async def cleanup_task(self):
        while self.running:
            if self.db_pool:
                try:
                    async with self.db_pool.acquire() as conn:
                        await conn.execute("DELETE FROM candles_1m WHERE bucket_time < NOW() - $1::interval",
                                           f"{RETENTION_DAYS} days")
                except:
                    pass
            await asyncio.sleep(3600)

    async def listen_ws(self, symbols: List[str]):
        while self.running:
            try:
                async with connect(WS_URL, additional_headers=list(HEADERS.items()), max_size=None) as ws:
                    all_targets = list(set(symbols + INDEX_CODES))
                    logger.success(f"🔌 Connected to Simplize WS. Tracking {len(all_targets)} targets.")

                    for i in range(0, len(all_targets), SUB_BATCH_SIZE):
                        if not self.running: break
                        batch = all_targets[i: i + SUB_BATCH_SIZE]
                        msg = orjson.dumps({"event": "sub", "topic": "STOCK_RETIME_LIST", "params": batch}).decode(
                            'utf-8')
                        await ws.send(msg)
                        await asyncio.sleep(0.5)

                    async for msg in ws:
                        if not self.running: break
                        try:
                            payload = orjson.loads(msg)
                            topic = payload.get("topic")
                            event = payload.get("event")

                            if topic == "quotes":
                                data = payload.get("data")
                                if isinstance(data, list):
                                    for item in data: await self.process_quote_payload(item)
                                elif data:
                                    await self.process_quote_payload(data)
                            elif event == "ping":
                                await ws.send('{"event":"pong"}')
                        except Exception as parse_err:
                            logger.error(f"Parse JSON Error: {parse_err}")

            except Exception as e:
                logger.warning(f"⚠️ WS Disconnected: {e}. Retrying in 5s...")
                await asyncio.sleep(5)

    async def run(self):
        await self.initialize()
        symbols = await self.load_symbols()
        await asyncio.gather(
            self.listen_ws(symbols),
            self.worker_db_quotes(),
            self.worker_db_candles(),
            self.worker_db_index_candles(),
            self.cleanup_task()
        )


if __name__ == "__main__":
    streamer = StockStreamer()
    loop = asyncio.new_event_loop()


    def signal_handler():
        asyncio.create_task(streamer.shutdown())


    if sys.platform != "win32":
        loop.add_signal_handler(signal.SIGINT, signal_handler)
        loop.add_signal_handler(signal.SIGTERM, signal_handler)
    try:
        asyncio.run(streamer.run())
    except KeyboardInterrupt:
        asyncio.run(streamer.shutdown())
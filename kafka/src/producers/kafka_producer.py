"""
Kafka Producer for Real-time Stock Data
Connects to Simplize WebSocket and streams data to Kafka topics
"""
import asyncio
import json
from typing import List, Dict, Any
from datetime import datetime

from websockets.asyncio.client import connect
from vnstock import Listing
from kafka import KafkaProducer
from kafka.errors import KafkaError

from src.common import config

# Kafka Producers (thread-safe)
quote_producer: KafkaProducer | None = None
candle_producer: KafkaProducer | None = None

# In-memory state
QUOTE_STATE: dict[str, dict] = {}
CURRENT_CANDLE: dict[str, Dict[int, Dict[str, Any]]] = {}


def to_int_or_none(x) -> int | None:
    """Convert value to int or None if conversion fails"""
    if x is None:
        return None
    try:
        return int(x)
    except (TypeError, ValueError):
        return None


def minute_bucket_from_ts(ts_ms: int) -> tuple[int, datetime]:
    """
    Convert timestamp in milliseconds to minute bucket key and datetime
    Returns: (minute_key, bucket_time)
    """
    minute_key = ts_ms // 60000
    dt = datetime.utcfromtimestamp(ts_ms / 1000)
    bucket_time = dt.replace(second=0, microsecond=0)
    return minute_key, bucket_time


def load_vietnam_symbols(limit: int | None = None) -> List[str]:
    """
    Load Vietnamese stock symbols from vnstock
    """
    listing = Listing()
    df = listing.symbols_by_exchange()

    symbol_col = "symbol"
    exchange_col = "exchange"

    valid_exchanges = {"HSX", "HOSE", "HNX", "UPCOM"}

    df_filtered = df[df[exchange_col].isin(valid_exchanges)].copy()
    symbols = df_filtered[symbol_col].dropna().unique().tolist()

    if limit is not None:
        symbols = symbols[:limit]

    return symbols


def send_to_kafka(producer: KafkaProducer, topic: str, key: str, value: dict) -> None:
    """
    Send message to Kafka topic with error handling
    """
    try:
        # Convert dict to JSON string
        value_json = json.dumps(value, ensure_ascii=False)
        
        # Send to Kafka
        future = producer.send(
            topic=topic,
            key=key,
            value=value_json
        )
        
        # Optional: Wait for confirmation (blocking)
        # record_metadata = future.get(timeout=10)
        # print(f"✅ Sent to {topic}: partition={record_metadata.partition}, offset={record_metadata.offset}")
        
    except KafkaError as e:
        print(f"❌ Kafka error sending to {topic}: {e}")
    except Exception as e:
        print(f"❌ Error sending to {topic}: {e}")


def handle_quotes(payload: dict) -> None:
    """
    Handle 'quotes' topic from WebSocket
    Send quote data to Kafka and build candles
    """
    if payload.get("topic") != "quotes":
        return

    data_list = payload.get("data") or []
    if not isinstance(data_list, list):
        data_list = [data_list]

    print(f"\n📊 QUOTES (received {len(data_list)} symbols)")

    for item in data_list:
        symbol = item.get("s")
        if not symbol:
            continue

        is_index = symbol in config.INDEX_CODES

        # Update state
        state = QUOTE_STATE.setdefault(symbol, {})
        state.update(item)

        # Extract quote data
        raw_ts = state.get("t")
        ts = to_int_or_none(raw_ts)
        if ts is None and raw_ts is not None:
            print(f"⚠️ Cannot convert ts={raw_ts} for symbol={symbol}")

        last_price = state.get("p") or state.get("c") or state.get("a")
        avg_price = state.get("a")
        last_vol = state.get("v")
        total_vol = state.get("tv")
        total_val = state.get("tva")

        foreign_buy_qty = state.get("bfq")
        foreign_sell_qty = state.get("sfq")
        foreign_buy_val = state.get("bfv")
        foreign_sell_val = state.get("sfv")

        pb1, qb1 = state.get("pb1"), state.get("qb1")
        pb2, qb2 = state.get("pb2"), state.get("qb2")
        pb3, qb3 = state.get("pb3"), state.get("qb3")
        pa1, qa1 = state.get("pa1"), state.get("qa1")
        pa2, qa2 = state.get("pa2"), state.get("qa2")
        pa3, qa3 = state.get("pa3"), state.get("qa3")

        ref_price = state.get("r")
        ceil_price = state.get("ce")
        floor_price = state.get("f")
        change_percent = state.get("pc")
        change_value = state.get("pn")
        high_p = state.get("h")
        low_p = state.get("l")

        # Build quote record
        quote_record = {
            "symbol": symbol,
            "ts": ts,
            "timestamp_iso": datetime.utcfromtimestamp(ts / 1000).isoformat() if ts else None,
            "is_index": is_index,
            "last_price": last_price,
            "avg_price": avg_price,
            "last_volume": last_vol,
            "total_volume": total_vol,
            "total_value": total_val,
            "foreign_buy_qty": foreign_buy_qty,
            "foreign_sell_qty": foreign_sell_qty,
            "foreign_buy_val": foreign_buy_val,
            "foreign_sell_val": foreign_sell_val,
            "bid1_price": pb1,
            "bid1_qty": qb1,
            "bid2_price": pb2,
            "bid2_qty": qb2,
            "bid3_price": pb3,
            "bid3_qty": qb3,
            "ask1_price": pa1,
            "ask1_qty": qa1,
            "ask2_price": pa2,
            "ask2_qty": qa2,
            "ask3_price": pa3,
            "ask3_qty": qa3,
            "ref_price": ref_price,
            "ceil_price": ceil_price,
            "floor_price": floor_price,
            "change_percent": change_percent,
            "change_value": change_value,
            "high_price": high_p,
            "low_price": low_p
        }

        # Send to Kafka
        send_to_kafka(quote_producer, config.TOPIC_STOCK_QUOTES, symbol, quote_record)

        # Build candles (1-minute OHLCV)
        if ts is not None and last_price is not None:
            minute_key, bucket_time = minute_bucket_from_ts(ts)
            symbol_candles = CURRENT_CANDLE.setdefault(symbol, {})

            candle = symbol_candles.get(minute_key)
            price_f = float(last_price)
            tv_f = float(total_vol) if total_vol is not None else None

            if candle is None:
                # Flush old candles to Kafka
                for old_key, old_candle in list(symbol_candles.items()):
                    if old_key != minute_key:
                        candle_to_flush = {
                            "symbol": old_candle["symbol"],
                            "bucket_time": old_candle["bucket_time"].isoformat(),
                            "open_price": old_candle["open_price"],
                            "high_price": old_candle["high_price"],
                            "low_price": old_candle["low_price"],
                            "close_price": old_candle["close_price"],
                            "volume": old_candle["volume"],
                        }
                        send_to_kafka(candle_producer, config.TOPIC_STOCK_CANDLES, symbol, candle_to_flush)
                        del symbol_candles[old_key]

                # Create new candle
                candle = {
                    "symbol": symbol,
                    "minute_key": minute_key,
                    "bucket_time": bucket_time,
                    "open_price": price_f,
                    "high_price": price_f,
                    "low_price": price_f,
                    "close_price": price_f,
                    "volume": 0.0,
                    "last_total_volume": tv_f,
                }
                symbol_candles[minute_key] = candle
            else:
                # Update existing candle
                candle["close_price"] = price_f
                candle["high_price"] = max(candle["high_price"], price_f)
                candle["low_price"] = min(candle["low_price"], price_f)
                if tv_f is not None:
                    last_tv = candle.get("last_total_volume")
                    if last_tv is not None and tv_f >= last_tv:
                        candle["volume"] += tv_f - last_tv
                    candle["last_total_volume"] = tv_f


def handle_stock_retime_list(payload: dict) -> None:
    """
    Handle STOCK_RETIME_LIST topic (optional, for debugging)
    """
    if payload.get("topic") != "STOCK_RETIME_LIST":
        return
    data_list = payload.get("data") or []
    if not isinstance(data_list, list):
        data_list = [data_list]
    print(f"\n📈 STOCK_RETIME_LIST count={len(data_list)}")


async def subscribe_batches(ws, symbols: List[str]) -> None:
    """
    Subscribe to symbols in batches
    """
    total = len(symbols)
    if total == 0:
        print("⚠️ No symbols to subscribe")
        return

    print(f"📦 Subscribing to {total} symbols (batch size = {config.SUB_BATCH_SIZE})")

    for i in range(0, total, config.SUB_BATCH_SIZE):
        batch = symbols[i: i + config.SUB_BATCH_SIZE]
        msg = {
            "event": "sub",
            "topic": "STOCK_RETIME_LIST",
            "params": batch,
        }
        msg_str = json.dumps(msg)
        print(f"📤 Sending subscription batch {i // config.SUB_BATCH_SIZE + 1} ({len(batch)} symbols)")
        await ws.send(msg_str)
        await asyncio.sleep(0.2)


async def listen(symbols: List[str]) -> None:
    """
    Connect to WebSocket and listen for messages
    """
    async with connect(
        config.WEBSOCKET_URL,
        additional_headers=list(config.WEBSOCKET_HEADERS.items()),
        max_size=None,
    ) as ws:
        print("✅ Connected to Simplize WebSocket")

        await subscribe_batches(ws, symbols)

        async for msg in ws:
            if isinstance(msg, bytes):
                try:
                    msg = msg.decode("utf-8")
                except Exception:
                    print("⚠️ Cannot decode bytes, skipping")
                    continue

            try:
                payload = json.loads(msg)
            except json.JSONDecodeError:
                print("⚠️ Not JSON:", msg[:120])
                continue

            event = payload.get("event")

            if event == "ping":
                pong_msg = json.dumps({"event": "pong"})
                print("📥 ping  →  📤 pong")
                await ws.send(pong_msg)
                continue
            elif event == "sub":
                print("✅ Subscribed topic:", payload.get("topic"))

            topic = payload.get("topic")
            if topic == "quotes":
                handle_quotes(payload)
            elif topic == "STOCK_RETIME_LIST":
                handle_stock_retime_list(payload)
            else:
                print("ℹ️ Other message:", payload)


async def main() -> None:
    """
    Main entry point
    """
    global quote_producer, candle_producer

    # Load symbols
    syms = load_vietnam_symbols(limit=config.SYMBOL_LIMIT)
    print(f"🔎 Loaded {len(syms)} symbols (examples: {syms[:10]})")

    # Initialize Kafka producers
    print("🔧 Initializing Kafka producers...")
    quote_producer = KafkaProducer(**config.PRODUCER_CONFIG)
    candle_producer = KafkaProducer(**config.PRODUCER_CONFIG)
    print("✅ Kafka producers initialized")

    try:
        # Start listening to WebSocket
        await listen(syms)
    except KeyboardInterrupt:
        print("\n⚠️ Shutting down...")
    finally:
        # Close producers
        if quote_producer:
            quote_producer.flush()
            quote_producer.close()
        if candle_producer:
            candle_producer.flush()
            candle_producer.close()
        print("✅ Kafka producers closed")


if __name__ == "__main__":
    asyncio.run(main())

"""
Fake Kafka Producer for Real-time Stock Data (DEMO MODE)
Generates simulated stock quote data for testing Kafka → DB pipeline
WITHOUT requiring real WebSocket connection
"""
import asyncio
import json
import random
from typing import List, Dict
from datetime import datetime

from vnstock import Listing
from kafka import KafkaProducer
from kafka.errors import KafkaError

import config

# Kafka Producer
quote_producer: KafkaProducer | None = None

# In-memory price state for realistic random walk
PRICE_STATE: Dict[str, float] = {}


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


def generate_fake_quote(symbol: str) -> dict:
    """
    Generate one fake quote message with realistic random price fluctuation
    
    Args:
        symbol: Stock symbol (e.g. 'VNM', 'HPG')
    
    Returns:
        Dict containing fake quote data matching real socket format
    """
    # Initialize base price if not exists (random 10k-100k VND)
    if symbol not in PRICE_STATE:
        PRICE_STATE[symbol] = random.uniform(10000, 100000)
    
    # Random walk: ±0.5% per update
    base_price = PRICE_STATE[symbol]
    change_percent = random.uniform(-0.5, 0.5)
    new_price = base_price * (1 + change_percent / 100)
    
    # Update state
    PRICE_STATE[symbol] = new_price
    
    # Generate realistic bid/ask spread (~0.1%)
    spread = new_price * 0.001
    
    # Reference price (yesterday's close, assumed)
    ref_price = base_price * random.uniform(0.98, 1.02)
    ceil_price = ref_price * 1.07  # +7% ceiling (Vietnam market rule)
    floor_price = ref_price * 0.93  # -7% floor
    
    # Random volume
    total_volume = random.randint(100000, 5000000)
    last_volume = random.randint(100, 10000)
    total_value = new_price * total_volume
    
    # Foreign trading
    foreign_buy_qty = random.randint(0, total_volume // 10)
    foreign_sell_qty = random.randint(0, total_volume // 10)
    
    # Bid/Ask levels
    bid1_price = new_price - spread
    bid2_price = bid1_price - spread
    bid3_price = bid2_price - spread
    
    ask1_price = new_price + spread
    ask2_price = ask1_price + spread
    ask3_price = ask2_price + spread
    
    # Generate quote record
    ts = int(datetime.now().timestamp() * 1000)  # milliseconds
    
    quote_record = {
        "symbol": symbol,
        "ts": ts,
        "timestamp_iso": datetime.now().isoformat(),
        "is_index": False,
        "last_price": round(new_price, 2),
        "avg_price": round(new_price * random.uniform(0.995, 1.005), 2),
        "last_volume": last_volume,
        "total_volume": total_volume,
        "total_value": round(total_value, 2),
        "foreign_buy_qty": foreign_buy_qty,
        "foreign_sell_qty": foreign_sell_qty,
        "foreign_buy_val": round(foreign_buy_qty * new_price, 2),
        "foreign_sell_val": round(foreign_sell_qty * new_price, 2),
        "bid1_price": round(bid1_price, 2),
        "bid1_qty": random.randint(1000, 50000),
        "bid2_price": round(bid2_price, 2),
        "bid2_qty": random.randint(1000, 50000),
        "bid3_price": round(bid3_price, 2),
        "bid3_qty": random.randint(1000, 50000),
        "ask1_price": round(ask1_price, 2),
        "ask1_qty": random.randint(1000, 50000),
        "ask2_price": round(ask2_price, 2),
        "ask2_qty": random.randint(1000, 50000),
        "ask3_price": round(ask3_price, 2),
        "ask3_qty": random.randint(1000, 50000),
        "ref_price": round(ref_price, 2),
        "ceil_price": round(ceil_price, 2),
        "floor_price": round(floor_price, 2),
        "change_percent": round(change_percent, 4),
        "change_value": round(new_price - ref_price, 2),
        "high_price": round(new_price * 1.02, 2),
        "low_price": round(new_price * 0.98, 2),
    }
    
    return quote_record


def send_to_kafka(producer: KafkaProducer, topic: str, key: str, value: dict) -> None:
    """
    Send message to Kafka topic with error handling
    """
    try:
        value_json = json.dumps(value, ensure_ascii=False)
        
        producer.send(
            topic=topic,
            key=key,
            value=value_json
        )
        
    except KafkaError as e:
        print(f"❌ Kafka error sending to {topic}: {e}")
    except Exception as e:
        print(f"❌ Error sending to {topic}: {e}")


async def fake_streaming_loop(symbols: List[str], interval: float = 2.0, batch_size: int = 100):
    """
    Continuously generate fake quotes and send to Kafka
    
    Args:
        symbols: List of stock symbols
        interval: Seconds between batches (default 2 seconds)
        batch_size: Number of symbols to update per batch (default 100)
    """
    global quote_producer
    
    print(f"🚀 Starting FAKE streaming loop...")
    print(f"   - Total symbols: {len(symbols)}")
    print(f"   - Batch size: {batch_size}")
    print(f"   - Interval: {interval}s")
    print(f"   - Topic: {config.TOPIC_STOCK_QUOTES}")
    print()
    
    iteration = 0
    
    try:
        while True:
            iteration += 1
            
            # Select random batch of symbols to update
            batch_symbols = random.sample(symbols, min(batch_size, len(symbols)))
            
            # Generate and send fake quotes
            for symbol in batch_symbols:
                quote = generate_fake_quote(symbol)
                send_to_kafka(quote_producer, config.TOPIC_STOCK_QUOTES, symbol, quote)
            
            # Flush to ensure delivery
            quote_producer.flush()
            
            print(f"📤 Iteration {iteration}: Sent {len(batch_symbols)} fake quotes to Kafka")
            
            # Wait before next batch
            await asyncio.sleep(interval)
            
    except KeyboardInterrupt:
        print("\n⚠️ Shutting down fake producer...")


async def main() -> None:
    """
    Main entry point for fake producer
    """
    global quote_producer
    
    # Load symbols
    symbol_limit = config.SYMBOL_LIMIT if hasattr(config, 'SYMBOL_LIMIT') else 500
    syms = load_vietnam_symbols(limit=symbol_limit)
    print(f"🔎 Loaded {len(syms)} symbols")
    print(f"   Examples: {syms[:10]}")
    print()
    
    # Initialize Kafka producer
    print("🔧 Initializing Kafka producer...")
    quote_producer = KafkaProducer(**config.PRODUCER_CONFIG)
    print("✅ Kafka producer initialized")
    print()
    
    try:
        # Start fake streaming
        await fake_streaming_loop(syms, interval=2.0, batch_size=100)
    except KeyboardInterrupt:
        print("\n⚠️ Shutting down...")
    finally:
        # Close producer
        if quote_producer:
            quote_producer.flush()
            quote_producer.close()
        print("✅ Kafka producer closed")


if __name__ == "__main__":
    asyncio.run(main())

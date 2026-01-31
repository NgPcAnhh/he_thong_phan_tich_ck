"""
Kafka Consumer for Stock Data
Consumes from Kafka topics and writes to PostgreSQL database
"""
import asyncio
import json
from typing import List, Dict, Any
from datetime import datetime

from kafka import KafkaConsumer
from kafka.errors import KafkaError
import asyncpg

from src.common import config

# Database connection pool
DB_POOL: asyncpg.Pool | None = None


async def insert_batch_quotes(records: List[Dict[str, Any]]) -> None:
    """
    Insert batch of quote records into PostgreSQL
    """
    global DB_POOL
    if not records or DB_POOL is None:
        return

    sql = f"""
    INSERT INTO {config.DB_SCHEMA}.realtime_quotes (
        symbol, ts,
        last_price, avg_price,
        last_volume, total_volume, total_value,
        foreign_buy_qty, foreign_sell_qty,
        foreign_buy_val, foreign_sell_val,
        bid1_price, bid1_qty,
        bid2_price, bid2_qty,
        bid3_price, bid3_qty,
        ask1_price, ask1_qty,
        ask2_price, ask2_qty,
        ask3_price, ask3_qty,
        ref_price, ceil_price, floor_price,
        change_percent, change_value,
        high_price, low_price
    )
    VALUES (
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
        $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
        $21, $22, $23, $24, $25, $26, $27, $28, $29, $30
    )
    ON CONFLICT (symbol, ts) DO UPDATE SET
        last_price       = EXCLUDED.last_price,
        avg_price        = EXCLUDED.avg_price,
        last_volume      = EXCLUDED.last_volume,
        total_volume     = EXCLUDED.total_volume,
        total_value      = EXCLUDED.total_value,
        foreign_buy_qty  = EXCLUDED.foreign_buy_qty,
        foreign_sell_qty = EXCLUDED.foreign_sell_qty,
        foreign_buy_val  = EXCLUDED.foreign_buy_val,
        foreign_sell_val = EXCLUDED.foreign_sell_val,
        bid1_price       = EXCLUDED.bid1_price,
        bid1_qty         = EXCLUDED.bid1_qty,
        bid2_price       = EXCLUDED.bid2_price,
        bid2_qty         = EXCLUDED.bid2_qty,
        bid3_price       = EXCLUDED.bid3_price,
        bid3_qty         = EXCLUDED.bid3_qty,
        ask1_price       = EXCLUDED.ask1_price,
        ask1_qty         = EXCLUDED.ask1_qty,
        ask2_price       = EXCLUDED.ask2_price,
        ask2_qty         = EXCLUDED.ask2_qty,
        ask3_price       = EXCLUDED.ask3_price,
        ask3_qty         = EXCLUDED.ask3_qty,
        ref_price        = EXCLUDED.ref_price,
        ceil_price       = EXCLUDED.ceil_price,
        floor_price      = EXCLUDED.floor_price,
        change_percent   = EXCLUDED.change_percent,
        change_value     = EXCLUDED.change_value,
        high_price       = EXCLUDED.high_price,
        low_price        = EXCLUDED.low_price
    ;
    """


    values = [
        (
            r.get("symbol"),
            datetime.fromtimestamp(r.get("ts") / 1000) if r.get("ts") else None,  # Convert ms to TIMESTAMP
            r.get("last_price"), r.get("avg_price"),
            r.get("last_volume"), r.get("total_volume"), r.get("total_value"),
            r.get("foreign_buy_qty"), r.get("foreign_sell_qty"),
            r.get("foreign_buy_val"), r.get("foreign_sell_val"),
            r.get("bid1_price"), r.get("bid1_qty"), r.get("bid2_price"), r.get("bid2_qty"),
            r.get("bid3_price"), r.get("bid3_qty"), r.get("ask1_price"), r.get("ask1_qty"),
            r.get("ask2_price"), r.get("ask2_qty"), r.get("ask3_price"), r.get("ask3_qty"),
            r.get("ref_price"), r.get("ceil_price"), r.get("floor_price"),
            r.get("change_percent"), r.get("change_value"),
            r.get("high_price"), r.get("low_price")
        )
        for r in records
    ]

    async with DB_POOL.acquire() as conn:
        await conn.executemany(sql, values)

    print(f"💾 realtime_quotes: inserted/updated {len(records)} records")


async def insert_batch_candles(records: List[Dict[str, Any]]) -> None:
    """
    Insert batch of candle records into PostgreSQL
    """
    global DB_POOL
    if not records or DB_POOL is None:
        return

    sql = f"""
    INSERT INTO {config.DB_SCHEMA}.candles_1m (
        symbol, bucket_time,
        open_price, high_price, low_price, close_price, volume
    )
    VALUES ($1, $2, $3, $4, $5, $6, $7)
    ON CONFLICT (symbol, bucket_time) DO UPDATE SET
        open_price  = EXCLUDED.open_price,
        high_price  = EXCLUDED.high_price,
        low_price   = EXCLUDED.low_price,
        close_price = EXCLUDED.close_price,
        volume      = EXCLUDED.volume
    ;
    """

    values = [
        (
            c["symbol"],
            datetime.fromisoformat(c["bucket_time"]),
            c["open_price"],
            c["high_price"],
            c["low_price"],
            c["close_price"],
            c["volume"],
        )
        for c in records
    ]

    async with DB_POOL.acquire() as conn:
        await conn.executemany(sql, values)

    print(f"💾 candles_1m: inserted/updated {len(records)} candles")


async def consume_quotes(consumer: KafkaConsumer) -> None:
    """
    Consume quote messages from Kafka and write to database
    """
    batch = []
    print("🔄 Starting to poll for quote messages...")
    
    while True:
        # Poll messages with timeout (in milliseconds)
        msg_dict = consumer.poll(timeout_ms=1000, max_records=500)
        
        if not msg_dict:
            # No messages, write remaining batch if any
            if batch:
                await insert_batch_quotes(batch)
                batch = []
            await asyncio.sleep(0.1)  # Small delay to prevent busy loop
            continue
        
        # Process messages from all partitions
        for topic_partition, messages in msg_dict.items():
            for message in messages:
                try:
                    # Deserialize JSON
                    record = json.loads(message.value)
                    batch.append(record)

                    # Write batch when size limit reached
                    if len(batch) >= config.DB_BATCH_SIZE:
                        await insert_batch_quotes(batch)
                        batch = []

                except json.JSONDecodeError as e:
                    print(f"❌ JSON decode error: {e}")
                except Exception as e:
                    print(f"❌ Error processing quote message: {e}")


async def consume_candles(consumer: KafkaConsumer) -> None:
    """
    Consume candle messages from Kafka and write to database
    """
    batch = []
    print("🔄 Starting to poll for candle messages...")
    
    while True:
        # Poll messages with timeout (in milliseconds)
        msg_dict = consumer.poll(timeout_ms=1000, max_records=500)
        
        if not msg_dict:
            # No messages, write remaining batch if any
            if batch:
                await insert_batch_candles(batch)
                batch = []
            await asyncio.sleep(0.1)  # Small delay to prevent busy loop
            continue
        
        # Process messages from all partitions
        for topic_partition, messages in msg_dict.items():
            for message in messages:
                try:
                    # Deserialize JSON
                    record = json.loads(message.value)
                    batch.append(record)

                    # Write batch when size limit reached
                    if len(batch) >= config.DB_BATCH_SIZE:
                        await insert_batch_candles(batch)
                        batch = []

                except json.JSONDecodeError as e:
                    print(f"❌ JSON decode error: {e}")
                except Exception as e:
                    print(f"❌ Error processing candle message: {e}")


async def main() -> None:
    """
    Main entry point for consumer
    """
    global DB_POOL

    # Initialize database connection pool
    print("🔧 Initializing database connection pool...")
    DB_POOL = await asyncpg.create_pool(config.DB_DSN, min_size=1, max_size=5)
    print("✅ Database pool initialized")

    # Create Kafka consumers
    print("🔧 Initializing Kafka consumers...")
    
    quote_consumer = KafkaConsumer(
        config.TOPIC_STOCK_QUOTES,
        **config.CONSUMER_CONFIG
    )
    
    candle_consumer = KafkaConsumer(
        config.TOPIC_STOCK_CANDLES,
        **config.CONSUMER_CONFIG
    )
    
    print("✅ Kafka consumers initialized")
    print(f"📥 Consuming from topics: {config.TOPIC_STOCK_QUOTES}, {config.TOPIC_STOCK_CANDLES}")

    try:
        # Run consumers concurrently
        await asyncio.gather(
            consume_quotes(quote_consumer),
            consume_candles(candle_consumer)
        )
    except KeyboardInterrupt:
        print("\n⚠️ Shutting down...")
    finally:
        # Close consumers
        quote_consumer.close()
        candle_consumer.close()
        
        # Close database pool
        if DB_POOL:
            await DB_POOL.close()
        
        print("✅ Consumers closed")


if __name__ == "__main__":
    asyncio.run(main())

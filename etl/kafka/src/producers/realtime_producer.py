import asyncio
import json
import logging
from datetime import datetime
from kafka import KafkaProducer
from websockets import connect
from vnstock import Listing

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

import os

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("TOPIC_STOCK_QUOTES", "market.quotes.raw")
WEBSOCKET_URL = os.getenv("WEBSOCKET_URL", "wss://stream2.simplize.vn/ws")

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks='all',
    retries=3
)

def get_all_symbols():
    """Get list of symbols from vnstock"""
    listing = Listing()
    df = listing.symbols_by_exchange()
    return df['symbol'].unique().tolist()

async def producer_worker():
    """Main worker to fetch from WebSocket and push to Kafka"""
    symbols = get_all_symbols()
    logger.info(f"Subscribing to {len(symbols)} symbols")
    
    while True:
        try:
            async with connect(WEBSOCKET_URL) as websocket:
                # 1. Subscribe to symbols
                sub_message = {
                    "event": "sub",
                    "topic": "quotes",
                    "symbols": symbols
                }
                await websocket.send(json.dumps(sub_message))
                logger.info("Sent subscription message")
                
                # 2. Receive messages
                async for message in websocket:
                    data = json.loads(message)
                    
                    # Handle if data is directly a list
                    if isinstance(data, list):
                        for item in data:
                            if not isinstance(item, dict): continue
                            item_copy = item.copy()
                            item_copy["ingested_at"] = datetime.utcnow().isoformat()
                            
                            key_val = item_copy.get("s", item_copy.get("symbol", "unknown"))
                            if not isinstance(key_val, str):
                                key_val = str(key_val)
                            
                            producer.send(
                                topic=KAFKA_TOPIC,
                                key=key_val.encode('utf-8'),
                                value={"event": "quotes", "data": item_copy, "topic": "quotes"}
                            )
                        logger.info(f"Pushed quotes for {len(data)} items to Kafka (from list)")
                        continue

                    # Handle ping/pong
                    if isinstance(data, dict) and data.get("event") == "ping":
                        await websocket.send(json.dumps({"event": "pong"}))
                        continue
                    
                    # Push data to Kafka
                    if isinstance(data, dict) and "data" in data:
                        # Sometimes data['data'] is a list or dict
                        payloads = data["data"] if isinstance(data["data"], list) else [data["data"]]
                        for payload in payloads:
                            if not isinstance(payload, dict):
                                continue
                            
                            # Add ingestion timestamp
                            item = payload.copy()
                            item["ingested_at"] = datetime.utcnow().isoformat()
                            
                            key_val = item.get("s", item.get("symbol", "unknown"))
                            if not isinstance(key_val, str):
                                key_val = str(key_val)
                            
                            producer.send(
                                topic=KAFKA_TOPIC,
                                key=key_val.encode('utf-8'),
                                value={"event": data.get("event", "quotes"), "data": item, "topic": data.get("topic", "quotes")}
                            )
                        logger.info(f"Pushed quotes for {len(payloads)} items to Kafka")
                        
        except Exception as e:
            import traceback
            logger.error(f"Error in producer worker (raw msg: {message if 'message' in locals() else 'None'}): {e}\n{traceback.format_exc()}")
            logger.info("Reconnecting in 5 seconds...")
            await asyncio.sleep(5)

if __name__ == "__main__":
    try:
        asyncio.run(producer_worker())
    except KeyboardInterrupt:
        logger.info("Producer stopped")
    finally:
        producer.close()

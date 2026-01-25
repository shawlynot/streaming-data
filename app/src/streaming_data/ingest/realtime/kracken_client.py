import json
import logging
import websockets
from ...db import DB_CLIENT

KRAKEN_WS_URL = "wss://ws.kraken.com/v2"  # v2 public market data

logger = logging.getLogger(__name__)


async def kraken_ws_client():
    async with websockets.connect(KRAKEN_WS_URL) as ws:

        # Subscribe to ticker data
        subscribe_msg = {
            "method": "subscribe",
            "params": {
                "channel": "ticker",
                "symbol": ["BTC/USD"],  # you can add more pairs
            },
        }

        await ws.send(json.dumps(subscribe_msg))
        print("Subscribed to BTC/USD ticker")
        
        with DB_CLIENT.connection() as conn:
            # Receive messages indefinitely
            while True:
                msg = await ws.recv()
                msg_json = json.loads(msg)
                if msg_json.get("channel") == "ticker":
                    data = msg_json["data"][0]
                    price = float(data["last"])
                    timestamp = data["timestamp"]
                    with conn.cursor() as cur:
                        cur.execute(
                            t"""
                            INSERT INTO ingested.tick_kraken (price, ts)
                            VALUES ({price}, {timestamp})
                            """
                        )
                        conn.commit()
                    logger.info(
                        'Inserted into ingested.tick_kraken: price=%s, timestamp=%s', price, timestamp)
                else:
                    logger.info('received %s', msg_json)  # other messages

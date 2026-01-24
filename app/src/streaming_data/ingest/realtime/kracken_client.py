import json
import websockets
from ...db.util import db_client

KRAKEN_WS_URL = "wss://ws.kraken.com/v2"  # v2 public market data


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

        # Receive messages indefinitely
        while True:
            msg = await ws.recv()
            data = json.loads(msg)

            print(data)
            

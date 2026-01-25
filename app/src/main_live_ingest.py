import logging
from streaming_data.ingest.realtime.kracken_client import kraken_ws_client

import asyncio

logger = logging.basicConfig(level=logging.INFO)

if __name__ == "__main__":
    asyncio.run(kraken_ws_client())

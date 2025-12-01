from streaming_data.ingest.kracken_client import kraken_ws_client

import asyncio


if __name__ == "__main__":
    asyncio.run(kraken_ws_client())

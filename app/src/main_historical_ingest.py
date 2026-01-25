import logging
from streaming_data.ingest.historical.kracken_client import KrakenOHLCClient

logger = logging.basicConfig(level=logging.INFO)

if __name__ == "__main__":
    KrakenOHLCClient().get_year()

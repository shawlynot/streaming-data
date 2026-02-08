import logging
from streaming_data.ingest.historical.massive import MassiveClient

logger = logging.basicConfig(level=logging.INFO)

if __name__ == "__main__":
    MassiveClient().get_data()

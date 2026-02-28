import logging
from streaming_data.ingest.historical.massive import MassiveHistorical

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

if __name__ == "__main__":
    MassiveHistorical().ingest_nvda()

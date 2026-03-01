import logging
from streaming_data.ingest.historical.massive import MassiveHistorical
from streaming_data.ingest.rates.fed import FedRates

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

if __name__ == "__main__":
    FedRates().ingest_sofr()
    MassiveHistorical().ingest_nvda()

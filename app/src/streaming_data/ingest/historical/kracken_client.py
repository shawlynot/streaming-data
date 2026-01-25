from datetime import date, datetime, timezone
import logging
from typing import NamedTuple
from ...db import DB_CLIENT
import requests

KRAKEN_REST_URL = "https://api.kraken.com/0/public/OHLC"  # v2 public market data

logger = logging.getLogger(__name__)

class KarackenCandle(NamedTuple):
    d: date
    open: float
    close: float


class KrakenOHLCClient:

    def get_year(self):
        params = {"pair": "BTCUSD", "interval": 1440}

        # get since
        with DB_CLIENT.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT EXTRACT(EPOCH FROM MAX(_date)) FROM ingested.historical_kraken
                    """
                )
                since_row = cur.fetchone()
                if since_row is not None and since_row[0] is not None:
                    params["since"] = int(since_row[0]) + 60 * 60 * 24  # add one day in seconds

        logger.info("Fetching OHLC data since %s", params)
        r = requests.get(KRAKEN_REST_URL, params=params,
                         timeout=10, headers={})
        r.raise_for_status()
        data = r.json()
        result = data.get("result", {})

        # The 'result' dict contains the pair key and possibly a 'last' id.
        # Find the first key that is not 'last'
        pair_keys = [k for k in result.keys() if k != "last"]
        if not pair_keys:
            raise RuntimeError("No OHLC data returned for pair.")
        data_key = pair_keys[0]
        rows = result[data_key]  # list of lists

        data = [KarackenCandle(datetime.fromtimestamp(
            r[0], timezone.utc).date(), r[1], r[4]) for i, r in enumerate(rows) if i < len(rows) - 1]

        with DB_CLIENT.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    """
                    INSERT INTO ingested.historical_kraken (_date, open, close)
                    VALUES (%s, %s, %s)
                    """,
                    data
                )

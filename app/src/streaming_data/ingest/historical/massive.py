from datetime import date, datetime, timedelta, timezone
import logging
import os
from massive import RESTClient
from streaming_data.db.util import DB_CLIENT


logger = logging.getLogger(__name__)

# NVDA260306C00180000


class MassiveClient:

    def __init__(self):
        token = os.environ["MASSIVE_API_KEY"]
        self.client = RESTClient(token)

    def get_data(self):
        today = datetime.now(timezone.utc).date()
        # get spot for the last
        spot = self.client.list_aggs(
            "NVDA",
            1,
            "day",
            today - timedelta(days=30),
            today.isoformat(),
            sort="desc",
            limit=5000
        )

        with DB_CLIENT.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    """
                    INSERT INTO ingested.spot_massive 
                     (open, close, _date)
                    VALUES (%s, %s, %s)
                    """,
                    [(
                        s.open,
                        s.close,
                        datetime.fromtimestamp(
                            s.timestamp / 1000, tz=timezone.utc).date(),
                    ) for s in spot]
                )

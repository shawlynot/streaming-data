import argparse
import logging
from datetime import datetime

from streaming_data.db import get_db_client
from streaming_data.market_data.market_data import MarketDataHandler
from streaming_data.model import TickEvent
from streaming_data.pricing.implied_vol import ImpliedVolCalculator

logger = logging.getLogger(__name__)


def replay_ticks(start: datetime):
    db = get_db_client()
    market_data = MarketDataHandler()
    pricer = ImpliedVolCalculator()

    with db.connection() as conn:
        with conn.cursor(name="tick_cursor") as cur:
            cur.itersize = 5000
            cur.execute(
                """
                SELECT m.security_id, m.price, m.time
                FROM market_data.massive m
                    JOIN security_master.asset a ON m.security_id = a.id AND a.instrument_type = 'option'
                WHERE time >= %s 
                ORDER BY time
                """,
                (start,),
            )
            for security_id, price, time in cur:
                market_data_event = market_data.on_tick(TickEvent(security_id, price, time))
                if market_data_event:
                    pricer.on_tick(market_data_event)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument("start_date", help="Start date (YYYY-MM-DD)")
    args = parser.parse_args()
    start = datetime.strptime(args.start_date, "%Y-%m-%d")
    replay_ticks(start)

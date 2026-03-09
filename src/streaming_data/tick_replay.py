from datetime import datetime
from typing import Generator

from streaming_data.db import get_db_client
from streaming_data.db.util import DBClient
from streaming_data.market_data.market_data import MarketDataHandler
from streaming_data.model import TickEvent
from streaming_data.pricing.implied_vol import ImpliedVolCalculator


def replay_ticks(start: datetime, db: DBClient | None = None) -> Generator[tuple[int, float]]:
    if db is None:
        db = get_db_client()
    market_data = MarketDataHandler(db)
    pricer = ImpliedVolCalculator(db)


    with db.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT m.security_id, m.price, (EXTRACT(EPOCH FROM m.time) * 1_000_000_000)::bigint AS time_nanos
                FROM market_data.massive m
                    JOIN security_master.asset a ON m.security_id = a.id
                WHERE time >= %s
                ORDER BY time
                """,
                (start,),
            )
            for security_id, price, time in cur:
                market_data_event = market_data.on_tick(TickEvent(security_id, price, time))
                if market_data_event:
                    result = pricer.on_tick(market_data_event)
                    if result is not None:
                        yield result


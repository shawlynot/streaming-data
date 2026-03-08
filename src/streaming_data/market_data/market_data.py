from decimal import Decimal

from streaming_data.model import MarketDataEvent, TickEvent
from streaming_data.db import get_db_client
from _core import MarketDataState


class MarketDataHandler:

    _state: MarketDataState
    _option_ids: set[int]

    def __init__(self):
        db = get_db_client()
        with db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT security_id, underlier_id
                    FROM security_master.options
                    """
                )
                option_id_to_underlier_id: dict[int, int] = {
                    row[0]: row[1] for row in cur.fetchall()
                }
        self._option_ids = set(option_id_to_underlier_id.keys())
        self._state = MarketDataState(option_id_to_underlier_id)

    def on_tick(self, tick: TickEvent) -> MarketDataEvent | None:
        if tick.security_id in self._option_ids:
            core = self._state.option_tick(tick.security_id, int(tick.price * 100), tick.time_nanos)
            if core is None:
                return None
            return MarketDataEvent(
                option_sec_id=core.option_sec_id,
                option_price=Decimal(core.option_price) / 100,
                underlier_price=Decimal(core.underlier_price) / 100,
                time_nanos=core.time_nanos,
            )
        else:
            self._state.underlier_tick(tick.security_id, int(tick.price * 100))
            return None

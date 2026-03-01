from datetime import datetime
from decimal import Decimal
from typing import NamedTuple


class Tick(NamedTuple):
    security_id: int
    price: Decimal
    time: datetime

class TickConsumer:

    def on_tick(self, tick: Tick):
        raise NotImplementedError
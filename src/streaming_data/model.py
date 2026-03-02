from datetime import datetime
from decimal import Decimal
from typing import NamedTuple


class TickEvent(NamedTuple):
    security_id: int
    price: Decimal
    time: datetime

class MarketDataEvent(NamedTuple):
    option_sec_id: int
    option_price: Decimal
    underlier_price: Decimal
    time: datetime
from decimal import Decimal
from typing import NamedTuple


class TickEvent(NamedTuple):
    security_id: int
    price: Decimal
    time_nanos: int

class MarketDataEvent(NamedTuple):
    option_sec_id: int
    option_price: Decimal
    underlier_price: Decimal
    time_nanos: int
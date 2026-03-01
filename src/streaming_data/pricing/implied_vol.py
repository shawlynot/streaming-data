from decimal import Decimal
import logging
from datetime import datetime, timezone
from typing import cast

import polars as pl

from ..db.util import get_db_client
from ..model import Tick, TickConsumer
from scipy.optimize import newton
from _core import bs_eur_call_price, bs_vega


logger = logging.getLogger(__name__)

class ImpliedVolCalculator(TickConsumer):

    sec_id_is_call: dict[int, bool]
    calls_ref_data: pl.DataFrame
    risk_free_rate: float

    def __init__(self):
        db = get_db_client()
        today = datetime.now(timezone.utc).date()
        with db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT security_id, contract_type FROM security_master.options"
                )
                self.sec_id_is_call = {
                    row[0]: row[1] == "call" for row in cur.fetchall()
                }
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT security_id, strike_price, expiration_date
                    FROM security_master.options
                    WHERE contract_type = 'call'
                    """
                )
                self.calls_ref_data = pl.DataFrame(
                    cur.fetchall(),
                    schema={"security_id": pl.Int64, "strike_price": pl.Float64, "expiration_date": pl.Date},
                    orient="row",
                ).with_columns(
                    time_to_expiry_years=(pl.col("expiration_date") - today).dt.total_days() / 365
                ).drop(
                    "expiration_date"
                )
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT f.rate / 100.0
                    FROM market_data.fed f
                        JOIN security_master.asset a ON f.security_id = a.id
                    WHERE a.ticker = 'SOFR'
                    ORDER BY f.effective_date DESC
                    LIMIT 1
                    """
                )
                row = cur.fetchone()
                if row is None:
                    raise RuntimeError("No SOFR rate found in market_data.fed")
                self.risk_free_rate = row[0]
        logger.info("Using SOFR risk-free rate: %.4f", self.risk_free_rate)
    
    def on_tick(self, tick: Tick):

        #TODO handle spot ticks: store last value as a field and use below

        if self.sec_id_is_call.get(tick.security_id):
            row = self.calls_ref_data.row(by_predicate=pl.col("security_id") == tick.security_id)
            strike_price = row[1]
            time_to_expiry_years = row[2]
            spot_price = 0 # TODO make this event handler handle
            implied_vol = self._newton(spot_price, strike_price, time_to_expiry_years, tick.price)
            logger.info("Implied vol %s", implied_vol)


    def _newton(self, spot: float, strike: pl.Float64, time_to_expiry_years: pl.Float64, option_price: Decimal) -> float | None:
        try:
            return cast(int, newton(
                func=self._f,
                x0=0.2,  # initial guess for vol
                fprime=self._f_prime,
                args=(float(spot), float(strike), float(time_to_expiry_years), float(option_price)), # type: ignore
                tol=0.01,
                maxiter=1000
            ))
        except:
            return None


    def _f(self, vol, spot, strike_price, time_to_expiry_years, option_price):
        bs_price = bs_eur_call_price(
            spot, strike_price, time_to_expiry_years, self.risk_free_rate, vol)
        return bs_price - option_price


    def _f_prime(self, vol, spot, strike_price, time_to_expiry_years, _):
        return bs_vega(spot, strike_price, time_to_expiry_years, self.risk_free_rate, vol)
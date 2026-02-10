from datetime import date
from typing import NamedTuple
from ..db.util import DB_CLIENT
from scipy.optimize import newton
import polars as pl
from _core import bs_eur_call_price, bs_vega

risk_free_rate = 0.035 # get from SOFR?


class SpotOptionStrikeExpiry(NamedTuple):
    spot: float
    option_price: float
    option_price: float
    strike_price: float
    expiration_date: date


def get_vol_call():
    # get spot for the day
    # get an option price for that day
    # get the expiry date, strike and time to expiry of WWthe that option, option type for that expiry.
    # assume no dividends, so calulate vol using European option formula

    sql = """
    SELECT sm."close" AS spot, om."close" AS option_price, o.strike_price, o.expiration_date, sm._date as as_of_date
    FROM ingested.spot_massive sm
    INNER JOIN ingested.option_massive om ON om._date = sm._date 
    INNER JOIN security_master."options" o ON o.ticker = om.ticker AND o.contract_type = 'call'
    ORDER BY sm._date DESC
    """

    with DB_CLIENT.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            result = cur.fetchall()

        df = pl.DataFrame(result, schema={
                          'spot': pl.Float64, 'option_price': pl.Float64, 'strike_price': pl.Float64, 'expiration_date': pl.Date, 'as_of_date': pl.Date},
                          orient="row")

    df = df.with_columns(
        time_to_expiry_years=(pl.col("expiration_date") - pl.col("as_of_date")).dt.total_days() / 365
    ).with_columns(
        vol=pl.struct(pl.all()).map_elements(_newton)
    )

    print(df)

def _newton(row: dict) -> float:
    try:
        return newton(
            func=_f,
            x0=0.2,  # initial guess for vol
            fprime=_f_prime,
            args=(float(row["spot"]), float(row["strike_price"]), float(row["time_to_expiry_years"]), float(row["option_price"])),
            tol=0.01,
            maxiter=1000
        )
    except:
        return None


def _f(vol, spot, strike_price, time_to_expiry_years, option_price):
    bs_price = bs_eur_call_price(
        spot, strike_price, time_to_expiry_years, risk_free_rate, vol)
    return bs_price - option_price


def _f_prime(vol, spot, strike_price, time_to_expiry_years, _):
    return bs_vega(spot, strike_price, time_to_expiry_years, risk_free_rate, vol)

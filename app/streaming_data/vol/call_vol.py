from datetime import date
from math import exp, log, sqrt
from typing import NamedTuple
from ..db.util import DB_CLIENT
from scipy.stats import norm
from scipy.optimize import newton
import polars as pl

risk_free_rate = 0.035


class SpotOptionStrikeExpiry(NamedTuple):
    spot: float
    option_price: float
    option_price: float
    strike_price: float
    expiration_date: date


def get_vol_call():
    # get spot for the day
    # get an option price for that day
    # get the expiry date, strike and time to expiry of the that option, option type for that expiry.
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
                          'spot': pl.Float64, 'option_price': pl.Float64, 'strike_price': pl.Float64, 'expiration_date': pl.Date, 'as_of_date': pl.Date})

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


def _get_bs_eur_call_price(spot, strike_price, time_to_expiry_years, vol):
    d1 = (log(spot / strike_price) + (risk_free_rate + 0.5 * vol ** 2)
          * time_to_expiry_years) / (vol * sqrt(time_to_expiry_years))
    d2 = d1 - vol * sqrt(time_to_expiry_years)

    call_price = spot * norm.cdf(d1) - strike_price * \
        exp(-risk_free_rate * time_to_expiry_years) * norm.cdf(d2)
    return call_price


def _get_bs_vega(spot, strike_price, time_to_expiry_years, vol):
    d1 = (log(spot / strike_price) + (risk_free_rate + 0.5 * vol ** 2)
          * time_to_expiry_years) / (vol * sqrt(time_to_expiry_years))
    return spot * norm.pdf(d1) * sqrt(time_to_expiry_years)


def _f(vol, spot, strike_price, time_to_expiry_years, option_price):
    bs_price = _get_bs_eur_call_price(
        spot, strike_price, time_to_expiry_years, vol)
    return bs_price - option_price


def _f_prime(vol, spot, strike_price, time_to_expiry_years, _):
    vega = _get_bs_vega(spot, strike_price, time_to_expiry_years, vol)
    return vega

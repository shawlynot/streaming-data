from datetime import date
from math import exp, log, sqrt
from ..db.util import DB_CLIENT
from scipy.stats import norm
from scipy.optimize import newton

risk_free_rate = 0.035


def get_vol_call(as_od_date=date.today()):
    # get spot for the day
    # get an option price for that day
    # get the expiry date, strike and time to expiry of the that option, option type for that expiry. Just use calls for now
    # assume no dividends, so calulate vol using European option formula

    sql = """
    SELECT sm."close" AS spot, om."close" AS option_price, o.strike_price, o.expiration_date
    FROM ingested.spot_massive sm
    INNER JOIN ingested.option_massive om ON om._date = sm._date 
    INNER JOIN security_master."options" o ON o.ticker = om.ticker AND o.contract_type = 'call'
    ORDER BY sm._date DESC
    LIMIT 1
    """

    with DB_CLIENT.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql,)
            result = cur.fetchone()
            assert result is not None, "No data found for the given date"
            spot, option_price, strike_price, expiration_date = result

    vol = newton(
        func=_f,
        x0=0.2,  # initial guess for vol
        fprime=_f_prime,
        args=(float(spot), float(strike_price), _get_time_to_expiry_years(expiration_date, as_od_date), float(option_price)),
        tol=0.01,
        maxiter=1000
    )

    print(f"Implied Volatility for call option expiring on {expiration_date} with strike {strike_price} is: {vol}")


def _get_time_to_expiry_years(expiration_date: date, as_of_date: date) -> float:
    if (expiration_date - as_of_date).days < 0:
        raise ValueError("Expiration date must be in the future")
    return (expiration_date - as_of_date).days / 365

def _get_bs_eur_call_price(spot, strike_price, time_to_expiry_years, vol):
    d1 = (log(spot / strike_price) + (risk_free_rate + 0.5 * vol ** 2)
          * time_to_expiry_years) / (vol * sqrt(time_to_expiry_years))
    d2 = d1 - vol * sqrt(time_to_expiry_years)

    call_price = spot * norm.cdf(d1) - strike_price * exp(-risk_free_rate * time_to_expiry_years) * norm.cdf(d2)
    return call_price

def _get_bs_vega(spot, strike_price, time_to_expiry_years, vol):
    d1 = (log(spot / strike_price) + (risk_free_rate + 0.5 * vol ** 2)
          * time_to_expiry_years) / (vol * sqrt(time_to_expiry_years))
    return spot * norm.pdf(d1) * sqrt(time_to_expiry_years)


def _f(vol, spot, strike_price, time_to_expiry_years, option_price):
    bs_price = _get_bs_eur_call_price(spot, strike_price, time_to_expiry_years, vol)
    return bs_price - option_price

def _f_prime(vol, spot, strike_price, time_to_expiry_years, _):
    vega = _get_bs_vega(spot, strike_price, time_to_expiry_years, vol)
    return vega
from datetime import datetime, timedelta, timezone
import logging
import os
from time import sleep
from massive import RESTClient
from ...db.util import DB_CLIENT


logger = logging.getLogger(__name__)


class MassiveClient:

    def __init__(self):
        token = os.environ["MASSIVE_API_KEY"]
        self.client = RESTClient(token)

    def get_data(self):
        today = datetime.now(timezone.utc).date()
        # get spot for the last
        spot = self.client.list_aggs(
            "NVDA",
            1,
            "minute",
            today - timedelta(days=30),
            today,
            sort="desc",
            limit=50000
        )

        with DB_CLIENT.connection() as conn:
            with conn.cursor() as cur, conn.transaction():
                cur.executemany(
                    """
                    INSERT INTO ingested.spot_massive 
                     (open, close, time)
                    VALUES (%(open)s, %(close)s, %(time)s)
                    ON CONFLICT DO NOTHING
                    """,
                    [{
                        'open': s.open,
                        'close': s.close,
                        'time': datetime.fromtimestamp(
                            s.timestamp / 1000, tz=timezone.utc),
                    } for s in spot]
                )

            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT close
                    FROM ingested.spot_massive
                    ORDER BY time DESC
                    LIMIT 1
                    """)
                fetch_close = cur.fetchone()[0]
                # round to nearest 10
                lastest_close = int(round(fetch_close, -1))

            options = self.client.list_options_contracts(
                underlying_ticker="NVDA",
                strike_price=lastest_close,
                order="asc",
                limit=10,
                sort="ticker",
                expiration_date_lte=today + timedelta(days=30),
            )

            american_options = [
                o for o in options if o.exercise_style == "american"]

            with conn.cursor() as cur, conn.transaction():
                cur.executemany(
                    """
                        INSERT INTO security_master.options
                        (ticker, contract_type, exercise_style, strike_price, expiration_date, shares_per_contract)
                        VALUES (%(ticker)s, %(contract_type)s, %(exercise_style)s, %(strike_price)s, %(expiration_date)s, %(shares_per_contract)s)
                        ON CONFLICT (ticker) DO NOTHING
                        """,
                    [{
                        'ticker': s.ticker,
                        'contract_type': s.contract_type,
                        'exercise_style': s.exercise_style,
                        'strike_price': s.strike_price,
                        'expiration_date': s.expiration_date,
                        'shares_per_contract': s.shares_per_contract,
                    } for s in american_options]
                )

            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT ticker
                    FROM security_master.options
                    """)
                fetch_tickers = cur.fetchall()
                tickers: list[str] = [f[0] for f in fetch_tickers]

            for ticker in tickers:
                sleep(15)  # to avoid hitting rate limits
                logger.info(f"Getting data for {ticker}")
                option_aggs = self.client.list_aggs(
                    ticker,
                    1,
                    "minute",
                    today - timedelta(days=30),
                    today,
                    sort="asc",
                    limit=50000
                )

                with conn.cursor() as cur, conn.transaction():
                    cur.executemany(
                        """
                        INSERT INTO ingested.option_massive 
                        (ticker, open, close, time)
                        VALUES (%(ticker)s, %(open)s, %(close)s, %(time)s)
                        ON CONFLICT DO NOTHING
                        """,
                        [{
                            'open': a.open,
                            'close': a.close,
                            'time': datetime.fromtimestamp(
                                a.timestamp / 1000, tz=timezone.utc),
                            'ticker': ticker,
                        } for a in option_aggs]
                    )

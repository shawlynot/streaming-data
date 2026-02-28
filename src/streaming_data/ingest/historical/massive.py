from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from io import BytesIO
import logging
import os
from typing import Any, cast

import boto3
from botocore.config import Config
from massive import RESTClient
import polars as pl

from streaming_data.db.util import DBClient

from ...db import get_db_client

from massive.rest.models import OptionsContract, TickerDetails

BUCKET = "flatfiles"


logger = logging.getLogger(__name__)


class MassiveHistorical:

    client: RESTClient
    db_client: DBClient
    s3: Any

    def __init__(self):
        token = os.environ["MASSIVE_API_KEY"]
        self.client = RESTClient(token)
        self.db_client = get_db_client()
        session = boto3.Session(
            aws_access_key_id=os.environ["MASSIVE_AWS_KEY"],
            aws_secret_access_key=os.environ["MASSIVE_AWS_SECRET"],
        )
        self.s3 = session.client(
            "s3",
            endpoint_url='https://files.massive.com',
            config=Config(signature_version='s3v4')
        )

    def ingest_nvda(self):
        today = datetime.now(timezone.utc).date()
        self._get_ref_data(today, today + timedelta(days=180))
        self._get_market_data(today - timedelta(days=180), today)

    def _get_ref_data(self, today: date, six_months: date):
        # 1) Get NVDA ref data and store as an equity asset
        details = cast(TickerDetails, self.client.get_ticker_details("NVDA"))
        logger.info("Fetched ticker details for NVDA: %s", details.name)

        with self.db_client.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO security_master.asset (ticker, instrument_type)
                    VALUES  ('NVDA', 'equity')
                    RETURNING id
                    """
                )
                row = cur.fetchone()
                assert row is not None
                equity_id = row[0]

                cur.execute(
                    """
                    INSERT INTO security_master.equities (security_id, ticker)
                    VALUES (%s, 'NVDA')
                    """,
                    (equity_id,),
                )
                conn.commit()

        logger.info("Stored NVDA equity asset with id=%d", equity_id)

        # 2) Get up to 1000 option contracts expiring in the next 6 months
        contracts = cast(
            list[OptionsContract],
            list(
                self.client.list_options_contracts(
                    underlying_ticker="NVDA",
                    expiration_date_gte=str(today),
                    expiration_date_lte=str(six_months),
                    limit=1000,
                )
            ),
        )
        logger.info("Fetched %d NVDA option contracts", len(contracts))

        # 3) Store each contract
        with self.db_client.connection() as conn:
            with conn.cursor() as cur:
                for contract in contracts:
                    cur.execute(
                        """
                        INSERT INTO security_master.asset (ticker, instrument_type)
                        VALUES (%s, %s)
                        RETURNING id
                        """,
                        (contract.ticker, "option"),
                    )
                    row = cur.fetchone()
                    assert row is not None
                    option_id = row[0]

                    cur.execute(
                        """
                        INSERT INTO security_master.options
                            (security_id, ticker, contract_type, exercise_style,
                             strike_price, expiration_date, shares_per_contract)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            option_id,
                            contract.ticker,
                            contract.contract_type,
                            contract.exercise_style,
                            contract.strike_price,
                            contract.expiration_date,
                            contract.shares_per_contract,
                        ),
                    )
                conn.commit()
        logger.info("Stored %d option contracts", len(contracts))

    def _get_market_data(self, start: date, end: date):
        minute_aggs_prefix = "us_stocks_sip/minute_aggs_v1"
        self._download_and_store("equity", minute_aggs_prefix, start, end)
        option_minute_aggs_prefix = "us_options_opra/minute_aggs_v1"
        self._download_and_store(
            "option", option_minute_aggs_prefix, start, end)

    def _download_and_store(self, asset: str, s3_prefix: str, start: date, end: date):
        with self.db_client.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT id, ticker FROM security_master.asset WHERE instrument_type = '{asset}'")
                rows = cur.fetchall()

        ticker_to_id: dict[str, int] = {ticker: sid for sid, ticker in rows}

        tickers = set(ticker_to_id.keys())
        logger.info(
            "Found %d assets in DB to fetch market data for", len(tickers))

        day = start
        while day <= end:
            # Skip weekends
            if day.weekday() >= 5:
                day += timedelta(days=1)
                continue
            key = f"{s3_prefix}/{day:%Y/%m/%Y-%m-%d}.csv.gz"
            logger.info("Downloading %s", key)

            buf = BytesIO()
            try:
                self.s3.download_fileobj(BUCKET, key, buf)
            except self.s3.exceptions.ClientError:
                logger.warning("No file for %s, skipping", day)
                day += timedelta(days=1)
                continue

            buf.seek(0)
            df = pl.read_csv(buf).filter(pl.col("ticker").is_in(tickers))

            if df.is_empty():
                logger.info("No matching tickers for %s", day)
                day += timedelta(days=1)
                continue

            # Map ticker to security_id and insert
            records = [
                (
                    ticker_to_id[row["ticker"]],
                    row["open"],
                    row["window_start"],
                )
                for row in df.iter_rows(named=True)
            ]

            with self.db_client.connection() as conn:
                with conn.cursor() as cur:
                    cur.executemany(
                        """
                        INSERT INTO market_data.massive (security_id, price, time)
                        VALUES (%s, %s, to_timestamp(%s / 1e9))
                        ON CONFLICT (security_id, time) DO NOTHING
                        """,
                        records,
                    )
                    conn.commit()

            logger.info("Inserted %d rows for %s", len(records), day)
            day += timedelta(days=1)

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import logging

import requests

from ...db import DBClient, get_db_client

logger = logging.getLogger(__name__)

RATES_URL = "https://markets.newyorkfed.org/api/rates/all/search.json"
VENUE = "NYFRB"


class FedRates:
    db_client: DBClient
    venue_id: int
    security_id: int

    def __init__(self):
        self.db_client = get_db_client()

    def ingest_sofr(self):
        today = datetime.now(timezone.utc).date()
        start = today - timedelta(days=180)

        self._ensure_venue()
        self._ensure_asset()
        self._fetch_and_store(start, today)

    def _ensure_venue(self):
        with self.db_client.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO security_master.venue (name)
                    VALUES (%s)
                    ON CONFLICT DO NOTHING
                    """,
                    (VENUE,),
                )
                cur.execute(
                    "SELECT id FROM security_master.venue WHERE name = %s",
                    (VENUE,),
                )
                row = cur.fetchone()
                assert row is not None
                self.venue_id = row[0]
                conn.commit()

        logger.info("Venue %s id=%d", VENUE, self.venue_id)

    def _ensure_asset(self):
        with self.db_client.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO security_master.asset (ticker, venue, instrument_type)
                    VALUES ('SOFR', %s, 'rate')
                    ON CONFLICT (ticker, venue, instrument_type) DO UPDATE
                        SET ticker = EXCLUDED.ticker
                    RETURNING id
                    """,
                    (self.venue_id,),
                )
                row = cur.fetchone()
                assert row is not None
                self.security_id = row[0]

                cur.execute(
                    """
                    INSERT INTO security_master.rates (security_id)
                    VALUES (%s)
                    ON CONFLICT DO NOTHING
                    """,
                    (self.security_id,),
                )
                conn.commit()

        logger.info("SOFR asset id=%d", self.security_id)

    def _fetch_and_store(self, start, end):
        logger.info("Fetching SOFR rates from %s to %s", start, end)

        resp = requests.get(
            RATES_URL,
            params={"startDate": str(start), "endDate": str(end)},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()

        sofr_rates = [r for r in data["refRates"] if r["type"] == "SOFR"]
        logger.info("Received %d SOFR rate records", len(sofr_rates))

        if not sofr_rates:
            return

        records = [
            (self.security_id, r["effectiveDate"], r["percentRate"])
            for r in sofr_rates
        ]

        with self.db_client.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    """
                    INSERT INTO market_data.fed (security_id, effective_date, rate)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (security_id, effective_date) DO NOTHING
                    """,
                    records,
                )
                conn.commit()

        logger.info("Inserted %d SOFR rates into market_data.fed", len(records))

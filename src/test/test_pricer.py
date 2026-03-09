import csv
import logging
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import pytest
from testcontainers.postgres import PostgresContainer

from streaming_data.db.util import DBClient

FIXTURES = Path(__file__).parent / "fixtures"
SCHEMA = Path(__file__).parent.parent / "sql" / "create.sql"

logging.basicConfig(level=logging.INFO)


@pytest.fixture(scope="session")
def db_client():
    with PostgresContainer("postgres:16") as pg:
        conninfo = pg.get_connection_url().replace("+psycopg2", "")
        client = DBClient(conninfo=conninfo)

        with client.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(SCHEMA.read_text())
            conn.commit()

        _load_csv(client, "security_master.venue", FIXTURES / "venue.csv")
        _load_csv(client, "security_master.asset", FIXTURES / "asset.csv")
        _load_csv(client, "security_master.options", FIXTURES / "options.csv")
        _load_csv(client, "security_master.equities", FIXTURES / "equities.csv")
        _load_csv(client, "security_master.rates", FIXTURES / "rates.csv")
        _load_csv(client, "market_data.massive", FIXTURES / "massive.csv")
        _load_csv(client, "market_data.fed", FIXTURES / "fed.csv")

        yield client


def _load_csv(client: DBClient, table: str, path: Path):
    with open(path, newline="") as f:
        reader = csv.reader(f)
        headers = next(reader)
        rows = list(reader)

    if not rows:
        return

    cols = ", ".join(headers)
    placeholders = ", ".join(["%s"] * len(headers))
    sql = f"INSERT INTO {table} ({cols}) VALUES ({placeholders})"

    with client.connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(sql, rows)
        conn.commit()


def test_replay_ticks(db_client):
    results: list[tuple[int, float | None]] = []

    from streaming_data.pricing.implied_vol import ImpliedVolCalculator

    original_on_tick = ImpliedVolCalculator.on_tick

    def capturing_on_tick(self, tick):
        iv = original_on_tick(self, tick)
        results.append((tick.option_sec_id, iv))
        return iv

    with (
        patch("streaming_data.db.util.get_db_client", return_value=db_client),
        patch("streaming_data.db.get_db_client", return_value=db_client),
        patch("main_pricer.get_db_client", return_value=db_client),
        patch("streaming_data.market_data.market_data.get_db_client", return_value=db_client),
        patch("streaming_data.pricing.implied_vol.get_db_client", return_value=db_client),
        patch.object(ImpliedVolCalculator, "on_tick", capturing_on_tick),
    ):
        from main_pricer import replay_ticks

        replay_ticks(start=datetime(2024, 1, 1))

    expected = _load_expected(FIXTURES / "expected_output.csv")

    assert len(results) == len(expected), (
        f"Expected {len(expected)} results, got {len(results)}"
    )

    for (actual_id, actual_iv), (expected_id, expected_iv) in zip(results, expected):
        assert actual_id == expected_id, (
            f"Security ID mismatch: {actual_id} != {expected_id}"
        )
        if expected_iv is None:
            assert actual_iv is None
        else:
            assert actual_iv is not None
            assert abs(actual_iv - expected_iv) < 0.01, (
                f"IV mismatch for {actual_id}: {actual_iv} != {expected_iv}"
            )


def _load_expected(path: Path) -> list[tuple[int, float | None]]:
    rows = []
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            sec_id = int(row["option_sec_id"])
            iv_str = row["implied_vol"].strip()
            iv = None if iv_str == "" else float(iv_str)
            rows.append((sec_id, iv))
    return rows

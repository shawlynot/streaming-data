import csv
import logging
from datetime import datetime
from pathlib import Path

import pytest
from testcontainers.postgres import PostgresContainer
from streaming_data.db.util import DBClient
from streaming_data.tick_replay import replay_ticks

FIXTURES = Path(__file__).parent / "fixtures"
SCHEMA = Path(__file__).parent.parent.parent / "sql" / "create.sql"

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
        _load_csv(client, "security_master.equities",
                  FIXTURES / "equities.csv")
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
    expected = _load_expected(FIXTURES / "expected_output.csv")
    results = list(replay_ticks(datetime(2026, 3, 6), db_client))
    assert len(results) == len(expected)
    for (sec_id, iv), (exp_sec_id, exp_iv) in zip(results, expected):
        assert sec_id == exp_sec_id
        if exp_iv is None:
            assert iv is None
        else:
            assert iv == pytest.approx(exp_iv, abs=0.01)



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

import os

from psycopg_pool import ConnectionPool


class DBClient:

    def __init__(self, conninfo: str):
        self.pool = ConnectionPool(conninfo=conninfo, min_size=1, max_size=2)

    def connection(self):
        return self.pool.connection()


def get_db_client() -> DBClient:
    password = os.environ["POSTGRES_PASSWORD"]
    conninfo = f"postgresql://admin:{password}@localhost:5432/streaming_data"
    return DBClient(conninfo=conninfo)

import os

from psycopg_pool import ConnectionPool


class DBClient:

    def __init__(self, conninfo: str):
        self.pool = ConnectionPool(conninfo=conninfo, min_size=1, max_size=2)

    def connection(self):
        return self.pool.connection()


def get_db_client() -> DBClient:
    password = os.environ["POSTGRES_PASSWORD"]
    username = os.getenv("POSTGRES_USER", "admin")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    conninfo = f"postgresql://{username}:{password}@{host}:{port}/streaming_data"
    return DBClient(conninfo=conninfo)

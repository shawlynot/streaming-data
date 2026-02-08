import os
from psycopg_pool import ConnectionPool


class DBClient:

    pool: ConnectionPool

    def __init__(self, password: str):
        self.pool = ConnectionPool(
            conninfo=f"postgresql://admin:{password}@localhost:5432/streaming_data",
            min_size=1,
            max_size=2,
        )

    def connection(self):
        return self.pool.connection()


DB_CLIENT = DBClient(password=os.environ["POSTGRES_PASSWORD"])

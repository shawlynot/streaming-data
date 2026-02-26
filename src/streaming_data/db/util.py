import os

import psycopg


class DBClient:

    password: str

    def __init__(self, password: str):
        self.password = password

    def connection(self):
        return psycopg.connect(
            conninfo=f"postgresql://admin:{self.password}@localhost:5432/streaming_data"
        )


DB_CLIENT = DBClient(password=os.environ["POSTGRES_PASSWORD"])

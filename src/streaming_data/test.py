from .connector import DatabricksConnector

if __name__ == "__main__":
    connector = DatabricksConnector()
    with connector.get_cursor() as cursor:
        cursor.execute("SELECT * from range(10)")
        print(cursor.fetchall())
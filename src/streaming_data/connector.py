from databricks import sql
import os

class DatabricksConnector:
    def __init__(self):
        self.connection = sql.connect(
                        server_hostname = "dbc-e31799ee-a7d6.cloud.databricks.com",
                        http_path = "/sql/1.0/warehouses/12e332d6bd88151e",
                        access_token = os.environ["PAT"]
)
    

    def get_cursor(self):
        return self.connection.cursor()
        
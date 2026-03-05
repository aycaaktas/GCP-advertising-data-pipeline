

from google.cloud import bigquery
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Config class for holding table information
class Config:
    PROJECT_ID = 'dreamcaseayca'
    BIGQUERY_DATASET = 'ad_data'
    BIGQUERY_TABLE = 'ad_network_data'
    FILE_TABLE = 'processed_files'
    STAGING_TABLE = 'staging_ad_network_data'

# BigQueryManager class specifically for truncating tables
class BigQueryManager:
    def __init__(self):
        self.client = bigquery.Client()

    def truncate_table(self, table_name):
        """Truncate the specified BigQuery table."""
        try:
            table_id = f"{Config.PROJECT_ID}.{Config.BIGQUERY_DATASET}.{table_name}"
            query = f"TRUNCATE TABLE `{table_id}`"
            self.client.query(query).result()
            logging.info(f"Truncated table: {table_id}")
        except Exception as e:
            logging.error(f"Error truncating table '{table_name}': {e}")

    def truncate_all_tables(self):
        """Truncate ad_network_data, processed_files, and staging_ad_network_data tables."""
        self.truncate_table(Config.BIGQUERY_TABLE)
        self.truncate_table(Config.FILE_TABLE)
        self.truncate_table(Config.STAGING_TABLE)

# Execution
if __name__ == "__main__":
    bigquery_manager = BigQueryManager()
    bigquery_manager.truncate_all_tables()

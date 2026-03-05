import os
import pandas as pd
import pyarrow.parquet as pq
from google.cloud import bigquery
from datetime import datetime, timezone
import hashlib
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class Config:
    DATA_DIRECTORY = 'data'  # Local directory where you have saved your Parquet files
    PROJECT_ID = 'dreamcaseayca'
    BIGQUERY_DATASET = 'ad_data'
    EXCHANGE_RATE_TABLE = 'exchange_rates'


class BigQueryManager:
    def __init__(self):
        self.client = bigquery.Client()

    def fetch_exchange_rates(self):
        """Fetch the latest exchange rates for each currency from BigQuery."""
        try:
            query = f"""
                SELECT currency_code, rate
                FROM `{Config.PROJECT_ID}.{Config.BIGQUERY_DATASET}.{Config.EXCHANGE_RATE_TABLE}`
                WHERE date = (SELECT MAX(date) FROM `{Config.PROJECT_ID}.{Config.BIGQUERY_DATASET}.{Config.EXCHANGE_RATE_TABLE}`)
            """
            query_job = self.client.query(query)
            results = query_job.result()

            # Cache exchange rates in a dictionary
            exchange_rates = {row.currency_code: row.rate for row in results}
            logging.info(f"Fetched exchange rates: {exchange_rates}")
            return exchange_rates
        except Exception as e:
            logging.error(f"Error fetching exchange rates from BigQuery: {e}")
            return {}


class LocalProcessor:
    def __init__(self, bigquery_manager):
        self.bigquery_manager = bigquery_manager
        self.exchange_rate_cache = self.bigquery_manager.fetch_exchange_rates()
        self.processed_files = {}

    def process_parquet_file(self, file_path):
        """Process a single Parquet file and prepare data for local inspection."""
        try:
            # Generate file hash to detect duplicate files
            file_hash = self.get_file_hash(file_path)
            if file_hash in self.processed_files:
                logging.info(f"Skipping already processed file '{file_path}' with hash '{file_hash}'")
                return pd.DataFrame()

            # Load and process data
            table = pq.read_table(file_path)
            df = table.to_pandas()
            required_columns = ['dt', 'network', 'currency', 'platform', 'cost']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                logging.error(f"Missing critical columns in '{file_path}': {missing_columns}")
                return pd.DataFrame()

            # Filter duplicates based on dt, network, currency, platform, keeping the highest cost
            df = df.sort_values('cost', ascending=False).drop_duplicates(subset=['dt', 'network', 'currency', 'platform'])

            # Calculate USD cost
            df['cost_usd'] = df.apply(self.calculate_cost_usd, axis=1)
            df = df.dropna(subset=['cost_usd'])
            df['last_processed_timestamp'] = datetime.now(timezone.utc)

            # Save processed file information to avoid re-processing
            self.processed_files[file_hash] = file_path

            logging.info(f"Processed file '{file_path}' and prepared data for display.")
            print(f"\nProcessed Data for '{file_path}':")
            print(df)
            return df
        except Exception as e:
            logging.error(f"Error processing file '{file_path}': {e}")
            return pd.DataFrame()

    def calculate_cost_usd(self, row):
        """Calculate cost in USD using exchange rate."""
        exchange_rate = self.exchange_rate_cache.get(row['currency'])
        if exchange_rate is None:
            logging.warning(f"Missing exchange rate for currency '{row['currency']}' in row.")
            return None
        return row['cost'] * exchange_rate

    def get_file_hash(self, file_path):
        """Generate a hash of the file's contents to detect duplicates."""
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    def simulate_target_table(self):
        """Simulate target table updates by consolidating results from processed files."""
        consolidated_df = pd.DataFrame()

        for filename in os.listdir(Config.DATA_DIRECTORY):
            if filename.endswith('.parquet'):
                file_path = os.path.join(Config.DATA_DIRECTORY, filename)
                df = self.process_parquet_file(file_path)
                
                # Merge with consolidated DataFrame based on update rules
                if not df.empty:
                    consolidated_df = self.update_target_table_simulation(consolidated_df, df)

        logging.info("Consolidated Data (Simulated Target Table):")
        print(consolidated_df)
        return consolidated_df

    def update_target_table_simulation(self, consolidated_df, new_data):
        """Update the simulated target table with new data according to rules."""
        if consolidated_df.empty:
            return new_data

        # Merge based on unique keys (dt, network, currency, platform)
        merged_df = pd.merge(consolidated_df, new_data, on=['dt', 'network', 'currency', 'platform'], suffixes=('_existing', '_new'), how='outer')
        merged_df['cost_usd'] = merged_df.apply(
            lambda row: row['cost_usd_new'] if pd.notnull(row['cost_usd_new']) and (
                pd.isnull(row['cost_usd_existing']) or row['cost_new'] > row['cost_existing']) else row['cost_usd_existing'], axis=1)
        merged_df['cost'] = merged_df.apply(
            lambda row: row['cost_new'] if pd.notnull(row['cost_usd_new']) and (
                pd.isnull(row['cost_usd_existing']) or row['cost_new'] > row['cost_existing']) else row['cost_existing'], axis=1)

        # Clean up extra columns after merge
        columns_to_keep = ['dt', 'network', 'currency', 'platform', 'cost', 'cost_usd']
        return merged_df[columns_to_keep]


def main():
    bigquery_manager = BigQueryManager()
    processor = LocalProcessor(bigquery_manager)

    # Process all Parquet files and consolidate results in a simulated target table
    processor.simulate_target_table()


if __name__ == "__main__":
    main()

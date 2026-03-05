import os
import tempfile
import pandas as pd
import pyarrow.parquet as pq
from google.cloud import bigquery, storage
from datetime import datetime, timezone
from uuid import uuid4
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class Config:
    BUCKET_NAME = 'case_test_data'
    BUCKET_PREFIX = 'case_daily/'
    PROJECT_ID = 'dreamcaseayca'
    BIGQUERY_DATASET = 'ad_data'
    BIGQUERY_TABLE = 'ad_network_data'
    FILE_TABLE = 'processed_files'
    EXCHANGE_RATE_TABLE = 'exchange_rates'


class BigQueryManager:
    def __init__(self):
        self.client = bigquery.Client()

    def is_file_processed(self, file_name, file_hash):
        """Check if a file is already processed based on file name and hash."""
        try:
            query = f"""
                SELECT 1 
                FROM `{Config.PROJECT_ID}.{Config.BIGQUERY_DATASET}.{Config.FILE_TABLE}`
                WHERE file_name = @file_name AND file_hash = @file_hash AND status = 'processed'
            """
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("file_name", "STRING", file_name),
                    bigquery.ScalarQueryParameter("file_hash", "STRING", file_hash)
                ]
            )
            result = self.client.query(query, job_config=job_config).result()
            is_processed = result.total_rows > 0
            logging.info(f"File '{file_name}' processed status: {is_processed}")
            return is_processed
        except Exception as e:
            logging.error(f"Error checking if file is processed in 'is_file_processed' for file '{file_name}': {e}")
            return False


    def previously_processed_different_name(self, file_name, file_hash):
        """Check if there is an existing record with the same hash but a different file name."""
        try:
            query = f"""
                SELECT file_version_id
                FROM `{Config.PROJECT_ID}.{Config.BIGQUERY_DATASET}.{Config.FILE_TABLE}`
                WHERE file_hash = @file_hash AND file_name != @file_name AND status = 'processed'
            """
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("file_name", "STRING", file_name),
                    bigquery.ScalarQueryParameter("file_hash", "STRING", file_hash)
                ]
            )
            result = self.client.query(query, job_config=job_config).result()
            rows = [row.file_version_id for row in result]
            file_version_id = rows[0] if rows else None
            logging.info(f"Previously processed different name found: {file_version_id} for file '{file_name}' with hash '{file_hash}'.")
            return file_version_id
        except Exception as e:
            logging.error(f"Error checking previously processed file in 'previously_processed_different_name' for file '{file_name}': {e}")
            return None
        

    def mark_file_deleted(self, file_version_id):
        """Mark a file entry as deleted in the processed files table and nullify the hash using file_version_id.."""
        try:
            table_id = f"{Config.PROJECT_ID}.{Config.BIGQUERY_DATASET}.{Config.FILE_TABLE}"
            query = f"""
                UPDATE `{table_id}`
                SET status = 'deleted', file_hash = NULL, processed_date = DATETIME(CURRENT_TIMESTAMP())
                WHERE file_version_id = @file_version_id AND status = 'processed'
            """
            job_config = bigquery.QueryJobConfig(
                query_parameters=[bigquery.ScalarQueryParameter("file_version_id", "STRING", file_version_id)]
            )
            self.client.query(query, job_config=job_config).result()
            logging.info(f"Marked file version '{file_version_id}' as deleted.")
        except Exception as e:
            logging.error(f"Error marking file as deleted in 'mark_file_deleted' for version ID '{file_version_id}': {e}")


    def record_file_status(self, file_name, file_hash, status, file_version_id):
        """Record the status of a processed file."""
        try:
            table_id = f"{Config.PROJECT_ID}.{Config.BIGQUERY_DATASET}.{Config.FILE_TABLE}"
            query = f"""
                MERGE `{table_id}` AS target
                USING (SELECT @file_version_id AS file_version_id, @file_name AS file_name, @file_hash AS file_hash, @status AS status) AS source
                ON target.file_version_id = source.file_version_id
                WHEN MATCHED THEN
                    UPDATE SET target.file_hash = @file_hash, target.status = @status, target.processed_date = DATETIME(CURRENT_TIMESTAMP())
                WHEN NOT MATCHED THEN
                    INSERT (file_name, file_hash, file_version_id, upload_date, processed_date, status)
                    VALUES (@file_name, @file_hash, @file_version_id, DATETIME(CURRENT_TIMESTAMP()), DATETIME(CURRENT_TIMESTAMP()), @status)
            """
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("file_name", "STRING", file_name),
                    bigquery.ScalarQueryParameter("file_hash", "STRING", file_hash),
                    bigquery.ScalarQueryParameter("status", "STRING", status),
                    bigquery.ScalarQueryParameter("file_version_id", "STRING", file_version_id),
                ]
            )
            self.client.query(query, job_config=job_config).result()
            logging.info(f"Recorded file '{file_name}' status as '{status}'.")
        except Exception as e:
            logging.error(f"Error recording file status in 'record_file_status' for file '{file_name}': {e}")


    def delete_records_by_file(self, file_version_id):
        """Delete records in the main table associated with a file version."""
        try:
            table_id = f"{Config.PROJECT_ID}.{Config.BIGQUERY_DATASET}.{Config.BIGQUERY_TABLE}"
            query = f"""
                DELETE FROM `{table_id}`
                WHERE file_version_id = @file_version_id
            """
            job_config = bigquery.QueryJobConfig(
                query_parameters=[bigquery.ScalarQueryParameter("file_version_id", "STRING", file_version_id)]
            )
            self.client.query(query, job_config=job_config).result()
            logging.info(f"Deleted records for file version '{file_version_id}'.")
        except Exception as e:
            logging.error(f"Error deleting records by file version in 'delete_records_by_file' for version ID '{file_version_id}': {e}")


    def check_and_delete_missing_files(self, bucket_blobs):
            """Check for processed files that no longer exist in the bucket and delete their records in BigQuery."""
            try:
                processed_files_query = f"""
                    SELECT file_name, file_version_id 
                    FROM `{Config.PROJECT_ID}.{Config.BIGQUERY_DATASET}.{Config.FILE_TABLE}`
                    WHERE status = 'processed'
                """
                processed_files = {row.file_name: row.file_version_id for row in self.client.query(processed_files_query)}
                bucket_files = {blob.name.split('/')[-1] for blob in bucket_blobs}

                missing_files = {file_name: file_version_id for file_name, file_version_id in processed_files.items() if file_name not in bucket_files}
                for file_name, file_version_id in missing_files.items():
                    self.delete_records_by_file(file_version_id)
                    self.mark_file_deleted(file_version_id)
                    logging.info(f"Deleted BigQuery records for missing file '{file_name}' with version '{file_version_id}'.")
            except Exception as e:
                logging.error(f"Error checking and deleting missing files in 'check_and_delete_missing_files': {e}")

    def fetch_exchange_rate(self, currency):
        """Fetch the latest exchange rate for a given currency to USD."""
        try:
            query = f"""
                SELECT rate 
                FROM `{Config.PROJECT_ID}.{Config.BIGQUERY_DATASET}.{Config.EXCHANGE_RATE_TABLE}`
                WHERE currency_code = '{currency}'
                ORDER BY date DESC
                LIMIT 1
            """
            result = self.client.query(query).result()
            rate = [row.rate for row in result]
            logging.info(f"Fetched exchange rate for '{currency}': {rate[0] if rate else None}")
            return rate[0] if rate else None
        except Exception as e:
            logging.error(f"Error fetching exchange rate in 'fetch_exchange_rate' for currency '{currency}': {e}")
            return None


    def update_bigquery_table(self, df):
        """Batch insert or update rows in BigQuery based on specified rules with a staging table and MERGE."""
        try:
            staging_table_id = f"{Config.PROJECT_ID}.{Config.BIGQUERY_DATASET}.staging_ad_network_data"
            main_table_id = f"{Config.PROJECT_ID}.{Config.BIGQUERY_DATASET}.{Config.BIGQUERY_TABLE}"
            
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_TRUNCATE",
                schema=[
                    bigquery.SchemaField("dt", "DATE"),
                    bigquery.SchemaField("network", "STRING"),
                    bigquery.SchemaField("currency", "STRING"),
                    bigquery.SchemaField("platform", "STRING"),
                    bigquery.SchemaField("cost", "FLOAT"),
                    bigquery.SchemaField("cost_usd", "FLOAT"),
                    bigquery.SchemaField("file_name", "STRING"),
                    bigquery.SchemaField("file_version_id", "STRING"),
                    bigquery.SchemaField("last_processed_timestamp", "TIMESTAMP"),
                ]
            )
            
            df['last_processed_timestamp'] = datetime.now(timezone.utc)
            self.client.load_table_from_dataframe(df, staging_table_id, job_config=job_config).result()
            logging.info("Successfully uploaded data to staging table.")
            
            merge_query = f"""
                MERGE `{main_table_id}` AS target
                USING `{staging_table_id}` AS source
                ON target.dt = source.dt 
                   AND target.network = source.network 
                   AND target.currency = source.currency 
                   AND target.platform = source.platform
                WHEN MATCHED AND target.cost < source.cost THEN 
                    UPDATE SET 
                        target.cost = source.cost,
                        target.cost_usd = source.cost_usd,
                        target.file_name = source.file_name,
                        target.file_version_id = source.file_version_id,
                        target.last_processed_timestamp = source.last_processed_timestamp
                WHEN NOT MATCHED THEN 
                    INSERT (dt, network, currency, platform, cost, cost_usd, file_name, file_version_id, last_processed_timestamp)
                    VALUES (source.dt, source.network, source.currency, source.platform, source.cost, source.cost_usd, source.file_name, source.file_version_id, source.last_processed_timestamp)
            """
            self.client.query(merge_query).result()
            logging.info("Successfully merged data into main table.")
        except Exception as e:
            logging.error(f"Error updating BigQuery table in 'update_bigquery_table': {e}")
        finally:
            # Clear staging table to prevent leftover data
            self.client.query(f"TRUNCATE TABLE `{staging_table_id}`").result()

class GCSManager:
    def __init__(self):
        self.client = storage.Client()
        self.bucket = self.client.bucket(Config.BUCKET_NAME)

    def list_files(self):
        """List files in the Google Cloud Storage bucket."""
        try:
            return list(self.bucket.list_blobs(prefix=Config.BUCKET_PREFIX))
        except Exception as e:
            logging.error(f"Error listing files in bucket in 'list_files': {e}")
            return []

    def download_file(self, blob, file_path):
        """Download a file from the Google Cloud Storage bucket."""
        try:
            blob.download_to_filename(file_path)
            logging.info(f"Downloaded file {blob.name} to {file_path}")
        except Exception as e:
            logging.error(f"Error downloading file '{blob.name}' in 'download_file': {e}")


class FileProcessor:
    def __init__(self, bigquery_manager, gcs_manager):
        self.bigquery_manager = bigquery_manager
        self.gcs_manager = gcs_manager
        self.exchange_rate_cache = {}

    def process_parquet_file(self, file_path, file_name, file_hash, file_version_id):
        """Process a single Parquet file and prepare data for BigQuery insertion."""
        try:
            table = pq.read_table(file_path)
            df = table.to_pandas()

            required_columns = ['dt', 'network', 'currency', 'platform', 'cost']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                logging.error(f"Missing critical columns in '{file_name}': {missing_columns}")
                return pd.DataFrame()
        
            df = df.sort_values('cost', ascending=False).drop_duplicates(subset=['dt', 'network', 'currency', 'platform'])

            unique_currencies = set(df['currency'].dropna().unique())
            self.prefetch_exchange_rates(unique_currencies)
            

            def calculate_cost_usd(row):
                exchange_rate = self.exchange_rate_cache.get(row['currency'])
                if exchange_rate is None:
                    logging.warning(f"Missing exchange rate for currency '{row['currency']}' in file '{file_name}'.")
                    return None
                return row['cost'] * exchange_rate

            df['cost_usd'] = df.apply(calculate_cost_usd, axis=1)
            df = df.dropna(subset=['cost_usd'])
            df['file_name'] = file_name
            df['file_version_id'] = file_version_id
            df['last_processed_timestamp'] = datetime.now(timezone.utc)
            
            self.bigquery_manager.record_file_status(file_name, file_hash, 'processed', file_version_id)
            logging.info(f"Processed file '{file_name}' and prepared data for insertion.")
            return df
        except Exception as e:
            logging.error(f"Error processing file '{file_name}' in 'process_parquet_file': {e}")
            return pd.DataFrame()

    def prefetch_exchange_rates(self, currencies):
        """Fetch all required exchange rates and cache them in memory."""
        for currency in currencies:
            if currency not in self.exchange_rate_cache:
                rate = self.bigquery_manager.fetch_exchange_rate(currency)
                self.exchange_rate_cache[currency] = rate
                if rate is None:
                    logging.warning(f"No exchange rate available for currency '{currency}'.")




class SchedulerApp:
    def __init__(self):
        self.bigquery_manager = BigQueryManager()
        self.gcs_manager = GCSManager()
        self.file_processor = FileProcessor(self.bigquery_manager, self.gcs_manager)

    def run(self):
        blobs = self.gcs_manager.list_files()

        self.bigquery_manager.check_and_delete_missing_files(blobs)

        today_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        found_today_file = False

        for blob in blobs:
            folder_name = blob.name.split('/')[-2]
            file_name = blob.name.split('/')[-1]

            if not file_name.endswith('.parquet'):
                logging.warning(f"Skipping non-Parquet file '{file_name}'.")
                continue

            if folder_name == today_date:
                found_today_file = True

            file_hash = blob.md5_hash
            file_version_id = str(uuid4())

            if self.bigquery_manager.is_file_processed(file_name, file_hash):
                continue

            previous_file_version_id = self.bigquery_manager.previously_processed_different_name(file_name, file_hash)
            if previous_file_version_id:
                self.bigquery_manager.delete_records_by_file(previous_file_version_id)
                self.bigquery_manager.mark_file_deleted(previous_file_version_id)


            with tempfile.NamedTemporaryFile(delete=False) as temp_file:
                try:
                    self.gcs_manager.download_file(blob, temp_file.name)
                    df = self.file_processor.process_parquet_file(temp_file.name, file_name, file_hash, file_version_id)
                    self.bigquery_manager.update_bigquery_table(df)
                finally:
                    os.remove(temp_file.name)

        if not found_today_file:
            logging.warning(f"No new files found for today ({today_date}). The file is late.")

        return "Processing complete", 200


# Entry point for the Cloud Function
def main(request):
    app = SchedulerApp()
    return app.run()

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

# Constants
BUCKET_NAME = 'case_test_data'
BUCKET_PREFIX = 'case_daily/'
PROJECT_ID = 'dreamcaseayca'
BIGQUERY_DATASET = 'ad_data'
BIGQUERY_TABLE = 'ad_network_data'
FILE_TABLE = 'processed_files'
EXCHANGE_RATE_TABLE = 'exchange_rates'


# Caching Exchange Rates
exchange_rate_cache = {}


def generate_file_version_id():
    """Generate a unique identifier for each file version processed."""
    return str(uuid4())


def is_file_processed(file_name, file_hash):
    """Check if the file is already processed and up-to-date in the 'processed_files' table."""
    try:
        client = bigquery.Client()
        query = f"""
            SELECT 1 
            FROM `{PROJECT_ID}.{BIGQUERY_DATASET}.{FILE_TABLE}`
            WHERE file_name = @file_name AND file_hash = @file_hash AND status = 'processed'
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("file_name", "STRING", file_name),
                bigquery.ScalarQueryParameter("file_hash", "STRING", file_hash)
            ]
        )
        result = client.query(query, job_config=job_config).result()
        is_processed = result.total_rows > 0
        logging.info(f"File '{file_name}' processed status: {is_processed}")
        return is_processed
    except Exception as e:
        logging.error(f"Error checking file processed status for '{file_name}': {e}")
        return False


def mark_exact_file_deleted(file_version_id):
    """Mark the exact file entry as deleted and nullify the hash using file_version_id."""
    try:
        client = bigquery.Client()
        table_id = f"{PROJECT_ID}.{BIGQUERY_DATASET}.{FILE_TABLE}"
        query = f"""
            UPDATE `{table_id}`
            SET status = 'deleted', file_hash = NULL, processed_date = DATETIME(CURRENT_TIMESTAMP())
            WHERE file_version_id = @file_version_id AND status = 'processed'
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("file_version_id", "STRING", file_version_id)]
        )
        client.query(query, job_config=job_config).result()
        logging.info(f"Marked file version '{file_version_id}' as deleted.")
    except Exception as e:
        logging.error(f"Error marking file version '{file_version_id}' as deleted: {e}")


def record_file_status(file_name, file_hash, status, file_version_id):
    """Insert or update file status in the 'processed_files' table using file_version_id as the unique identifier."""
    try:
        client = bigquery.Client()
        table_id = f"{PROJECT_ID}.{BIGQUERY_DATASET}.{FILE_TABLE}"
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
        client.query(query, job_config=job_config).result()
        logging.info(f"Recorded file '{file_name}' status as '{status}'.")
    except Exception as e:
        logging.error(f"Error recording status for file '{file_name}': {e}")


def delete_records_by_file(file_version_id):
    """Delete records associated with a specific file version from BigQuery."""
    try:
        client = bigquery.Client()
        table_id = f"{PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}"
        query = f"""
            DELETE FROM `{table_id}`
            WHERE file_version_id = @file_version_id
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("file_version_id", "STRING", file_version_id)]
        )
        client.query(query, job_config=job_config).result()
        logging.info(f"Deleted records for file version '{file_version_id}'.")
    except Exception as e:
        logging.error(f"Error deleting records for file version '{file_version_id}': {e}")


def process_parquet_file(file_path, file_name, file_hash, file_version_id):
    """Process a single Parquet file and prepare data for BigQuery insertion."""
    try:
        table = pq.read_table(file_path)
        df = table.to_pandas()

        required_columns = ['dt', 'network', 'currency', 'platform', 'cost', 'cost_usd', 
                            'file_name', 'file_version_id', 'last_processed_timestamp']
        for col in required_columns:
            if col not in df.columns:
                df[col] = None  
               
        df = df.sort_values('cost', ascending=False).drop_duplicates(subset=['dt', 'network', 'currency', 'platform'])
        
        df['cost_usd'] = df.apply(lambda row: row['cost'] * fetch_exchange_rate(row['currency']) if fetch_exchange_rate(row['currency']) else None, axis=1)
        df['file_name'] = file_name
        df['file_version_id'] = file_version_id
        df['last_processed_timestamp'] = datetime.now(timezone.utc)
        
        record_file_status(file_name, file_hash, 'processed', file_version_id)
        logging.info(f"Processed file '{file_name}' and prepared data for insertion.")
        return df
    except Exception as e:
        logging.error(f"Error processing file '{file_name}': {e}")
        return pd.DataFrame()  # Return an empty DataFrame if processing fails


def check_and_delete_missing_files(bucket_blobs):
    """Check for processed files that no longer exist in the bucket and delete their records in BigQuery."""
    try:
        client = bigquery.Client()
        processed_files_query = f"""
            SELECT file_name, file_version_id 
            FROM `{PROJECT_ID}.{BIGQUERY_DATASET}.{FILE_TABLE}`
            WHERE status = 'processed'
        """   
        processed_files = {row.file_name: row.file_version_id for row in client.query(processed_files_query)}
        bucket_files = {blob.name.split('/')[-1] for blob in bucket_blobs}
        
        missing_files = {file_name: file_version_id for file_name, file_version_id in processed_files.items() if file_name not in bucket_files}
        for file_name, file_version_id in missing_files.items():
            delete_records_by_file(file_version_id)  
            mark_exact_file_deleted(file_version_id)
            logging.info(f"Deleted BigQuery records for missing file '{file_name}' with version '{file_version_id}'.")
    except Exception as e:
        logging.error(f"Error checking and deleting missing files: {e}") 


def previously_processed_different_name(file_name, file_hash):
    """Check if there is an existing record with the same hash but a different file name, returning the file_version_id."""
    try:
        client = bigquery.Client()
        query = f"""
            SELECT file_version_id
            FROM `{PROJECT_ID}.{BIGQUERY_DATASET}.{FILE_TABLE}`
            WHERE file_hash = @file_hash AND file_name != @file_name AND status = 'processed'
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("file_name", "STRING", file_name),
                bigquery.ScalarQueryParameter("file_hash", "STRING", file_hash)
            ]
        )
        result = client.query(query, job_config=job_config).result()
        rows = [row.file_version_id for row in result]
        file_version_id = rows[0] if rows else None
        logging.info(f"Previously processed different name found: {file_version_id} for file '{file_name}' with hash '{file_hash}'.")
        return file_version_id
    except Exception as e:
        logging.error(f"Error checking previously processed file '{file_name}': {e}")
        return None


def fetch_exchange_rate(currency):
    """Fetch the latest exchange rate for a given currency to USD, using cache where possible."""
    if currency in exchange_rate_cache:
        return exchange_rate_cache[currency]

    try:
        client = bigquery.Client()
        query = f"""
            SELECT rate 
            FROM `{PROJECT_ID}.{BIGQUERY_DATASET}.{EXCHANGE_RATE_TABLE}`
            WHERE currency_code = '{currency}'
            ORDER BY date DESC
            LIMIT 1
        """
        result = client.query(query).result()
        rate = [row.rate for row in result]
        exchange_rate_cache[currency] = rate[0] if rate else None
        logging.info(f"Fetched exchange rate for '{currency}': {exchange_rate_cache[currency]}")
        return exchange_rate_cache[currency]
    except Exception as e:
        logging.error(f"Error fetching exchange rate for '{currency}': {e}")
        return None



def update_bigquery_table(df):
    """Batch insert or update rows in BigQuery based on specified rules with a staging table and MERGE."""
    client = bigquery.Client()
    staging_table_id = f"{PROJECT_ID}.{BIGQUERY_DATASET}.staging_ad_network_data"
    main_table_id = f"{PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}"
    
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
        
    try:
        
        logging.info("Starting to upload data to staging table.")
        client.load_table_from_dataframe(df, staging_table_id, job_config=job_config).result()
        logging.info("Successfully uploaded data to staging table.")
        
    except Exception as e:
        logging.error(f"Error loading data to staging table '{staging_table_id}': {e}")
        return  

    try:
        logging.info("Starting MERGE operation to update main table.")
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
        client.query(merge_query).result()
        logging.info("Successfully merged data into main table.")
        
    except Exception as e:
        logging.error(f"Error merging data into main table '{main_table_id}': {e}")





def main(request):
    """
    Main function for orchestrating file ingestion and processing.
    This function checks Google Cloud Storage for files, processes and
    uploads them to BigQuery, and logs if today's file is missing.
    """
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blobs = list(bucket.list_blobs(prefix=BUCKET_PREFIX))

    today_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    found_today_file = False

    check_and_delete_missing_files(blobs)
    
    for blob in blobs:
        folder_name = blob.name.split('/')[-2]
        file_name = blob.name.split('/')[-1]

        if not file_name.endswith('.parquet'):
            logging.warning(f"Skipping non-Parquet file '{file_name}'.")
            continue

        if folder_name == today_date:
            found_today_file = True  

        file_hash = blob.md5_hash  
        file_version_id = generate_file_version_id() 

        if is_file_processed(file_name, file_hash):
            continue  

        with tempfile.NamedTemporaryFile(delete=False) as tmp_file:

            blob.download_to_filename(tmp_file.name)

            previous_file_version_id = previously_processed_different_name(file_name, file_hash)
            if previous_file_version_id:
                delete_records_by_file(previous_file_version_id)
                mark_exact_file_deleted(previous_file_version_id)

            df = process_parquet_file(tmp_file.name, file_name, file_hash, file_version_id)
            
            update_bigquery_table(df)

            os.remove(tmp_file.name)

    if not found_today_file:
        logging.warning(f"No new files found for today ({today_date}). The file is late.")


    return "Processing complete", 200  # 200 OK status code






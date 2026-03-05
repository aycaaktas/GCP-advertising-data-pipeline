import requests
from google.cloud import bigquery
from datetime import datetime, timezone
import json
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Constants
API_KEY = 'C9IDSKD63YgUsEOMLccvpdAjSbyiQWVIQMd8'
EXCHANGE_API_URL = f"https://currencyapi.net/api/v1/rates?key={API_KEY}&base=USD"
BIGQUERY_TABLE_ID = "dreamcaseayca.ad_data.exchange_rates"
TEMP_FILE_PATH = "/tmp/exchange_rates_data.json"

def fetch_exchange_rates():
    """
    Fetches current exchange rates from the currency API.
    Returns:
        dict: A dictionary with currency codes as keys and exchange rates as values.
    Raises:
        Exception: If the API request fails or an error occurs while fetching rates.
    """
    try:
        logging.info("Fetching exchange rates from the API.")
        response = requests.get(EXCHANGE_API_URL)
        response.raise_for_status()
        data = response.json()
        logging.info("Exchange rates fetched successfully.")
        return data.get('rates', {})
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch exchange rates: {e}")
        return {}


def prepare_exchange_rate_data(exchange_rates):
    """
    Prepares exchange rate data for BigQuery insertion.
    Args:
        exchange_rates (dict): Dictionary of exchange rates.
    Returns:
        list: List of dictionaries containing formatted exchange rate data.
    """
    logging.info("Preparing exchange rate data for BigQuery insertion.")
    current_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return [
        {
            "date": current_date,
            "currency_code": currency,
            "rate": rate
        }
        for currency, rate in exchange_rates.items()
    ]


def store_exchange_rates_to_bigquery(rows_to_insert):
    """
    Stores the exchange rates in BigQuery using a temporary JSON file.
    Args:
        rows_to_insert (list): List of dictionaries with the prepared exchange rate data.
    """
    client = bigquery.Client()
    
    # Write data to a local JSON file for BigQuery upload
    try:
        logging.info("Writing exchange rate data to a local JSON file.")
        with open(TEMP_FILE_PATH, "w") as f:
            for row in rows_to_insert:
                f.write(json.dumps(row) + "\n")
        logging.info("Data written to JSON file successfully.")
        
    except Exception as e:
        logging.error(f"Error writing data to JSON file: {e}")
        return  

    # Configure job for BigQuery load
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=[
            bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("currency_code", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("rate", "FLOAT", mode="REQUIRED"),
        ]
    )


    # Load JSON file to BigQuery
    try:
        logging.info("Starting to load data from JSON file to BigQuery.")
        with open(TEMP_FILE_PATH, "rb") as source_file:
            load_job = client.load_table_from_file(source_file, BIGQUERY_TABLE_ID, job_config=job_config)
        
        load_job.result()
        logging.info("Exchange rates inserted successfully into BigQuery.")
        
    except Exception as e:
        logging.error(f"Error loading data into BigQuery table '{BIGQUERY_TABLE_ID}': {e}")

    finally:
        if os.path.exists(TEMP_FILE_PATH):
            os.remove(TEMP_FILE_PATH)
            logging.info("Temporary JSON file removed.")

def main(request):
    """
    Main function to fetch exchange rates, prepare data, and store it in BigQuery.
    Args:
        request (flask.Request): The request object.
    Returns:
        str: Success or error message.
    """
    logging.info("Starting exchange rate fetching and storage process.")
    
    # Fetch exchange rates
    exchange_rates = fetch_exchange_rates()
    if not exchange_rates:
        logging.error("No exchange rates were fetched. Exiting.")
        return "Failed to fetch exchange rates."

    # Prepare data for BigQuery
    rows_to_insert = prepare_exchange_rate_data(exchange_rates)
    if not rows_to_insert:
        logging.error("No data to insert into BigQuery. Exiting.")
        return "No data prepared for BigQuery insertion."
    
    # Store data in BigQuery
    store_exchange_rates_to_bigquery(rows_to_insert)
    logging.info("Process completed successfully.")
    return "Exchange rates successfully fetched and stored in BigQuery."
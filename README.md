
# Ad Spend Data Pipeline (GCP)

A production-style **data engineering pipeline built on Google Cloud Platform** for ingesting advertising data, performing currency normalization, and maintaining clean analytical datasets in BigQuery.

This project demonstrates a **cloud-native ETL pipeline** that processes Parquet files from Google Cloud Storage, converts advertising costs to USD using exchange rates, and loads the processed data into BigQuery using a scalable architecture.

---

# Architecture

The pipeline is composed of two main components:

1. **Exchange Rate Pipeline**
2. **Advertising Data Ingestion Pipeline**

```

GCS (Parquet files)
        │
        ▼
Cloud Function (Scheduler Trigger)
        │
        ▼
Data Processing (Python)
        │
        ▼
BigQuery Staging Table
        │
        ▼
MERGE into Production Table
        │
        ▼
Analytics Ready Dataset

Exchange Rate API
        │
        ▼
Cloud Function
        │
        ▼
BigQuery Exchange Rate Table


---

# Features

- Automated **daily data ingestion**
- **Parquet file processing**
- **Currency normalization to USD**
- **Exchange rate API integration**
- **File deduplication using file hashes**
- **File version tracking**
- **Automatic deletion of missing files**
- **MERGE-based upserts in BigQuery**
- **Serverless architecture**
- **Cloud Scheduler automation**

---

# Project Structure


project-root
│
├── ad_network
│   ├── main.py
│   ├── main_org.py
│   ├── check.py
│   ├── truncate_tables.py
│   └── requirements.txt
│
├── exchangerate
│   ├── main.py
│   └── requirements.txt
│
├── data
│   └── sample parquet files


---

# Component 1: Exchange Rate Pipeline

This component retrieves currency exchange rates from an external API and stores them in BigQuery.

Responsibilities:

- Fetch exchange rates from API
- Prepare structured data
- Store exchange rates historically in BigQuery
- Run daily using Cloud Scheduler

Example schema:

| Column | Type |
|------|------|
| date | DATE |
| currency_code | STRING |
| rate | FLOAT |

Exchange rates are used to convert advertising costs to USD during data processing.

---

# Component 2: Advertising Data Pipeline

This pipeline processes advertising spend data stored in Google Cloud Storage.

Input files are delivered as **Parquet files** containing advertising performance metrics.

Example input schema:

| Column | Description |
|------|-------------|
| dt | Operation date |
| network | Advertising network |
| currency | Cost currency |
| platform | Device platform (ios/android) |
| cost | Advertising spend |

---

# Processing Logic

Each Parquet file is processed according to the following rules.

## Deduplication

If multiple rows exist for the same:

dt + network + currency + platform


only the row with the **highest cost** is retained.

---

## Currency Conversion

The system retrieves the **latest exchange rate** and computes:


cost_usd = cost * exchange_rate


Exchange rates are cached during processing to reduce repeated queries.

---

## File Version Tracking

Each file is tracked using the following metadata:


file_name
file_hash
file_version_id


This ensures:

- files are not processed twice
- renamed files are handled correctly
- deleted files remove their data from BigQuery

---

# Data Loading Strategy

The pipeline follows a **staging → merge pattern**.

1. Load processed data into a **staging table**
2. Perform a **MERGE operation** into the production table

Rules:

- If record exists and new cost is higher → update
- If record does not exist → insert
- Maintain only one record per key

Key:


dt + network + currency + platform


This ensures consistent and idempotent updates to the dataset.

---

# Target Table Schema

| Column | Type |
|------|------|
| dt | DATE |
| network | STRING |
| currency | STRING |
| platform | STRING |
| cost | FLOAT |
| cost_usd | FLOAT |
| file_name | STRING |
| file_version_id | STRING |
| last_processed_timestamp | TIMESTAMP |

---

# Handling Edge Cases

The pipeline handles several real-world scenarios.

## Late arriving files

If today's file does not arrive, the system logs a warning but continues execution.

## File re-upload with same name

Files are identified using **hash values** to avoid duplicate processing.

## File renamed

If the same content appears with a different name:

- old records are removed
- new records are inserted

## Missing files

If a previously processed file disappears from storage:

- associated records are deleted
- file status is marked as deleted

---

# Local Testing

To simulate processing locally:


python check.py


This script processes Parquet files locally and simulates the BigQuery merge logic.

---

# Deployment

The pipeline is designed to run using **Google Cloud Functions**.

Trigger:


Google Cloud Scheduler


Execution frequency:


Daily


Deployment steps:

1. Upload code to the cloud environment
2. Deploy the Cloud Function
3. Configure Cloud Scheduler trigger
4. Grant BigQuery and GCS permissions

---

# Requirements


google-cloud-bigquery
google-cloud-storage
requests
pandas
pyarrow


---

# Example Workflow

1. Ad network uploads Parquet file to Google Cloud Storage
2. Cloud Scheduler triggers the Cloud Function
3. Function scans the storage bucket
4. New files are detected
5. Data is processed
6. Costs are converted to USD
7. Data is loaded into staging table
8. MERGE operation updates the main BigQuery table

---

# Technologies Used

- Python
- Google Cloud Storage
- Google BigQuery
- Google Cloud Functions
- Cloud Scheduler
- Parquet
- Pandas
- REST APIs

---

# Author

Ayça Aktaş  

Computer Science & Engineering  
Sabancı University  

---

# License

MIT License


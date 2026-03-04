# 📊 Ad Spend Data Pipeline (GCP)

![GCP](https://img.shields.io/badge/Platform-Google%20Cloud-blue)
![Python](https://img.shields.io/badge/Python-3.x-yellow)
![BigQuery](https://img.shields.io/badge/Data%20Warehouse-BigQuery-blue)
![License](https://img.shields.io/badge/License-MIT-green)
![ETL](https://img.shields.io/badge/Pipeline-ETL-orange)

A **production-style data engineering pipeline built on Google Cloud Platform** for ingesting advertising data, performing currency normalization, and maintaining clean analytical datasets in **BigQuery**.

This project demonstrates a **cloud-native ETL pipeline** that processes **Parquet files from Google Cloud Storage**, converts advertising costs to **USD using exchange rates**, and loads the processed data into **BigQuery using a scalable architecture**.

---

# 🏗 Architecture

The pipeline consists of **two main components**:

1. **Exchange Rate Pipeline**
2. **Advertising Data Ingestion Pipeline**

## Data Ingestion Pipeline

    ┌────────────────────────┐
    │  Parquet Files (GCS)   │
    └─────────────┬──────────┘
                  │
                  ▼
    ┌────────────────────────┐
    │ Cloud Scheduler        │
    │ (Daily Trigger)        │
    └─────────────┬──────────┘
                  │
                  ▼
    ┌────────────────────────┐
    │ Cloud Function         │
    │ Python Processing      │
    └─────────────┬──────────┘
                  │
                  ▼
    ┌────────────────────────┐
    │ BigQuery Staging Table │
    └─────────────┬──────────┘
                  │
                  ▼
    ┌────────────────────────┐
    │ BigQuery MERGE         │
    │ Production Table       │
    └─────────────┬──────────┘
                  │
                  ▼
    ┌────────────────────────┐
    │ Analytics Ready Data   │
    └────────────────────────┘

## Exchange Rate Pipeline

    ┌────────────────────────┐
    │  Exchange Rate API     │
    └─────────────┬──────────┘
                  │
                  ▼
    ┌────────────────────────┐
    │ Cloud Function         │
    │ Fetch + Transform      │
    └─────────────┬──────────┘
                  │
                  ▼
    ┌────────────────────────┐
    │ BigQuery Table         │
    │ Exchange Rates         │
    └────────────────────────┘

---

# ✨ Features

✔ Automated **daily data ingestion**  
✔ **Parquet file processing**  
✔ **Currency normalization to USD**  
✔ **Exchange rate API integration**  
✔ **File deduplication using file hashes**  
✔ **File version tracking**  
✔ **Automatic deletion of missing files**  
✔ **MERGE-based upserts in BigQuery**  
✔ **Serverless architecture**  
✔ **Cloud Scheduler automation**

---

# 📁 Project Structure

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
│
└── README.md


# 🔄 Component 1: Exchange Rate Pipeline

This component retrieves **currency exchange rates from an external API** and stores them in **BigQuery**.

### Responsibilities

- Fetch exchange rates from API
- Prepare structured data
- Store exchange rates historically
- Run daily using **Cloud Scheduler**

### Example Schema

| Column | Type |
|------|------|
| date | DATE |
| currency_code | STRING |
| rate | FLOAT |

Exchange rates are used to **convert advertising costs to USD** during processing.

---

# 📥 Component 2: Advertising Data Pipeline

This pipeline processes **advertising spend data stored in Google Cloud Storage**.

Input files are delivered as **Parquet files** containing advertising performance metrics.

### Example Input Schema

| Column | Description |
|------|-------------|
| dt | Operation date |
| network | Advertising network |
| currency | Cost currency |
| platform | Device platform (ios/android) |
| cost | Advertising spend |

---

# ⚙ Processing Logic

Each Parquet file is processed according to the following rules.

---

## Deduplication

If multiple rows exist for the same key:


dt + network + currency + platform


Only the row with the **highest cost** is retained.

---

## Currency Conversion

The system retrieves the **latest exchange rate** and computes:


cost_usd = cost * exchange_rate


Exchange rates are **cached during processing** to reduce repeated queries.

---

## File Version Tracking

Each file is tracked using the following metadata:


file_name
file_hash
file_version_id


This ensures:

- Files are not processed twice
- Renamed files are handled correctly
- Deleted files remove their data from BigQuery

---

# 🗄 Data Loading Strategy

The pipeline follows a **staging → merge pattern**.

1️⃣ Load processed data into a **staging table**  
2️⃣ Perform a **MERGE operation** into the production table  

### Rules

- If record exists and new cost is higher → **update**
- If record does not exist → **insert**
- Maintain only **one record per key**

### Primary Key


dt + network + currency + platform


This ensures **idempotent and consistent updates**.

---

# 📊 Target Table Schema

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

# ⚠ Handling Edge Cases

The pipeline handles several real-world scenarios.

### Late Arriving Files

If today's file does not arrive, the system logs a warning but continues execution.

### File Re-upload with Same Name

Files are identified using **hash values** to avoid duplicate processing.

### File Renamed

If the same content appears with a different name:

- Old records are removed
- New records are inserted

### Missing Files

If a previously processed file disappears from storage:

- Associated records are deleted
- File status is marked as **deleted**

---

# 🧪 Local Testing

To simulate processing locally:

```bash
python check.py

This script:

Processes Parquet files locally

Simulates BigQuery merge logic

☁ Deployment

The pipeline runs using Google Cloud Functions.

Trigger
Google Cloud Scheduler
Frequency
Daily
Deployment Steps

Upload code to the cloud environment

Deploy the Cloud Function

Configure the Cloud Scheduler trigger

Grant BigQuery and GCS permissions

📦 Requirements
google-cloud-bigquery
google-cloud-storage
requests
pandas
pyarrow

Install dependencies:

pip install -r requirements.txt

🔁 Example Workflow

1️⃣ Ad network uploads Parquet file to Google Cloud Storage
2️⃣ Cloud Scheduler triggers the Cloud Function
3️⃣ Function scans the storage bucket
4️⃣ New files are detected
5️⃣ Data is processed
6️⃣ Costs are converted to USD
7️⃣ Data is loaded into staging table
8️⃣ MERGE operation updates the production BigQuery table

🛠 Technologies Used
Python	Data processing
Google Cloud Storage	Data lake
BigQuery	Data warehouse
Cloud Functions	Serverless compute
Cloud Scheduler	Workflow automation
Pandas	Data transformation
Parquet	Efficient columnar storage
REST APIs	Exchange rate data


📜 License

MIT License

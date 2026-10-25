
![Profile Picture](assets/banset.png)

A Distributed Ride Analytics Warehouse that processes raw NYC taxi data into clean, structured datasets for analytics, using batch ELT, CDC, SCD Type 2, and dimensional modeling.

## Problem

Raw trip data is not immediately ready for reliable analytics. It needs to be cleaned, validated, structured, and loaded into a warehouse. Source records can also change over time, making it important to distinguish the latest state of a record from its historical versions.

REINEX explores these challenges by building a pipeline that handles data ingestion and transformation, processes source changes, tracks historical changes, and prepares data for analytical queries.

## Solution

REINEX brings raw taxi-trip data through two connected engineering workflows:

• **Local data pipeline** — Processes incoming changes through Bronze and Silver layers, applies inserts, updates, and deletes, and loads the current records into PostgreSQL.

• **Historical tracking** — Uses dbt snapshots with SCD Type 2 to preserve previous versions of records in PostgreSQL.

• **Cloud analytics** — Loads 12 monthly Parquet datasets into BigQuery and uses dbt to build a staging model, four analytical dimensions, and a fact view.

• **Data validation** — Checks ingestion results, validates important fields, and tests analytical models to catch data-quality issues.

The result is a project that explores both how data changes are managed and how large datasets are prepared for analytics.

## System Architecture

```text
              ┌─────────────────────────┐
              │   NYC Taxi Dataset      │
              │   Monthly Parquet Files │
              └────────────┬────────────┘
                           │
                    Data Ingestion
                           │
              ┌────────────┴────────────┐
              │                         │
       CDC Event Generation      Batch Ingestion
              │                         │
              ▼                         ▼
    ┌──────────────────┐     ┌──────────────────┐
    │ Python Producer  │     │ Python Loader    │
    │ CDC Events       │     │ BigQuery Ingest  │
    └────────┬─────────┘     └────────┬─────────┘
             │                         │
      Bronze / Silver             Raw Data Load
             │                         │
             ▼                         ▼
    ┌──────────────────┐     ┌──────────────────┐
    │ Parquet Files    │     │ BigQuery Raw     │
    │ Bronze / Silver  │     │ Table            │
    └────────┬─────────┘     └────────┬─────────┘
             │                         │
      Current State              dbt Transformations
             │                         │
             ▼                         ▼
    ┌──────────────────┐     ┌──────────────────┐
    │ PostgreSQL       │     │ dbt Models       │
    │ Current Records  │     │ Staging + Dims   │
    └────────┬─────────┘     └────────┬─────────┘
             │                         │
      Historical Tracking        Analytics Modeling
             │                         │
             ▼                         ▼
    ┌──────────────────┐     ┌──────────────────┐
    │ dbt Snapshot     │     │ fact_trip        │
    │ SCD Type 2       │     │ Analytics View   │
    └──────────────────┘     └──────────────────┘
```

### Local CDC Pipeline

```text
       ┌──────────────────────┐
       │ Source Trip Records  │
       │ Parquet Dataset      │
       └──────────┬───────────┘
                  │
             Generate Events
                  │
                  ▼
       ┌──────────────────────┐
       │ Python Producer      │
       │ CDC Event Generation │
       └──────────┬───────────┘
                  │
          Write CDC Events + State
                  │
                  ▼
       ┌──────────────────────┐
       │ Bronze Parquet       │
       │ Events + Source State│
       └──────────┬───────────┘
                  │
             Apply CDC Changes
                  │
                  ▼
       ┌──────────────────────┐
       │ Silver Processing    │
       │ Clean + Deduplicate  │
       └──────────┬───────────┘
                  │
             Validate + Load
                  │
                  ▼
       ┌──────────────────────┐
       │ PostgreSQL           │
       │ Current Trip Records │
       └──────────┬───────────┘
                  │
          Track Record Changes
                  │
                  ▼
       ┌──────────────────────┐
       │ dbt Snapshot         │
       │ SCD Type 2 History   │
       └──────────────────────┘
```

### BigQuery Modeling & Transformation

```text
       ┌──────────────────────┐
       │ Monthly Parquet Files│
       └──────────┬───────────┘
                  │
             Batch Ingestion
                  │
                  ▼
       ┌──────────────────────┐
       │ BigQuery Loader      │
       │ Normalize + Validate │
       └──────────┬───────────┘
                  │
              Load Raw Data
                  │
                  ▼
       ┌──────────────────────┐
       │ BigQuery Raw Table   │
       └──────────┬───────────┘
                  │
             Transform + Model
                  │
                  ▼
       ┌──────────────────────┐
       │ dbt Staging + Dims   │
       └──────────┬───────────┘
                  │
              Build Fact View
                  │
                  ▼
       ┌──────────────────────┐
       │ fact_trip View       │
       │ Analytics Output     │
       └──────────────────────┘
```
## Key Engineering Decisions

• **Separated Bronze and Silver layers** — Kept incoming data separate from cleaned records, making transformations easier to inspect and debug.

• **Implemented CDC operations** — Handled inserts, updates, and deletes explicitly so the current dataset reflects changes instead of relying on full replacements.

• **Used SCD Type 2 snapshots** — Preserved historical record versions rather than losing previous values when records change.

• **Used Parquet for intermediate storage** — Organized intermediate datasets in a columnar file format suitable for data processing.

• **Built dimensional models with dbt** — Separated reusable dimensions from trip-level facts to make the analytical structure easier to query.

• **Kept the BigQuery fact model as a view** — Avoided physically storing another copy of the full 38-million-row result under the BigQuery Sandbox storage constraints.

## What Broke & How I Improved It

### BigQuery Column Mapping

• **What broke** — The source Parquet files contained mixed-case column names, but the loader normalized them to lowercase. The mismatch caused important fields, including vendor and location identifiers, to be loaded as `NULL`.

• **How I improved it** — Standardized the column mapping and reloaded all 12 monthly datasets. I then verified the total row count and checked that the vendor, pickup, and drop-off identifiers were populated.

### Handling Updates and Deletes

• **The challenge** — Updating a record in place can overwrite its previous values, while deleting a record can remove information needed to understand its history.

• **How I improved it** — Implemented explicit CDC operations in the local pipeline and added PostgreSQL dbt snapshots for SCD Type 2 history. This separates maintaining the current state from preserving historical versions.

### Identifying Records Reliably

• **The limitation** — The BigQuery staging model generates trip identifiers by hashing row contents. Identical source rows can therefore produce the same identifier.

• **What I learned** — A row-content hash is useful for identifying distinct row contents, but it cannot guarantee a unique identity for every real-world trip when the source does not provide a reliable unique trip ID.

## Results & What Improved

| What was verified | Result |
|---|---:|
| Monthly Parquet files loaded into BigQuery | 12 |
| Rows loaded and verified | 38,310,226 |
| Columns in the BigQuery raw table | 19 |
| Vendor dimension | 3 rows |
| Passenger-count dimension | 10 rows |
| Location dimension | 263 rows |
| Time dimension | 376 rows |
| dbt models in the project | 6 |

• **More reliable ingestion** — The BigQuery row count matched the combined row count of the 12 monthly source files, and the required vendor and location identifiers were non-null.

• **More structured analytics** — The four dimensions and fact view were built successfully, with the configured dbt tests passing across the tested models.

• **Better handling of changes** — The local workflow supports CDC operations, while PostgreSQL snapshots preserve historical versions.

• **Clearer separation of responsibilities** — Local change processing and cloud analytics can be understood and validated as separate workflows.

These results demonstrate successful implementation and validation, not measured production performance or cost savings.


## Tech Stack

### Data Processing & Ingestion
- **Python** — Handles data ingestion, transformation, validation, and pipeline logic.
- **pandas** — Processes tabular taxi-trip data, cleans records, and applies CDC operations.
- **Apache Parquet** — Stores source and intermediate datasets in a columnar format for efficient data processing.
- **SHA-256 Hashing** — Generates deterministic identifiers from trip attributes in the BigQuery staging model.

### Data Storage & Warehousing
- **PostgreSQL** — Stores the current processed dataset and supports the local CDC and historical-tracking workflow.
- **Google BigQuery** — Stores the 12 monthly taxi datasets and provides the cloud warehouse for analytical queries.
- **Docker & Docker Compose** — Provides a containerized local environment for running PostgreSQL.

### Data Modeling & Transformation
- **dbt Core** — Organizes SQL transformations into staging and dimensional models, manages PostgreSQL snapshots, and runs data-quality tests.
- **CDC (Change Data Capture)** — Processes inserts, updates, and deletes in the local pipeline.
- **SCD Type 2** — Preserves historical record versions through PostgreSQL dbt snapshots.
- **Dimensional Modeling** — Structures analytical data into fact and dimension models for easier querying.

### Development & Validation
- **Git & GitHub** — Tracks source-code changes and maintains the project repository.
- **dbt Tests & Pipeline Checks** — Validate model constraints, required fields, and ingestion row counts.
- **Python Virtual Environment** — Keeps project dependencies isolated from the system Python installation.

## Folder Structure

```text
REINEX/
├── bronze/
│   ├── trips.parquet
│   └── source_state.parquet
├── silver/
│   └── trips_clean.parquet
├── datasets/
│   ├── 2023-01.parquet
│   ├── 2023-02.parquet
│   └── ... monthly datasets
├── reinex_dbt/
│   ├── models/
│   │   ├── staging/
│   │   └── marts/
│   ├── snapshots/
│   └── dbt_project.yml
├── producer.py
├── silver.py
├── load_silver.py
├── load_bq.py
├── schemas.py
├── docker-compose.yaml
└── README.md
```
• **`producer.py`** — Generates the local source data and CDC events.

• **`silver.py`** — Applies source changes and produces the cleaned current dataset.

• **`load_silver.py`** — Validates and loads the Silver dataset into PostgreSQL.

• **`load_bq.py`** — Loads monthly Parquet datasets into BigQuery and validates the results.

• **`reinex_dbt/`** — Contains staging and dimensional models, tests, and PostgreSQL snapshots.

• **`bronze/`, `silver/`, and `datasets/`** — Store the intermediate and source data files.


## Running REINEX Locally

### 1. Clone the Repository

```bash
git clone https://github.com/anriam-codes/REINEX.git
cd REINEX
```

### 2. Set Up Python

```cmd
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

### 3. Start the Local Database

```cmd
docker compose up -d
```

Configure the PostgreSQL connection to match the project's database settings.

### 4. Run the Local Pipeline

Run the scripts in sequence:

```cmd
python producer.py
python silver.py
python load_silver.py
```

### 5. Run dbt Against PostgreSQL

```cmd
dbt debug --project-dir reinex_dbt --target dev
dbt build --project-dir reinex_dbt --target dev
```

This workflow builds the local analytical models and runs the PostgreSQL SCD Type 2 snapshot.

### 6. Run the BigQuery Workflow

Configure Google Cloud credentials and the BigQuery target before running:

```cmd
python load_bq.py
dbt debug --project-dir reinex_dbt --target bigquery
dbt build --project-dir reinex_dbt --target bigquery
```

The SCD Type 2 snapshot is configured for PostgreSQL and disabled for BigQuery, so the BigQuery build does not require a snapshot exclusion.

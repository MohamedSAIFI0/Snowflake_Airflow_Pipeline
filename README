### Ecommerce Data Pipeline Project

## Overview
This project implements a modern data pipeline for an ecommerce platform, leveraging AWS S3 for data storage and Snowflake for data warehousing. The pipeline ingests raw data (CSV, JSON), stages it in S3, and loads it into Snowflake using Snowpipe for automated, scalable data ingestion. The data is then processed through Bronze, Silver, and Gold layers for analytics and reporting.

![Architecture Diagram](Architecture_image.png)

---

## Architecture
- **Data Sources:**
  - CSV and JSON files (e.g., customers, products, sales)
- **Ingestion:**
  - Python scripts upload data to AWS S3
- **Staging:**
  - S3 bucket acts as the raw data lake
- **Snowpipe Streaming:**
  - Snowpipe automatically loads new data from S3 into Snowflake
- **Data Warehouse (Snowflake):**
  - Bronze Layer: Raw data
  - Silver Layer: Cleaned/transformed data
  - Gold Layer: Aggregated/curated data for analytics

---

## Folder Structure
```
├── Architecture_image.png         # Pipeline architecture diagram
├── data/                         # Source data files (CSV, JSON)
├── log/                          # Log files
├── scripts/                      # Python and shell scripts for data handling
├── sql_code/                     # SQL scripts for Snowflake (DDL, Snowpipe, etc.)
├── next_steps.txt                # Project TODOs and next steps
├── README                        # Project documentation
```

---

## Key Components

### 1. Data Files (`data/`)
- `customers.csv`, `products.csv`, `sales.json`: Example source data files.

### 2. Scripts (`scripts/`)
- `generate_fake_data.py`: Generates synthetic data for testing.
- `from_local_to_s3.py`: Uploads local data files to AWS S3 bucket.
- `commands.sh`: Shell commands for automation.

### 3. SQL Code (`sql_code/`)
- `bronze.sql`: Creates Snowflake warehouse, database, schema, and raw tables.
- `snowpipe.sql`: Sets up S3 stage and Snowpipe for automated data loading.
- `silver.sql`, `gold.sql`: Transformations for Silver and Gold layers.

---

## How the Pipeline Works
1. **Generate or collect data** and place it in the `data/` folder.
2. **Upload data to S3** using the provided Python script:
   ```bash
   python scripts/from_local_to_s3.py
   ```
3. **Snowpipe** detects new files in S3 and loads them into the Bronze layer tables in Snowflake.
4. **Transform data** using the provided SQL scripts for Silver and Gold layers.

---

## Setting Up
1. **Configure AWS S3 and Snowflake:**
   - Set up an S3 bucket and Snowflake storage integration.
   - Update connection details in scripts as needed.
2. **Run SQL scripts in order:**
   - `bronze.sql` → `snowpipe.sql` → `silver.sql` → `gold.sql`
3. **Automate with Event Notifications:**
   - Set up S3 event notifications to trigger Snowpipe (see comments in `snowpipe.sql`).

---

## Next Steps
- Implement Airflow DAGs for orchestration (optional)
- Add data quality checks and monitoring
- Expand data sources and transformations

---

## References
- [Snowflake Documentation](https://docs.snowflake.com/)
- [AWS S3 Documentation](https://docs.aws.amazon.com/s3/)

---

## Author
- Mohamed SAIFI

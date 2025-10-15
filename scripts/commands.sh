#!/bin/bash
set -e
LOG_FILE="/home/saifi/Desktop/Data_Projects/eccomerce_project/log/log.txt"


python3 -m venv venv 
source venv/bin/activate
mkdir data
mkdir scripts && touch from_local_to_s3.py generate_fake_data.py
mkdir sql_code && touch bronze.sql gold.sql silver.sql snowpipe.sql
pip install -r requirements.txt

echo "[$(date '+%Y-%m-%d %H:%M:%S')] Project setup completed successfully!" >> $LOG_FILE
python3 -m venv venv 
source venv/bin/activate

mkdir data
mkdir scripts && touch from_local_to_s3.py generate_fake_data.py
mkdir sql_code && touch bronze.sql gold.sql silver.sql snowpipe.sql

"""
Ecommerce Data Pipeline DAG
Author: Mohamed SAIFI
Description: Orchestrates the end-to-end data pipeline from data generation to Gold layer analytics
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import boto3
import os
from faker import Faker
import pandas as pd
import random
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Default arguments
default_args = {
    "owner": "Mohamed_SAIFI",
    "depends_on_past": False,
    "email": ["saifimsc@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    "ecommerce_data_pipeline",
    default_args=default_args,
    description="End-to-end ecommerce data pipeline with Bronze, Silver, and Gold layers",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["ecommerce", "snowflake", "s3", "data-pipeline"],
)

# ============================================================
# Python Functions
# ============================================================

def generate_fake_data(**context):
    """Generate synthetic ecommerce data"""
    fake = Faker()
    execution_date = context["ds"]

    data_dir = "/tmp/ecommerce_data"
    os.makedirs(data_dir, exist_ok=True)

    # Customers
    customers = [
        {
            "customer_id": i,
            "name": fake.name(),
            "country": fake.country(),
            "email": fake.email(),
        }
        for i in range(1, 101)
    ]
    pd.DataFrame(customers).to_csv(
        f"{data_dir}/customers_{execution_date}.csv", index=False
    )

    # Products
    products = [
        {
            "product_id": i,
            "name": fake.word(),
            "category": random.choice(
                ["Electronics", "Clothing", "Sports", "Home", "Books"]
            ),
            "price": round(random.uniform(10, 1000), 2),
        }
        for i in range(1, 51)
    ]
    pd.DataFrame(products).to_csv(
        f"{data_dir}/products_{execution_date}.csv", index=False
    )

    # Sales
    sales = [
        {
            "sale_id": i,
            "customer_id": random.randint(1, 100),
            "product_id": random.randint(1, 50),
            "quantity": random.randint(1, 5),
            "sale_date": fake.date_this_year(),
        }
        for i in range(1, 1001)
    ]
    pd.DataFrame(sales).to_json(
        f"{data_dir}/sales_{execution_date}.json",
        orient="records",
        lines=True,
    )

    logger.info(f"Data generated successfully for execution date: {execution_date}")
    logger.info(f"Customers generated: {len(customers)}")
    logger.info(f"Products generated: {len(products)}")
    logger.info(f"Sales records generated: {len(sales)}")


def upload_to_s3(**context):
    """Upload generated files to S3"""
    execution_date = context["ds"]

    s3_client = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION", "us-east-1"),
    )

    bucket_name = os.getenv("S3_BUCKET_NAME")
    data_dir = "/tmp/ecommerce_data"

    uploaded_files = []

    for filename in os.listdir(data_dir):
        if execution_date in filename:
            local_path = os.path.join(data_dir, filename)

            if "customers" in filename:
                s3_key = f"raw/customers/{filename}"
            elif "products" in filename:
                s3_key = f"raw/products/{filename}"
            elif "sales" in filename:
                s3_key = f"raw/sales/{filename}"
            else:
                continue

            s3_client.upload_file(local_path, bucket_name, s3_key)
            uploaded_files.append(s3_key)
            logger.info(f"File {filename} uploaded to s3://{bucket_name}/{s3_key}")

    return uploaded_files


def data_quality_check(**context):
    """Run data quality validations in Snowflake"""
    from snowflake.connector import connect

    conn = connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse="ecommerce_wh",
        database="ecommerce_db",
        schema="silver",
    )

    cursor = conn.cursor()

    checks = {
        "customers_null_check": """
            SELECT COUNT(*) 
            FROM ecommerce_db.silver.customers_clean
            WHERE customer_id IS NULL OR email IS NULL
        """,
        "products_price_check": """
            SELECT COUNT(*) 
            FROM ecommerce_db.silver.products_clean
            WHERE price <= 0
        """,
        "sales_orphan_check": """
            SELECT COUNT(*)
            FROM ecommerce_db.raw.sales s
            LEFT JOIN ecommerce_db.raw.customers c 
            ON s.customer_id = c.customer_id
            WHERE c.customer_id IS NULL
        """,
    }

    issues = []

    for check_name, query in checks.items():
        cursor.execute(query)
        result = cursor.fetchone()[0]

        if result > 0:
            issues.append(f"{check_name}: {result} issues found")
            logger.error(
                f"Data quality issue detected in {check_name}: {result} records"
            )
        else:
            logger.info(f"Data quality check passed: {check_name}")

    cursor.close()
    conn.close()

    if issues:
        raise ValueError(f"Data quality checks failed: {', '.join(issues)}")

    return "All data quality checks passed"


def send_metrics_notification(**context):
    """Display pipeline metrics"""
    from snowflake.connector import connect

    conn = connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse="ecommerce_wh",
        database="ecommerce_db",
        schema="gold",
    )

    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM ecommerce_db.gold.sales_by_country")
    countries_count = cursor.fetchone()[0]

    cursor.execute("SELECT SUM(total_sales) FROM ecommerce_db.gold.sales_by_country")
    total_revenue = cursor.fetchone()[0] or 0

    cursor.execute("SELECT COUNT(*) FROM ecommerce_db.gold.top_products")
    products_count = cursor.fetchone()[0]

    execution_date = context["ds"]

    logger.info("Ecommerce Data Pipeline Execution Summary")
    logger.info(f"Execution date: {execution_date}")
    logger.info(f"Countries analyzed: {countries_count}")
    logger.info(f"Total revenue: ${total_revenue:,.2f}")
    logger.info(f"Products in analytics table: {products_count}")
    logger.info("Pipeline execution completed successfully")

    cursor.close()
    conn.close()


# ============================================================
# Task Definitions
# ============================================================

generate_data_task = PythonOperator(
    task_id="generate_fake_data",
    python_callable=generate_fake_data,
    dag=dag,
)

upload_s3_task = PythonOperator(
    task_id="upload_to_s3",
    python_callable=upload_to_s3,
    dag=dag,
)

wait_for_snowpipe = PythonOperator(
    task_id="wait_for_snowpipe",
    python_callable=lambda: logger.info(
        "Waiting for Snowpipe to process files"
    ),
    dag=dag,
)

refresh_bronze = SnowflakeOperator(
    task_id="refresh_bronze_layer",
    snowflake_conn_id="snowflake_default",
    sql="""
        SELECT COUNT(*) FROM ecommerce_db.raw.customers;
        SELECT COUNT(*) FROM ecommerce_db.raw.products;
        SELECT COUNT(*) FROM ecommerce_db.raw.sales;
    """,
    dag=dag,
)

transform_to_silver = SnowflakeOperator(
    task_id="transform_to_silver",
    snowflake_conn_id="snowflake_default",
    sql="""
        CREATE OR REPLACE TABLE ecommerce_db.silver.customers_clean AS
        SELECT DISTINCT
            customer_id,
            INITCAP(TRIM(name)) AS name,
            UPPER(TRIM(country)) AS country,
            LOWER(TRIM(email)) AS email
        FROM ecommerce_db.raw.customers
        WHERE customer_id IS NOT NULL
          AND email IS NOT NULL;

        CREATE OR REPLACE TABLE ecommerce_db.silver.products_clean AS
        SELECT DISTINCT
            product_id,
            INITCAP(TRIM(name)) AS name,
            INITCAP(TRIM(category)) AS category,
            ROUND(price, 2) AS price
        FROM ecommerce_db.raw.products
        WHERE product_id IS NOT NULL
          AND price > 0;

        CREATE OR REPLACE TABLE ecommerce_db.silver.sales_enriched AS
        SELECT
            s.sale_id,
            s.sale_date,
            c.customer_id,
            c.name AS customer_name,
            c.country,
            p.product_id,
            p.name AS product_name,
            p.category,
            s.quantity,
            s.quantity * p.price AS total_amount
        FROM ecommerce_db.raw.sales s
        JOIN ecommerce_db.silver.customers_clean c 
        ON s.customer_id = c.customer_id
        JOIN ecommerce_db.silver.products_clean p 
        ON s.product_id = p.product_id
        WHERE s.sale_id IS NOT NULL;
    """,
    dag=dag,
)

quality_check_task = PythonOperator(
    task_id="data_quality_checks",
    python_callable=data_quality_check,
    dag=dag,
)

create_gold_layer = SnowflakeOperator(
    task_id="create_gold_analytics",
    snowflake_conn_id="snowflake_default",
    sql="""
        CREATE OR REPLACE TABLE ecommerce_db.gold.sales_by_country AS
        SELECT
            c.country,
            SUM(s.total_amount) AS total_sales,
            COUNT(DISTINCT s.sale_id) AS number_of_sales,
            COUNT(DISTINCT c.customer_id) AS number_of_customers
        FROM ecommerce_db.silver.sales_enriched s
        JOIN ecommerce_db.silver.customers_clean c
        ON s.customer_id = c.customer_id
        GROUP BY c.country;

        CREATE OR REPLACE TABLE ecommerce_db.gold.top_products AS
        SELECT
            p.name,
            p.category,
            SUM(s.quantity) AS total_quantity_sold,
            SUM(s.total_amount) AS total_revenue
        FROM ecommerce_db.silver.sales_enriched s
        JOIN ecommerce_db.silver.products_clean p
        ON s.product_id = p.product_id
        GROUP BY p.name, p.category
        ORDER BY total_revenue DESC;

        CREATE OR REPLACE TABLE ecommerce_db.gold.client_activity AS
        SELECT
            c.customer_id,
            c.name,
            c.country,
            COUNT(s.sale_id) AS number_of_purchases,
            SUM(s.total_amount) AS total_spent,
            MAX(s.sale_date) AS last_purchase_date
        FROM ecommerce_db.silver.sales_enriched s
        JOIN ecommerce_db.silver.customers_clean c
        ON s.customer_id = c.customer_id
        GROUP BY c.customer_id, c.name, c.country;
    """,
    dag=dag,
)

notify_completion = PythonOperator(
    task_id="send_completion_metrics",
    python_callable=send_metrics_notification,
    dag=dag,
)

# ============================================================
# Task Dependencies
# ============================================================

generate_data_task >> upload_s3_task >> wait_for_snowpipe >> refresh_bronze
refresh_bronze >> transform_to_silver >> quality_check_task >> create_gold_layer >> notify_completion

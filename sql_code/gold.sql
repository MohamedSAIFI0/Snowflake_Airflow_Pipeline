-- Create Gold schema
CREATE OR REPLACE SCHEMA ecommerce_db.gold;
USE SCHEMA ecommerce_db.gold;

-- Total sales by country
CREATE OR REPLACE TABLE ecommerce_db.gold.sales_by_country AS
SELECT
    c.country,
    SUM(s.total_amount)          AS total_sales,
    COUNT(DISTINCT s.sale_id)    AS number_of_sales,
    COUNT(DISTINCT c.customer_id) AS number_of_customers
FROM ecommerce_db.silver.sales_enriched  s
JOIN ecommerce_db.silver.customers_clean c ON s.customer_id = c.customer_id
GROUP BY c.country;

-- Top Products
CREATE OR REPLACE TABLE ecommerce_db.gold.top_products AS
SELECT
    p.name,
    p.category,
    SUM(s.quantity)     AS total_quantity_sold,
    SUM(s.total_amount) AS total_revenue
FROM ecommerce_db.silver.sales_enriched s
JOIN ecommerce_db.silver.products_clean p ON s.product_id = p.product_id
GROUP BY p.name, p.category
ORDER BY total_revenue DESC;

-- Client Activity
CREATE OR REPLACE TABLE ecommerce_db.gold.client_activity AS
SELECT
    c.customer_id,
    c.name,
    c.country,
    COUNT(s.sale_id)        AS number_of_purchases,
    SUM(s.total_amount)     AS total_spent,
    MAX(s.sale_date)        AS last_purchase_date
FROM ecommerce_db.silver.sales_enriched  s
JOIN ecommerce_db.silver.customers_clean c ON s.customer_id = c.customer_id
GROUP BY c.customer_id, c.name, c.country;
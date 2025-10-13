--Total sales by country
CREATE OR REPLACE TABLE eccomerce_db.gold.sales_by_country AS 
SELECT 
   c.country,
   SUM(s.total_amount) AS total_sales,
   COUNT(DISTINCT s.sale_id) AS number_of_sales,
   COUNT(DISTINCT c.customer_id) AS number_of_customers
FROM eccomerce_db.silver.sales_enriched s
JOIN eccomerce_db.silver.customers_clean c ON s.customer_id = c.customer_id
GROUP BY c.country;

--Top Products
CREATE OR REPLACE TABLE eccomerce_db.gold.top_products AS
SELECT 
  p.name,
  p.category,
  SUM(s.quantity) AS total_quantity_sold,
  SUM(s.total_amount) AS total_revenue
FROM eccomerce_db.silver.sales_enriched s
JOIN eccomerce_db.silver.products_clean p ON s.product_id = p.product_id
GROUP BY p.name, p.category;

--Activity Client
CREATE OR REPLACE TABLE eccomerce_db.gold.client_activity AS
SELECT 
  c.customer_id,
  c.name,
  c.country,
  COUNT(s.sale_id) AS number_of_purchases,
  SUM(s.total_amount) AS total_spent,
  MAX(s.sale_date) AS last_purchase_date,
FROM eccomerce_db.silver.sales_enriched s
JOIN eccomerce_db.silver.customers_clean c ON s.CUSTOMER_ID = c.CUSTOMER_ID
GROUP BY c.customer_id, c.name, c.country;



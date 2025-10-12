--On crée un nouveau schéma pour stocker les données transformées :
CREATE OR REPLACE SCHEMA ecommerce_db.silver;
USE SCHEMA ecommerce_db.silver;


--Tu vas créer des tables avec des types cohérents, sans doublons, et avec des jointures logiques.
CREATE OR REPLACE TABLE ecommerce_db.silver.customers_clean AS
SELECT DISTINCT
    customer_id,
    INITCAP(TRIM(name)) AS name,
    UPPER(TRIM(country)) AS country,
    LOWER(TRIM(email)) AS email
FROM ecommerce_db.raw.customers
WHERE customer_id IS NOT NULL
  AND email IS NOT NULL;

--Table products_clean
CREATE OR REPLACE TABLE ecommerce_db.silver.products_clean AS
SELECT DISTINCT
    product_id,
    INITCAP(TRIM(name)) AS name,
    INITCAP(TRIM(category)) AS category,
    ROUND(price, 2) AS price
FROM ecommerce_db.raw.products
WHERE product_id IS NOT NULL
  AND price > 0;


--Ici tu vas joindre les tables customers et products pour avoir une vision complète
CREATE OR REPLACE TABLE ecommerce_db.silver.sales_clean AS
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
JOIN ecommerce_db.raw.customers c ON s.customer_id = c.customer_id
JOIN ecommerce_db.raw.products p ON s.product_id = p.product_id
WHERE s.sale_id IS NOT NULL;



-- Create a Warehouse (Compute)
CREATE OR REPLACE WAREHOUSE eccomerce_wh
  WITH WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 300
  AUTO_RESUME = TRUE;

-- Use the warehouse
USE WAREHOUSE bronze_wh;

--  Create Database and Schema
CREATE OR REPLACE DATABASE ecommerce_db;
CREATE OR REPLACE SCHEMA ecommerce_db.raw;

-- Create Tables for Bronze Layer

-- Customers Table
CREATE OR REPLACE TABLE ecommerce_db.raw.customers (
    customer_id INT,
    name STRING,
    country STRING,
    email STRING
);

-- Products Table
CREATE OR REPLACE TABLE ecommerce_db.raw.products (
    product_id INT,
    name STRING,
    category STRING,
    price FLOAT
);

-- Sales Table
CREATE OR REPLACE TABLE ecommerce_db.raw.sales (
    sale_id INT,
    customer_id INT,
    product_id INT,
    quantity INT,
    sale_date DATE
);


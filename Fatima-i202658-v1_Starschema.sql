-- Drop the database if it exists
DROP DATABASE IF EXISTS electronica;

-- Create the database
CREATE DATABASE electronica;

-- Use the database
USE electronica;

-- Create the fact table for sales data
CREATE TABLE fact_sales (
    fact_sales_id INT AUTO_INCREMENT PRIMARY KEY,
    order_id INT,
    order_date DATETIME,
    product_id INT,
    customer_id INT,
    quantity_ordered INT,
    amount DECIMAL(10, 2)
);
-- Delete all data from the fact table
-- DELETE FROM fact_sales;

-- Create the dim_products table for master data
CREATE TABLE dim_products (
    dim_products_id INT AUTO_INCREMENT PRIMARY KEY,
    product_id INT,
    product_name VARCHAR(255),
    product_price DECIMAL(10, 2),
    supplier_id INT,
    supplier_name VARCHAR(255),
    store_id INT,
    store_name VARCHAR(255)
);
-- Delete all data from the dimension table
-- DELETE FROM dim_products;

-- Create the dim_customers table for customer data
CREATE TABLE dim_customers (
    dim_customers_id INT AUTO_INCREMENT PRIMARY KEY,
    customer_id INT,
    customer_name VARCHAR(255),
    gender VARCHAR(255),
    age INT

);
CREATE INDEX idx_product_id ON dim_products(product_id);

-- Delete all data from the customer dimension table
-- DELETE FROM dim_customers;
CREATE INDEX idx_order_date ON fact_sales(order_date);
CREATE INDEX idx_customer_id ON dim_customers(customer_id);
-- Add foreign key constraints in the fact table
ALTER TABLE fact_sales
ADD CONSTRAINT fk_dim_products
FOREIGN KEY (product_id)
REFERENCES dim_products(product_id);

ALTER TABLE fact_sales
ADD CONSTRAINT fk_dim_customers
FOREIGN KEY (customer_id)
REFERENCES dim_customers(customer_id);

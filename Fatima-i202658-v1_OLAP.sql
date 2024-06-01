
-- Qs1
SELECT
    dp.supplier_id,
    YEAR(fs.order_date) AS order_year,
    QUARTER(fs.order_date) AS order_quarter,
    MONTH(fs.order_date) AS order_month,
    SUM(fs.quantity_ordered) AS total_sales
FROM
    fact_sales fs
JOIN
    dim_products dp ON fs.product_id = dp.product_id
GROUP BY
    dp.supplier_id, order_year, order_quarter, order_month
ORDER BY
    dp.supplier_id, order_year, order_quarter, order_month;

-- Qs2
SELECT
    dp.product_id,
    YEAR(fs.order_date) AS order_year,
    MONTH(fs.order_date) AS order_month,
    SUM(fs.quantity_ordered) AS total_sales
FROM
    fact_sales fs
JOIN
    dim_products dp ON fs.product_id = dp.product_id
WHERE
    dp.supplier_name = 'DJI' AND YEAR(fs.order_date) = 2019
GROUP BY
    dp.product_id, order_year, order_month WITH ROLLUP
ORDER BY
    dp.product_id, order_year, order_month;
-- QS3
SELECT
    product_id,
    COUNT(*) AS total_sales
FROM
    fact_sales
WHERE
    DAYOFWEEK(order_date) IN (1, 7) -- 1 for Sunday, 7 for Saturday
GROUP BY
    product_id
ORDER BY
    total_sales DESC
LIMIT 5;
-- qs4
SELECT
    product_id,
    SUM(CASE WHEN QUARTER(order_date) = 1 THEN quantity_ordered ELSE 0 END) AS Q1_sales,
    SUM(CASE WHEN QUARTER(order_date) = 2 THEN quantity_ordered ELSE 0 END) AS Q2_sales,
    SUM(CASE WHEN QUARTER(order_date) = 3 THEN quantity_ordered ELSE 0 END) AS Q3_sales,
    SUM(CASE WHEN QUARTER(order_date) = 4 THEN quantity_ordered ELSE 0 END) AS Q4_sales,
    SUM(quantity_ordered) AS yearly_sales
FROM
    fact_sales
WHERE
    YEAR(order_date) = 2019
GROUP BY
    product_id
ORDER BY
    product_id;
    -- QS5
    -- Example: Identify anomalies based on unusually high sales amounts
UPDATE fact_sales
SET order_date = '01-01-1970 00:00:00'
WHERE order_date = '00-00-0000 00:00:00';

SELECT *
FROM fact_sales
WHERE order_date IS NULL;
-- QS6

select *from storeanalysis_mv;
CREATE VIEW STOREANALYSIS_MV AS
SELECT
    dp.store_id,
    fs.product_id,
    SUM(fs.quantity_ordered) AS store_total
FROM
    fact_sales fs
JOIN
    dim_products dp ON fs.product_id = dp.product_id
GROUP BY
    dp.store_id, fs.product_id;
-- QS7
SELECT
    dp.store_id,
    fs.product_id,
    MONTH(fs.order_date) AS order_month,
    SUM(fs.quantity_ordered) AS total_sales
FROM
    fact_sales fs
JOIN
    dim_products dp ON fs.product_id = dp.product_id
WHERE
    dp.store_name = 'Tech Haven'
GROUP BY
    dp.store_id,
    fs.product_id,
    order_month
ORDER BY
    dp.store_id,
    fs.product_id,
    order_month;
-- QS8
select *from supplier_performance_mv;
CREATE VIEW SUPPLIER_PERFORMANCE_MV AS
SELECT
    dp.supplier_id,
    MONTH(fs.order_date) AS order_month,
    SUM(fs.quantity_ordered) AS total_sales
FROM
    fact_sales fs
JOIN
    dim_products dp ON fs.product_id = dp.product_id
GROUP BY
    dp.supplier_id,
    order_month;

-- QS9
SELECT
    customer_id,
    COUNT(DISTINCT product_id) AS unique_products_purchased,
    SUM(quantity_ordered) AS total_sales
FROM
    fact_sales
WHERE
    YEAR(order_date) = 2019
GROUP BY
    customer_id
ORDER BY
    total_sales DESC
LIMIT 5;



-- QS10
select *from customer_store_sales_mv;
CREATE VIEW CUSTOMER_STORE_SALES_MV AS
SELECT
    dp.store_id,
    fs.customer_id,
    MONTH(fs.order_date) AS order_month,
    SUM(fs.quantity_ordered) AS total_sales
FROM
    fact_sales fs
JOIN
    dim_products dp ON fs.product_id = dp.product_id
GROUP BY
    dp.store_id,
    fs.customer_id,
    order_month;

/* =========================================================
   GOLD LAYER TRANSFORMATIONS
   Purpose:
   - Business logic
   - Aggregations
   - Analytics-ready star schema
   Rules:
   - Aggregations allowed
   - BI-friendly tables
   ========================================================= */

BEGIN;

/* =========================
   GOLD: DIM_DATE
   Grain: 1 row per date
   ========================= */

DROP TABLE IF EXISTS gold.dim_date;

CREATE TABLE gold.dim_date (
  date_id DATE PRIMARY KEY,
  year INT,
  month INT,
  month_name TEXT,
  day INT,
  quarter INT,
  day_of_week INT,
  day_name TEXT
);

INSERT INTO gold.dim_date
SELECT DISTINCT
  order_purchase_timestamp::DATE,
  EXTRACT(YEAR FROM order_purchase_timestamp)::INT,
  EXTRACT(MONTH FROM order_purchase_timestamp)::INT,
  TO_CHAR(order_purchase_timestamp, 'Month'),
  EXTRACT(DAY FROM order_purchase_timestamp)::INT,
  EXTRACT(QUARTER FROM order_purchase_timestamp)::INT,
  EXTRACT(DOW FROM order_purchase_timestamp)::INT,
  TO_CHAR(order_purchase_timestamp, 'Day')
FROM silver.orders
WHERE order_purchase_timestamp IS NOT NULL;


/* =========================
   GOLD: DIM_CUSTOMERS
   Grain: 1 row per customer_id
   ========================= */

DROP TABLE IF EXISTS gold.dim_customers;

CREATE TABLE gold.dim_customers (
  customer_id TEXT PRIMARY KEY,
  customer_unique_id TEXT,
  customer_zip_code INT,
  customer_city TEXT,
  customer_state TEXT
);

INSERT INTO gold.dim_customers
SELECT
  customer_id,
  customer_unique_id,
  customer_zip_code_prefix,
  customer_city,
  customer_state
FROM silver.customers;


/* =========================
   GOLD: DIM_PRODUCTS
   Grain: 1 row per product_id
   ========================= */

DROP TABLE IF EXISTS gold.dim_products;

CREATE TABLE gold.dim_products (
  product_id TEXT PRIMARY KEY,
  product_category_name TEXT,
  product_name_length INT,
  product_description_length INT,
  product_photos_qty INT,
  product_weight_g INT,
  product_length_cm INT,
  product_height_cm INT,
  product_width_cm INT
);

INSERT INTO gold.dim_products
SELECT
  product_id,
  product_category_name,
  product_name_length,
  product_description_length,
  product_photos_qty,
  product_weight_g,
  product_length_cm,
  product_height_cm,
  product_width_cm
FROM silver.products;


/* =========================
   GOLD: FACT_ORDERS
   Grain: 1 row per order_id
   ========================= */

DROP TABLE IF EXISTS gold.fact_orders;

CREATE TABLE gold.fact_orders (
  order_id TEXT PRIMARY KEY,
  customer_id TEXT,
  order_date DATE,
  order_status TEXT,
  order_total_value NUMERIC(12,2),
  total_freight_value NUMERIC(12,2),
  total_items INT
);

INSERT INTO gold.fact_orders
SELECT
  o.order_id,
  o.customer_id,
  o.order_purchase_timestamp::DATE,
  o.order_status,
  SUM(oi.price),
  SUM(oi.freight_value),
  COUNT(oi.order_item_id)
FROM silver.orders o
JOIN silver.order_items oi
  ON o.order_id = oi.order_id
GROUP BY
  o.order_id,
  o.customer_id,
  o.order_purchase_timestamp,
  o.order_status;


/* =========================
   GOLD: FACT_PAYMENTS
   Grain: (order_id, payment_type)
   ========================= */

DROP TABLE IF EXISTS gold.fact_payments;

CREATE TABLE gold.fact_payments (
  order_id TEXT,
  payment_type TEXT,
  payment_date DATE,
  total_payment_value NUMERIC(12,2),
  total_installments INT,
  PRIMARY KEY (order_id, payment_type)
);

INSERT INTO gold.fact_payments
SELECT
  p.order_id,
  p.payment_type,
  o.order_purchase_timestamp::DATE,
  SUM(p.payment_value),
  SUM(p.payment_installments)
FROM silver.order_payments p
JOIN silver.orders o
  ON p.order_id = o.order_id
GROUP BY
  p.order_id,
  p.payment_type,
  o.order_purchase_timestamp;

COMMIT;

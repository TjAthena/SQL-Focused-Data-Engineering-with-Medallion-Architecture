/* =========================================================
   SILVER LAYER TRANSFORMATIONS
   Purpose:
   - Clean raw data
   - Enforce correct data types
   - Deduplicate
   - Enforce correct grain
   Rules:
   - NO aggregations
   - NO KPIs
   - Rebuildable from Bronze
   ========================================================= */

BEGIN;

/* =========================
   SILVER: CUSTOMERS
   Grain: 1 row per customer_id
   ========================= */

DROP TABLE IF EXISTS silver.customers;

CREATE TABLE silver.customers (
  customer_id TEXT PRIMARY KEY,
  customer_unique_id TEXT,
  customer_zip_code_prefix INT,
  customer_city TEXT,
  customer_state TEXT
);

INSERT INTO silver.customers
SELECT DISTINCT
  customer_id,
  customer_unique_id,
  NULLIF(customer_zip_code_prefix, '')::INT,
  customer_city,
  customer_state
FROM bronze.customers
WHERE customer_id IS NOT NULL;


/* =========================
   SILVER: ORDERS
   Grain: 1 row per order_id
   ========================= */

DROP TABLE IF EXISTS silver.orders;

CREATE TABLE silver.orders (
  order_id TEXT PRIMARY KEY,
  customer_id TEXT,
  order_status TEXT,
  order_purchase_timestamp TIMESTAMP,
  order_approved_at TIMESTAMP,
  order_delivered_carrier_date TIMESTAMP,
  order_delivered_customer_date TIMESTAMP,
  order_estimated_delivery_date TIMESTAMP
);

INSERT INTO silver.orders
SELECT DISTINCT
  order_id,
  customer_id,
  order_status,
  NULLIF(order_purchase_timestamp, '')::TIMESTAMP,
  NULLIF(order_approved_at, '')::TIMESTAMP,
  NULLIF(order_delivered_carrier_date, '')::TIMESTAMP,
  NULLIF(order_delivered_customer_date, '')::TIMESTAMP,
  NULLIF(order_estimated_delivery_date, '')::TIMESTAMP
FROM bronze.orders
WHERE order_id IS NOT NULL;


/* =========================
   SILVER: ORDER ITEMS
   Grain: (order_id, order_item_id)
   ========================= */

DROP TABLE IF EXISTS silver.order_items;

CREATE TABLE silver.order_items (
  order_id TEXT,
  order_item_id INT,
  product_id TEXT,
  seller_id TEXT,
  shipping_limit_date TIMESTAMP,
  price NUMERIC(10,2),
  freight_value NUMERIC(10,2),
  PRIMARY KEY (order_id, order_item_id)
);

INSERT INTO silver.order_items
SELECT DISTINCT
  order_id,
  (NULLIF(order_item_id, '')::NUMERIC)::INT,
  product_id,
  seller_id,
  NULLIF(shipping_limit_date, '')::TIMESTAMP,
  NULLIF(price, '')::NUMERIC(10,2),
  NULLIF(freight_value, '')::NUMERIC(10,2)
FROM bronze.order_items
WHERE order_id IS NOT NULL;


/* =========================
   SILVER: ORDER PAYMENTS
   Grain: (order_id, payment_sequential)
   ========================= */

DROP TABLE IF EXISTS silver.order_payments;

CREATE TABLE silver.order_payments (
  order_id TEXT,
  payment_sequential INT,
  payment_type TEXT,
  payment_installments INT,
  payment_value NUMERIC(10,2),
  PRIMARY KEY (order_id, payment_sequential)
);

INSERT INTO silver.order_payments
SELECT DISTINCT
  order_id,
  (NULLIF(payment_sequential, '')::NUMERIC)::INT,
  payment_type,
  (NULLIF(payment_installments, '')::NUMERIC)::INT,
  NULLIF(payment_value, '')::NUMERIC(10,2)
FROM bronze.order_payments
WHERE order_id IS NOT NULL;


/* =========================
   SILVER: PRODUCTS
   Grain: 1 row per product_id
   ========================= */

DROP TABLE IF EXISTS silver.products;

CREATE TABLE silver.products (
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

INSERT INTO silver.products
SELECT DISTINCT
  product_id,
  product_category_name,
  (NULLIF(product_name_lenght, '')::NUMERIC)::INT,
  (NULLIF(product_description_lenght, '')::NUMERIC)::INT,
  (NULLIF(product_photos_qty, '')::NUMERIC)::INT,
  (NULLIF(product_weight_g, '')::NUMERIC)::INT,
  (NULLIF(product_length_cm, '')::NUMERIC)::INT,
  (NULLIF(product_height_cm, '')::NUMERIC)::INT,
  (NULLIF(product_width_cm, '')::NUMERIC)::INT
FROM bronze.products
WHERE product_id IS NOT NULL;

COMMIT;

-- Row counts
SELECT 'orders' AS table, COUNT(*) FROM bronze.orders
UNION ALL
SELECT 'customers', COUNT(*) FROM bronze.customers
UNION ALL
SELECT 'order_items', COUNT(*) FROM bronze.order_items
UNION ALL
SELECT 'order_payments', COUNT(*) FROM bronze.order_payments
UNION ALL
SELECT 'products', COUNT(*) FROM bronze.products
UNION ALL
SELECT 'sellers', COUNT(*) FROM bronze.sellers
UNION ALL
SELECT 'order_reviews', COUNT(*) FROM bronze.order_reviews
UNION ALL
SELECT 'geolocation', COUNT(*) FROM bronze.geolocation
UNION ALL
SELECT 'category_translation', COUNT(*) FROM bronze.product_category_name_translation;



-- Total row count across all tables:
-- Row counts
WITH count_rows AS (
    SELECT 'orders' AS table, COUNT(*) FROM bronze.orders
    UNION ALL
    SELECT 'customers', COUNT(*) FROM bronze.customers
    UNION ALL
    SELECT 'order_items', COUNT(*) FROM bronze.order_items
    UNION ALL
    SELECT 'order_payments', COUNT(*) FROM bronze.order_payments
    UNION ALL
    SELECT 'products', COUNT(*) FROM bronze.products
    UNION ALL
    SELECT 'sellers', COUNT(*) FROM bronze.sellers
    UNION ALL
    SELECT 'order_reviews', COUNT(*) FROM bronze.order_reviews
    UNION ALL
    SELECT 'geolocation', COUNT(*) FROM bronze.geolocation
    UNION ALL
    SELECT 'category_translation', COUNT(*) FROM bronze.product_category_name_translation
)
SELECT SUM(count) FROM count_rows;


-- result = 1550922 Rows injested in the bronze layer(Raw table data)



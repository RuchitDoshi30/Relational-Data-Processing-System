/*
 * TRANSFORMATIONS (Raw -> Processed)
 * ==================================
 * Set-based SQL cleaning and validation.
 * Optimized for bulk processing over row-by-row iteration.
 */

BEGIN;

-- 1. CLEAN CUSTOMERS
-- Deduplicate logic: Priority to first occurrence based on ID sort.
-- Data Quality Rule: Valid email required.
TRUNCATE TABLE customers CASCADE;

INSERT INTO customers (customer_id, name, email, city)
SELECT DISTINCT ON (email)
    CAST(customer_id AS INTEGER),
    name,
    email,
    city
FROM raw_customers
WHERE customer_id IS NOT NULL 
  AND email LIKE '%@%'
ORDER BY email, customer_id; 

-- 2. CLEAN PRODUCTS
-- Type casting and sanity checks.
TRUNCATE TABLE products CASCADE;

INSERT INTO products (product_id, product_name, category, price)
SELECT 
    CAST(product_id AS INTEGER),
    product_name,
    category,
    CAST(price AS DECIMAL(10,2))
FROM raw_products
WHERE product_id IS NOT NULL
  AND CAST(price AS DECIMAL(10,2)) >= 0;

-- 3. CLEAN ORDERS
-- Referential Integrity: Only insert orders for known customers.
TRUNCATE TABLE orders CASCADE;

INSERT INTO orders (order_id, customer_id, order_date)
SELECT 
    CAST(r.order_id AS INTEGER),
    CAST(r.customer_id AS INTEGER),
    CAST(r.order_date AS DATE)
FROM raw_orders r
INNER JOIN customers c ON CAST(r.customer_id AS INTEGER) = c.customer_id
WHERE r.order_id IS NOT NULL;

-- 4. CLEAN ORDER ITEMS
-- Referential Integrity: Must map to valid Order AND valid Product.
TRUNCATE TABLE order_items CASCADE;

INSERT INTO order_items (order_item_id, order_id, product_id, quantity, price)
SELECT 
    CAST(roi.order_item_id AS INTEGER),
    CAST(roi.order_id AS INTEGER),
    CAST(roi.product_id AS INTEGER),
    CAST(roi.quantity AS INTEGER),
    CAST(roi.price AS DECIMAL(10,2))
FROM raw_order_items roi
INNER JOIN orders o ON CAST(roi.order_id AS INTEGER) = o.order_id
INNER JOIN products p ON CAST(roi.product_id AS INTEGER) = p.product_id
WHERE roi.quantity IS NOT NULL 
  AND CAST(roi.quantity AS INTEGER) > 0;

COMMIT;

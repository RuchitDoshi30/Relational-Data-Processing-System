/*
 * DATABASE SCHEMA DESIGN
 * ======================
 * Layers: Raw (Bronze) -> Processed (Silver) -> Reporting (Gold).
 * Strategy: Strict 3NF for core data, Star Schema dependent for reporting.
 */

-- ============================================================================
-- 1. RAW LAYER (Bronze)
-- Immutable landing zone. Loosely typed to accept all ingestion.
-- ============================================================================

DROP TABLE IF EXISTS raw_customers CASCADE;
CREATE TABLE raw_customers (
    customer_id TEXT,
    name TEXT,
    email TEXT,
    city TEXT
);

DROP TABLE IF EXISTS raw_products CASCADE;
CREATE TABLE raw_products (
    product_id TEXT,
    product_name TEXT,
    category TEXT,
    price TEXT
);

DROP TABLE IF EXISTS raw_orders CASCADE;
CREATE TABLE raw_orders (
    order_id TEXT,
    customer_id TEXT,
    order_date TEXT
);

DROP TABLE IF EXISTS raw_order_items CASCADE;
CREATE TABLE raw_order_items (
    order_item_id TEXT,
    order_id TEXT,
    product_id TEXT,
    quantity TEXT,
    price TEXT
);

-- ============================================================================
-- 2. PROCESSED LAYER (Silver)
-- Intent: Single source of truth. Enforces business rules & referential integrity.
-- ============================================================================

DROP TABLE IF EXISTS customers CASCADE;
CREATE TABLE customers (
    customer_id INTEGER PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    city VARCHAR(100)
);

DROP TABLE IF EXISTS products CASCADE;
CREATE TABLE products (
    product_id INTEGER PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    category VARCHAR(100),
    price DECIMAL(10, 2) CHECK (price >= 0) -- Business rule: no negative prices
);

DROP TABLE IF EXISTS orders CASCADE;
CREATE TABLE orders (
    order_id INTEGER PRIMARY KEY,
    customer_id INTEGER REFERENCES customers(customer_id) ON DELETE CASCADE,
    order_date DATE NOT NULL
);

DROP TABLE IF EXISTS order_items CASCADE;
CREATE TABLE order_items (
    order_item_id INTEGER PRIMARY KEY,
    order_id INTEGER REFERENCES orders(order_id) ON DELETE CASCADE,
    product_id INTEGER REFERENCES products(product_id) ON DELETE RESTRICT, -- Prevent deleting sold products
    quantity INTEGER CHECK (quantity > 0),
    price DECIMAL(10, 2) CHECK (price >= 0) -- Capture historical price at transaction time
);

-- Performance Optimization: Index FKs for join performance
CREATE INDEX IF NOT EXISTS idx_orders_customer_id ON orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_orders_order_date ON orders(order_date); -- Filter optimization
CREATE INDEX IF NOT EXISTS idx_order_items_order_id ON order_items(order_id);
CREATE INDEX IF NOT EXISTS idx_order_items_product_id ON order_items(product_id);

-- ============================================================================
-- 3. REPORTING LAYER (Gold)
-- Intent: Read-optimized schemas for dashboards/BI.
-- ============================================================================

DROP TABLE IF EXISTS sales_summary CASCADE;
CREATE TABLE sales_summary (
    sales_date DATE PRIMARY KEY,
    total_revenue DECIMAL(15, 2),
    total_orders INTEGER,
    avg_order_value DECIMAL(10, 2)
);

DROP TABLE IF EXISTS customer_stats CASCADE;
CREATE TABLE customer_stats (
    customer_id INTEGER PRIMARY KEY,
    customer_name VARCHAR(255),
    total_spend DECIMAL(15, 2),
    items_purchased INTEGER,
    last_order_date DATE
);

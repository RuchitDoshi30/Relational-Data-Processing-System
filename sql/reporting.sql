/*
 * REPORTING AGGREGATIONS (Processed -> Reporting)
 * ===============================================
 * Idempotent, full-recalc aggregations for analytical consistency.
 */

BEGIN;

-- METRIC: SALES PERFORMANCE
-- Granularity: Daily
TRUNCATE TABLE sales_summary;

INSERT INTO sales_summary (sales_date, total_revenue, total_orders, avg_order_value)
SELECT 
    o.order_date,
    SUM(oi.quantity * oi.price) AS total_revenue,
    COUNT(DISTINCT o.order_id) AS total_orders,
    -- Sub-aggregation required for accurate average of sums
    ROUND(AVG(order_totals.total_amt), 2) AS avg_order_value
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
JOIN (
    SELECT order_id, SUM(quantity * price) as total_amt
    FROM order_items
    GROUP BY order_id
) order_totals ON o.order_id = order_totals.order_id
GROUP BY o.order_date;

-- METRIC: CUSTOMER LIFETIME VALUE (CLTV)
-- Granularity: Customer
TRUNCATE TABLE customer_stats;

INSERT INTO customer_stats (customer_id, customer_name, total_spend, items_purchased, last_order_date)
SELECT 
    c.customer_id,
    c.name,
    COALESCE(SUM(oi.quantity * oi.price), 0) AS total_spend,
    COALESCE(SUM(oi.quantity), 0) AS items_purchased,
    MAX(o.order_date) AS last_order_date
FROM customers c
-- LEFT JOIN: Include customers even if they haven't purchased yet (though we filter active below)
LEFT JOIN orders o ON c.customer_id = o.customer_id
LEFT JOIN order_items oi ON o.order_id = oi.order_id
GROUP BY c.customer_id, c.name
HAVING SUM(oi.quantity * oi.price) > 0; -- Active customers only

COMMIT;

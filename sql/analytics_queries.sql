-- ======================================================
-- E-commerce ETL Pipeline — Analytics Queries
-- ======================================================
-- Business insight queries for the star schema.
-- Works with: SQLite, BigQuery, PostgreSQL
-- ======================================================


-- -------------------------------------------------------
-- 1. MONTHLY REVENUE TREND
-- -------------------------------------------------------
-- Shows revenue aggregated by year-month for trend analysis.

SELECT
    d.year,
    d.month,
    d.month_name,
    COUNT(DISTINCT f.order_id)          AS total_orders,
    SUM(f.quantity)                     AS total_units_sold,
    ROUND(SUM(f.revenue), 2)           AS total_revenue,
    ROUND(AVG(f.revenue), 2)           AS avg_order_revenue
FROM fact_orders f
JOIN dim_date d ON f.date_id = d.date_id
WHERE f.order_status != 'cancelled'
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year, d.month;


-- -------------------------------------------------------
-- 2. TOP 10 PRODUCTS BY REVENUE
-- -------------------------------------------------------
-- Ranks products by total revenue generated.

SELECT
    p.product_id,
    p.product_name,
    p.category,
    COUNT(f.order_id)                   AS times_ordered,
    SUM(f.quantity)                     AS total_units_sold,
    ROUND(SUM(f.revenue), 2)           AS total_revenue,
    ROUND(AVG(f.unit_price), 2)        AS avg_unit_price
FROM fact_orders f
JOIN dim_products p ON f.product_id = p.product_id
WHERE f.order_status != 'cancelled'
GROUP BY p.product_id, p.product_name, p.category
ORDER BY total_revenue DESC
LIMIT 10;


-- -------------------------------------------------------
-- 3. CUSTOMER LIFETIME VALUE (CLV)
-- -------------------------------------------------------
-- Total spend per customer, ranked highest to lowest.

SELECT
    c.customer_id,
    c.name,
    c.location,
    COUNT(DISTINCT f.order_id)          AS total_orders,
    ROUND(SUM(f.revenue), 2)           AS lifetime_value,
    ROUND(AVG(f.revenue), 2)           AS avg_order_value,
    MIN(d.full_date)                   AS first_order_date,
    MAX(d.full_date)                   AS last_order_date
FROM fact_orders f
JOIN dim_customers c ON f.customer_id = c.customer_id
JOIN dim_date d ON f.date_id = d.date_id
WHERE f.order_status != 'cancelled'
GROUP BY c.customer_id, c.name, c.location
ORDER BY lifetime_value DESC
LIMIT 20;


-- -------------------------------------------------------
-- 4. REPEAT VS NEW CUSTOMERS (by month)
-- -------------------------------------------------------
-- Counts customers making their first-ever order vs returning.

WITH customer_first_order AS (
    SELECT
        customer_id,
        MIN(date_id) AS first_order_date_id
    FROM fact_orders
    WHERE order_status != 'cancelled'
    GROUP BY customer_id
),
monthly_customers AS (
    SELECT
        d.year,
        d.month,
        f.customer_id,
        CASE
            WHEN cfo.first_order_date_id = f.date_id THEN 'new'
            ELSE 'repeat'
        END AS customer_type
    FROM fact_orders f
    JOIN dim_date d ON f.date_id = d.date_id
    JOIN customer_first_order cfo ON f.customer_id = cfo.customer_id
    WHERE f.order_status != 'cancelled'
)
SELECT
    year,
    month,
    customer_type,
    COUNT(DISTINCT customer_id) AS customer_count
FROM monthly_customers
GROUP BY year, month, customer_type
ORDER BY year, month, customer_type;


-- -------------------------------------------------------
-- 5. REVENUE BY PRODUCT CATEGORY
-- -------------------------------------------------------

SELECT
    p.category,
    COUNT(DISTINCT f.order_id)          AS total_orders,
    SUM(f.quantity)                     AS total_units,
    ROUND(SUM(f.revenue), 2)           AS total_revenue,
    ROUND(AVG(f.revenue), 2)           AS avg_order_revenue,
    ROUND(100.0 * SUM(f.revenue) / (
        SELECT SUM(revenue) FROM fact_orders WHERE order_status != 'cancelled'
    ), 1)                              AS revenue_pct
FROM fact_orders f
JOIN dim_products p ON f.product_id = p.product_id
WHERE f.order_status != 'cancelled'
GROUP BY p.category
ORDER BY total_revenue DESC;


-- -------------------------------------------------------
-- 6. AVERAGE ORDER VALUE (AOV) TREND
-- -------------------------------------------------------

SELECT
    d.year,
    d.month,
    d.month_name,
    COUNT(DISTINCT f.order_id)          AS total_orders,
    ROUND(SUM(f.revenue), 2)           AS total_revenue,
    ROUND(SUM(f.revenue) / COUNT(DISTINCT f.order_id), 2) AS aov
FROM fact_orders f
JOIN dim_date d ON f.date_id = d.date_id
WHERE f.order_status != 'cancelled'
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year, d.month;


-- -------------------------------------------------------
-- 7. ORDER STATUS BREAKDOWN
-- -------------------------------------------------------

SELECT
    order_status,
    COUNT(*)                            AS order_count,
    ROUND(SUM(revenue), 2)             AS total_revenue,
    ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM fact_orders), 1) AS pct_of_orders
FROM fact_orders
GROUP BY order_status
ORDER BY order_count DESC;


-- -------------------------------------------------------
-- 8. DAY-OF-WEEK ANALYSIS
-- -------------------------------------------------------

SELECT
    d.day_name,
    d.day_of_week,
    COUNT(DISTINCT f.order_id)          AS total_orders,
    ROUND(SUM(f.revenue), 2)           AS total_revenue,
    ROUND(AVG(f.revenue), 2)           AS avg_revenue
FROM fact_orders f
JOIN dim_date d ON f.date_id = d.date_id
WHERE f.order_status != 'cancelled'
GROUP BY d.day_name, d.day_of_week
ORDER BY d.day_of_week;

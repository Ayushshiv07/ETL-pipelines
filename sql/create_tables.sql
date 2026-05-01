-- ======================================================
-- E-commerce ETL Pipeline — Star Schema DDL
-- ======================================================
-- Compatible with: SQLite, BigQuery, PostgreSQL
-- ======================================================

-- -------------------------------------------------------
-- Dimension: Customers
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_customers (
    customer_id     INTEGER PRIMARY KEY,
    name            TEXT NOT NULL,
    email           TEXT,
    location        TEXT
);

-- -------------------------------------------------------
-- Dimension: Products
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_products (
    product_id      INTEGER PRIMARY KEY,
    product_name    TEXT NOT NULL,
    category        TEXT,
    price           REAL NOT NULL,
    supplier        TEXT
);

-- -------------------------------------------------------
-- Dimension: Date
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_date (
    date_id         INTEGER PRIMARY KEY,   -- YYYYMMDD format
    full_date       DATE NOT NULL,
    day             INTEGER NOT NULL,
    month           INTEGER NOT NULL,
    year            INTEGER NOT NULL,
    quarter         INTEGER NOT NULL,
    day_of_week     INTEGER NOT NULL,      -- 0=Monday, 6=Sunday
    day_name        TEXT NOT NULL,
    month_name      TEXT NOT NULL,
    is_weekend      BOOLEAN NOT NULL
);

-- -------------------------------------------------------
-- Fact: Orders
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_orders (
    order_id        INTEGER PRIMARY KEY,
    customer_id     INTEGER NOT NULL,
    product_id      INTEGER NOT NULL,
    date_id         INTEGER NOT NULL,
    quantity        INTEGER NOT NULL,
    unit_price      REAL NOT NULL,
    revenue         REAL NOT NULL,
    order_status    TEXT NOT NULL,

    FOREIGN KEY (customer_id) REFERENCES dim_customers(customer_id),
    FOREIGN KEY (product_id)  REFERENCES dim_products(product_id),
    FOREIGN KEY (date_id)     REFERENCES dim_date(date_id)
);

-- -------------------------------------------------------
-- Indexes for query performance
-- -------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_fact_orders_customer  ON fact_orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_fact_orders_product   ON fact_orders(product_id);
CREATE INDEX IF NOT EXISTS idx_fact_orders_date      ON fact_orders(date_id);
CREATE INDEX IF NOT EXISTS idx_fact_orders_status    ON fact_orders(order_status);
CREATE INDEX IF NOT EXISTS idx_dim_date_year_month   ON dim_date(year, month);
CREATE INDEX IF NOT EXISTS idx_dim_products_category ON dim_products(category);

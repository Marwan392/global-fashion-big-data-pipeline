-- ============================================================
-- GLOBAL FASHION BIG DATA PIPELINE
-- Spark SQL Analytics
-- ============================================================

-- Read processed Parquet data directly from HDFS
CREATE OR REPLACE TEMP VIEW fact_transactions
USING PARQUET
OPTIONS (
    path 'hdfs://namenode:9000/data/processed/fact_transactions'
);

-- ============================================================
-- 1. BASIC DATA OVERVIEW
-- ============================================================

SELECT
    COUNT(*) AS total_transactions,
    COUNT(DISTINCT `Invoice ID`) AS total_invoices,
    COUNT(DISTINCT `Customer ID`) AS total_customers,
    COUNT(DISTINCT `Product ID`) AS total_products
FROM fact_transactions;

-- ============================================================
-- 2. TOTAL SALES AND PROFIT
-- ============================================================

SELECT
    ROUND(SUM(`Line Total USD`), 2) AS total_revenue_usd,
    ROUND(SUM(`Profit USD`), 2) AS total_profit_usd,
    ROUND(AVG(`Line Total USD`), 2) AS average_transaction_value_usd
FROM fact_transactions;

-- ============================================================
-- 3. SALES BY YEAR
-- ============================================================

SELECT
    `Purchase Year`,
    ROUND(SUM(`Line Total USD`), 2) AS revenue_usd,
    ROUND(SUM(`Profit USD`), 2) AS profit_usd,
    SUM(`Quantity`) AS units_sold
FROM fact_transactions
GROUP BY `Purchase Year`
ORDER BY `Purchase Year`;

-- ============================================================
-- 4. MONTHLY SALES TREND
-- ============================================================

SELECT
    `Purchase Year`,
    `Purchase Month`,
    ROUND(SUM(`Line Total USD`), 2) AS revenue_usd,
    ROUND(SUM(`Profit USD`), 2) AS profit_usd,
    SUM(`Quantity`) AS units_sold
FROM fact_transactions
GROUP BY
    `Purchase Year`,
    `Purchase Month`
ORDER BY
    `Purchase Year`,
    `Purchase Month`;

-- ============================================================
-- 5. SALES BY PRODUCT CATEGORY
-- ============================================================

SELECT
    `Product Category`,
    ROUND(SUM(`Line Total USD`), 2) AS revenue_usd,
    ROUND(SUM(`Profit USD`), 2) AS profit_usd,
    SUM(`Quantity`) AS units_sold
FROM fact_transactions
GROUP BY `Product Category`
ORDER BY revenue_usd DESC;

-- ============================================================
-- 6. SALES BY PRODUCT SUB-CATEGORY
-- ============================================================

SELECT
    `Product Sub Category`,
    ROUND(SUM(`Line Total USD`), 2) AS revenue_usd,
    ROUND(SUM(`Profit USD`), 2) AS profit_usd,
    SUM(`Quantity`) AS units_sold
FROM fact_transactions
GROUP BY `Product Sub Category`
ORDER BY revenue_usd DESC
LIMIT 20;

-- ============================================================
-- 7. TOP 10 PRODUCTS BY REVENUE
-- ============================================================

SELECT
    `Product ID`,
    `Product Description`,
    ROUND(SUM(`Line Total USD`), 2) AS revenue_usd,
    SUM(`Quantity`) AS units_sold,
    ROUND(SUM(`Profit USD`), 2) AS profit_usd
FROM fact_transactions
GROUP BY
    `Product ID`,
    `Product Description`
ORDER BY revenue_usd DESC
LIMIT 10;

-- ============================================================
-- 8. SALES BY CUSTOMER COUNTRY
-- ============================================================

SELECT
    `Customer Country`,
    ROUND(SUM(`Line Total USD`), 2) AS revenue_usd,
    ROUND(SUM(`Profit USD`), 2) AS profit_usd,
    COUNT(DISTINCT `Customer ID`) AS customers
FROM fact_transactions
GROUP BY `Customer Country`
ORDER BY revenue_usd DESC
LIMIT 20;

-- ============================================================
-- 9. SALES BY STORE COUNTRY
-- ============================================================

SELECT
    `Store Country`,
    ROUND(SUM(`Line Total USD`), 2) AS revenue_usd,
    ROUND(SUM(`Profit USD`), 2) AS profit_usd,
    COUNT(DISTINCT `Store ID`) AS stores
FROM fact_transactions
GROUP BY `Store Country`
ORDER BY revenue_usd DESC;

-- ============================================================
-- 10. PAYMENT METHOD ANALYSIS
-- ============================================================

SELECT
    `Payment Method`,
    COUNT(*) AS transactions,
    ROUND(SUM(`Line Total USD`), 2) AS revenue_usd,
    ROUND(AVG(`Line Total USD`), 2) AS average_transaction_usd
FROM fact_transactions
GROUP BY `Payment Method`
ORDER BY revenue_usd DESC;

-- ============================================================
-- 11. CUSTOMER GENDER ANALYSIS
-- ============================================================

SELECT
    `Customer Gender`,
    COUNT(DISTINCT `Customer ID`) AS customers,
    ROUND(SUM(`Line Total USD`), 2) AS revenue_usd,
    ROUND(SUM(`Profit USD`), 2) AS profit_usd
FROM fact_transactions
GROUP BY `Customer Gender`
ORDER BY revenue_usd DESC;

-- ============================================================
-- 12. WEEKDAY VS WEEKEND
-- ============================================================

SELECT
    CASE
        WHEN `Is Weekend` = true THEN 'Weekend'
        ELSE 'Weekday'
    END AS day_type,
    COUNT(*) AS transactions,
    ROUND(SUM(`Line Total USD`), 2) AS revenue_usd,
    ROUND(SUM(`Profit USD`), 2) AS profit_usd
FROM fact_transactions
GROUP BY
    CASE
        WHEN `Is Weekend` = true THEN 'Weekend'
        ELSE 'Weekday'
    END
ORDER BY revenue_usd DESC;

-- ============================================================
-- 13. DISCOUNT ANALYSIS
-- ============================================================

SELECT
    CASE
        WHEN `Promo Discount Pct` = 0 THEN 'No Discount'
        WHEN `Promo Discount Pct` <= 10 THEN '1-10%'
        WHEN `Promo Discount Pct` <= 20 THEN '11-20%'
        WHEN `Promo Discount Pct` <= 30 THEN '21-30%'
        ELSE '30%+'
    END AS discount_range,
    COUNT(*) AS transactions,
    ROUND(SUM(`Line Total USD`), 2) AS revenue_usd,
    ROUND(SUM(`Profit USD`), 2) AS profit_usd
FROM fact_transactions
GROUP BY
    CASE
        WHEN `Promo Discount Pct` = 0 THEN 'No Discount'
        WHEN `Promo Discount Pct` <= 10 THEN '1-10%'
        WHEN `Promo Discount Pct` <= 20 THEN '11-20%'
        WHEN `Promo Discount Pct` <= 30 THEN '21-30%'
        ELSE '30%+'
    END
ORDER BY revenue_usd DESC;

-- ============================================================
-- 14. TOP 10 CUSTOMERS BY REVENUE
-- ============================================================

SELECT
    `Customer ID`,
    `Customer Name`,
    `Customer Country`,
    ROUND(SUM(`Line Total USD`), 2) AS revenue_usd,
    ROUND(SUM(`Profit USD`), 2) AS profit_usd
FROM fact_transactions
GROUP BY
    `Customer ID`,
    `Customer Name`,
    `Customer Country`
ORDER BY revenue_usd DESC
LIMIT 10;

-- ============================================================
-- 15. TOP 10 STORES BY REVENUE
-- ============================================================

SELECT
    `Store ID`,
    `Store Name`,
    `Store Country`,
    ROUND(SUM(`Line Total USD`), 2) AS revenue_usd,
    ROUND(SUM(`Profit USD`), 2) AS profit_usd
FROM fact_transactions
GROUP BY
    `Store ID`,
    `Store Name`,
    `Store Country`
ORDER BY revenue_usd DESC
LIMIT 10;
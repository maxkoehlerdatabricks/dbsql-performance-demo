-- Databricks notebook source
-- MAGIC %md
-- MAGIC # DBSQL Query Optimization — SAP Purchasing Analytics
-- MAGIC
-- MAGIC This notebook demonstrates **query-side** best practices on the star schema built by `01_etl_sap_purchasing`.
-- MAGIC Run this on a **Serverless SQL Warehouse** to get Photon, PQE, and automatic caching.
-- MAGIC
-- MAGIC | Section | Best Practice |
-- MAGIC |---------|--------------|
-- MAGIC | 1 | Filter early, aggregate late |
-- MAGIC | 2 | Leverage Liquid Clustering for data skipping |
-- MAGIC | 3 | Explicit column lists (not SELECT *) |
-- MAGIC | 4 | QUALIFY instead of subqueries |
-- MAGIC | 5 | Deterministic queries for result caching |
-- MAGIC | 6 | Materialized Views for repeated aggregations |
-- MAGIC | 7 | Star schema joins with PK/FK optimizer hints |
-- MAGIC | 8 | Anti-patterns vs. optimized equivalents |
-- MAGIC | 9 | Monitoring & diagnostics |

-- COMMAND ----------

USE CATALOG classic_stable_4rp118_catalog;
USE SCHEMA sap_purchasing_gold;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Filter Early, Aggregate Late
-- MAGIC
-- MAGIC Push `WHERE` clauses as close to the base tables as possible.
-- MAGIC This reduces data scanned **before** joins and aggregations — the single biggest query tuning lever.

-- COMMAND ----------

-- GOOD: Filters applied BEFORE the join and aggregation
-- The optimizer pushes date_key filter into the fact scan, leveraging Liquid Clustering
SELECT
  v.vendor_group,
  v.country,
  count(*)             AS po_line_count,
  sum(po.net_value)    AS total_spend,
  avg(po.net_price)    AS avg_unit_price
FROM fact_purchase_orders po
JOIN dim_vendor v ON po.vendor_key = v.vendor_key
JOIN dim_date   d ON po.date_key   = d.date_key
WHERE d.year = 2024                -- ← filter early on date dimension
  AND po.status = 'Closed'         -- ← filter early on fact
GROUP BY v.vendor_group, v.country
ORDER BY total_spend DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Leverage Liquid Clustering for Data Skipping
-- MAGIC
-- MAGIC Tables are clustered by `(date_key, vendor_key)`. Queries that filter on these columns
-- MAGIC skip entire file groups — check the **"rows scanned"** in Query Profile.

-- COMMAND ----------

-- This query benefits from Liquid Clustering on date_key
-- Only files containing Q1 2025 data are read
SELECT
  po.po_number,
  po.po_item,
  po.net_value,
  po.status
FROM fact_purchase_orders po
WHERE po.date_key BETWEEN 20250101 AND 20250331   -- Q1 2025 — clustered column
ORDER BY po.net_value DESC
LIMIT 20;

-- COMMAND ----------

-- Compound filter on BOTH clustering keys — maximum data skipping
SELECT
  po.po_number,
  po.net_value,
  v.vendor_name
FROM fact_purchase_orders po
JOIN dim_vendor v ON po.vendor_key = v.vendor_key
WHERE po.date_key BETWEEN 20250101 AND 20250331   -- clustering key 1
  AND po.vendor_key BETWEEN 1 AND 20               -- clustering key 2
ORDER BY po.net_value DESC
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. Explicit Column Lists — Avoid SELECT *
-- MAGIC
-- MAGIC Only read the columns you need. On wide tables this dramatically reduces I/O.

-- COMMAND ----------

-- BAD: SELECT * reads all columns including those you don't need
-- SELECT * FROM fact_purchase_orders LIMIT 10;

-- GOOD: Only read the 4 columns actually needed
SELECT po_number, po_item, net_value, status
FROM fact_purchase_orders
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. QUALIFY — Window Function Filtering Without Subqueries
-- MAGIC
-- MAGIC `QUALIFY` filters rows **after** window functions, eliminating the need for
-- MAGIC a wrapping subquery. Cleaner SQL, same performance.

-- COMMAND ----------

-- GOOD: Get the latest PO per vendor using QUALIFY (no subquery needed)
SELECT
  po.vendor_key,
  v.vendor_name,
  po.po_number,
  po.date_key,
  po.net_value
FROM fact_purchase_orders po
JOIN dim_vendor v ON po.vendor_key = v.vendor_key
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY po.vendor_key
  ORDER BY po.date_key DESC
) = 1
ORDER BY po.net_value DESC
LIMIT 20;

-- COMMAND ----------

-- Compare: The traditional subquery approach (functionally identical, harder to read)
-- SELECT * FROM (
--   SELECT po.vendor_key, v.vendor_name, po.po_number, po.date_key, po.net_value,
--          ROW_NUMBER() OVER (PARTITION BY po.vendor_key ORDER BY po.date_key DESC) AS rn
--   FROM fact_purchase_orders po
--   JOIN dim_vendor v ON po.vendor_key = v.vendor_key
-- ) WHERE rn = 1
-- ORDER BY net_value DESC LIMIT 20;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5. Deterministic Queries for Result Caching
-- MAGIC
-- MAGIC DBSQL automatically caches query results. Cache is invalidated when data changes.
-- MAGIC **Non-deterministic functions** (`NOW()`, `CURRENT_TIMESTAMP()`, `RAND()`) **break caching**.

-- COMMAND ----------

-- BAD: CURRENT_DATE() makes this non-deterministic — never cached
-- SELECT count(*) FROM fact_purchase_orders
-- WHERE date_key >= cast(date_format(current_date() - INTERVAL 90 DAYS, 'yyyyMMdd') AS INT);

-- GOOD: Use a literal date — result is cached until underlying data changes
-- For dashboards, parameterize this value externally
SELECT
  count(*)          AS po_count,
  sum(net_value)    AS total_value
FROM fact_purchase_orders
WHERE date_key >= 20250101;   -- literal → deterministic → cacheable

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 6. Materialized Views — Pre-Compute Repeated Aggregations
-- MAGIC
-- MAGIC Materialized Views are the best optimization for frequently run dashboard queries.
-- MAGIC DBSQL refreshes them on a schedule and serves results directly from the MV.

-- COMMAND ----------

-- Monthly spend by vendor group — a classic dashboard query
-- Without an MV, this scans the full fact + dimension every time
CREATE OR REPLACE MATERIALIZED VIEW mv_monthly_vendor_spend
  CLUSTER BY (year, month)
  COMMENT 'Pre-aggregated monthly spend by vendor group. Refreshes hourly.'
AS
SELECT
  d.year,
  d.month,
  d.month_name,
  v.vendor_group,
  v.country                     AS vendor_country,
  count(*)                      AS po_line_count,
  count(DISTINCT po.po_number)  AS po_count,
  sum(po.net_value)             AS total_spend,
  avg(po.net_price)             AS avg_unit_price
FROM fact_purchase_orders po
JOIN dim_vendor v ON po.vendor_key = v.vendor_key
JOIN dim_date   d ON po.date_key   = d.date_key
GROUP BY d.year, d.month, d.month_name, v.vendor_group, v.country;

-- COMMAND ----------

-- Now this query reads from the MV — sub-second, no fact table scan
SELECT
  year, month_name, vendor_group,
  total_spend,
  po_count
FROM mv_monthly_vendor_spend
WHERE year = 2024
ORDER BY total_spend DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 7. Star Schema Joins — PK/FK Optimizer Hints
-- MAGIC
-- MAGIC Because we defined PK/FK constraints, the DBSQL optimizer can:
-- MAGIC - Eliminate unnecessary joins (if no columns from a dimension are selected)
-- MAGIC - Choose optimal join orders
-- MAGIC - Use broadcast joins for small dimensions

-- COMMAND ----------

-- Full star schema query — 3-way procurement analysis
-- The optimizer uses FK constraints to plan optimal join order
SELECT
  d.fiscal_year,
  d.fiscal_quarter,
  p.plant_name,
  p.country                   AS plant_country,
  po_org.purch_org_name,
  m.material_group,
  count(DISTINCT po.po_number)  AS unique_pos,
  count(*)                      AS line_items,
  sum(po.net_value)             AS total_spend,
  sum(po.quantity)              AS total_qty
FROM fact_purchase_orders po
JOIN dim_date          d      ON po.date_key      = d.date_key
JOIN dim_plant         p      ON po.plant_key     = p.plant_key
JOIN dim_purchase_org  po_org ON po.purch_org_key = po_org.purch_org_key
JOIN dim_material      m      ON po.material_key  = m.material_key
WHERE d.fiscal_year = 2025
  AND po.status IN ('Closed', 'Partially Delivered')
GROUP BY d.fiscal_year, d.fiscal_quarter, p.plant_name, p.country,
         po_org.purch_org_name, m.material_group
ORDER BY total_spend DESC
LIMIT 30;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 8. Anti-Patterns vs. Optimized Equivalents
-- MAGIC
-- MAGIC Side-by-side comparison of common mistakes and their fixes.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Anti-Pattern A: Using Python UDFs for string logic
-- MAGIC
-- MAGIC UDFs serialize data from JVM to Python and back — 10-100x slower than native SQL.
-- MAGIC Always check if a built-in function exists first.

-- COMMAND ----------

-- GOOD: Native SQL functions (no UDF needed)
SELECT
  vendor_name,
  upper(vendor_name)                                      AS name_upper,
  split(vendor_name, ' ')[0]                              AS first_word,
  regexp_extract(vendor_name, '(Corp|GmbH|Ltd|Inc)', 1)   AS entity_type
FROM dim_vendor
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Anti-Pattern B: Self-join instead of window function

-- COMMAND ----------

-- BAD: Self-join to get running total (shuffles entire table twice)
-- SELECT a.po_number, a.date_key, a.net_value,
--        sum(b.net_value) AS running_total
-- FROM fact_purchase_orders a
-- JOIN fact_purchase_orders b ON a.vendor_key = b.vendor_key AND b.date_key <= a.date_key
-- GROUP BY a.po_number, a.date_key, a.net_value;

-- GOOD: Window function — single pass, no self-join
SELECT
  po_number,
  date_key,
  net_value,
  sum(net_value) OVER (
    PARTITION BY vendor_key
    ORDER BY date_key
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS running_total
FROM fact_purchase_orders
WHERE vendor_key = 1
ORDER BY date_key
LIMIT 20;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Anti-Pattern C: Filtering AFTER aggregation instead of BEFORE

-- COMMAND ----------

-- BAD: Aggregates ALL data, then filters (scans entire table)
-- SELECT vendor_key, sum(net_value) AS total
-- FROM fact_purchase_orders
-- GROUP BY vendor_key
-- HAVING sum(net_value) > 1000000;

-- GOOD: Filter on a clustered column first, then aggregate (reads ~1/3 of data)
SELECT vendor_key, sum(net_value) AS total
FROM fact_purchase_orders
WHERE date_key >= 20250101          -- filter first on clustered column
GROUP BY vendor_key
HAVING sum(net_value) > 100000
ORDER BY total DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 9. Advanced Analytics — Pipe Syntax (DBR 16.1+)
-- MAGIC
-- MAGIC Pipe syntax (`|>`) makes complex analytical queries more readable
-- MAGIC by expressing transformations in execution order (top to bottom).

-- COMMAND ----------

-- Three-way match analysis: PO → Goods Receipt → Invoice
-- Using pipe syntax for readability
FROM fact_purchase_orders po
  |> WHERE status = 'Closed' AND date_key >= 20240101
  |> JOIN fact_goods_receipts gr ON po.po_number = gr.po_number
  |> JOIN fact_invoices inv ON po.po_number = inv.po_number
  |> JOIN dim_vendor v ON po.vendor_key = v.vendor_key
  |> AGGREGATE
       count(DISTINCT po.po_number) AS matched_pos,
       sum(po.net_value)            AS po_value,
       sum(inv.invoice_amount)      AS invoice_value,
       sum(inv.invoice_amount) - sum(po.net_value) AS variance
     GROUP BY v.vendor_group, v.country
  |> WHERE abs(variance) > 10000
  |> ORDER BY abs(variance) DESC
  |> LIMIT 20;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 10. Monitoring & Diagnostics
-- MAGIC
-- MAGIC Use these queries to understand table health and identify optimization opportunities.

-- COMMAND ----------

-- Check table sizes, file counts, and clustering status
SELECT 'fact_purchase_orders' AS tbl, * FROM (DESCRIBE DETAIL fact_purchase_orders)
UNION ALL
SELECT 'fact_goods_receipts', * FROM (DESCRIBE DETAIL fact_goods_receipts)
UNION ALL
SELECT 'fact_invoices', * FROM (DESCRIBE DETAIL fact_invoices);

-- COMMAND ----------

-- Check column statistics are populated (confirms ANALYZE TABLE ran)
DESCRIBE EXTENDED fact_purchase_orders date_key;

-- COMMAND ----------

-- Identify tables with high small-file counts that need OPTIMIZE
-- (In production, query system.access.table_lineage or system.billing for deeper insights)
SELECT
  'fact_purchase_orders' AS table_name,
  numFiles AS file_count,
  round(sizeInBytes / 1024 / 1024, 2) AS size_mb
FROM (DESCRIBE DETAIL fact_purchase_orders)
UNION ALL
SELECT 'fact_goods_receipts', numFiles, round(sizeInBytes / 1024 / 1024, 2)
FROM (DESCRIBE DETAIL fact_goods_receipts)
UNION ALL
SELECT 'fact_invoices', numFiles, round(sizeInBytes / 1024 / 1024, 2)
FROM (DESCRIBE DETAIL fact_invoices);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 11. Bonus: Purchasing KPI Dashboard Query
-- MAGIC
-- MAGIC A realistic executive dashboard query combining all best practices.

-- COMMAND ----------

-- Executive procurement dashboard — single query, all best practices applied
WITH po_summary AS (
  SELECT
    d.fiscal_year,
    d.fiscal_quarter,
    po.vendor_key,
    po.plant_key,
    count(DISTINCT po.po_number)  AS unique_pos,
    count(*)                      AS line_items,
    sum(po.net_value)             AS po_spend
  FROM fact_purchase_orders po
  JOIN dim_date d ON po.date_key = d.date_key
  WHERE d.fiscal_year IN (2024, 2025)          -- filter early on date
    AND po.status IN ('Closed', 'Partially Delivered')
  GROUP BY d.fiscal_year, d.fiscal_quarter, po.vendor_key, po.plant_key
),
inv_summary AS (
  SELECT
    d.fiscal_year,
    d.fiscal_quarter,
    inv.vendor_key,
    count(*)                 AS invoice_count,
    sum(inv.invoice_amount)  AS invoice_total,
    sum(CASE WHEN inv.payment_status = 'Overdue' THEN inv.invoice_amount ELSE 0 END) AS overdue_amount
  FROM fact_invoices inv
  JOIN dim_date d ON inv.date_key = d.date_key
  WHERE d.fiscal_year IN (2024, 2025)
  GROUP BY d.fiscal_year, d.fiscal_quarter, inv.vendor_key
)
SELECT
  po.fiscal_year,
  po.fiscal_quarter,
  p.plant_name,
  v.vendor_group,
  po.unique_pos,
  po.line_items,
  po.po_spend,
  coalesce(inv.invoice_total, 0)   AS invoiced_amount,
  coalesce(inv.overdue_amount, 0)  AS overdue_amount,
  round(coalesce(inv.overdue_amount, 0) / nullif(inv.invoice_total, 0) * 100, 1) AS overdue_pct
FROM po_summary po
JOIN dim_vendor v ON po.vendor_key = v.vendor_key
JOIN dim_plant  p ON po.plant_key  = p.plant_key
LEFT JOIN inv_summary inv
  ON  po.fiscal_year    = inv.fiscal_year
  AND po.fiscal_quarter = inv.fiscal_quarter
  AND po.vendor_key     = inv.vendor_key
ORDER BY po.po_spend DESC
LIMIT 50;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Summary of Query Best Practices Demonstrated
-- MAGIC
-- MAGIC | # | Practice | Impact |
-- MAGIC |---|---------|--------|
-- MAGIC | 1 | Filter early, aggregate late | Reduces data scanned before joins |
-- MAGIC | 2 | Query on Liquid Clustering keys | Enables file-level data skipping |
-- MAGIC | 3 | Explicit column lists | Reduces I/O on wide tables |
-- MAGIC | 4 | QUALIFY for window filtering | Cleaner SQL, no subquery overhead |
-- MAGIC | 5 | Deterministic queries | Enables query result caching |
-- MAGIC | 6 | Materialized Views | Sub-second for repeated aggregations |
-- MAGIC | 7 | PK/FK constraints | Optimizer join elimination & ordering |
-- MAGIC | 8 | Native functions over UDFs | 10-100x faster than Python UDFs |
-- MAGIC | 9 | Window functions over self-joins | Single pass vs. quadratic shuffle |
-- MAGIC | 10 | Pipe syntax | Readable complex analytics |
-- MAGIC
-- MAGIC **Tip:** Use **Query Profile** (click the bar chart icon on any query result) to see
-- MAGIC exactly how many rows were scanned, which files were skipped, and where time was spent.

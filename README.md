# DBSQL Performance Best Practices — SAP Purchasing Demo

A hands-on demo showing how to maximize Databricks SQL (DBSQL) query performance, covering both the **ETL that writes data** and the **queries that read it**.

The demo uses a star schema modeled after **SAP MM (Materials Management)** purchasing data in a manufacturing company.

## Data Model

```
                  dim_vendor ──────┐
                dim_material ──────┤
      dim_plant ───────────────────┼── fact_purchase_orders (100K rows)
      dim_purchase_org ────────────┤
      dim_date ────────────────────┘

                dim_material ──────┤
      dim_plant ───────────────────┼── fact_goods_receipts  (80K rows)
      dim_date ────────────────────┘

                  dim_vendor ──────┤
      dim_date ────────────────────┼── fact_invoices         (70K rows)
```

**SAP tables simulated:** LFA1 (Vendor Master), MARA/MAKT (Material Master), T001W (Plants), T024E (Purchasing Orgs), EKKO/EKPO (Purchase Orders), MSEG/MKPF (Goods Receipts), RBKP/RSEG (Invoices).

## How to Run

### Prerequisites

- Access to a Databricks workspace with a **SQL Warehouse** (Serverless recommended)
- Permission to create a catalog (`sap_purchasing_demo`)

### Step 1: Run the ETL Notebook

1. Open **`01_etl_sap_purchasing`** in the Databricks workspace
2. Attach to a **SQL Warehouse** (Serverless recommended) or an all-purpose cluster
3. Click **Run All** or run cells top-to-bottom
4. This creates the catalog, schema, all tables, generates synthetic data, and runs the optimization pipeline

**Expected runtime:** ~2-5 minutes

### Step 2: Run the Query Optimization Notebook

1. Open **`02_dbsql_query_optimization`** in the Databricks workspace
2. Attach to a **Serverless SQL Warehouse** (to get Photon, PQE, and caching)
3. Run cells top-to-bottom, reading the commentary in each section
4. Use **Query Profile** (bar chart icon on results) to see data skipping and execution details

### Step 3: Explore Query Profile

After running queries in notebook 2, click the **Query Profile** icon to observe:
- How many files/rows were **skipped** (Liquid Clustering benefit)
- Join elimination from **PK/FK constraints**
- **Cache hits** on deterministic queries (run the same cell twice)

## Best Practices Demonstrated

### ETL / Data Writing (Notebook 1)

| Practice | Why It Matters |
|----------|---------------|
| **Liquid Clustering** on all tables | Replaces partitioning + Z-ORDER; 30-60% faster queries |
| **PK/FK constraints** | Optimizer uses these for join elimination and ordering |
| **Integer surrogate keys** (`IDENTITY`) | Faster joins than string keys |
| **DECIMAL for money** | Avoids floating-point precision errors |
| **CREATE OR REPLACE** | Preserves time travel, no reader interruption |
| **MERGE for upserts** | Less write amplification than DELETE + INSERT |
| **OPTIMIZE → VACUUM → ANALYZE** | Compacts files, cleans storage, computes statistics |
| **dataSkippingStatsColumns** | Targets statistics collection to high-value columns |
| **Comments on all objects** | Enables AI/BI discoverability |

### Query Optimization (Notebook 2)

| Practice | Why It Matters |
|----------|---------------|
| **Filter early, aggregate late** | Reduces data before joins — biggest tuning lever |
| **Query on clustering keys** | File-level data skipping |
| **Explicit column lists** | Reduces I/O on wide tables |
| **QUALIFY** | Window filtering without wrapping subqueries |
| **Deterministic queries** | Enables automatic result caching |
| **Materialized Views** | Sub-second for repeated dashboard aggregations |
| **Native functions (not UDFs)** | 10-100x faster than Python UDFs |
| **Window functions (not self-joins)** | Single-pass vs. quadratic shuffle |
| **Pipe syntax** | Readable complex analytics |

## Cleanup

To remove all demo objects:

```sql
DROP CATALOG IF EXISTS sap_purchasing_demo CASCADE;
```

## References

- [Liquid Clustering](https://docs.databricks.com/en/delta/clustering.html)
- [DBSQL Best Practices](https://docs.databricks.com/en/sql/best-practices.html)
- [Materialized Views](https://docs.databricks.com/en/sql/materialized-views.html)
- [ANALYZE TABLE](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-aux-analyze-table.html)

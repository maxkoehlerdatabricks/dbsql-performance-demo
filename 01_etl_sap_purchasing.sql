-- Databricks notebook source
-- MAGIC %md
-- MAGIC # SAP Purchasing ETL Pipeline — DBSQL Performance Best Practices
-- MAGIC
-- MAGIC This notebook builds a **star schema** data model simulating SAP MM (Materials Management)
-- MAGIC purchasing data for a manufacturing company. It demonstrates every recommended ETL best practice
-- MAGIC for maximizing downstream DBSQL query performance:
-- MAGIC
-- MAGIC | Best Practice | Where Demonstrated |
-- MAGIC |---|---|
-- MAGIC | Liquid Clustering (replaces partitioning + Z-ORDER) | All table DDL |
-- MAGIC | PK / FK constraints for optimizer hints | All table DDL |
-- MAGIC | Integer surrogate keys (`IDENTITY`) | All dimensions |
-- MAGIC | `DECIMAL` for financial data | Fact tables |
-- MAGIC | `CREATE OR REPLACE` (not DROP + CREATE) | All table DDL |
-- MAGIC | `MERGE` for upserts (not DELETE + INSERT) | SCD Type 1 & 2 |
-- MAGIC | `OPTIMIZE` → `VACUUM` → `ANALYZE TABLE` pipeline | Final cells |
-- MAGIC | `dataSkippingStatsColumns` table property | Fact tables |
-- MAGIC | Comments & tags for AI/BI discoverability | All tables |
-- MAGIC
-- MAGIC **Data Model (SAP MM Purchasing):**
-- MAGIC ```
-- MAGIC                 dim_vendor ──┐
-- MAGIC               dim_material ──┤
-- MAGIC     dim_plant ───────────────┼── fact_purchase_orders
-- MAGIC     dim_purchase_org ────────┤
-- MAGIC     dim_date ────────────────┘
-- MAGIC                               │
-- MAGIC               dim_material ──┤
-- MAGIC     dim_plant ───────────────┼── fact_goods_receipts
-- MAGIC     dim_date ────────────────┘
-- MAGIC                               │
-- MAGIC                 dim_vendor ──┤
-- MAGIC     dim_date ────────────────┼── fact_invoices
-- MAGIC ```

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 0: Setup — Create Catalog & Schema

-- COMMAND ----------

-- Use a dedicated schema for the demo
CREATE CATALOG IF NOT EXISTS sap_purchasing_demo;
USE CATALOG sap_purchasing_demo;
CREATE SCHEMA IF NOT EXISTS gold;
USE SCHEMA gold;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 1: Dimension Tables
-- MAGIC
-- MAGIC **Best practices demonstrated:**
-- MAGIC - `BIGINT GENERATED ALWAYS AS IDENTITY` for surrogate keys (faster joins than strings)
-- MAGIC - `PRIMARY KEY` constraints inform the DBSQL optimizer
-- MAGIC - Liquid Clustering on PK + common filter columns
-- MAGIC - Rich `COMMENT` on every table and column for AI/BI discoverability

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### dim_vendor (SAP LFA1 — Vendor Master)

-- COMMAND ----------

CREATE OR REPLACE TABLE dim_vendor (
  vendor_key    BIGINT GENERATED ALWAYS AS IDENTITY
                COMMENT 'Surrogate key (auto-generated)',
  vendor_id     STRING NOT NULL
                COMMENT 'SAP vendor number (LIFNR)',
  vendor_name   STRING
                COMMENT 'Vendor name (NAME1)',
  country       STRING
                COMMENT 'Country key (LAND1)',
  city          STRING
                COMMENT 'City (ORT01)',
  vendor_group  STRING
                COMMENT 'Account group (KTOKK) — e.g. Raw Material, MRO, Services, Packaging',
  payment_terms STRING
                COMMENT 'Payment terms key (ZTERM)',
  is_active     BOOLEAN DEFAULT TRUE
                COMMENT 'Active flag for SCD Type 1',
  CONSTRAINT pk_vendor PRIMARY KEY (vendor_key)
)
CLUSTER BY (vendor_key, vendor_group)
COMMENT 'Vendor master dimension — sourced from SAP LFA1. Clustered by PK + vendor_group for filtered joins.';

-- COMMAND ----------

-- Generate 200 realistic SAP vendors
INSERT INTO dim_vendor (vendor_id, vendor_name, country, city, vendor_group, payment_terms)
SELECT
  concat('V', lpad(cast(id as STRING), 6, '0'))                                      AS vendor_id,
  concat(
    CASE mod(id, 8)
      WHEN 0 THEN 'Acme'      WHEN 1 THEN 'Global'    WHEN 2 THEN 'Premier'
      WHEN 3 THEN 'Apex'      WHEN 4 THEN 'Atlas'     WHEN 5 THEN 'Summit'
      WHEN 6 THEN 'Vanguard'  ELSE 'Pacific'
    END, ' ',
    CASE mod(id, 6)
      WHEN 0 THEN 'Steel'     WHEN 1 THEN 'Chemical'  WHEN 2 THEN 'Electronics'
      WHEN 3 THEN 'Plastics'  WHEN 4 THEN 'Tooling'   ELSE 'Logistics'
    END, ' ',
    CASE mod(id, 4)
      WHEN 0 THEN 'Corp'      WHEN 1 THEN 'GmbH'
      WHEN 2 THEN 'Ltd'       ELSE 'Inc'
    END
  )                                                                                    AS vendor_name,
  CASE mod(id, 5)
    WHEN 0 THEN 'US' WHEN 1 THEN 'DE' WHEN 2 THEN 'CN' WHEN 3 THEN 'JP' ELSE 'MX'
  END                                                                                  AS country,
  CASE mod(id, 10)
    WHEN 0 THEN 'Detroit'     WHEN 1 THEN 'Stuttgart'  WHEN 2 THEN 'Shanghai'
    WHEN 3 THEN 'Tokyo'       WHEN 4 THEN 'Monterrey'  WHEN 5 THEN 'Chicago'
    WHEN 6 THEN 'Munich'      WHEN 7 THEN 'Shenzhen'   WHEN 8 THEN 'Osaka'
    ELSE 'Guadalajara'
  END                                                                                  AS city,
  CASE mod(id, 4)
    WHEN 0 THEN 'Raw Material' WHEN 1 THEN 'MRO'
    WHEN 2 THEN 'Services'     ELSE 'Packaging'
  END                                                                                  AS vendor_group,
  CASE mod(id, 3)
    WHEN 0 THEN 'NET30' WHEN 1 THEN 'NET60' ELSE 'NET90'
  END                                                                                  AS payment_terms
FROM (SELECT explode(sequence(1, 200)) AS id);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### dim_material (SAP MARA/MAKT — Material Master)

-- COMMAND ----------

CREATE OR REPLACE TABLE dim_material (
  material_key    BIGINT GENERATED ALWAYS AS IDENTITY
                  COMMENT 'Surrogate key',
  material_id     STRING NOT NULL
                  COMMENT 'SAP material number (MATNR)',
  material_desc   STRING
                  COMMENT 'Material description (MAKTX)',
  material_type   STRING
                  COMMENT 'Material type (MTART) — ROH/HALB/FERT/HIBE',
  material_group  STRING
                  COMMENT 'Material group (MATKL)',
  base_uom        STRING
                  COMMENT 'Base unit of measure (MEINS)',
  net_weight      DECIMAL(10,3)
                  COMMENT 'Net weight in KG',
  CONSTRAINT pk_material PRIMARY KEY (material_key)
)
CLUSTER BY (material_key, material_type)
COMMENT 'Material master dimension — sourced from SAP MARA/MAKT. Clustered for join + type filtering.';

-- COMMAND ----------

INSERT INTO dim_material (material_id, material_desc, material_type, material_group, base_uom, net_weight)
SELECT
  concat('M', lpad(cast(id AS STRING), 8, '0'))  AS material_id,
  concat(
    CASE mod(id, 5)
      WHEN 0 THEN 'Steel'     WHEN 1 THEN 'Aluminum'
      WHEN 2 THEN 'Copper'    WHEN 3 THEN 'Polymer'    ELSE 'Ceramic'
    END, ' ',
    CASE mod(id, 6)
      WHEN 0 THEN 'Sheet'     WHEN 1 THEN 'Rod'        WHEN 2 THEN 'Wire'
      WHEN 3 THEN 'Tube'      WHEN 4 THEN 'Bearing'    ELSE 'Gasket'
    END, ' ',
    CASE mod(id, 4)
      WHEN 0 THEN '10mm' WHEN 1 THEN '25mm' WHEN 2 THEN '50mm' ELSE '100mm'
    END
  )                                                AS material_desc,
  CASE mod(id, 4)
    WHEN 0 THEN 'ROH'   -- Raw material
    WHEN 1 THEN 'HALB'  -- Semi-finished
    WHEN 2 THEN 'FERT'  -- Finished
    ELSE        'HIBE'  -- Operating supplies / MRO
  END                                              AS material_type,
  CASE mod(id, 5)
    WHEN 0 THEN 'Metals'      WHEN 1 THEN 'Chemicals'
    WHEN 2 THEN 'Electronics' WHEN 3 THEN 'Packaging'  ELSE 'MRO Supplies'
  END                                              AS material_group,
  CASE mod(id, 3)
    WHEN 0 THEN 'KG' WHEN 1 THEN 'PC' ELSE 'M'
  END                                              AS base_uom,
  round(rand() * 50 + 0.5, 3)                     AS net_weight
FROM (SELECT explode(sequence(1, 500)) AS id);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### dim_plant (SAP T001W — Plant Master)

-- COMMAND ----------

CREATE OR REPLACE TABLE dim_plant (
  plant_key     BIGINT GENERATED ALWAYS AS IDENTITY  COMMENT 'Surrogate key',
  plant_id      STRING NOT NULL                      COMMENT 'SAP plant code (WERKS)',
  plant_name    STRING                               COMMENT 'Plant name (NAME1)',
  company_code  STRING                               COMMENT 'Company code (BUKRS)',
  country       STRING                               COMMENT 'Country key',
  region        STRING                               COMMENT 'Region / state',
  CONSTRAINT pk_plant PRIMARY KEY (plant_key)
)
CLUSTER BY (plant_key)
COMMENT 'Plant/manufacturing site dimension — sourced from SAP T001W.';

-- COMMAND ----------

INSERT INTO dim_plant (plant_id, plant_name, company_code, country, region)
VALUES
  ('1000', 'Detroit Assembly Plant',      '1000', 'US', 'Michigan'),
  ('1100', 'Chicago Components Plant',    '1000', 'US', 'Illinois'),
  ('1200', 'Houston Chemicals Plant',     '1000', 'US', 'Texas'),
  ('2000', 'Stuttgart Main Plant',        '2000', 'DE', 'Baden-Württemberg'),
  ('2100', 'Munich Precision Plant',      '2000', 'DE', 'Bavaria'),
  ('3000', 'Shanghai Assembly Plant',     '3000', 'CN', 'Shanghai'),
  ('3100', 'Shenzhen Electronics Plant',  '3000', 'CN', 'Guangdong'),
  ('4000', 'Tokyo R&D Plant',            '4000', 'JP', 'Kanto'),
  ('5000', 'Monterrey Assembly Plant',    '5000', 'MX', 'Nuevo León'),
  ('5100', 'Guadalajara Components Plant','5000', 'MX', 'Jalisco');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### dim_purchase_org (SAP T024E — Purchasing Organization)

-- COMMAND ----------

CREATE OR REPLACE TABLE dim_purchase_org (
  purch_org_key   BIGINT GENERATED ALWAYS AS IDENTITY  COMMENT 'Surrogate key',
  purch_org_id    STRING NOT NULL                      COMMENT 'Purchasing org ID (EKORG)',
  purch_org_name  STRING                               COMMENT 'Purchasing organization name',
  company_code    STRING                               COMMENT 'Assigned company code (BUKRS)',
  CONSTRAINT pk_purch_org PRIMARY KEY (purch_org_key)
)
CLUSTER BY (purch_org_key)
COMMENT 'Purchasing organization dimension — sourced from SAP T024E.';

-- COMMAND ----------

INSERT INTO dim_purchase_org (purch_org_id, purch_org_name, company_code)
VALUES
  ('PO01', 'North America Purchasing',  '1000'),
  ('PO02', 'Europe Purchasing',         '2000'),
  ('PO03', 'Asia-Pacific Purchasing',   '3000'),
  ('PO04', 'Japan Purchasing',          '4000'),
  ('PO05', 'LatAm Purchasing',          '5000');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### dim_date (Calendar Dimension — 3 Years)

-- COMMAND ----------

CREATE OR REPLACE TABLE dim_date (
  date_key        INT NOT NULL      COMMENT 'Integer key YYYYMMDD',
  full_date       DATE              COMMENT 'Calendar date',
  year            INT               COMMENT 'Calendar year',
  quarter         INT               COMMENT 'Calendar quarter (1-4)',
  month           INT               COMMENT 'Calendar month (1-12)',
  month_name      STRING            COMMENT 'Month name',
  week_of_year    INT               COMMENT 'ISO week number',
  day_of_week     INT               COMMENT 'Day of week (1=Mon, 7=Sun)',
  day_name        STRING            COMMENT 'Day name',
  is_weekend      BOOLEAN           COMMENT 'Weekend flag',
  fiscal_year     INT               COMMENT 'Fiscal year (Oct start)',
  fiscal_quarter  INT               COMMENT 'Fiscal quarter',
  CONSTRAINT pk_date PRIMARY KEY (date_key)
)
CLUSTER BY (date_key)
COMMENT 'Date dimension covering 2023-01-01 to 2025-12-31. Fiscal year starts October.';

-- COMMAND ----------

INSERT INTO dim_date
SELECT
  cast(date_format(d, 'yyyyMMdd') AS INT)                          AS date_key,
  d                                                                 AS full_date,
  year(d)                                                           AS year,
  quarter(d)                                                        AS quarter,
  month(d)                                                          AS month,
  date_format(d, 'MMMM')                                           AS month_name,
  weekofyear(d)                                                     AS week_of_year,
  dayofweek(d)                                                      AS day_of_week,
  date_format(d, 'EEEE')                                           AS day_name,
  dayofweek(d) IN (1, 7)                                            AS is_weekend,
  CASE WHEN month(d) >= 10 THEN year(d) + 1 ELSE year(d) END       AS fiscal_year,
  CASE
    WHEN month(d) >= 10 THEN (month(d) - 10) / 3 + 1
    ELSE (month(d) + 2) / 3
  END                                                               AS fiscal_quarter
FROM (
  SELECT explode(sequence(
    to_date('2023-01-01'),
    to_date('2025-12-31'),
    INTERVAL 1 DAY
  )) AS d
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 2: Fact Tables
-- MAGIC
-- MAGIC **Best practices demonstrated:**
-- MAGIC - `FOREIGN KEY` constraints → optimizer can eliminate unnecessary joins
-- MAGIC - `DECIMAL(18,2)` for all monetary values (never FLOAT)
-- MAGIC - Liquid Clustering on the most-filtered foreign keys (date_key first)
-- MAGIC - `dataSkippingStatsColumns` table property for targeted statistics

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### fact_purchase_orders (SAP EKKO/EKPO — PO Header & Items)

-- COMMAND ----------

CREATE OR REPLACE TABLE fact_purchase_orders (
  po_key          BIGINT GENERATED ALWAYS AS IDENTITY  COMMENT 'Surrogate key',
  po_number       STRING NOT NULL    COMMENT 'SAP PO number (EBELN) — degenerate dimension',
  po_item         INT NOT NULL       COMMENT 'PO line item number (EBELP)',
  vendor_key      BIGINT NOT NULL    COMMENT 'FK → dim_vendor',
  material_key    BIGINT NOT NULL    COMMENT 'FK → dim_material',
  plant_key       BIGINT NOT NULL    COMMENT 'FK → dim_plant',
  purch_org_key   BIGINT NOT NULL    COMMENT 'FK → dim_purchase_org',
  date_key        INT NOT NULL       COMMENT 'FK → dim_date (PO creation date)',
  quantity        DECIMAL(13,3)      COMMENT 'Order quantity (MENGE)',
  net_price       DECIMAL(18,2)      COMMENT 'Net price per unit (NETPR)',
  net_value       DECIMAL(18,2)      COMMENT 'Net order value (NETWR = quantity × net_price)',
  currency        STRING DEFAULT 'USD' COMMENT 'Currency key (WAERS)',
  po_type         STRING             COMMENT 'PO type (BSART) — NB/FO/UB/ZNB',
  status          STRING             COMMENT 'PO status — Open/Partially Delivered/Closed',
  CONSTRAINT fk_po_vendor    FOREIGN KEY (vendor_key)    REFERENCES dim_vendor(vendor_key),
  CONSTRAINT fk_po_material  FOREIGN KEY (material_key)  REFERENCES dim_material(material_key),
  CONSTRAINT fk_po_plant     FOREIGN KEY (plant_key)     REFERENCES dim_plant(plant_key),
  CONSTRAINT fk_po_porg      FOREIGN KEY (purch_org_key) REFERENCES dim_purchase_org(purch_org_key)
)
CLUSTER BY (date_key, vendor_key)
COMMENT 'Purchase order line items — sourced from SAP EKKO/EKPO. ~100K rows.';

-- COMMAND ----------

-- Set data skipping stats on the most filtered columns
ALTER TABLE fact_purchase_orders
SET TBLPROPERTIES ('delta.dataSkippingStatsColumns' = 'date_key,vendor_key,material_key,plant_key,po_type,status');

-- COMMAND ----------

-- Generate ~100K PO line items across 3 years
INSERT INTO fact_purchase_orders
  (po_number, po_item, vendor_key, material_key, plant_key, purch_org_key,
   date_key, quantity, net_price, net_value, currency, po_type, status)
SELECT
  concat('45000', lpad(cast(po_id AS STRING), 5, '0'))                AS po_number,
  cast(item_num * 10 AS INT)                                          AS po_item,
  -- Distribute across dimension keys
  cast(mod(abs(hash(po_id, item_num, 1)), 200) + 1 AS BIGINT)        AS vendor_key,
  cast(mod(abs(hash(po_id, item_num, 2)), 500) + 1 AS BIGINT)        AS material_key,
  cast(mod(abs(hash(po_id, item_num, 3)), 10)  + 1 AS BIGINT)        AS plant_key,
  cast(mod(abs(hash(po_id, item_num, 4)), 5)   + 1 AS BIGINT)        AS purch_org_key,
  -- Random date in 2023-2025 range
  cast(date_format(
    date_add('2023-01-01', cast(mod(abs(hash(po_id, item_num, 5)), 1095) AS INT)),
    'yyyyMMdd'
  ) AS INT)                                                            AS date_key,
  round(rand() * 1000 + 1, 3)                                         AS quantity,
  round(rand() * 500 + 5, 2)                                          AS net_price,
  round((rand() * 1000 + 1) * (rand() * 500 + 5), 2)                 AS net_value,
  CASE mod(abs(hash(po_id, item_num, 6)), 3)
    WHEN 0 THEN 'USD' WHEN 1 THEN 'EUR' ELSE 'CNY'
  END                                                                  AS currency,
  CASE mod(abs(hash(po_id, item_num, 7)), 4)
    WHEN 0 THEN 'NB'  WHEN 1 THEN 'FO' WHEN 2 THEN 'UB' ELSE 'ZNB'
  END                                                                  AS po_type,
  CASE mod(abs(hash(po_id, item_num, 8)), 3)
    WHEN 0 THEN 'Open' WHEN 1 THEN 'Partially Delivered' ELSE 'Closed'
  END                                                                  AS status
FROM (
  SELECT explode(sequence(1, 25000)) AS po_id
) po
CROSS JOIN (
  SELECT explode(sequence(1, 4)) AS item_num
) items;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### fact_goods_receipts (SAP MSEG/MKPF — Goods Movements)

-- COMMAND ----------

CREATE OR REPLACE TABLE fact_goods_receipts (
  gr_key          BIGINT GENERATED ALWAYS AS IDENTITY  COMMENT 'Surrogate key',
  gr_number       STRING NOT NULL    COMMENT 'Goods receipt document number (MBLNR)',
  po_number       STRING             COMMENT 'Reference PO number (EBELN)',
  material_key    BIGINT NOT NULL    COMMENT 'FK → dim_material',
  plant_key       BIGINT NOT NULL    COMMENT 'FK → dim_plant',
  date_key        INT NOT NULL       COMMENT 'FK → dim_date (posting date)',
  quantity        DECIMAL(13,3)      COMMENT 'Received quantity',
  amount          DECIMAL(18,2)      COMMENT 'GR value in local currency',
  movement_type   STRING             COMMENT 'SAP movement type (BWART) — 101/102/122',
  CONSTRAINT fk_gr_material  FOREIGN KEY (material_key) REFERENCES dim_material(material_key),
  CONSTRAINT fk_gr_plant     FOREIGN KEY (plant_key)    REFERENCES dim_plant(plant_key)
)
CLUSTER BY (date_key, plant_key)
COMMENT 'Goods receipt line items — sourced from SAP MSEG/MKPF. ~80K rows.';

-- COMMAND ----------

ALTER TABLE fact_goods_receipts
SET TBLPROPERTIES ('delta.dataSkippingStatsColumns' = 'date_key,plant_key,material_key,movement_type');

-- COMMAND ----------

INSERT INTO fact_goods_receipts
  (gr_number, po_number, material_key, plant_key, date_key, quantity, amount, movement_type)
SELECT
  concat('50000', lpad(cast(id AS STRING), 5, '0'))                    AS gr_number,
  concat('45000', lpad(cast(mod(abs(hash(id, 10)), 25000) + 1 AS STRING), 5, '0')) AS po_number,
  cast(mod(abs(hash(id, 11)), 500) + 1 AS BIGINT)                     AS material_key,
  cast(mod(abs(hash(id, 12)), 10) + 1  AS BIGINT)                     AS plant_key,
  cast(date_format(
    date_add('2023-01-15', cast(mod(abs(hash(id, 13)), 1080) AS INT)),
    'yyyyMMdd'
  ) AS INT)                                                             AS date_key,
  round(rand() * 800 + 1, 3)                                           AS quantity,
  round(rand() * 50000 + 100, 2)                                       AS amount,
  CASE mod(abs(hash(id, 14)), 10)
    WHEN 0 THEN '102'  -- reversal
    WHEN 1 THEN '122'  -- return
    ELSE '101'          -- standard GR (80% of records)
  END                                                                   AS movement_type
FROM (SELECT explode(sequence(1, 80000)) AS id);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### fact_invoices (SAP RBKP/RSEG — Invoice Verification)

-- COMMAND ----------

CREATE OR REPLACE TABLE fact_invoices (
  invoice_key      BIGINT GENERATED ALWAYS AS IDENTITY  COMMENT 'Surrogate key',
  invoice_number   STRING NOT NULL    COMMENT 'Invoice document number (BELNR)',
  po_number        STRING             COMMENT 'Reference PO number (EBELN)',
  vendor_key       BIGINT NOT NULL    COMMENT 'FK → dim_vendor',
  date_key         INT NOT NULL       COMMENT 'FK → dim_date (invoice posting date)',
  invoice_amount   DECIMAL(18,2)      COMMENT 'Invoice gross amount',
  tax_amount       DECIMAL(18,2)      COMMENT 'Tax amount',
  currency         STRING DEFAULT 'USD' COMMENT 'Currency key (WAERS)',
  payment_status   STRING             COMMENT 'Paid/Pending/Overdue/Blocked',
  payment_block    STRING             COMMENT 'Payment block reason (ZLSPR)',
  CONSTRAINT fk_inv_vendor FOREIGN KEY (vendor_key) REFERENCES dim_vendor(vendor_key)
)
CLUSTER BY (date_key, vendor_key)
COMMENT 'Vendor invoice line items — sourced from SAP RBKP/RSEG. ~70K rows.';

-- COMMAND ----------

ALTER TABLE fact_invoices
SET TBLPROPERTIES ('delta.dataSkippingStatsColumns' = 'date_key,vendor_key,payment_status');

-- COMMAND ----------

INSERT INTO fact_invoices
  (invoice_number, po_number, vendor_key, date_key,
   invoice_amount, tax_amount, currency, payment_status, payment_block)
SELECT
  concat('51000', lpad(cast(id AS STRING), 5, '0'))                     AS invoice_number,
  concat('45000', lpad(cast(mod(abs(hash(id, 20)), 25000) + 1 AS STRING), 5, '0')) AS po_number,
  cast(mod(abs(hash(id, 21)), 200) + 1 AS BIGINT)                      AS vendor_key,
  cast(date_format(
    date_add('2023-02-01', cast(mod(abs(hash(id, 22)), 1065) AS INT)),
    'yyyyMMdd'
  ) AS INT)                                                              AS date_key,
  round(rand() * 100000 + 500, 2)                                       AS invoice_amount,
  round(rand() * 10000 + 50, 2)                                         AS tax_amount,
  CASE mod(abs(hash(id, 23)), 3)
    WHEN 0 THEN 'USD' WHEN 1 THEN 'EUR' ELSE 'CNY'
  END                                                                    AS currency,
  CASE mod(abs(hash(id, 24)), 10)
    WHEN 0 THEN 'Overdue'
    WHEN 1 THEN 'Blocked'
    WHEN 2 THEN 'Pending'
    WHEN 3 THEN 'Pending'
    ELSE 'Paid'                                                          -- 60% paid
  END                                                                    AS payment_status,
  CASE mod(abs(hash(id, 24)), 10)
    WHEN 1 THEN CASE mod(abs(hash(id, 25)), 3)
                  WHEN 0 THEN 'R'  -- Invoice verification
                  WHEN 1 THEN 'A'  -- Posting block
                  ELSE 'B'          -- Quality block
                END
    ELSE NULL
  END                                                                    AS payment_block
FROM (SELECT explode(sequence(1, 70000)) AS id);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 3: SCD Type 1 — MERGE Upsert Demo
-- MAGIC
-- MAGIC **Best practice:** Always use `MERGE` instead of DELETE + INSERT for upserts.
-- MAGIC This reduces write amplification and preserves Delta transaction history.

-- COMMAND ----------

-- Simulate an incoming vendor update batch (5 changed records + 2 new)
CREATE OR REPLACE TEMP VIEW vendor_updates AS
SELECT * FROM VALUES
  ('V000001', 'Acme Steel Corp — RENAMED',    'US', 'Detroit',    'Raw Material', 'NET45', true),
  ('V000002', 'Global Chemical Inc',           'DE', 'Frankfurt',  'Raw Material', 'NET30', true),
  ('V000050', 'Premier Electronics Ltd',       'CN', 'Beijing',    'Electronics',  'NET60', false),
  ('V000100', 'Apex Plastics GmbH',            'DE', 'Berlin',     'Packaging',    'NET30', true),
  ('V000150', 'Atlas Tooling Inc',             'US', 'Cleveland',  'MRO',          'NET90', true),
  ('V000201', 'NewSupplier Metals AG',         'DE', 'Essen',      'Raw Material', 'NET30', true),
  ('V000202', 'NewSupplier Packaging Co',      'MX', 'Querétaro',  'Packaging',    'NET60', true)
AS t(vendor_id, vendor_name, country, city, vendor_group, payment_terms, is_active);

-- COMMAND ----------

-- SCD Type 1: Overwrite changed attributes, insert new records
MERGE INTO dim_vendor AS t
USING vendor_updates AS s
ON t.vendor_id = s.vendor_id
WHEN MATCHED THEN UPDATE SET
  t.vendor_name   = s.vendor_name,
  t.country       = s.country,
  t.city          = s.city,
  t.vendor_group  = s.vendor_group,
  t.payment_terms = s.payment_terms,
  t.is_active     = s.is_active
WHEN NOT MATCHED THEN INSERT
  (vendor_id, vendor_name, country, city, vendor_group, payment_terms, is_active)
VALUES
  (s.vendor_id, s.vendor_name, s.country, s.city, s.vendor_group, s.payment_terms, s.is_active);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 4: Post-Load Optimization Pipeline
-- MAGIC
-- MAGIC **Critical sequence:** `OPTIMIZE` → `VACUUM` → `ANALYZE TABLE`
-- MAGIC
-- MAGIC | Step | What it does | Why it matters |
-- MAGIC |------|-------------|----------------|
-- MAGIC | OPTIMIZE | Compacts small files; Liquid Clustering reorganizes only unclustered ZCubes | Reduces file count, improves data skipping |
-- MAGIC | VACUUM | Removes stale files beyond retention period | Reduces storage cost, speeds metadata ops |
-- MAGIC | ANALYZE TABLE | Computes column-level statistics | Feeds AQE + data skipping for better query plans |

-- COMMAND ----------

-- OPTIMIZE all tables (with Liquid Clustering, only unclustered data is reorganized)
OPTIMIZE dim_vendor;
OPTIMIZE dim_material;
OPTIMIZE dim_plant;
OPTIMIZE dim_purchase_org;
OPTIMIZE dim_date;
OPTIMIZE fact_purchase_orders;
OPTIMIZE fact_goods_receipts;
OPTIMIZE fact_invoices;

-- COMMAND ----------

-- VACUUM to clean up old files (using default 7-day retention)
VACUUM dim_vendor;
VACUUM dim_material;
VACUUM dim_plant;
VACUUM dim_purchase_org;
VACUUM dim_date;
VACUUM fact_purchase_orders;
VACUUM fact_goods_receipts;
VACUUM fact_invoices;

-- COMMAND ----------

-- ANALYZE TABLE — compute statistics for ALL columns used in filters, joins, GROUP BY
-- This is the SINGLE MOST IMPACTFUL optimization many teams skip!
ANALYZE TABLE dim_vendor       COMPUTE STATISTICS FOR ALL COLUMNS;
ANALYZE TABLE dim_material     COMPUTE STATISTICS FOR ALL COLUMNS;
ANALYZE TABLE dim_plant        COMPUTE STATISTICS FOR ALL COLUMNS;
ANALYZE TABLE dim_purchase_org COMPUTE STATISTICS FOR ALL COLUMNS;
ANALYZE TABLE dim_date         COMPUTE STATISTICS FOR ALL COLUMNS;
ANALYZE TABLE fact_purchase_orders COMPUTE STATISTICS FOR ALL COLUMNS;
ANALYZE TABLE fact_goods_receipts  COMPUTE STATISTICS FOR ALL COLUMNS;
ANALYZE TABLE fact_invoices        COMPUTE STATISTICS FOR ALL COLUMNS;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 5: Verify Data Model

-- COMMAND ----------

-- Row counts
SELECT 'dim_vendor'            AS table_name, count(*) AS row_count FROM dim_vendor
UNION ALL SELECT 'dim_material',          count(*) FROM dim_material
UNION ALL SELECT 'dim_plant',             count(*) FROM dim_plant
UNION ALL SELECT 'dim_purchase_org',      count(*) FROM dim_purchase_org
UNION ALL SELECT 'dim_date',             count(*) FROM dim_date
UNION ALL SELECT 'fact_purchase_orders',  count(*) FROM fact_purchase_orders
UNION ALL SELECT 'fact_goods_receipts',   count(*) FROM fact_goods_receipts
UNION ALL SELECT 'fact_invoices',         count(*) FROM fact_invoices
ORDER BY table_name;

-- COMMAND ----------

-- Verify Liquid Clustering is active
DESCRIBE DETAIL fact_purchase_orders;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Summary
-- MAGIC
-- MAGIC This ETL pipeline has:
-- MAGIC 1. Created a **star schema** with 5 dimensions and 3 fact tables
-- MAGIC 2. Used **Liquid Clustering** on every table (not partitioning or Z-ORDER)
-- MAGIC 3. Defined **PK/FK constraints** so the DBSQL optimizer can eliminate joins
-- MAGIC 4. Used **DECIMAL** for all monetary values
-- MAGIC 5. Used **MERGE** for upserts (not DELETE + INSERT)
-- MAGIC 6. Used **CREATE OR REPLACE** (not DROP + CREATE)
-- MAGIC 7. Run the full **OPTIMIZE → VACUUM → ANALYZE TABLE** pipeline
-- MAGIC 8. Set **dataSkippingStatsColumns** on fact tables
-- MAGIC
-- MAGIC **Next:** Open `02_dbsql_query_optimization` to run queries that leverage this data layout.

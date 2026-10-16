# RetailPulse — Complete Website User Manual & Screen-by-Screen Walkthrough

> **Interactive Frontend Architecture & UI Component Reference**  
> **Platform Version:** 2.4.0 (Enterprise Data Engineering Edition)  
> **Target Environment:** Windows 10/11 | Streamlit 1.35+ | Flask 3.1  

---

## 1. Platform Overview & Layout Hierarchy

RetailPulse uses a three-tier layout hierarchy:
1. **Top Navigation Bar (Global Header):** Real-time monitoring indicators, anomaly incident center, and RBAC persona selector.
2. **Left Sidebar (Primary Navigation):** Exactly three primary navigation views providing single-click switching between executive, streaming, and technical tooling.
3. **Main Content Stage:** The active view workspace featuring dynamic Plotly charts, interactive forms, high-speed dataframes, and nested secondary tabs.

---

## 2. Top Navigation Bar (Global Header)

* **A. System Identifier:** Displays platform title (`RetailPulse Data Engineering Platform`) and active release version.
* **B. Global Stream Health Indicator (`Stream: Online / Offline`):** Probes the background RabbitMQ broker or local queue daemon. When the message broker is actively ingesting events, the badge displays green (`🟢 Stream: Online`).
* **C. Anomaly & Incident Alerts Center (`🔔 Alerts (N)`):**
  * Popover aggregating alerts from:
    1. Quarantined staging events in `staging_sales`.
    2. Dead-Letter Queue (DLQ) depth in `retail_sales_events_dlq`.
    3. Great Expectations automated data quality failures.
  * Includes a **"Trigger Test Anomaly Alert"** button for live demonstrations.
* **D. User Profile & Role-Based Access Control (RBAC):**
  * **Michael (Analyst):** Read-only exploration. Destructive actions (resetting tables, mutating schemas, clearing queues) are strictly blocked with an informative permission prompt.
  * **Sarah (Admin):** Administrative engineer with elevated privileges to initialize Delta Lake tables, execute schema evolution, trigger Airflow DAG runs, and commit transactional modifications.

---

## 3. Primary Navigation 1: Executive Dashboard

* **Target Audience:** C-suite executives, Chief Data Officers, and Operations Leads.
* **A. Interactive Filter Bar:**
  * Date Range Presets (*All Time*, *Last 7/30/90 Days*) & Custom Date Pickers.
  * Region Multi-Select (*North America*, *Europe*, *Asia-Pacific*, *Latin America*, *Middle East*, *Africa*).
  * Category Multi-Select (*Electronics*, *Home & Kitchen*, *Sports & Outdoors*, *Apparel*, *Books*).
  * Ingestion Source Selector (*CSV Batch*, *Live REST API*, *WooCommerce Webhook*, *Streaming Simulator*).
* **B. 5 Dynamic Executive KPI Cards:**
  1. **Net Revenue ($):** Total gross sales minus returns.
  2. **Total Orders:** Count of completed transactional orders meeting filter criteria.
  3. **Average Order Value (AOV):** Mean financial basket value per customer purchase.
  4. **Repeat Customer Rate (%):** Percentage of customers with $\ge 2$ lifetime purchases.
  5. **Month-over-Month (MoM) Growth:** Revenue growth trajectory compared to the prior calendar month.
* **C. Core Visualizations:**
  * High-contrast daily and monthly Plotly revenue trajectory curves.
  * Top 10 selling products ranked by revenue and units sold.
  * Regional sales distribution breakdown.
* **D. Multidimensional OLAP Data Cube Drawer (Practical 10):**
  * **`CUBE(region, category, status)`:** Cross-tabulations across all $2^3 = 8$ dimension combinations.
  * **`ROLLUP(region, category)`:** Hierarchical subtotals from product category up to regional and grand totals.
  * **`GROUPING SETS`:** Custom analytical slices.
  * **Export to CSV:** Direct download of calculated OLAP cubes.

---

## 4. Primary Navigation 2: Live Sales Stream

* **Target Audience:** Streaming Data Engineers, Site Reliability Engineers, and Operations Teams.
* **A. Real-Time Simulation Controls:**
  * Background Simulator Toggle (Start / Stop worker).
  * Emission Frequency Slider ($0.2\text{s}$ to $5.0\text{s}$).
  * Burst Generator (Push single orders or 10-event bursts).
* **B. Message Broker Telemetry Grid:**
  1. **Ingestion Throughput:** Live events per second.
  2. **Queue Depth:** Unconsumed messages waiting in `retail_sales_events`.
  3. **End-to-End Latency:** Processing time from producer emission to database commit (ms).
  4. **Dead-Letter Queue (DLQ) Depth:** Quarantined events in `retail_sales_events_dlq`.
  5. **Duplicate Detection Counter:** Tally of duplicate transactions intercepted by idempotent hashing.
* **C. Live Sales Event Feed:**
  * Event cards with badges: `⚡ Real API Order`, `🌐 Webhook Order`, or `🤖 Simulated Event`.
  * Expandable JSON payload viewer for deep packet inspection.
* **D. WooCommerce Webhook Security Auditor (Practical 1):**
  * Audits incoming `X-WC-Webhook-Signature` HMAC-SHA256 headers against the configured secret key.

---

## 5. Primary Navigation 3: Sales Analytics & Data Engineering Workbench (9 Tabs)

### Tab 1: Data Explorer (EDA & Automated Cleaning)
* **Syllabus Practicals:** 3 & 4 | **Source:** `processing/cleaning.py`
* **Features:** Data source switcher (1,500 production records, edge-case test set, custom CSV upload), statistical 5-number distributions, duplicate detection, missing cell breakdown, automated cleaning (IQR outlier capping, median/mode imputation, deduplication), and one-click cleaned CSV export.

### Tab 2: ETL Pipeline (Extract, Transform, Validate, Load)
* **Syllabus Practical:** 7 | **Source:** `processing/etl.py`
* **Features:** 4-stage idempotent pipeline ingesting raw data into `staging_sales`, standardizing datetimes/types, validating constraints, and upserting into 3NF models. Contains run lineage tables, duration tracking, and staging quarantine inspection for poison pills.

### Tab 3: SQL Workbench (Window Functions & Execution Plans)
* **Syllabus Practicals:** 8 & 9 | **Source:** `analytics/sql_analytics.py`
* **Features:** Interactive SQL query editor with presets for `DENSE_RANK()` by category, `LAG()`/`LEAD()` consecutive customer spend deltas, and cumulative monthly sums. Features `EXPLAIN` query execution plan visualizer (Sequential vs Index scan analysis) and index inspection (`pg_indexes`).

### Tab 4: Data Lake & Batch (DuckDB, Polars & Delta Lake)
* **Syllabus Practicals:** 11 & 12 | **Source:** `batch/batch_processor.py`, `delta_lake/delta_manager.py`
* **Sub-Tab 1 (Columnar Batch Benchmark):** Vectorized benchmark comparing DuckDB on CSV vs DuckDB on Snappy Parquet vs Polars parallel engine, highlighting execution latency (ms) and compression ratios.
* **Sub-Tab 2 (Delta Lake ACID & Time Travel):** Lakehouse status KPIs, 4 transaction actions (`Reset / Init Table`, `Append New Batch`, `Test Schema Guard`, `Evolve Schema`), live table viewer, commit history log (`_delta_log`), and interactive version slider for historical snapshot inspection.

### Tab 5: Data Quality Suite (Great Expectations)
* **Syllabus Practical:** 14 | **Source:** `validation/quality_checker.py`
* **Features:** Evaluates non-null, uniqueness, categorical set membership, and range constraints. Computes overall dataset quality percentage score, persists test results to `data_quality_results`, and generates downloadable standalone HTML reports.

### Tab 6: Workflow Scheduler (Apache Airflow Orchestration)
* **Syllabus Practical:** 13 | **Source:** `workflows/dags/retailpulse_batch_etl.py`
* **Features:** Visualizes the 7-stage DAG dependency graph (`check_ingestion` $\rightarrow$ `extract` $\rightarrow$ `validate` $\rightarrow$ `transform` $\rightarrow$ `load` $\rightarrow$ `build_olap` $\rightarrow$ `summary`), offers manual execution triggers, and displays task execution logs with retry counters.

### Tab 7: Database Management (PostgreSQL & MongoDB NoSQL)
* **Syllabus Practicals:** 5 & 6 | **Source:** `database/models.py`, `database/nosql.py`
* **Features:** Relational 3NF Schema Browser (inspects `customers`, `products`, `orders`, `order_items`, `staging_sales` tables, primary/foreign keys, and row counts) and MongoDB polymorphic NoSQL store (customer feedback documents, JSON projection queries, and index inspection).

### Tab 8: API Tools (REST API, Pydantic & Webhooks)
* **Syllabus Practical:** 1 | **Source:** `api/routes.py`, `api/schemas.py`
* **Features:** Interactive REST API console for testing `GET /health` and `POST /api/orders`, Pydantic schema validation error simulation (HTTP 400 rejection on invalid data), and WooCommerce webhook HMAC-SHA256 signature generator.

### Tab 9: Search & Observability (Elasticsearch)
* **Syllabus Practical:** 15 | **Source:** `search/search_engine.py`
* **Features:** Multi-field inverted index search across orders, products, and pipeline errors with automated fallback to in-memory search if external Elasticsearch cluster is offline.

---

## 6. Step-by-Step Live Demonstration Script for Examiners

1. **Step 1: Executive Overview (1 Minute):** Open Executive Dashboard. Point out the 5 KPI cards. Select "Last 30 Days" and "Electronics" in the filter bar to show instant dynamic recalculation. Expand the "Data Cube Explorer" drawer, run a `CUBE` query, and point out calculated regional subtotals.
2. **Step 2: Real-Time Stream Ingestion (1 Minute):** Navigate to Live Sales Stream. Click "Start Simulator Worker". Show Throughput and Latency telemetry cards updating. Point out live order feed with "Simulated Event" badges. Click "Stop Simulator Worker".
3. **Step 3: Data Explorer & Automated Cleaning (1 Minute):** Go to Sales Analytics $\rightarrow$ Data Explorer. Select `test_retail_batch.csv`. Point out detected duplicate rows and missing values. Click "Execute Automated Cleaning" and show that duplicates were removed and outliers capped.
4. **Step 4: Vectorized Benchmarks & Delta Lake ACID (1 Minute):** Go to Sales Analytics $\rightarrow$ Data Lake & Batch. Click "Run Comparative Engine Benchmark" to show DuckDB and Polars sub-10ms aggregations. Switch to the "Delta Lake ACID" tab, click "Append New Batch" to create a commit, and use the Time Travel slider to inspect an earlier commit snapshot.
5. **Step 5: API Ingestion & Data Quality (1 Minute):** Go to Sales Analytics $\rightarrow$ API Tools. Click "Test Order Ingestion (POST /api/orders)" to dispatch a live transaction to the Flask backend. Finally, go to Data Quality and click "Execute Great Expectations Test Suite" to show automated assertion passes and download the HTML report.

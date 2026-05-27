# E-commerce Event-Driven Data Pipeline (Delta Lake)

An industrial-grade, event-driven ETL/ELT data pipeline built on Databricks to process multi-source retail data. The system ingests raw transactional data, runs schema and business validation, applies multi-layered data enrichments, tracks historical changes using SCD Type 2 logic, and outputs analytics-ready tables for business intelligence.

---

## 🏗️ Architecture Overview

The pipeline follows a modular Medallion-adjacent architecture, orchestrated as an event-driven workflow in Databricks Workflows (`ecomm_event_driven_dbx_pipeline`).



[ Raw Files / Volume ] ──> [ Parallel Staging Loads ] ──> [ Data Validation ] ──> [ Data Enrichment ] ──> [ SCD2 Merge ]



1\. **Ingestion Layer (Databricks Volumes):** Raw files arrive partitioned by domain alongside an event-driven JSON `trigger_file`.
2\. **Staging Layer (Bronze):** Schema-validated, parallel loading of ingestion files into staging Delta tables.
3\. **Quality & Validation Layer (Silver):** Comprehensive data integrity checks and severity-based validation scoring.
4\. **Enrichment & Metrics Layer (Gold):** Business logic processing (CLV, segmentation, seasonal trends).
5\. **Historical Layer (SCD Type 2):** High-integrity historical tracking for shifting dimensions.

---

## 🛠️ Tech Stack & Tools

* **Platform:** Databricks (Single Node Compute: `n2-highmem-4`, Runtime: `16.4.24`)
* **Core Engine:** PySpark & Delta Lake
* **Storage Governance:** Unity Catalog & Databricks Volumes
* **Orchestration:** Databricks Workflows (DAG Execution)
* **Version Control:** Git (GitHub integration via Databricks Repos)

---

## 📂 Project Structure

```text
delta-lake-retail-pipeline/
├── .gitignore
├── README.md
├── Images
├── projectStructure.txt
└── Notebooks/
    ├── 01_orders_stage_load
    ├── 02_customers_stage_load
    ├── 03_products_stage_load
    ├── 04_inventory_stage_load
    ├── 05_shipping_stage_load
    ├── 06_data_validation
    ├── 07_data_enrichment
    └── 08_final_scd2_merge

```

🗄️ Database & Catalog Schema
-----------------------------

Managed within Unity Catalog under`incremental_load_catalog.default.*` .

### 📂 Databricks Volumes

-   Path:`/Volumes/incremental_load_catalog/default/incremental_load/`

-   Ingestion Sub-directories: `Shipping_data`, `customers_data`, `inventory_data`, `orders_data`, `products_data`,`trigger_file`

### 📊 Tables (17 Total)

| **Table Type** | **Table Name** | **Description** |
| --- | --- | --- |
| **Staging (Bronze)** | `customers_stage`, `inventory_stage`, `orders_stage`, `products_stage`,`shipping_stage` | Raw schema-validated data extracted from landing volumes. |
| **Core / Target (Silver)** | `customers_target`, `products_target`, `orders_target`,`enriched_orders` | Validated target data; tracks operational history. |
| **Analytics (Gold)** | `analytics_summary`, `category_analysis`, `customer_analytics`, `product_analytics`, `seasonal_analysis`,`segment_analysis` | Summarized KPIs, customer segmentation (CLV), and trend tables. |
| **System / Audit** | `processing_log`,`validation_results` | Contains batch execution metadata and severity-based validation scores. |

🔄 Workflow & Pipeline DAG
--------------------------

The workflow is automatically triggered by file arrival ( `trigger_file`). The pipeline total execution duration spans roughly **2m 57s** across 8 tasks structured sequentially and concurrently:

```
[ customer_stage_load ] ──┐
[ inventory_stage_load ] ─┤
[ orders_stage_load ] ────┼─> [ data_validation ] ──> [ data_enrichment ] ──> [ final_scd2_merge ]
[ product_stage_load ] ───┤
[ shipping_stage_load ] ──┘

```

### Task Metrics Summary

-   **Parallel Stage Loading:** Extracts raw source chunks simultaneously (~52s - 58s per notebook).

-   **Data Validation:** Aggregates checks and logs constraints (~35s).

-   **Data Enrichment:** Calculates metrics, cohorts, and performance matrices (~34s).

-   **SCD2 Merge Operation:** Merges updates safely using automated upsert expressions (~48s).

🚀Key Implementation Details
----------------------------

### 1\. Advanced Quality Assurance

Cross-references metrics across staging domains before promotional merging. Builds a decoupled validation ledger ( `validation_results`) capturing non-blocking and blocking discrepancies using severity scoring.

### 2\. SCD Type 2 Trackers

Monitors historical mutations in the target layer (eg, `customers_target`). Implements transactional timestamps ( `effective_date`, `expiry_date`, and `is_current`flags) utilizing optimized Delta Lake `MERGE INTO`operations.

### 3\. Archive & Housekeeping

Post-execution, source raw datasets are isolated, archived from active ingestion tracks, and logged within historical metadata tables to guarantee exactly-once processing constraints.

⚙️ Configuration & Deployment
-----------------------------

1.  **Clone Repo into Databricks Workspaces:** Configure your Git provider credentials under User Settings and clone using the Databricks Repos UI.

2.  **Setup Unity Catalog Storage:** Ensure the catalog `incremental_load_catalog`and schema `default`exist. Create the `incremental_load`volume and verify raw structured directories match the schema specification.

3.  **Configure Workflow Task DAG:** Create a new Databricks Job pointing to the relative repository paths for notebooks `01`through `08`. Attach to a compatible Spark Cluster runtime.

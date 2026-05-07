# Retail Sales Data Warehouse – ETL & Data Quality Validation

## Table of Contents

1. [Project Overview](#project-overview)
2. [Problem Statement](#problem-statement)
3. [Tech Stack](#tech-stack)
4. [Architecture](#architecture)
5. [Data Sources](#data-sources)
6. [File Naming Convention](#file-naming-convention)
7. [Pipeline Stages](#pipeline-stages)
   - [Stage 0 – Pipeline Orchestrator & Archival](#stage-0--pipeline-orchestrator--archival)
   - [Stage 1 – Bronze Ingestion (Raw)](#stage-1--bronze-ingestion-raw)
   - [Stage 2 – Silver Cleaning & Transformation](#stage-2--silver-cleaning--transformation)
   - [Stage 3 – Gold Modelling (Dimensional)](#stage-3--gold-modelling-dimensional)
   - [Stage 4 – Incremental Customer SCD Type 2](#stage-4--incremental-customer-scd-type-2)
   - [Stage 5 – Incremental Sales Load](#stage-5--incremental-sales-load)
   - [Stage 6 – Final Validations](#stage-6--final-validations)
8. [Data Model](#data-model)
   - [Dimension Tables](#dimension-tables)
   - [Fact Table](#fact-table)
9. [Archival Process](#archival-process)
10. [Data Quality & Validation Strategy](#data-quality--validation-strategy)
11. [Project Structure](#project-structure)
12. [Setup & Execution](#setup--execution)
13. [Key Design Decisions](#key-design-decisions)
14. [Future Enhancements](#future-enhancements)

---

## Project Overview

This project implements an **end-to-end ETL pipeline** for a retail chain's **Sales Data Warehouse** using **Databricks notebooks** on **AWS S3**. The pipeline follows the **Medallion Architecture** (Bronze → Silver → Gold) to ingest, clean, transform, and model data for analytics-ready consumption. It also includes a robust **archival mechanism** and comprehensive **data quality validations** to ensure pipeline reliability.

---

## Problem Statement

A retail chain needs a **Sales Data Warehouse** consisting of **Customers**, **Products**, **Stores**, and **Daily Sales** facts. The pipeline must:

- Handle files arriving in an **SFTP zone** with timestamp-based naming conventions.
- Implement **incremental loading** — when a new file arrives, the previous file is archived.
- Maintain **SFTP**, **Raw**, and **Processed** zones along with an **Archival** location.
- Apply **archival logic consistently** across all zones.
- Support **SCD Type 2** for slowly changing customer dimensions.
- **Validate end-to-end** that data is correctly loaded from source OLTP to the DWH.
- **Detect and report** any failures in the archival and loading process.

---

## Tech Stack

| Component         | Technology                                  |
|-------------------|---------------------------------------------|
| **Cloud Platform**| AWS (S3 for storage)                        |
| **Compute**       | Databricks (Spark-based notebooks)          |
| **Language**      | Python (PySpark), SQL                       |
| **Storage**       | S3 Bucket (`salessprintt9`)                 |
| **Data Format**   | CSV (source), Delta/Parquet (warehouse)     |
| **Orchestration** | Databricks Workflows (Jobs + Task Values)   |
| **Database**      | `retail_dw` (Databricks managed database)   |

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                        S3 Bucket (salessprintt9)                 │
│                                                                  │
│  sftp/                                                           │
│  ├── customers/   ← CSV files with timestamps                   │
│  ├── products/                                                   │
│  ├── stores/                                                     │
│  └── sales/                                                      │
│                                                                  │
│  archive/                                                        │
│  ├── customers/   ← Older files moved here                      │
│  ├── products/                                                   │
│  ├── stores/                                                     │
│  └── sales/                                                      │
└──────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────┐    ┌──────────────────┐    ┌──────────────────┐
│   BRONZE LAYER  │───▶│   SILVER LAYER   │───▶│    GOLD LAYER    │
│   (Raw Tables)  │    │ (Cleaned Tables) │    │ (Star Schema)    │
│                 │    │                  │    │                  │
│ bronze_customers│    │ silver_customers │    │ dim_customers    │
│ bronze_products │    │ silver_products  │    │ dim_products     │
│ bronze_stores   │    │ silver_stores    │    │ dim_stores       │
│ bronze_sales    │    │ silver_sales     │    │ fact_sales       │
└─────────────────┘    └──────────────────┘    └──────────────────┘
                                                       │
                                                       ▼
                                              ┌──────────────────┐
                                              │   INCREMENTAL    │
                                              │     LOADS        │
                                              │                  │
                                              │ SCD Type 2       │
                                              │ (Customers)      │
                                              │                  │
                                              │ Append-only      │
                                              │ (Sales)          │
                                              └──────────────────┘
```

---

## Data Sources

The pipeline ingests **four CSV datasets** from the SFTP zone:

| Entity      | Description                                                         |
|-------------|---------------------------------------------------------------------|
| **Customers** | Customer details — ID, Name, Email, City, Address, LastUpdated    |
| **Products**  | Product catalog — ID, Name, Category, UnitPrice                   |
| **Stores**    | Store locations — ID, StoreName, Region                           |
| **Sales**     | Daily transactions — TransactionID, CustomerID, ProductID, StoreID, Quantity, TxnDate |

---

## File Naming Convention

Source files follow this naming pattern:

```
<filename>_DDMMYYYYHHMMSS.csv
```

- **DD** – Day (2 digits)
- **MM** – Month (2 digits)
- **YYYY** – Year (4 digits)
- **HH** – Hour (2 digits)
- **MM** – Minutes (2 digits)
- **SS** – Seconds (2 digits)

**Example:** `customers_07052026143022.csv`

The **14-digit timestamp** embedded in each filename is extracted via regex (`r'_(\d{14})\.csv$'`) and parsed using the format `%d%m%Y%H%M%S` to determine file recency.

---

## Pipeline Stages

### Stage 0 – Pipeline Orchestrator & Archival
**File:** `00_pipeline_orchestrator.py`

**Purpose:** Identifies the latest file for each entity and archives older files.

**How it works:**
1. Scans each SFTP folder (`customers/`, `products/`, `stores/`, `sales/`) in the S3 bucket.
2. Extracts the embedded timestamp from every CSV filename.
3. Sorts files by timestamp in **descending order** — the most recent file is the **latest**.
4. Moves all **older files** to the corresponding `archive/` folder using `dbutils.fs.cp()` followed by `dbutils.fs.rm()`.
5. Passes the **latest file paths** to downstream tasks via `dbutils.jobs.taskValues.set()`.

**Key function:**
- `extract_timestamp(filename)` – Parses the 14-digit timestamp from a filename.
- `get_latest_file(path)` – Returns the latest file and a list of old files to archive.

---

### Stage 1 – Bronze Ingestion (Raw)
**File:** `01_bronze_ingestion.py`

**Purpose:** Loads raw CSV data into Bronze tables without any transformation.

**How it works:**
1. Retrieves latest file paths from Task 1 via `dbutils.jobs.taskValues.get()`.
2. Creates the `retail_dw` database if it doesn't exist.
3. Reads each CSV file with headers into a Spark DataFrame.
4. Writes each DataFrame as a managed table with **overwrite** mode:
   - `bronze_customers`
   - `bronze_products`
   - `bronze_stores`
   - `bronze_sales`

---

### Stage 2 – Silver Cleaning & Transformation
**File:** `02_silver_Cleaned.sql`

**Purpose:** Cleanses, deduplicates, type-casts, and validates raw data.

**Transformations applied:**

| Table               | Cleaning Logic                                                                 |
|---------------------|--------------------------------------------------------------------------------|
| `silver_customers`  | `DISTINCT`, `CAST(CustomerID AS INT)`, `INITCAP(CustomerName)`, `LOWER(Email)`, `TRIM` on text fields, `TO_DATE(LastUpdated, 'dd-MM-yyyy')` |
| `silver_products`   | `DISTINCT`, `CAST(ProductID AS INT)`, `TRIM` on text fields, `CAST(UnitPrice AS DECIMAL(10,2))`, **filter out** `UnitPrice <= 0` |
| `silver_stores`     | `DISTINCT`, `CAST(StoreID AS INT)`, `INITCAP(StoreName)`, `COALESCE(NULLIF(Region, ''), 'Unknown')` for missing regions |
| `silver_sales`      | `DISTINCT`, all IDs cast to `INT`, `TO_DATE(TxnDate, 'dd-MM-yyyy')`, **filter out** `Quantity <= 0` |

**Inline validations at Silver stage:**
- Duplicate `CustomerID` detection
- Same `CustomerID` mapped to different names/emails
- Null checks on all customer fields
- Invalid product prices (`UnitPrice <= 0` or `NULL`)
- Missing store regions
- Duplicate `TransactionID` detection
- Invalid quantities (`Quantity <= 0` or `NULL`)
- Referential integrity check — sales with `CustomerID` not present in the customers table
- Row counts for each Silver table

---

### Stage 3 – Gold Modelling (Dimensional)
**File:** `03_gold_modelling.py`

**Purpose:** Builds a **Star Schema** with dimension and fact tables.

**Tables created:**

| Table            | Schema                                                                                   |
|------------------|------------------------------------------------------------------------------------------|
| `dim_customers`  | `CustomerSK` (surrogate key), `CustomerID`, `CustomerName`, `Email`, `City`, `Address`, `StartDate`, `EndDate`, `IsActive` |
| `dim_products`   | `ProductSK`, `ProductID`, `ProductName`, `Category`, `UnitPrice`, `EffectiveDate`        |
| `dim_stores`     | `StoreSK`, `StoreID`, `StoreName`, `Region`                                              |
| `fact_sales`     | `SalesSK`, `TransactionID`, `CustomerSK`, `ProductSK`, `StoreSK`, `Quantity`, `Amount` (calculated: `Quantity × UnitPrice`), `TxnDate` |

**Key design details:**
- Surrogate keys (`*SK`) are generated using `ROW_NUMBER() OVER (ORDER BY ...)`.
- `dim_customers` is designed for **SCD Type 2** with `StartDate`, `EndDate`, and `IsActive` columns.
- `fact_sales` joins with the **latest active** customer record using a windowed subquery (`ROW_NUMBER() OVER (PARTITION BY CustomerID ORDER BY CustomerSK DESC)`).
- `Amount` is a **derived measure** calculated as `Quantity × UnitPrice`.

---

### Stage 4 – Incremental Customer SCD Type 2
**File:** `04_incremental_customers_scd.py`

**Purpose:** Implements **Slowly Changing Dimension Type 2** for the customer dimension on subsequent runs.

**How it works:**
1. **Detect changes** — Creates a temp view `changed_customers` by comparing `silver_customers` against `dim_customers` where `IsActive = 1`, looking for changes in `City` or `Address`.
2. **Expire old records** — Sets `EndDate = CURRENT_DATE()` and `IsActive = 0` for customers found in `changed_customers`.
3. **Insert new/changed rows** — Inserts rows for:
   - **New customers** (exist in Silver but not in the active dimension)
   - **Changed customers** (detected in Step 1)
   - Assigns new surrogate keys starting from `MAX(CustomerSK) + 1`.

---

### Stage 5 – Incremental Sales Load
**File:** `05_incremental_sales_load.py`

**Purpose:** Appends only **new transactions** to `fact_sales` (avoids duplicates).

**How it works:**
1. Joins `silver_sales` with the latest active customer, products, and stores dimensions.
2. Calculates `Amount = Quantity × UnitPrice`.
3. Filters out any `TransactionID` that **already exists** in `fact_sales` using `WHERE s.TransactionID NOT IN (SELECT TransactionID FROM fact_sales)`.
4. Generates new surrogate keys starting from `MAX(SalesSK) + 1`.

---

### Stage 6 – Final Validations
**File:** `06_validations.py`

**Purpose:** Post-load validation checks on the Gold layer.

**Checks performed:**
1. **Duplicate TransactionID in fact_sales** — Ensures no duplicate transactions exist after incremental load.
2. **Active record consistency in dim_customers** — Validates that all active records (`IsActive = 1`) have `EndDate = '9999-12-31'`. Any mismatch indicates an SCD processing error.

---

## Data Model

### Dimension Tables

#### dim_customers (SCD Type 2)
| Column       | Type        | Description                          |
|-------------|-------------|--------------------------------------|
| CustomerSK  | INT         | Surrogate Key (auto-generated)       |
| CustomerID  | INT         | Natural/Business Key                 |
| CustomerName| STRING      | Cleaned customer name (InitCap)      |
| Email       | STRING      | Cleaned email (lowercase)            |
| City        | STRING      | Customer city                        |
| Address     | STRING      | Customer address                     |
| StartDate   | DATE        | Row effective start date             |
| EndDate     | DATE        | Row effective end date (9999-12-31 if active) |
| IsActive    | INT         | 1 = current record, 0 = expired     |

#### dim_products
| Column       | Type          | Description              |
|-------------|---------------|--------------------------|
| ProductSK   | INT           | Surrogate Key            |
| ProductID   | INT           | Natural Key              |
| ProductName | STRING        | Product name             |
| Category    | STRING        | Product category         |
| UnitPrice   | DECIMAL(10,2) | Unit price (> 0)         |
| EffectiveDate| DATE         | Date the record was loaded|

#### dim_stores
| Column    | Type   | Description                        |
|-----------|--------|------------------------------------|
| StoreSK   | INT    | Surrogate Key                      |
| StoreID   | INT    | Natural Key                        |
| StoreName | STRING | Store name (InitCap)               |
| Region    | STRING | Region (defaults to 'Unknown')     |

### Fact Table

#### fact_sales
| Column        | Type          | Description                           |
|--------------|---------------|---------------------------------------|
| SalesSK      | INT           | Surrogate Key                         |
| TransactionID| INT           | Natural transaction identifier        |
| CustomerSK   | INT           | FK → dim_customers.CustomerSK         |
| ProductSK    | INT           | FK → dim_products.ProductSK           |
| StoreSK      | INT           | FK → dim_stores.StoreSK               |
| Quantity     | INT           | Number of units sold                  |
| Amount       | DECIMAL       | Derived: Quantity × UnitPrice         |
| TxnDate      | DATE          | Transaction date                      |

---

## Archival Process

The archival logic ensures that **only the latest file is retained** in each SFTP folder while older files are safely moved to the archive zone.

| Requirement                                                  | How It's Handled                                                      |
|--------------------------------------------------------------|-----------------------------------------------------------------------|
| Previous file is archived when a new file arrives            | Orchestrator compares timestamps; older files are copied to `archive/` and removed from `sftp/` |
| Only the latest file is present in each active zone          | `get_latest_file()` sorts by extracted timestamp descending; only the top file is kept |
| Archival logic is consistent across all zones                | The same `get_latest_file()` + archive loop runs for all 4 entities (customers, products, stores, sales) |
| Files are identified using the date-time naming convention   | Regex `r'_(\d{14})\.csv$'` extracts the timestamp; parsed via `datetime.strptime` |
| Failures in the archival process are detected and reported   | If no valid timestamped CSV files are found, an `Exception` is raised with a descriptive message |

---

## Data Quality & Validation Strategy

Validations are embedded across **multiple pipeline stages** to catch issues early:

### Bronze → Silver Validations (Stage 2)
| Check                                | Query/Logic                                        | Purpose                                      |
|--------------------------------------|----------------------------------------------------|----------------------------------------------|
| Duplicate `CustomerID`               | `GROUP BY CustomerID HAVING COUNT(*) > 1`          | Detect duplicate customer records            |
| Conflicting customer attributes      | `COUNT(DISTINCT CustomerName)` / `Email` per ID    | Same ID mapped to different names/emails     |
| Null fields in customers             | `WHERE ... IS NULL`                                | Ensure no critical fields are missing        |
| Invalid product prices               | `WHERE UnitPrice <= 0 OR UnitPrice IS NULL`        | Filter out bad pricing data                  |
| Missing store regions                | `WHERE Region IS NULL OR TRIM(Region) = ''`        | Replaced with 'Unknown' during cleaning      |
| Duplicate `TransactionID`            | `GROUP BY TransactionID HAVING COUNT(*) > 1`       | Detect duplicate transactions                |
| Invalid quantities                   | `WHERE Quantity <= 0 OR Quantity IS NULL`           | Filter out zero/negative quantities          |
| Referential integrity (Sales → Customers) | `LEFT JOIN` where `CustomerID IS NULL`         | Orphaned sales records detection             |

### Gold Layer Validations (Stage 6)
| Check                                | Query/Logic                                        | Purpose                                      |
|--------------------------------------|----------------------------------------------------|----------------------------------------------|
| Duplicate `TransactionID` in fact    | `GROUP BY TransactionID HAVING COUNT(*) > 1`       | Ensure incremental load didn't create dupes  |
| Active record SCD consistency        | `IsActive = 1 AND EndDate <> '9999-12-31'`         | Validate SCD Type 2 processing correctness   |

### Row Count Validations
Row counts are checked at both Silver and Gold stages to confirm data completeness across all four entities.

---

## Project Structure

```
ETL/
├── 00_pipeline_orchestrator.py    # Stage 0: File selection, archival, path passing
├── 01_bronze_ingestion.py         # Stage 1: Raw CSV → Bronze tables
├── 02_silver_Cleaned.sql          # Stage 2: Bronze → Silver (clean + validate)
├── 03_gold_modelling.py           # Stage 3: Silver → Gold (Star Schema)
├── 04_incremental_customers_scd.py # Stage 4: SCD Type 2 for customers
├── 05_incremental_sales_load.py   # Stage 5: Incremental fact_sales append
├── 06_validations.py              # Stage 6: Post-load validation checks
└── README.md                      # This file
```

---

## Setup & Execution

### Prerequisites

- **Databricks Workspace** with access to a cluster (Spark-enabled).
- **AWS S3 Bucket** (`salessprintt9`) with the following structure:
  ```
  s3://salessprintt9/
  ├── sftp/
  │   ├── customers/
  │   ├── products/
  │   ├── stores/
  │   └── sales/
  └── archive/
      ├── customers/
      ├── products/
      ├── stores/
      └── sales/
  ```
- Source CSV files uploaded to the respective `sftp/` subfolders following the `<filename>_DDMMYYYYHHMMSS.csv` naming convention.

### Execution Order

The notebooks must be executed **sequentially** as a Databricks Workflow (Job) with task dependencies:

```
Task 1: 00_pipeline_orchestrator.py
   └──▶ Task 2: 01_bronze_ingestion.py
           └──▶ Task 3: 02_silver_Cleaned.sql
                   └──▶ Task 4: 03_gold_modelling.py     (First run only)
                           └──▶ Task 5: 04_incremental_customers_scd.py  (Subsequent runs)
                                   └──▶ Task 6: 05_incremental_sales_load.py  (Subsequent runs)
                                           └──▶ Task 7: 06_validations.py
```

> **Note:** On the **first run**, Stage 3 (Gold Modelling) creates the dimension and fact tables from scratch. On **subsequent runs**, Stages 4 and 5 handle incremental loads (SCD + new sales).

### Running in Databricks

1. Import all `.py` and `.sql` files as Databricks notebooks.
2. Create a **Databricks Workflow (Job)** with tasks chained in the order above.
3. Configure task dependencies so that each task depends on the successful completion of the previous one.
4. Set Task 1's key name to `Task1` so that downstream notebooks can retrieve file paths via `dbutils.jobs.taskValues.get(taskKey="Task1", ...)`.
5. Schedule the job or trigger it manually when new files land in the SFTP zone.

---

## Key Design Decisions

| Decision                            | Rationale                                                                 |
|-------------------------------------|---------------------------------------------------------------------------|
| **Medallion Architecture**          | Separates raw, cleaned, and modeled layers for data quality and traceability |
| **SCD Type 2 for Customers**        | Tracks historical changes in customer `City` and `Address` over time     |
| **Surrogate Keys via ROW_NUMBER()** | Generates unique integer keys without needing a sequence/identity column |
| **Overwrite mode for Bronze**       | Each run gets a fresh snapshot; history is maintained in Silver/Gold      |
| **Archive via copy + delete**       | Ensures file safety — archive is confirmed before source is removed      |
| **Task Values for inter-task communication** | Leverages Databricks native mechanism to pass file paths between workflow tasks |
| **Derived Amount in fact_sales**    | Calculated at load time (`Quantity × UnitPrice`) for consistent analytics |

---

## Future Enhancements

- **Automated alerting** — Integrate with email/Slack for pipeline failure notifications.
- **Delta Lake** — Convert tables to Delta format for ACID transactions, time travel, and schema enforcement.
- **Data quality framework** — Implement Great Expectations or Databricks expectations for declarative validation rules.
- **CI/CD pipeline** — Automate notebook deployment via GitHub Actions or Azure DevOps.
- **Unit testing** — Add pytest-based unit tests for the `extract_timestamp()` and archival logic.
- **Logging & monitoring** — Add structured logging and integrate with Databricks monitoring dashboards.
- **Partitioning** — Partition `fact_sales` by `TxnDate` for improved query performance.
- **SCD for Products** — Track product price changes over time using SCD Type 2.

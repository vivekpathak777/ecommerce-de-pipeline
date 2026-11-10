# 🛒 EcomKart | End-to-End Data Engineering & Analytics Pipeline

### 🚀 Databricks • PySpark • Delta Lake • dbt • Apache Airflow • Power BI

An end-to-end e-commerce data engineering project that transforms raw CSV datasets into analytics-ready business insights using a **Medallion Architecture (Bronze → Silver → Gold)**.

The project demonstrates batch ingestion, distributed data processing, SQL-based transformations, data quality testing, workflow orchestration, and business intelligence reporting.

**🔗 [GitHub Repository](https://github.com/vivekpathak777/ecommerce-de-pipeline)**

---

## 🏗️ 1. Project Architecture

![EcomKart Data Engineering Architecture](assets/architecture.png)

### 🔄 Data Flow

```text
🛒 Synthetic E-Commerce Data
          |
          v
📁 CSV Files (Customers, Products, Orders, Payments)
          |
          v
☁️ Databricks Unity Catalog Volume
          |
          v
🥉 BRONZE LAYER
   PySpark Raw Data Ingestion
   Delta Lake Tables
          |
          v
🥈 SILVER LAYER
   Data Cleaning & Validation
   Type Casting & Deduplication
          |
          v
🥇 GOLD LAYER
   dbt SQL Models
   Fact Tables & Aggregations
   Data Quality Tests
          |
          v
📊 POWER BI
   Interactive Sales Analytics Dashboard
```

**⚙️ Orchestration:** Apache Airflow triggers the Databricks Bronze/Silver job, waits for completion, and then triggers the dbt Cloud Gold transformation job.

---

## 🎯 2. Business Problem

E-commerce businesses generate data from customers, orders, products, and payments.

Raw data alone is not suitable for reliable business reporting. It must be ingested, cleaned, validated, transformed, and made available for analysis.

### 💡 Our Solution

EcomKart implements a lakehouse-based data pipeline that:

- 📥 Ingests raw e-commerce CSV files.
- 🧹 Cleans and standardizes data using PySpark.
- 🗄️ Stores data in Delta Lake tables.
- 🔗 Creates analytical models using dbt.
- 🧪 Validates data quality.
- ⚙️ Orchestrates processing using Apache Airflow.
- 📊 Visualizes business performance in Power BI.

---

## 🛠️ 3. Technology Stack

| Technology | Purpose |
|---|---|
| 🐍 Python | Synthetic data generation and API orchestration |
| ⚡ PySpark | Distributed data transformations |
| 🧱 Databricks | Lakehouse processing platform |
| 🗄️ Delta Lake | Reliable table storage |
| 🔐 Unity Catalog | Catalog, schema and Volume management |
| 🔶 dbt | SQL modeling, lineage and data quality tests |
| 🌬️ Apache Airflow | Workflow orchestration |
| 📊 Power BI | Business intelligence dashboard |
| 🐙 GitHub | Version control and documentation |
| 💻 GitHub Codespaces | Cloud-based development environment |

---

## 📁 4. Dataset

Four synthetic datasets represent a simplified e-commerce system.

| Dataset | Description |
|---|---|
| 👥 `customers.csv` | Customer IDs, names, cities and emails |
| 📦 `products.csv` | Product details, categories and prices |
| 🛍️ `orders.csv` | Order details, quantities and statuses |
| 💳 `payments.csv` | Payment transactions, methods and statuses |

The source data was generated using `scripts/generate_data.py`.

Initial CSV files were uploaded manually to a Databricks Unity Catalog Volume.

---

## 🥉 5. Bronze Layer — Raw Data Ingestion

**Notebook:** `databricks/01_bronze_ingestion.py`

The Bronze layer reads CSV files from a Unity Catalog Volume and stores them as managed Delta tables.

### Key Operations

- Read raw CSV files using PySpark.
- Preserve source columns as strings.
- Create managed Delta tables.
- Verify ingested record counts.

**Bronze tables:**

```text
demo_ecommerce.ecomm_bronze.customers
demo_ecommerce.ecomm_bronze.products
demo_ecommerce.ecomm_bronze.orders
demo_ecommerce.ecomm_bronze.payments
```

The current implementation uses full-table overwrite rather than incremental ingestion.

---

## 🥈 6. Silver Layer — Data Cleaning

**Notebook:** `databricks/02_silver_transformation.py`

The Silver layer transforms raw Bronze records into cleaned, structured datasets.

### Transformations

- 🔢 Convert IDs and numeric fields to appropriate data types.
- 📅 Standardize date columns.
- 🧹 Trim text fields and normalize emails.
- 🚫 Filter invalid records.
- 🔍 Deduplicate records using business identifiers.
- ⏱️ Add processing timestamps.
- 🔗 Validate relationships across datasets.

Silver tables are rebuilt from the complete Bronze datasets.

---

## 🥇 7. Gold Layer — dbt Transformations

The Gold layer uses dbt SQL models to create business-ready datasets.

### 📌 `fct_sales`

Combines order, product, and payment information.

Includes:

- Order and customer identifiers
- Product and category
- Order quantity and price
- Order and payment statuses
- Calculated revenue

### 📌 `monthly_sales`

Aggregates sales into monthly business metrics.

Includes:

- Sales month
- Total orders
- Completed orders
- Total revenue

### 🧪 dbt Data Quality Tests

The project implements:

| Test | Purpose |
|---|---|
| `unique` | Verify unique identifiers |
| `not_null` | Validate required fields |
| `relationships` | Validate foreign-key relationships |

### 🔗 dbt Lineage

![dbt Model Lineage](assets/dbt_lineage.png)

The lineage graph shows dependencies between Silver sources, `fct_sales`, and `monthly_sales`.

---

## 🌬️ 8. Apache Airflow Orchestration

**DAG:** `dags/ecommerce_pipeline.py`

Airflow coordinates Databricks and dbt Cloud through their APIs.

### Execution Flow

```text
          ▶️ Manual DAG Trigger
                    |
                    v
          run_databricks_etl
                    |
           Databricks Job API
                    |
           Bronze → Silver
                    |
             Success Check
                    |
                    v
              run_dbt_gold
                    |
             dbt Cloud API
                    |
         Gold Models + dbt Tests
                    |
             Success Check
                    |
                    v
          ✅ Pipeline Completed
```

### ⚙️ Airflow Features

- Task dependencies
- API-based job triggering
- Remote job status polling
- Retry configuration
- Execution timeouts
- Task logs and monitoring

### 📸 Successful Airflow Execution

![Successful Airflow DAG](assets/airflow_success.png)

**Validated execution:** Both orchestration tasks completed successfully in approximately **3 minutes and 5 seconds**.

The DAG is manually triggered in the current implementation.

---

## 📊 9. Power BI — EcomKart Sales Dashboard

Power BI connects to the Databricks SQL Warehouse and imports the Gold tables.

### 📈 Dashboard Preview

![EcomKart Power BI Dashboard](assets/powerbi_dashboard.png)

### Business KPIs

| KPI | Observed Value |
|---|---:|
| 💰 Total Revenue | $11.09M |
| 🛍️ Total Orders | 500 |
| ✅ Completed Orders | 144 |
| 📊 Order Completion Rate | 28.80% |
| 💵 Average Order Value | 77.04K |

*Values reflect the demonstration dataset at the time of dashboard creation. Currency presentation follows the Power BI report configuration.*

### Dashboard Visualizations

- 📅 Monthly Revenue Trend
- 💳 Revenue by Payment Method
- 🏷️ Revenue by Product Category
- 🏆 Top 5 Products by Revenue
- 📌 Business KPI Cards

Power BI uses Import mode, so a dataset refresh is required to retrieve updated Databricks data.

---

## 📂 10. Repository Structure

```text
ecommerce-de-pipeline/
│
├── assets/
│   ├── architecture.png
│   ├── airflow_success.png
│   ├── dbt_lineage.png
│   └── powerbi_dashboard.png
│
├── dags/
│   ├── ecommerce_pipeline.py
│   └── hello_airflow.py
│
├── databricks/
│   ├── 01_bronze_ingestion.py
│   └── 02_silver_transformation.py
│
├── models/
│   ├── fct_sales.sql
│   ├── monthly_sales.sql
│   ├── schema.yml
│   └── sources.yml
│
├── data/
│   ├── customers.csv
│   ├── products.csv
│   ├── orders.csv
│   └── payments.csv
│
├── scripts/
│   └── generate_data.py
│
├── dbt_project.yml
├── .gitignore
└── README.md
```

The structure above highlights the main project files and intended screenshot assets. The repository may also contain standard dbt directories and configuration files.

---

## 🚀 11. How to Run the Project

### Prerequisites

- Databricks workspace and SQL Warehouse
- Unity Catalog access
- dbt project and execution environment
- Apache Airflow
- Python dependencies
- Power BI Desktop

### Execution Steps

1. Generate synthetic datasets using the Python generator.
2. Upload the CSV files to the configured Databricks Volume.
3. Configure Bronze and Silver Databricks notebooks.
4. Configure dbt models and data quality tests.
5. Create a Databricks job for Bronze and Silver processing.
6. Configure a dbt deployment job or equivalent dbt execution.
7. Set up Airflow connections for Databricks and dbt.
8. Update environment-specific account and job IDs in the Airflow DAG.
9. Trigger `ecommerce_end_to_end_pipeline`.
10. Verify successful execution and refresh Power BI.

**🔐 Security:** API tokens and credentials are managed outside source control. Never commit credentials, Airflow metadata databases or virtual environments to GitHub.

---

## 🔮 12. Future Improvements

- 🔄 Incremental Bronze ingestion using Delta MERGE
- ☁️ Automated file ingestion from object storage
- 📅 Scheduled Airflow executions
- 🔔 Failure notifications and alerting
- 📈 Incremental dbt models
- 🧪 Automated CI/CD checks
- 📊 Automated Power BI refresh
- 🏭 Production-grade deployment and monitoring

---

## 🏆 13. Project Outcome

Successfully designed, implemented and tested a manually triggered end-to-end batch data engineering pipeline.

The project demonstrates practical experience in:

**Data ingestion • PySpark transformations • Delta Lake • Medallion Architecture • dbt modeling • Data quality • REST API orchestration • Airflow • Power BI • GitHub**

### ⭐ Thank you for exploring EcomKart!

*From raw e-commerce data to business-ready analytics.*

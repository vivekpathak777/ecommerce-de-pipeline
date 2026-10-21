# End-to-End E-Commerce Data Engineering Pipeline

An end-to-end data engineering project built using **Databricks, Delta Lake, Apache Spark, dbt Cloud, Apache Airflow, Python, SQL, and GitHub**.

The project processes synthetic e-commerce datasets through a Medallion Architecture (Bronze, Silver, Gold), applies data quality tests, and orchestrates the complete workflow using Apache Airflow.

## Architecture

```text
Synthetic E-Commerce CSV Files
              |
              v
     Databricks Unity Catalog
          Volume Storage
              |
              v
      BRONZE LAYER
   Raw Data Ingestion
   PySpark + Delta Lake
              |
              v
      SILVER LAYER
   Cleaning & Transformation
   PySpark + Delta Lake
              |
              v
       GOLD LAYER
    dbt Cloud + SQL
   Fact Tables & Aggregations
              |
              v
     Analytics-Ready Tables
```

**Orchestration:** Apache Airflow triggers the Databricks Bronze/Silver job, waits for successful completion, then triggers the dbt Cloud Gold transformation job.

## Technology Stack

| Technology | Purpose |
|---|---|
| Python | Synthetic data generation and pipeline logic |
| Databricks | Distributed data processing |
| PySpark | Data ingestion and transformations |
| Delta Lake | Reliable lakehouse table storage |
| Unity Catalog | Data organization and governance |
| dbt Cloud | SQL transformations and data quality testing |
| Apache Airflow | Workflow orchestration and monitoring |
| GitHub | Version control and code management |
| GitHub Codespaces | Cloud development environment |

## Dataset

Four synthetic e-commerce datasets are used:

| Dataset | Description |
|---|---|
| `customers.csv` | Customer information |
| `products.csv` | Product information and prices |
| `orders.csv` | Customer orders |
| `payments.csv` | Payment information and status |

The datasets are generated for demonstration purposes and do not contain real customer information.

## Data Pipeline

### 1. Bronze Layer — Raw Data Ingestion

**Notebook:** `databricks/01_bronze_ingestion.py`

- Reads CSV files from a Databricks Unity Catalog Volume.
- Loads raw data using PySpark.
- Stores the data as Delta tables.
- Preserves the raw source structure for downstream processing.

### 2. Silver Layer — Data Cleaning

**Notebook:** `databricks/02_silver_transformation.py`

- Reads Bronze Delta tables.
- Applies cleaning and type conversions.
- Removes duplicate records.
- Produces structured Silver tables.

### 3. Gold Layer — dbt Transformations

**Models:** `models/`

**`fct_sales.sql`**

Combines order, product and payment data to create an analytics-ready sales fact table.

**`monthly_sales.sql`**

Aggregates order-level data into monthly metrics:

- Total orders
- Completed orders
- Total revenue

Gold tables are materialized in Databricks using dbt.

### 4. Data Quality Testing

dbt tests validate the transformed data, including:

- `unique` — Checks that identifiers are unique.
- `not_null` — Checks required fields.
- `relationships` — Checks referential integrity between related datasets.

The tests run as part of the dbt build process.

### 5. Apache Airflow Orchestration

**DAG:** `dags/ecommerce_pipeline.py`

The Airflow DAG contains two main tasks:

1. `run_databricks_etl` — Triggers the Databricks job that executes Bronze ingestion followed by Silver transformation.
2. `run_dbt_gold` — Triggers the dbt Cloud deployment job that builds Gold models and runs tests.

Airflow waits for successful completion before executing the next task.

The DAG supports manual triggering, retries, timeout handling, and task execution monitoring.

## Project Structure

```text
ecommerce-de-pipeline/
|
|-- dags/
|   |-- ecommerce_pipeline.py
|   |-- hello_airflow.py
|
|-- databricks/
|   |-- 01_bronze_ingestion.py
|   |-- 02_silver_transformation.py
|
|-- models/
|   |-- fct_sales.sql
|   |-- monthly_sales.sql
|   |-- schema.yml
|   |-- sources.yml
|
|-- data/
|   |-- customers.csv
|   |-- products.csv
|   |-- orders.csv
|   |-- payments.csv
|
|-- scripts/
|   |-- generate_data.py
|
|-- dbt_project.yml
|-- .gitignore
|-- README.md
```

## Execution

The pipeline was successfully executed manually through Apache Airflow.

**Execution sequence:**

```text
Airflow DAG Triggered
         |
         v
run_databricks_etl
         |
         v
Databricks Bronze Ingestion
         |
         v
Databricks Silver Transformation
         |
         v
run_dbt_gold
         |
         v
dbt Gold Models + Data Tests
         |
         v
    Pipeline Success
```

### Validation Results

- Databricks Bronze and Silver job completed successfully.
- dbt Gold transformations and data quality tests completed successfully.
- Airflow completed both orchestration tasks successfully.
- End-to-end DAG execution time: approximately 3 minutes and 5 seconds.

## Running the Project

This project requires access to a configured Databricks workspace, a dbt Cloud deployment job, and an Apache Airflow environment.

1. Generate or upload the synthetic datasets.
2. Configure the Databricks catalog, schemas, and Volume.
3. Configure the Databricks notebook job for Bronze and Silver.
4. Configure dbt Cloud to build Gold models in Databricks.
5. Set up Airflow with the required dependencies.
6. Create Airflow connections named `databricks_api` and `dbt_cloud_api`, with valid API credentials.
7. Update the job and account identifiers in `dags/ecommerce_pipeline.py` for your environment.
8. Start Airflow and manually trigger `ecommerce_end_to_end_pipeline`.

**Security:** API tokens and credentials must be stored securely in Airflow Connections, not committed to GitHub.

## Future Improvements

- Automatic pipeline scheduling
- Incremental ingestion and processing
- Monitoring and failure notifications
- CI/CD testing and deployment
- Parameterized configuration for multiple environments
- Improved scalability and production-ready infrastructure

## Project Outcome

Successfully built and executed a complete data engineering pipeline integrating Databricks, Delta Lake, dbt Cloud, and Apache Airflow, with GitHub-based version control.

The project demonstrates practical experience in data ingestion, lakehouse architecture, SQL transformations, data quality validation, API integration, and workflow orchestration.
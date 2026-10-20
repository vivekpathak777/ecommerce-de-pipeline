# Databricks notebook source
# Location of our uploaded raw CSV files
source_path = "/Volumes/demo_ecommerce/ecomm_bronze/raw_files/raw data/"

# Read customers.csv into a Spark DataFrame
df_customers = (
    spark.read
    .option("header", "true")  # First CSV row contains column names
    .csv(source_path + "customers.csv")
)

# Display the data and verify successful ingestion
display(df_customers.limit(10))

# Check the total number of customer records
print("Total customers:", df_customers.count())

# COMMAND ----------

# Save raw customer data as a managed Delta table
(
    df_customers.write
    .format("delta")  # Store data in Delta Lake format
    .mode("overwrite")  # Replace table data if it already exists
    .saveAsTable("demo_ecommerce.ecomm_bronze.customers")
)

print("Bronze customers Delta table created successfully!")

# COMMAND ----------

# Path where our raw CSV files are stored
source_path = "/Volumes/demo_ecommerce/ecomm_bronze/raw_files/raw data/"

# Files to ingest and their expected record counts
datasets = {
    "products": 50,
    "orders": 500,
    "payments": 500
}

# Process each dataset using the same ingestion logic
for table_name, expected_count in datasets.items():

    # Read raw CSV data (preserve all columns as strings)
    df = (
        spark.read
        .option("header", "true")
        .csv(f"{source_path}{table_name}.csv")
    )

    # Save the DataFrame as a managed Delta table
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(f"demo_ecommerce.ecomm_bronze.{table_name}")
    )

    # Verify the number of ingested records
    actual_count = df.count()
    print(f"{table_name}: {actual_count} rows")

    assert actual_count == expected_count, f"Unexpected row count for {table_name}"

print("All 3 Bronze Delta tables created successfully!")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN demo_ecommerce.ecomm_bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS demo_ecommerce.ecomm_silver;
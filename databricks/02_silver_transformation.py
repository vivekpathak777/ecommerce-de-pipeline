# Databricks notebook source
from pyspark.sql.functions import col, trim, lower, current_timestamp

# 1. Read raw customers from the Bronze Delta table
df = spark.table("demo_ecommerce.ecomm_bronze.customers")

# 2. Clean and transform the data
df_silver = (
    df
    .withColumn("customer_id", col("customer_id").cast("int"))
    .withColumn("name", trim(col("name")))
    .withColumn("city", trim(col("city")))
    .withColumn("email", lower(trim(col("email"))))

    # Remove records with missing customer IDs
    .filter(col("customer_id").isNotNull())

    # Keep one record per customer ID
    .dropDuplicates(["customer_id"])

    # Record when the data was processed
    .withColumn("processed_at", current_timestamp())
)

# 3. Save cleaned data as a Silver Delta table
(
    df_silver.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("demo_ecommerce.ecomm_silver.customers")
)

# 4. Verify results
display(df_silver.limit(10))
print("Silver customers:", df_silver.count())

# COMMAND ----------

from pyspark.sql.functions import col, trim, to_date, current_timestamp

# Read products from Bronze
df_products = spark.table("demo_ecommerce.ecomm_bronze.products")

# Clean data and convert column types
df_products_silver = (
    df_products
    .withColumn("product_id", col("product_id").cast("int"))
    .withColumn("product_name", trim(col("product_name")))
    .withColumn("category", trim(col("category")))
    .withColumn("price", col("price").cast("decimal(12,2)"))
    .filter(col("product_id").isNotNull() & (col("price") > 0))
    .dropDuplicates(["product_id"])
    .withColumn("processed_at", current_timestamp())
)

# Save cleaned products as a Delta table
df_products_silver.write.format("delta").mode("overwrite").saveAsTable(
    "demo_ecommerce.ecomm_silver.products"
)

print("Silver products:", df_products_silver.count())

# COMMAND ----------

# Read raw orders
df_orders = spark.table("demo_ecommerce.ecomm_bronze.orders")

# Convert IDs, quantity, and date into proper data types
df_orders_silver = (
    df_orders
    .withColumn("order_id", col("order_id").cast("int"))
    .withColumn("customer_id", col("customer_id").cast("int"))
    .withColumn("product_id", col("product_id").cast("int"))
    .withColumn("quantity", col("quantity").cast("int"))
    .withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))
    .withColumn("status", trim(col("status")))
    # Reject records with invalid essential fields
    .filter(
        col("order_id").isNotNull()
        & col("customer_id").isNotNull()
        & col("product_id").isNotNull()
        & (col("quantity") > 0)
        & col("order_date").isNotNull()
        & col("status").isin("Completed", "Pending", "Cancelled")
    )
    .dropDuplicates(["order_id"])
    .withColumn("processed_at", current_timestamp())
)

# Write to Silver
df_orders_silver.write.format("delta").mode("overwrite").saveAsTable(
    "demo_ecommerce.ecomm_silver.orders"
)

print("Silver orders:", df_orders_silver.count())

# COMMAND ----------

# Read raw payments
df_payments = spark.table("demo_ecommerce.ecomm_bronze.payments")

# Clean payment records
df_payments_silver = (
    df_payments
    .withColumn("payment_id", col("payment_id").cast("int"))
    .withColumn("order_id", col("order_id").cast("int"))
    .withColumn("payment_method", trim(col("payment_method")))
    .withColumn("payment_status", trim(col("payment_status")))
    .withColumn("payment_date", to_date(col("payment_date"), "yyyy-MM-dd"))
    .filter(
        col("payment_id").isNotNull()
        & col("order_id").isNotNull()
        & col("payment_date").isNotNull()
        & col("payment_status").isin("Success", "Pending", "Refunded")
    )
    .dropDuplicates(["payment_id"])
    .withColumn("processed_at", current_timestamp())
)

# Write to Silver
df_payments_silver.write.format("delta").mode("overwrite").saveAsTable(
    "demo_ecommerce.ecomm_silver.payments"
)

print("Silver payments:", df_payments_silver.count())

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'customers' AS table_name, COUNT(*) AS records
# MAGIC FROM demo_ecommerce.ecomm_silver.customers
# MAGIC UNION ALL
# MAGIC SELECT 'products', COUNT(*)
# MAGIC FROM demo_ecommerce.ecomm_silver.products
# MAGIC UNION ALL
# MAGIC SELECT 'orders', COUNT(*)
# MAGIC FROM demo_ecommerce.ecomm_silver.orders
# MAGIC UNION ALL
# MAGIC SELECT 'payments', COUNT(*)
# MAGIC FROM demo_ecommerce.ecomm_silver.payments;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'customers' AS table_name,
# MAGIC        COUNT(*) - COUNT(DISTINCT customer_id) AS duplicates
# MAGIC FROM demo_ecommerce.ecomm_silver.customers
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'products',
# MAGIC        COUNT(*) - COUNT(DISTINCT product_id)
# MAGIC FROM demo_ecommerce.ecomm_silver.products
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'orders',
# MAGIC        COUNT(*) - COUNT(DISTINCT order_id)
# MAGIC FROM demo_ecommerce.ecomm_silver.orders
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'payments',
# MAGIC        COUNT(*) - COUNT(DISTINCT payment_id)
# MAGIC FROM demo_ecommerce.ecomm_silver.payments;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Orders referencing nonexistent customers
# MAGIC SELECT 'invalid_customer' AS issue, COUNT(*) AS records
# MAGIC FROM demo_ecommerce.ecomm_silver.orders o
# MAGIC LEFT ANTI JOIN demo_ecommerce.ecomm_silver.customers c
# MAGIC     ON o.customer_id = c.customer_id
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- Orders referencing nonexistent products
# MAGIC SELECT 'invalid_product', COUNT(*)
# MAGIC FROM demo_ecommerce.ecomm_silver.orders o
# MAGIC LEFT ANTI JOIN demo_ecommerce.ecomm_silver.products p
# MAGIC     ON o.product_id = p.product_id
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- Payments referencing nonexistent orders
# MAGIC SELECT 'invalid_order', COUNT(*)
# MAGIC FROM demo_ecommerce.ecomm_silver.payments p
# MAGIC LEFT ANTI JOIN demo_ecommerce.ecomm_silver.orders o
# MAGIC     ON p.order_id = o.order_id;
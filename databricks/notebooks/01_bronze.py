# Databricks notebook source
# ─────────────────────────────────────────────
# 01_bronze.py
# ─────────────────────────────────────────────
# Reads raw Olist CSVs from the Unity Catalog volume
# and writes them as Delta tables in the bronze schema.
# Each table gets _ingested_at and _source_file columns
# for lineage tracking.
# ─────────────────────────────────────────────

from pyspark.sql import functions as F
from datetime import datetime, timezone

# Base path to raw CSV files
RAW_PATH = "/Volumes/olist/default/olist_raw"

# Target catalog and schema for bronze tables
CATALOG = "olist"
BRONZE_SCHEMA = "bronze"

# Create bronze schema if it doesn't exist
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")
print(f"Schema '{CATALOG}.{BRONZE_SCHEMA}' ready")

# COMMAND ----------

# ─────────────────────────────────────────────
# Define CSV files and target table names
# ─────────────────────────────────────────────

OLIST_FILES = {
    "olist_customers_dataset.csv": "customers",
    "olist_orders_dataset.csv": "orders",
    "olist_order_items_dataset.csv": "order_items",
    "olist_order_payments_dataset.csv": "order_payments",
    "olist_order_reviews_dataset.csv": "order_reviews",
    "olist_products_dataset.csv": "products",
    "olist_sellers_dataset.csv": "sellers",
    "product_category_name_translation.csv": "product_category_translation",
}

def load_csv_to_bronze(filename, table_name):
    """
    Reads a single CSV from the raw volume and writes it
    as a Delta table in the bronze schema with lineage columns.
    """
    file_path = f"{RAW_PATH}/{filename}"
    
    print(f"Loading {filename} → {CATALOG}.{BRONZE_SCHEMA}.{table_name}")
    
    df = (spark.read
          .option("header", "true")
          .option("inferSchema", "true")
          .csv(file_path))
    
    # Add lineage metadata columns
    df = df.withColumn("_ingested_at", F.current_timestamp()) \
           .withColumn("_source_file", F.lit(filename))
    
    # Write as Delta table
    (df.write
       .format("delta")
       .mode("overwrite")
       .saveAsTable(f"{CATALOG}.{BRONZE_SCHEMA}.{table_name}"))
    
    row_count = df.count()
    print(f"  ✓ Loaded {row_count:,} rows → {CATALOG}.{BRONZE_SCHEMA}.{table_name}")

print("Function defined successfully")

# COMMAND ----------

# ─────────────────────────────────────────────
# Run ingestion for all tables
# ─────────────────────────────────────────────

from datetime import datetime

start_time = datetime.now()
print("=" * 60)
print("Olist Bronze Ingestion Starting")
print("=" * 60)

success_count = 0
skip_count = 0

for filename, table_name in OLIST_FILES.items():
    try:
        load_csv_to_bronze(filename, table_name)
        success_count += 1
    except Exception as e:
        print(f"  ✗ Failed to load {filename}: {e}")
        skip_count += 1

end_time = datetime.now()
duration = (end_time - start_time).seconds

print("=" * 60)
print(f"Bronze ingestion complete in {duration}s")
print(f"Success: {success_count} | Failed: {skip_count}")
print("=" * 60)

# COMMAND ----------

# Verify all bronze tables exist and show row counts
print("Bronze layer verification:")
print("-" * 50)

tables = spark.sql(f"SHOW TABLES IN {CATALOG}.{BRONZE_SCHEMA}")

for row in tables.collect():
    table_name = row.tableName
    count = spark.sql(f"SELECT COUNT(*) as cnt FROM {CATALOG}.{BRONZE_SCHEMA}.{table_name}").collect()[0].cnt
    print(f"  {table_name:<40} {count:>10,} rows")

# COMMAND ----------

# Verify Delta time travel is working
# This shows the history of writes to the customers table
print("Delta time travel history for customers table:")
print("-" * 50)

spark.sql(f"""
    DESCRIBE HISTORY {CATALOG}.{BRONZE_SCHEMA}.customers
""").select("version", "timestamp", "operation", "operationMetrics").show(truncate=False)

# COMMAND ----------

display(dbutils.fs.ls("/Volumes/olist/default/olist_raw/"))

# COMMAND ----------


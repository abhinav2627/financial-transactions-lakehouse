# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze Layer — Raw Ingestion (Batch Read)
# MAGIC ### Reads CSVs from Landing Volume → Writes raw Delta table (Bronze)
# MAGIC
# MAGIC > **Note:** Using batch read instead of streaming for reliability on Free Edition Serverless.
# MAGIC > The medallion architecture and Delta Lake properties are identical to streaming.

# COMMAND ----------

# MAGIC %md ## 0 · Config

# COMMAND ----------

MY_CATALOG   = "workspace"   # ← your catalog name
SCHEMA_NAME  = "fintech_lakehouse"
LANDING_VOL  = f"/Volumes/{MY_CATALOG}/{SCHEMA_NAME}/landing"
BRONZE_TABLE = f"{MY_CATALOG}.{SCHEMA_NAME}.bronze_transactions"

spark.sql(f"USE {MY_CATALOG}.{SCHEMA_NAME}")
from pyspark.sql import functions as F

print("✅ Config ready")
print(f"   Reading from : {LANDING_VOL}")
print(f"   Writing to   : {BRONZE_TABLE}")

# COMMAND ----------

# MAGIC %md ## 1 · Verify Landing Volume Has Files

# COMMAND ----------

files = dbutils.fs.ls(LANDING_VOL)
print(f"✅ Landing volume has {len(files)} batch folder(s):")
for f in files:
    inner = dbutils.fs.ls(f.path)
    csv_files = [x for x in inner if x.name.endswith(".csv")]
    print(f"   {f.name}  →  {len(csv_files)} CSV file(s)")

# COMMAND ----------

# MAGIC %md ## 2 · Drop Old Broken Table (Clean Slate)

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {BRONZE_TABLE}")
print(f"✅ Dropped old table: {BRONZE_TABLE}")

# COMMAND ----------

# MAGIC %md ## 3 · Read All CSVs from Landing Volume (Batch)

# COMMAND ----------

# Read all CSV files from all batch subfolders
raw_df = (
    spark.read
         .format("csv")
         .option("header", "true")        # use the actual header row
         .option("inferSchema", "false")  # keep everything as string for safety
         .option("recursiveFileLookup", "true")  # look inside subfolders
         .load(LANDING_VOL)
)

print(f"✅ Raw CSV read complete")
print(f"   Rows    : {raw_df.count():,}")
print(f"   Columns : {raw_df.columns}")

# COMMAND ----------

# MAGIC %md ## 4 · Add Ingestion Metadata Columns

# COMMAND ----------

bronze_df = (
    raw_df
    .withColumn("_ingest_timestamp", F.current_timestamp())
    .withColumn("_source_file",      F.input_file_name())
)

print("✅ Metadata columns added:")
print("   _ingest_timestamp → current timestamp")
print("   _source_file      → source CSV path")

# COMMAND ----------

# MAGIC %md ## 5 · Write to Bronze Delta Table

# COMMAND ----------

bronze_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(BRONZE_TABLE)

bronze_count = spark.table(BRONZE_TABLE).count()
print(f"✅ Bronze table written successfully!")
print(f"   Table     : {BRONZE_TABLE}")
print(f"   Row count : {bronze_count:,}")
print(f"   Columns   : {len(spark.table(BRONZE_TABLE).columns)}")

# COMMAND ----------

# MAGIC %md ## 6 · Validate — Check Columns Are Correct

# COMMAND ----------

df = spark.table(BRONZE_TABLE)

# Verify key columns have correct data
sample = df.select(
    "transaction_id", "user_id", "merchant_name",
    "amount", "currency", "timestamp", "status"
).limit(5)

print("=== Sample Bronze Data (verify columns look correct) ===")
sample.show(5, truncate=True)

# COMMAND ----------

# Quick column check
first_row = df.select("transaction_id", "amount", "timestamp").first()

print("=== Column Validation ===")
print(f"  transaction_id : {first_row['transaction_id']}  ← should be a UUID like abc123-...")
print(f"  amount         : {first_row['amount']}  ← should be a number like 45.23")
print(f"  timestamp      : {first_row['timestamp']}  ← should be a date like 2024-01-01 08:23:11")

# COMMAND ----------

# MAGIC %md ## 7 · Delta Table History

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {BRONZE_TABLE}").select(
    "version", "timestamp", "operation", "operationMetrics"
).limit(5))

# COMMAND ----------

print("🎉 Bronze layer complete! Proceed to notebook 02_silver_cleaning_dq")

# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver Layer — Cleaning, Casting & Data Quality
# MAGIC ### Reads Bronze → Applies 7 DQ Rules → Writes Silver + Quarantine tables
# MAGIC
# MAGIC **What this notebook does:**
# MAGIC - Reads Bronze Delta table (batch)
# MAGIC - Casts all columns to correct types
# MAGIC - Normalises currencies to USD
# MAGIC - Runs 7 Data Quality rules on every row
# MAGIC - ✅ Good rows → `silver_transactions`
# MAGIC - ⚠️ Bad rows → `silver_quarantine`

# COMMAND ----------

# MAGIC %md ## 0 · Config

# COMMAND ----------

MY_CATALOG       = "workspace"   # ← your catalog name
SCHEMA_NAME      = "fintech_lakehouse"
BRONZE_TABLE     = f"{MY_CATALOG}.{SCHEMA_NAME}.bronze_transactions"
SILVER_TABLE     = f"{MY_CATALOG}.{SCHEMA_NAME}.silver_transactions"
QUARANTINE_TABLE = f"{MY_CATALOG}.{SCHEMA_NAME}.silver_quarantine"

spark.sql(f"USE {MY_CATALOG}.{SCHEMA_NAME}")
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DoubleType
from functools import reduce

print("✅ Config ready")
print(f"   Bronze   : {BRONZE_TABLE}")
print(f"   Silver   : {SILVER_TABLE}")
print(f"   Quarantine: {QUARANTINE_TABLE}")

# COMMAND ----------

# MAGIC %md ## 1 · Drop Old Tables (Clean Slate)

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {SILVER_TABLE}")
spark.sql(f"DROP TABLE IF EXISTS {QUARANTINE_TABLE}")
print("✅ Old tables dropped — clean slate ready")

# COMMAND ----------

# MAGIC %md ## 2 · Read Bronze Table

# COMMAND ----------

bronze_df = spark.table(BRONZE_TABLE)
print(f"✅ Bronze loaded")
print(f"   Row count : {bronze_df.count():,}")
print(f"   Columns   : {bronze_df.columns}")

# COMMAND ----------

# MAGIC %md ## 3 · Transformation Function
# MAGIC
# MAGIC Applies:
# MAGIC - Type casting (amount → Double, is_online → Boolean, timestamp → Timestamp)
# MAGIC - Currency normalisation to USD (EUR × 1.08, GBP × 1.27, CAD × 0.74)
# MAGIC - Derived columns (txn_date, txn_hour)
# MAGIC - String standardisation (trim, lower, upper)

# COMMAND ----------

def transform_bronze(df):
    return (
        df
        # ── Type casting ───────────────────────────────────────────────────────
        .withColumn("amount",        F.col("amount").cast(DoubleType()))
        .withColumn("is_online",     F.col("is_online").cast(BooleanType()))
        .withColumn("txn_timestamp", F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
        # ── Derived columns ────────────────────────────────────────────────────
        .withColumn("txn_date",      F.to_date(F.col("txn_timestamp")))
        .withColumn("txn_hour",      F.hour(F.col("txn_timestamp")))
        # ── Normalise to USD ───────────────────────────────────────────────────
        .withColumn("amount_usd",
            F.when(F.col("currency") == "EUR", F.round(F.col("amount") * 1.08, 2))
             .when(F.col("currency") == "GBP", F.round(F.col("amount") * 1.27, 2))
             .when(F.col("currency") == "CAD", F.round(F.col("amount") * 0.74, 2))
             .otherwise(F.col("amount").cast(DoubleType()))
        )
        # ── Standardise strings ────────────────────────────────────────────────
        .withColumn("merchant_name",     F.trim(F.col("merchant_name")))
        .withColumn("merchant_category", F.lower(F.trim(F.col("merchant_category"))))
        .withColumn("status",            F.lower(F.trim(F.col("status"))))
        .withColumn("country",           F.upper(F.trim(F.col("country"))))
        # ── Drop raw timestamp replaced by txn_timestamp ───────────────────────
        .drop("timestamp")
        # ── Silver metadata ────────────────────────────────────────────────────
        .withColumn("_silver_timestamp", F.current_timestamp())
    )

print("✅ Transformation function defined")

# COMMAND ----------

# MAGIC %md ## 4 · Data Quality Rules
# MAGIC
# MAGIC | Rule | Check |
# MAGIC |---|---|
# MAGIC | dq_valid_amount | amount_usd > 0 |
# MAGIC | dq_valid_amount_ceiling | amount_usd < 50,000 |
# MAGIC | dq_no_null_txn_id | transaction_id IS NOT NULL |
# MAGIC | dq_no_null_user | user_id IS NOT NULL |
# MAGIC | dq_valid_status | status in (completed, pending, failed) |
# MAGIC | dq_valid_timestamp | txn_timestamp IS NOT NULL |
# MAGIC | dq_valid_currency | currency in (USD, EUR, GBP, CAD) |

# COMMAND ----------

DQ_RULES = {
    "dq_valid_amount"        : F.col("amount_usd") > 0,
    "dq_valid_amount_ceiling": F.col("amount_usd") < 50000,
    "dq_no_null_txn_id"      : F.col("transaction_id").isNotNull(),
    "dq_no_null_user"        : F.col("user_id").isNotNull(),
    "dq_valid_status"        : F.col("status").isin("completed","pending","failed"),
    "dq_valid_timestamp"     : F.col("txn_timestamp").isNotNull(),
    "dq_valid_currency"      : F.col("currency").isin("USD","EUR","GBP","CAD"),
}

def apply_dq(df):
    for rule_name, rule_expr in DQ_RULES.items():
        df = df.withColumn(rule_name, rule_expr)
    all_checks = [F.col(r) for r in DQ_RULES.keys()]
    return df.withColumn("_dq_passed", reduce(lambda a, b: a & b, all_checks))

print("✅ DQ rules defined:")
for rule in DQ_RULES.keys():
    print(f"   {rule}")

# COMMAND ----------

# MAGIC %md ## 5 · Apply Transformations + DQ + Deduplicate

# COMMAND ----------

# Step 1: Transform
transformed = transform_bronze(bronze_df)
print("✅ Transformations applied")

# Step 2: Apply DQ rules
with_dq = apply_dq(transformed)
print("✅ DQ rules applied")

# Step 3: Deduplicate on transaction_id
deduped = with_dq.dropDuplicates(["transaction_id"])
print("✅ Deduplication complete")

# COMMAND ----------

# MAGIC %md ## 6 · Split Good vs Quarantine

# COMMAND ----------

# Good rows — passed ALL 7 DQ rules
good = (
    deduped
    .filter(F.col("_dq_passed") == True)
    .drop(*DQ_RULES.keys(), "_dq_passed")
)

# Bad rows — failed at least 1 DQ rule
bad = (
    deduped
    .filter(F.col("_dq_passed") == False)
    .withColumn(
        "_quarantine_reason",
        F.concat_ws(", ", *[F.when(~F.col(r), F.lit(r)) for r in DQ_RULES.keys()])
    )
)

good_count = good.count()
bad_count  = bad.count()
total      = good_count + bad_count

print(f"✅ Split complete")
print(f"   Good rows        : {good_count:,}  ({good_count/total*100:.1f}%)")
print(f"   Quarantined rows : {bad_count:,}  ({bad_count/total*100:.1f}%)")
print(f"   DQ Pass Rate     : {good_count/total*100:.1f}%")

# COMMAND ----------

# MAGIC %md ## 7 · Write Silver Table

# COMMAND ----------

good.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(SILVER_TABLE)

silver_count = spark.table(SILVER_TABLE).count()
print(f"✅ Silver table written!")
print(f"   Table     : {SILVER_TABLE}")
print(f"   Row count : {silver_count:,}")

# COMMAND ----------

# MAGIC %md ## 8 · Write Quarantine Table

# COMMAND ----------

bad.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(QUARANTINE_TABLE)

quarantine_count = spark.table(QUARANTINE_TABLE).count()
print(f"✅ Quarantine table written!")
print(f"   Table     : {QUARANTINE_TABLE}")
print(f"   Row count : {quarantine_count:,}")

# COMMAND ----------

# MAGIC %md ## 9 · Preview Silver Data

# COMMAND ----------

print("=== Silver Table Sample ===")
display(spark.table(SILVER_TABLE).limit(10))

# COMMAND ----------

# MAGIC %md ## 10 · Quarantine Breakdown

# COMMAND ----------

print("=== Quarantine Breakdown by Reason ===")
display(
    spark.table(QUARANTINE_TABLE)
         .groupBy("_quarantine_reason")
         .count()
         .orderBy(F.desc("count"))
)

# COMMAND ----------

# MAGIC %md ## 11 · Final Summary

# COMMAND ----------

bronze_count = spark.table(BRONZE_TABLE).count()
silver_count = spark.table(SILVER_TABLE).count()
qcount       = spark.table(QUARANTINE_TABLE).count()

print("=" * 50)
print("   SILVER LAYER SUMMARY")
print("=" * 50)
print(f"   Bronze rows in      : {bronze_count:,}")
print(f"   Silver rows (good)  : {silver_count:,}")
print(f"   Quarantine (bad)    : {qcount:,}")
print(f"   DQ Pass Rate        : {silver_count/bronze_count*100:.1f}%")
print("=" * 50)
print("\n🎉 Silver layer complete! Proceed to notebook 03_gold_aggregations")

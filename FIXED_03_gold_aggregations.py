# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Gold Layer — Aggregations & Business KPIs
# MAGIC ### Reads Silver → Builds 2 Gold tables with business-ready metrics
# MAGIC
# MAGIC **Two Gold tables:**
# MAGIC - `gold_merchant_agg` → Daily spend metrics per merchant
# MAGIC - `gold_daily_kpis`   → Company-wide daily KPIs

# COMMAND ----------

# MAGIC %md ## 0 · Config

# COMMAND ----------

MY_CATALOG   = "workspace"   # ← your catalog name
SCHEMA_NAME  = "fintech_lakehouse"
SILVER_TABLE = f"{MY_CATALOG}.{SCHEMA_NAME}.silver_transactions"
GOLD_MERCH   = f"{MY_CATALOG}.{SCHEMA_NAME}.gold_merchant_agg"
GOLD_KPI     = f"{MY_CATALOG}.{SCHEMA_NAME}.gold_daily_kpis"

spark.sql(f"USE {MY_CATALOG}.{SCHEMA_NAME}")
from pyspark.sql import functions as F
from pyspark.sql.window import Window

print("✅ Config ready")
print(f"   Silver : {SILVER_TABLE}")
print(f"   Gold 1 : {GOLD_MERCH}")
print(f"   Gold 2 : {GOLD_KPI}")

# COMMAND ----------

# MAGIC %md ## 1 · Drop Old Gold Tables

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {GOLD_MERCH}")
spark.sql(f"DROP TABLE IF EXISTS {GOLD_KPI}")
print("✅ Old Gold tables dropped")

# COMMAND ----------

# MAGIC %md ## 2 · Read Silver

# COMMAND ----------

silver_df = spark.table(SILVER_TABLE)
print(f"✅ Silver loaded : {silver_df.count():,} rows")
display(silver_df.limit(5))

# COMMAND ----------

# MAGIC %md ## 3 · Gold Table 1 — Merchant Daily Aggregations
# MAGIC
# MAGIC **Grain:** one row per `(merchant_name, txn_date)`
# MAGIC
# MAGIC | Column | Description |
# MAGIC |---|---|
# MAGIC | total_txn_count | Total number of transactions |
# MAGIC | completed_count | Only completed transactions |
# MAGIC | total_amount_usd | Gross spend in USD |
# MAGIC | avg_amount_usd | Average transaction value |
# MAGIC | max_amount_usd | Largest single transaction |
# MAGIC | unique_users | Distinct customers |
# MAGIC | online_pct | % of online transactions |
# MAGIC | failed_pct | % of failed transactions |

# COMMAND ----------

merchant_agg = (
    silver_df
    .filter(F.col("txn_date").isNotNull())
    .groupBy("merchant_name", "merchant_category", "txn_date")
    .agg(
        F.count("*")
            .alias("total_txn_count"),
        F.sum(F.when(F.col("status") == "completed", 1).otherwise(0))
            .alias("completed_count"),
        F.round(F.sum("amount_usd"), 2)
            .alias("total_amount_usd"),
        F.round(F.avg("amount_usd"), 2)
            .alias("avg_amount_usd"),
        F.round(F.max("amount_usd"), 2)
            .alias("max_amount_usd"),
        F.countDistinct("user_id")
            .alias("unique_users"),
        F.round(
            F.sum(F.when(F.col("is_online") == True, 1).otherwise(0)) * 100.0
            / F.count("*"), 1)
            .alias("online_pct"),
        F.round(
            F.sum(F.when(F.col("status") == "failed", 1).otherwise(0)) * 100.0
            / F.count("*"), 2)
            .alias("failed_pct"),
    )
    .withColumn("_gold_updated_at", F.current_timestamp())
)

# Write to Gold
merchant_agg.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(GOLD_MERCH)

merch_count = spark.table(GOLD_MERCH).count()
print(f"✅ Gold Merchant Agg written!")
print(f"   Table     : {GOLD_MERCH}")
print(f"   Row count : {merch_count:,}")

# COMMAND ----------

# Preview top merchants
print("Top 10 Merchants by Total Spend:")
display(
    spark.table(GOLD_MERCH)
         .groupBy("merchant_name")
         .agg(F.round(F.sum("total_amount_usd"), 2).alias("total_usd"),
              F.sum("total_txn_count").alias("total_txns"))
         .orderBy(F.desc("total_usd"))
         .limit(10)
)

# COMMAND ----------

# MAGIC %md ## 4 · Gold Table 2 — Daily Company KPIs
# MAGIC
# MAGIC **Grain:** one row per `txn_date`
# MAGIC
# MAGIC | Column | Description |
# MAGIC |---|---|
# MAGIC | daily_revenue_usd | Total completed transaction value |
# MAGIC | total_transactions | All transactions that day |
# MAGIC | active_users | Unique users transacting |
# MAGIC | avg_txn_value | Mean transaction size |
# MAGIC | failed_txn_rate_pct | Failed % (risk indicator) |
# MAGIC | online_share_pct | % of online transactions |
# MAGIC | top_category | Highest spend merchant category |

# COMMAND ----------

# Daily base metrics
daily_base = (
    silver_df
    .filter(F.col("txn_date").isNotNull())
    .groupBy("txn_date")
    .agg(
        F.round(F.sum(
            F.when(F.col("status") == "completed", F.col("amount_usd")).otherwise(0)
        ), 2)                                           .alias("daily_revenue_usd"),
        F.count("*")                                    .alias("total_transactions"),
        F.countDistinct("user_id")                      .alias("active_users"),
        F.round(F.avg("amount_usd"), 2)                 .alias("avg_txn_value"),
        F.round(
            F.sum(F.when(F.col("status") == "failed", 1).otherwise(0)) * 100.0
            / F.count("*"), 2)                          .alias("failed_txn_rate_pct"),
        F.round(
            F.sum(F.when(F.col("is_online") == True, 1).otherwise(0)) * 100.0
            / F.count("*"), 1)                          .alias("online_share_pct"),
    )
)

# Top spending category per day using window function
top_category = (
    silver_df
    .filter(F.col("txn_date").isNotNull())
    .groupBy("txn_date", "merchant_category")
    .agg(F.sum("amount_usd").alias("cat_revenue"))
    .withColumn("rank", F.rank().over(
        Window.partitionBy("txn_date").orderBy(F.desc("cat_revenue"))
    ))
    .filter(F.col("rank") == 1)
    .select("txn_date", F.col("merchant_category").alias("top_category"))
)

# Join base metrics with top category
daily_kpis = (
    daily_base
    .join(top_category, on="txn_date", how="left")
    .withColumn("_gold_updated_at", F.current_timestamp())
    .orderBy("txn_date")
)

# Write to Gold
daily_kpis.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(GOLD_KPI)

kpi_count = spark.table(GOLD_KPI).count()
print(f"✅ Gold Daily KPIs written!")
print(f"   Table     : {GOLD_KPI}")
print(f"   Row count : {kpi_count:,}")

# COMMAND ----------

# Preview daily KPIs
print("Daily KPIs:")
display(spark.table(GOLD_KPI))

# COMMAND ----------

# MAGIC %md ## 5 · Business Summary Report

# COMMAND ----------

kpis       = spark.table(GOLD_KPI)
total_rev  = kpis.agg(F.sum("daily_revenue_usd")).collect()[0][0]
total_txns = kpis.agg(F.sum("total_transactions")).collect()[0][0]
avg_fail   = kpis.agg(F.avg("failed_txn_rate_pct")).collect()[0][0]
peak       = kpis.orderBy(F.desc("daily_revenue_usd")).first()

print("=" * 60)
print("   FINANCIAL TRANSACTIONS LAKEHOUSE — FINAL REPORT")
print("=" * 60)
print()
print("  PIPELINE SUMMARY")
bronze_count = spark.table(f"{MY_CATALOG}.{SCHEMA_NAME}.bronze_transactions").count()
silver_count = spark.table(f"{MY_CATALOG}.{SCHEMA_NAME}.silver_transactions").count()
qcount       = spark.table(f"{MY_CATALOG}.{SCHEMA_NAME}.silver_quarantine").count()
print(f"    Bronze rows         : {bronze_count:,}")
print(f"    Silver rows (clean) : {silver_count:,}")
print(f"    Quarantined rows    : {qcount:,}")
print(f"    DQ Pass Rate        : {silver_count/bronze_count*100:.1f}%")
print()
print("  BUSINESS KPIs (10-day period)")
print(f"    Total Revenue       : ${total_rev:>12,.2f}")
print(f"    Total Transactions  : {int(total_txns):>12,}")
print(f"    Avg Failure Rate    : {avg_fail:>11.2f}%")
print(f"    Peak Revenue Day    : {peak['txn_date']}  (${peak['daily_revenue_usd']:,.2f})")
print()
print("  TOP 5 MERCHANTS BY SPEND")
top5 = (
    spark.table(GOLD_MERCH)
         .groupBy("merchant_name")
         .agg(F.round(F.sum("total_amount_usd"), 2).alias("total_usd"),
              F.sum("total_txn_count").alias("txns"))
         .orderBy(F.desc("total_usd"))
         .limit(5)
)
display(top5)
print("=" * 60)
print()
print("🎉 Pipeline complete! Bronze → Silver → Gold all healthy.")
print()
print("  TABLES CREATED:")
print(f"    ✅ bronze_transactions     ({bronze_count:,} rows)")
print(f"    ✅ silver_transactions     ({silver_count:,} rows)")
print(f"    ✅ silver_quarantine       ({qcount:,} rows)")
print(f"    ✅ gold_merchant_agg       ({spark.table(GOLD_MERCH).count():,} rows)")
print(f"    ✅ gold_daily_kpis         ({spark.table(GOLD_KPI).count():,} rows)")

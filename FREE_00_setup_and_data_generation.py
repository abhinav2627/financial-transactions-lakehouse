# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup & Data Generation
# MAGIC ### Financial Transactions Lakehouse — Databricks Free Edition (Serverless + Unity Catalog)
# MAGIC
# MAGIC **What this notebook does:**
# MAGIC 1. Creates a Unity Catalog schema and Volumes (replaces DBFS)
# MAGIC 2. Generates 5,000 synthetic financial transactions
# MAGIC 3. Drops them as CSV files into the landing Volume to simulate streaming

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⚠️ Before Running — Do This Once
# MAGIC
# MAGIC You need to find your **catalog name** first:
# MAGIC 1. Click **Catalog** in the left sidebar
# MAGIC 2. You will see a catalog named after your workspace (e.g. `workspace` or your email prefix)
# MAGIC 3. Copy that catalog name and paste it in the cell below

# COMMAND ----------

# ── CHANGE THIS to your catalog name (check Catalog Explorer in left sidebar) ──
MY_CATALOG = "workspace"   # ← replace with your actual catalog name

# ── Everything else is automatic ───────────────────────────────────────────────
SCHEMA_NAME     = "fintech_lakehouse"
CATALOG_BASE    = f"{MY_CATALOG}.{SCHEMA_NAME}"

# Volume paths (Unity Catalog volumes replace DBFS on Free Edition)
LANDING_VOL     = f"/Volumes/{MY_CATALOG}/{SCHEMA_NAME}/landing"
BRONZE_VOL      = f"/Volumes/{MY_CATALOG}/{SCHEMA_NAME}/bronze"
SILVER_VOL      = f"/Volumes/{MY_CATALOG}/{SCHEMA_NAME}/silver"
QUARANTINE_VOL  = f"/Volumes/{MY_CATALOG}/{SCHEMA_NAME}/quarantine"
GOLD_MERCH_VOL  = f"/Volumes/{MY_CATALOG}/{SCHEMA_NAME}/gold_merchant"
GOLD_KPI_VOL    = f"/Volumes/{MY_CATALOG}/{SCHEMA_NAME}/gold_kpis"

print("✅ Config loaded")
print(f"   Catalog  : {MY_CATALOG}")
print(f"   Schema   : {SCHEMA_NAME}")
print(f"   Landing  : {LANDING_VOL}")

# COMMAND ----------

# MAGIC %md ## 1 · Create Schema and Volumes

# COMMAND ----------

# Create schema under your catalog
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {MY_CATALOG}.{SCHEMA_NAME}")
print(f"✅ Schema created: {MY_CATALOG}.{SCHEMA_NAME}")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Run this cell to create all volumes
# MAGIC -- Unity Catalog volumes are the storage layer on Free Edition (replaces DBFS)

# COMMAND ----------

# Create all volumes programmatically
volumes = ["landing", "bronze", "silver", "quarantine", "gold_merchant", "gold_kpis"]

for vol in volumes:
    spark.sql(f"""
        CREATE VOLUME IF NOT EXISTS {MY_CATALOG}.{SCHEMA_NAME}.{vol}
    """)
    print(f"  ✅ Volume created: /Volumes/{MY_CATALOG}/{SCHEMA_NAME}/{vol}")

print(f"\n✅ All volumes ready under {MY_CATALOG}.{SCHEMA_NAME}")

# COMMAND ----------

# MAGIC %md ## 2 · Generate Synthetic Financial Transactions

# COMMAND ----------

import random
import uuid
from datetime import datetime, timedelta

random.seed(42)

MERCHANTS  = ["Amazon","Walmart","Starbucks","Shell","Netflix",
               "Apple","Uber","DoorDash","Target","CVS"]
CATEGORIES = {"Amazon":"ecommerce","Walmart":"retail","Starbucks":"food_beverage",
               "Shell":"fuel","Netflix":"subscription","Apple":"electronics",
               "Uber":"transport","DoorDash":"food_beverage","Target":"retail","CVS":"pharmacy"}
CURRENCIES = ["USD","USD","USD","USD","EUR","GBP","CAD"]
CARD_TYPES = ["credit","debit","credit","credit","debit"]
COUNTRIES  = ["US","US","US","US","GB","CA","DE","FR"]
USER_IDS   = [f"USR_{i:05d}" for i in range(1, 501)]

def random_txn(base_dt):
    merchant = random.choice(MERCHANTS)
    ts       = base_dt + timedelta(seconds=random.randint(0, 86399))
    amount   = round(random.lognormvariate(3.5, 1.2), 2)
    if random.random() < 0.03:   # inject ~3% bad data
        amount = round(random.choice([-amount, amount * 50]), 2)
    return {
        "transaction_id"    : str(uuid.uuid4()),
        "user_id"           : random.choice(USER_IDS),
        "merchant_name"     : merchant,
        "merchant_category" : CATEGORIES[merchant],
        "amount"            : amount,
        "currency"          : random.choice(CURRENCIES),
        "card_type"         : random.choice(CARD_TYPES),
        "country"           : random.choice(COUNTRIES),
        "is_online"         : str(random.choice([True, False])),
        "timestamp"         : ts.strftime("%Y-%m-%d %H:%M:%S"),
        "status"            : random.choices(
                                ["completed","pending","failed"],
                                weights=[90, 7, 3])[0],
    }

# COMMAND ----------

# MAGIC %md ## 3 · Write CSV Batches to Landing Volume

# COMMAND ----------

base_date  = datetime(2024, 1, 1)
total_rows = 0

for batch_num in range(10):
    batch_date = base_date + timedelta(days=batch_num)
    rows       = [random_txn(batch_date) for _ in range(500)]
    df         = spark.createDataFrame(rows)

    # Write to Unity Catalog Volume (POSIX path, works like DBFS)
    out_path = f"{LANDING_VOL}/batch_{batch_num:02d}"
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(out_path)
    total_rows += 500
    print(f"  batch_{batch_num:02d} → 500 rows  ({batch_date.date()})")

print(f"\n✅ Generated {total_rows:,} transactions in {LANDING_VOL}")

# COMMAND ----------

# MAGIC %md ## 4 · Verify Landing Volume

# COMMAND ----------

# List files in the landing volume
files = dbutils.fs.ls(LANDING_VOL)
print(f"Landing volume contains {len(files)} batch folder(s):\n")
for f in files:
    print(f"  {f.name}")

print(f"\n✅ Setup complete! Proceed to notebook 01_bronze_ingestion")

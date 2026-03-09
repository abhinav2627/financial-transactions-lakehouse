# 💳 Financial Transactions Lakehouse
### End-to-End Data Engineering Project on Databricks

![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=Databricks&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-00ADD8?style=for-the-badge&logo=apachespark&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![SQL](https://img.shields.io/badge/SQL-4479A1?style=for-the-badge&logo=mysql&logoColor=white)

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                  FINANCIAL TRANSACTIONS LAKEHOUSE                    │
│                     Medallion Architecture                           │
└─────────────────────────────────────────────────────────────────────┘

  [Unity Catalog Volumes]
  /landing/*.csv  (5,000 synthetic financial transactions)
        │
        ▼  Batch Ingestion (PySpark)
  ┌─────────────────────────────────┐
  │         BRONZE LAYER            │
  │   bronze_transactions           │
  │   • Raw data, no transforms     │
  │   • + _ingest_timestamp         │
  │   • + _source_file              │
  │   • 5,000 rows                  │
  └─────────────┬───────────────────┘
                │
                ▼  7 DQ Rules + Type Casting + Currency Normalisation
  ┌─────────────────────────────────┐    ┌──────────────────────────┐
  │         SILVER LAYER            │    │      QUARANTINE          │
  │   silver_transactions           │    │   silver_quarantine      │
  │   • Cleaned & typed             │    │   • Failed DQ rows       │
  │   • USD normalised              │    │   • _quarantine_reason   │
  │   • 4,929 rows (98.6% pass)     │    │   • 71 rows (1.4%)       │
  └─────────────┬───────────────────┘    └──────────────────────────┘
                │
                ▼  Aggregations + Window Functions
  ┌─────────────────────────────────┐    ┌──────────────────────────┐
  │    GOLD LAYER — Merchant Agg    │    │   GOLD LAYER — KPIs      │
  │   gold_merchant_agg             │    │   gold_daily_kpis        │
  │   • Daily spend per merchant    │    │   • Daily revenue        │
  │   • 100 rows                    │    │   • Failure rates        │
  │                                 │    │   • Top categories       │
  └─────────────────────────────────┘    └──────────────────────────┘
```

---

## 📊 Results

| Metric | Value |
|---|---|
| Total Transactions Processed | 5,000 |
| Clean Records (Silver) | 4,929 (98.6%) |
| Quarantined Records | 71 (1.4%) |
| Gold Tables Built | 2 |
| Top Merchant by Spend | Shell ($84,731) |
| Data Period | 10 days (Jan 2024) |

---

## 🛠️ Tech Stack

| Tool | Purpose |
|---|---|
| **Databricks Free Edition** | Cloud compute & notebooks |
| **Delta Lake** | ACID transactions, time travel, schema enforcement |
| **PySpark** | Data processing & transformations |
| **Unity Catalog** | Data governance & storage (Volumes + Tables) |
| **Databricks SQL** | Dashboard & visualization layer |
| **Python 3.10** | Notebook language |

---

## 📁 Project Structure

```
financial-transactions-lakehouse/
│
├── notebooks/
│   ├── 00_setup_and_data_generation.py   # Generate synthetic data
│   ├── 01_bronze_ingestion.py            # Raw ingestion → Bronze Delta table
│   ├── 02_silver_cleaning_dq.py          # Cleaning + 7 DQ rules → Silver
│   └── 03_gold_aggregations.py           # KPI aggregations → Gold tables
│
└── README.md
```

---

## 🚀 How to Run

### Prerequisites
- Databricks Free Edition account ([sign up here](https://www.databricks.com/try-databricks))
- Unity Catalog enabled (default on Free Edition)

### Steps

**1. Clone this repo**
```bash
git clone https://github.com/YOUR_USERNAME/financial-transactions-lakehouse.git
```

**2. Import notebooks to Databricks**
- Go to Databricks Workspace → your folder
- Click ⋮ → Import → File
- Upload each `.py` file from the `notebooks/` folder

**3. Set your catalog name**
In each notebook, update:
```python
MY_CATALOG = "workspace"  # ← replace with your catalog name
```

**4. Run notebooks in order**
```
00 → 01 → 02 → 03
```
Connect each notebook to **Serverless** compute before running.

---

## 🔍 Data Quality Framework

7 rules applied to every row in the Silver layer:

| Rule | Check | Action on Fail |
|---|---|---|
| `dq_valid_amount` | `amount_usd > 0` | Quarantine |
| `dq_valid_amount_ceiling` | `amount_usd < 50,000` | Quarantine |
| `dq_no_null_txn_id` | `transaction_id IS NOT NULL` | Quarantine |
| `dq_no_null_user` | `user_id IS NOT NULL` | Quarantine |
| `dq_valid_status` | status in (completed, pending, failed) | Quarantine |
| `dq_valid_timestamp` | `txn_timestamp IS NOT NULL` | Quarantine |
| `dq_valid_currency` | currency in (USD, EUR, GBP, CAD) | Quarantine |

Failed rows are **never deleted** — they are written to `silver_quarantine` with a `_quarantine_reason` column explaining exactly which rules failed.

---

## 📈 Gold Layer KPIs

### `gold_merchant_agg` (Grain: merchant + date)
- Total & completed transaction count
- Gross spend in USD
- Average & max transaction value
- Unique users
- Online transaction %
- Failure rate %

### `gold_daily_kpis` (Grain: date)
- Daily revenue (completed transactions only)
- Total transactions
- Active unique users
- Average transaction value
- Failure rate %
- Online share %
- Top spending merchant category

---

## 💡 Key Learnings

- **Medallion Architecture** separates raw, clean, and business-ready data into distinct layers
- **Delta Lake** provides ACID transactions, time travel, and schema enforcement out of the box
- **Data Quality rules** should quarantine bad data rather than silently drop it — every row needs to be accounted for
- **Unity Catalog Volumes** replace DBFS on modern Databricks workspaces for file storage
- **Currency normalisation** at the Silver layer ensures all downstream Gold metrics are comparable

---

## 🗺️ Roadmap

This is **Project 1** of a 6-month data engineering portfolio series:

- ✅ Project 1 — Financial Transactions Lakehouse (Databricks + Delta Lake)
- 🔜 Project 2 — Real-Time NYC Taxi Pipeline (Kafka + Spark)
- 🔜 Project 3 — COVID Data Warehouse (dbt + Snowflake + Airflow)
- 🔜 Project 4 — E-Commerce ETL Pipeline (Python + PostgreSQL)
- 🔜 Project 5 — Weather API Data Lake (AWS S3 + Lambda + Glue)

---

## 👤 Author

**Abhinav Mandal**
- LinkedIn: [linkedin.com/in/YOUR_HANDLE](https://linkedin.com)
- GitHub: [github.com/YOUR_USERNAME](https://github.com)

---

## 📄 License

MIT License — feel free to use this project as a template for your own portfolio.

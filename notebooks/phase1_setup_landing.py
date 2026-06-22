# Databricks notebook source
# MAGIC %md
# MAGIC # 🛒 Phase 1 · Setup + landing zone (simulating the daily file drop)
# MAGIC
# MAGIC **Context (the ticket):** at "NorthPeak Retail", an order export landed every morning
# MAGIC and someone processed it by hand. Here we prepare the ground:
# MAGIC
# MAGIC 1. Create a **Volume** (the landing zone where files "drop").
# MAGIC 2. Fetch the real dataset (Online Retail, UCI) — the full table, all columns.
# MAGIC 3. Split it into **one CSV per day**, simulating the daily exports.
# MAGIC 4. Land them in the Volume, ready for **Auto Loader** to ingest.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0 · Dependencies

# COMMAND ----------

# MAGIC %pip install ucimlrepo

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Configuration

# COMMAND ----------

CATALOG = "workspace"
SCHEMA = "retail_project"
VOLUME = "landing"
LANDING_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/orders"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Create the schema and the Volume (the landing zone)

# COMMAND ----------

import os

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")
os.makedirs(LANDING_PATH, exist_ok=True)

print("✅ Landing zone ready:", LANDING_PATH)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Fetch the real dataset (Online Retail, UCI)
# MAGIC We use `data.original` (the full table) so all 8 columns are present, including
# MAGIC `InvoiceNo`, `StockCode`, and `Description`.

# COMMAND ----------

from ucimlrepo import fetch_ucirepo
import pandas as pd

online_retail = fetch_ucirepo(id=352)   # Online Retail (Dec 2010 – Dec 2011)
data = online_retail.data

if getattr(data, "original", None) is not None:
    df = data.original.copy()
else:
    parts = [
        p for p in [getattr(data, "ids", None), data.features, getattr(data, "targets", None)]
        if p is not None
    ]
    df = pd.concat(parts, axis=1)

print("Rows:", len(df))
print("Columns:", list(df.columns))
df.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Derive each order's day

# COMMAND ----------

df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"], errors="coerce")
df = df.dropna(subset=["InvoiceDate"])
df["order_date"] = df["InvoiceDate"].dt.date

print("Date range:", df["order_date"].min(), "→", df["order_date"].max())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Split into daily files and land them in the Volume
# MAGIC One day = one CSV. This mimics the export that arrived every morning.
# MAGIC *(Note: this generates ~300 small files; it may take a couple of minutes.)*

# COMMAND ----------

dates = sorted(df["order_date"].unique())
print(f"Days with data: {len(dates)}")

count = 0
for d in dates:
    day_df = df[df["order_date"] == d].drop(columns=["order_date"])
    day_df.to_csv(f"{LANDING_PATH}/orders_{d}.csv", index=False)
    count += 1

print(f"✅ {count} daily files written to the landing zone.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · Verify

# COMMAND ----------

files = dbutils.fs.ls(LANDING_PATH)
print(f"Files in the landing zone: {len(files)}")
for f in files[:5]:
    print(" -", f.name, f"({f.size} bytes)")


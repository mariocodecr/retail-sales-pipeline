"""
silver_orders.py
Lakeflow Spark Declarative Pipeline — Silver layer.

Reads the raw bronze table, casts types, computes revenue, and applies data-quality
expectations:
  - DROP rows that would break the sales math (missing keys, non-positive qty/price).
  - WARN (but keep) rows with no customer id — they're still valid revenue.

Design note: rows with Quantity <= 0 are cancellations/returns; we exclude them here so
the sales layer represents completed sales. Returns could be modeled separately.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="silver_orders",
    comment="Cleaned order line items with revenue. Invalid rows dropped; missing customers flagged.",
)
# Drop rows that have no key or break the revenue calculation
@dp.expect_or_drop("valid_invoice", "invoice_no IS NOT NULL")
@dp.expect_or_drop("valid_stock_code", "stock_code IS NOT NULL")
@dp.expect_or_drop("positive_quantity", "quantity > 0")
@dp.expect_or_drop("positive_price", "unit_price > 0")
# Keep but flag rows with no customer id (still valid revenue)
@dp.expect("has_customer_id", "customer_id IS NOT NULL")
def silver_orders():
    return spark.read.table("bronze_orders").select(
        F.col("InvoiceNo").alias("invoice_no"),
        F.col("StockCode").alias("stock_code"),
        F.col("Description").alias("description"),
        F.col("Quantity").cast("int").alias("quantity"),
        F.col("UnitPrice").cast("double").alias("unit_price"),
        F.round(F.col("Quantity") * F.col("UnitPrice"), 2).alias("revenue"),
        F.col("CustomerID").cast("int").alias("customer_id"),
        F.col("Country").alias("country"),
        F.to_timestamp("InvoiceDate").alias("invoice_ts"),
        F.to_date(F.to_timestamp("InvoiceDate")).alias("order_date"),
    )
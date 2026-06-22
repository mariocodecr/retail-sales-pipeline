"""
gold_metrics.py
Lakeflow Spark Declarative Pipeline — Gold layer.

Business-ready materialized views built from silver_orders. The pipeline figures out
the dependency on silver automatically — no manual ordering needed.

  - gold_daily_revenue       -> the automated version of the analyst's manual report
  - gold_top_products        -> best sellers by revenue
  - gold_revenue_by_country  -> revenue and orders per country
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="gold_daily_revenue",
    comment="Total revenue, orders, and units per day — replaces the manual daily report.",
)
def gold_daily_revenue():
    return (
        spark.read.table("silver_orders")
        .groupBy("order_date")
        .agg(
            F.round(F.sum("revenue"), 2).alias("revenue"),
            F.countDistinct("invoice_no").alias("orders"),
            F.sum("quantity").alias("units_sold"),
        )
        .orderBy("order_date")
    )


@dp.materialized_view(
    name="gold_top_products",
    comment="Top 20 products by total revenue.",
)
def gold_top_products():
    return (
        spark.read.table("silver_orders")
        .groupBy("stock_code", "description")
        .agg(
            F.round(F.sum("revenue"), 2).alias("revenue"),
            F.sum("quantity").alias("units_sold"),
        )
        .orderBy(F.desc("revenue"))
        .limit(20)
    )


@dp.materialized_view(
    name="gold_revenue_by_country",
    comment="Revenue and orders by country.",
)
def gold_revenue_by_country():
    return (
        spark.read.table("silver_orders")
        .groupBy("country")
        .agg(
            F.round(F.sum("revenue"), 2).alias("revenue"),
            F.countDistinct("invoice_no").alias("orders"),
        )
        .orderBy(F.desc("revenue"))
    )
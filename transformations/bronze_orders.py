"""
bronze_orders.py
Lakeflow Spark Declarative Pipeline — Bronze layer.

Incrementally ingests the daily order CSVs from the landing volume using Auto Loader.
Auto Loader detects only NEW files on each run (exactly-once via checkpoint), so old
days are never reprocessed. The pipeline manages schema and checkpoint automatically.
"""

from pyspark import pipelines as dp

# Landing zone where the daily order files arrive
LANDING_PATH = "/Volumes/workspace/retail_project/landing/orders"


@dp.table(
    name="bronze_orders",
    comment="Raw orders ingested incrementally from the landing zone via Auto Loader.",
)
def bronze_orders():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(LANDING_PATH)
    )
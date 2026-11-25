from pyspark.sql import SparkSession
from pyspark.sql import functions as F

INPUT_PATH = "s3://spark-apex-freight-data/raw/shipments/"
OUTPUT_PATH = "s3://spark-apex-freight-data/aggregated_output/"

spark = (
    SparkSession.builder.appName("freight-route-metrics")
    # Overwrite only the partitions this run writes, so reruns are safe..
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    .getOrCreate()
)

shipments = spark.read.parquet(INPUT_PATH)

active_shipments = shipments.where(F.col("status") != "CANCELLED")

route_metrics = active_shipments.groupBy(
    "year", "month", "day", "origin_hub", "destination_hub", "route_id"
).agg(
    F.count("shipment_id").alias("total_shipments"),
    F.round(F.sum("weight_lbs"), 2).alias("total_weight_lbs"),
    F.round(F.avg("distance_miles"), 1).alias("avg_distance_miles"),
)

(
    route_metrics.write.mode("overwrite")
    .partitionBy("year", "month", "day")
    .parquet(OUTPUT_PATH)
)

spark.stop()

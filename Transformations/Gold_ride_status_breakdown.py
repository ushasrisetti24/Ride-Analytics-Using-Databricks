from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(comment="Ride status counts per day")
def gold_ride_status_breakdown():
    return (
        spark.read.table("silver_ride_event")
        .groupBy("event_date", "status")
        .agg(F.count("ride_id").alias("ride_count"),
            F.round(F.col("ride_count") * 100.0 / F.sum("ride_count") OVER (PARTITION BY F.col("event_date"), 1).alias("pct_of_total"))
        
    )
    )
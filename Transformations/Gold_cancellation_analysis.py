from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    comment= "Cancelled rides breakdown by date, city and reason for root cause analysis of cancellation of rides"
)
def gold_cancellation_analysis():
    return (
        spark.read.table("silver_ride_event")
        .filter(F.col("is_cancelled")==True)
        .groupBy("event_date","city","status","cancellation_reason")
        .agg(
            F.count("ride_id").alias("cancellation_count"),
            F.round(F.avg("surge_multiplier"),2).alias("avg_sruge_at_cancel"),
            F.round(F.avg("distance_km"),1).alias("avg_distance_km")

        )
        .orderBy(F.col("cancellation_count").desc())
    )
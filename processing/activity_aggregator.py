"""
Vessel daily activity aggregator: compute per-vessel per-day statistics.
"""

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from config.paths import PathConfig

logger = logging.getLogger("ais_pipeline.activity_aggregator")


def build_daily_activity(spark: SparkSession, silver_df: DataFrame) -> DataFrame:
    """
    Aggregate vessel positions into daily activity summaries.

    Metrics per (mmsi, activity_date):
        - point_count
        - avg_sog, max_sog, min_sog
        - first_seen_time, last_seen_time

    Args:
        spark: SparkSession.
        silver_df: Silver layer DataFrame with event_time, mmsi, sog.

    Returns:
        Daily activity DataFrame.
    """
    logger.info("Building vessel daily activity")

    df = silver_df.withColumn("activity_date", F.to_date("event_time"))

    activity = df.groupBy("mmsi", "activity_date").agg(
        F.count("*").alias("point_count"),
        F.round(F.avg("sog"), 2).alias("avg_sog"),
        F.round(F.max("sog"), 2).alias("max_sog"),
        F.round(F.min("sog"), 2).alias("min_sog"),
        F.min("event_time").alias("first_seen_time"),
        F.max("event_time").alias("last_seen_time"),
    )

    row_count = activity.count()
    logger.info("Daily activity: %d vessel-day records", row_count)

    # Write — overwrite with dynamic partition mode only replaces the current day.
    output_path = str(PathConfig.GOLD_ACTIVITY_DIR)
    activity.write.mode("overwrite").partitionBy("activity_date").parquet(output_path)
    logger.info("Activity data written to %s", output_path)

    return activity

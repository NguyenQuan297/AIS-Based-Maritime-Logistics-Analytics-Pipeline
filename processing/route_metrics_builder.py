"""
Route metrics builder: aggregate statistics across voyage candidates.
"""

import logging
from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from config.paths import PathConfig

logger = logging.getLogger("ais_pipeline.route_metrics_builder")


def build_route_metrics(spark: SparkSession, voyages_df: DataFrame) -> DataFrame:
    """
    Compute route-level metrics from voyage candidates.

    Groups voyages by route type and date, computing:
        - vessel_count
        - avg_duration_hours
        - avg_speed
        - total point_count

    Args:
        spark: SparkSession.
        voyages_df: Voyage candidates DataFrame.

    Returns:
        Route metrics DataFrame.
    """
    logger.info("Building route metrics")

    # Add metric_date from start_time
    df = voyages_df.withColumn("metric_date", F.to_date("start_time"))

    metrics = df.groupBy("candidate_route_type", "metric_date").agg(
        F.countDistinct("mmsi").alias("vessel_count"),
        F.round(F.avg("duration_hours"), 2).alias("avg_duration_hours"),
        F.round(F.avg("avg_sog"), 2).alias("avg_speed"),
        F.sum("point_count").alias("point_count"),
    )

    # Add route_id (hash of route type + date for uniqueness)
    metrics = metrics.withColumn(
        "route_id",
        F.sha2(F.concat_ws("_", "candidate_route_type", "metric_date"), 256),
    )

    # Reorder columns
    metrics = metrics.select(
        "route_id",
        "metric_date",
        "candidate_route_type",
        "vessel_count",
        "avg_duration_hours",
        "avg_speed",
        "point_count",
    )

    row_count = metrics.count()
    logger.info("Route metrics: %d records", row_count)

    # Write
    output_path = str(PathConfig.GOLD_ROUTE_DIR)
    metrics.write.mode("append").partitionBy("metric_date").parquet(output_path)
    logger.info("Route metrics written to %s", output_path)

    return metrics

"""
Voyage candidate builder: identify potential voyage segments from vessel positions.
"""

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from config.paths import PathConfig
from utils.geo_utils import haversine_udf

logger = logging.getLogger("ais_pipeline.voyage_candidate_builder")

# Time gap threshold to split voyages (hours)
VOYAGE_GAP_HOURS = 4.0
# Minimum points required to consider a segment a voyage
MIN_VOYAGE_POINTS = 10


def build_voyage_candidates(spark: SparkSession, silver_df: DataFrame) -> DataFrame:
    """
    Identify voyage candidate segments from silver vessel positions.

    Logic:
        1. Order positions by (mmsi, event_time)
        2. Compute time gap between consecutive positions
        3. Flag new voyage when gap exceeds threshold
        4. Group consecutive positions into voyage segments
        5. Compute voyage-level statistics

    Args:
        spark: SparkSession.
        silver_df: Silver layer DataFrame.

    Returns:
        Voyage candidates DataFrame.
    """
    logger.info("Building voyage candidates")

    # Window for consecutive position analysis
    vessel_window = Window.partitionBy("mmsi").orderBy("event_time")

    # Compute time gap and previous position
    df = silver_df.withColumn("prev_time", F.lag("event_time").over(vessel_window))
    df = df.withColumn(
        "time_gap_hours",
        (F.unix_timestamp("event_time") - F.unix_timestamp("prev_time")) / 3600.0,
    )

    # Flag new voyage segments
    df = df.withColumn(
        "new_voyage",
        F.when(
            F.col("time_gap_hours").isNull() | (F.col("time_gap_hours") > VOYAGE_GAP_HOURS),
            F.lit(1),
        ).otherwise(F.lit(0)),
    )

    # Cumulative sum to create voyage IDs
    df = df.withColumn(
        "voyage_id",
        F.sum("new_voyage").over(vessel_window),
    )

    # Aggregate voyage segments.
    # min_by/max_by pick the position at min/max event_time — deterministic,
    # unlike F.first/F.last which don't respect any ordering after groupBy.
    voyages = df.groupBy("mmsi", "voyage_id").agg(
        F.min("event_time").alias("start_time"),
        F.max("event_time").alias("end_time"),
        F.min_by("longitude", "event_time").alias("start_longitude"),
        F.min_by("latitude", "event_time").alias("start_latitude"),
        F.max_by("longitude", "event_time").alias("end_longitude"),
        F.max_by("latitude", "event_time").alias("end_latitude"),
        F.count("*").alias("point_count"),
        F.round(F.avg("sog"), 2).alias("avg_sog"),
    )

    # Compute duration
    voyages = voyages.withColumn(
        "duration_hours",
        F.round(
            (F.unix_timestamp("end_time") - F.unix_timestamp("start_time")) / 3600.0,
            2,
        ),
    )

    # Classify voyage type
    voyages = voyages.withColumn(
        "candidate_route_type",
        F.when(F.col("avg_sog") < 0.5, "stationary")
        .when(F.col("duration_hours") < 1.0, "short_move")
        .when(F.col("duration_hours") < 12.0, "coastal")
        .otherwise("transit"),
    )

    # Filter minimum points
    voyages = voyages.where(F.col("point_count") >= MIN_VOYAGE_POINTS)

    # Drop internal voyage_id; add start_date partition column
    voyages = voyages.drop("voyage_id")
    voyages = voyages.withColumn("start_date", F.to_date("start_time"))

    row_count = voyages.count()
    logger.info("Voyage candidates: %d segments", row_count)

    # Write. overwrite + dynamic partitionOverwriteMode only replaces partitions
    # for dates present in this run (set in spark_session), keeping prior days.
    output_path = str(PathConfig.GOLD_VOYAGE_DIR)
    voyages.write.mode("overwrite").partitionBy("start_date").parquet(output_path)
    logger.info("Voyage candidates written to %s", output_path)

    return voyages

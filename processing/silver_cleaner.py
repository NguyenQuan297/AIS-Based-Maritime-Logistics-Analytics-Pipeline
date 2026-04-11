"""
Silver layer builder: cleaned, typed, deduplicated vessel positions.
"""

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, LongType

from config.paths import PathConfig
from config.settings import Settings
from utils.time_utils import normalize_timestamp_column
from utils.validation import validate_row

logger = logging.getLogger("ais_pipeline.silver_cleaner")


def build_silver(spark: SparkSession, bronze_df: DataFrame, source_date: str) -> DataFrame:
    """
    Transform bronze data into silver: clean, type, deduplicate.

    Steps:
        1. Normalize timestamp
        2. Cast columns to correct types
        3. Select only needed columns
        4. Remove nulls on critical fields
        5. Validate coordinates, SOG
        6. Remove duplicates by (mmsi, event_time)

    Args:
        spark: SparkSession.
        bronze_df: Bronze layer DataFrame.
        source_date: Date string for partition.

    Returns:
        Cleaned silver DataFrame.
    """
    logger.info("Building silver layer for %s", source_date)

    # Step 1: Normalize timestamp
    df = normalize_timestamp_column(bronze_df, "base_date_time")

    # Step 2: Cast columns
    df = (
        df.withColumn("mmsi", F.col("mmsi").cast(LongType()))
        .withColumn("latitude", F.col("latitude").cast(DoubleType()))
        .withColumn("longitude", F.col("longitude").cast(DoubleType()))
        .withColumn("sog", F.col("sog").cast(DoubleType()))
        .withColumn("cog", F.col("cog").cast(DoubleType()))
        .withColumn("heading", F.col("heading").cast(DoubleType()))
        .withColumn("vessel_type", F.col("vessel_type").cast(IntegerType()))
        .withColumn("status", F.col("status").cast(IntegerType()))
        .withColumn("draft", F.col("draft").cast(DoubleType()))
        .withColumn("cargo", F.col("cargo").cast(IntegerType()))
    )

    # Step 3: Select columns
    df = df.select(
        "mmsi",
        "event_time",
        "latitude",
        "longitude",
        "sog",
        "cog",
        "heading",
        "vessel_type",
        "status",
        "draft",
        "cargo",
    )

    # Step 4: Remove nulls on critical fields
    df = df.where(
        F.col("mmsi").isNotNull()
        & F.col("event_time").isNotNull()
        & F.col("latitude").isNotNull()
        & F.col("longitude").isNotNull()
    )

    # Step 5: Validate
    df = validate_row(df)

    # Step 6: Deduplicate
    df = df.dropDuplicates(["mmsi", "event_time"])

    # Add source_date for partitioning
    df = df.withColumn("source_date", F.lit(source_date))

    # Write silver parquet (count happens during write, no extra scan)
    output_path = str(PathConfig.SILVER_DIR)
    df.write.mode("append").partitionBy("source_date").parquet(output_path)
    logger.info("Silver data written to %s", output_path)

    return df

"""
Bronze layer reader: read raw normalized parquet from bronze storage.
Provides filtered access to ingested bronze data.
"""

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from config.paths import PathConfig

logger = logging.getLogger("ais_pipeline.bronze_reader")


def build_bronze(spark: SparkSession, source_date: str = None) -> DataFrame:
    """
    Read bronze parquet data, optionally filtered by source_date.

    Args:
        spark: SparkSession.
        source_date: Optional date filter (YYYY-MM-DD).

    Returns:
        Bronze DataFrame.
    """
    logger.info("Reading bronze layer from %s", PathConfig.BRONZE_DIR)

    df = spark.read.parquet(str(PathConfig.BRONZE_DIR))

    if source_date:
        parts = source_date.split("-")
        df = df.where(
            (F.col("year") == int(parts[0]))
            & (F.col("month") == int(parts[1]))
            & (F.col("day") == int(parts[2]))
        )
        logger.info("Filtered bronze to source_date=%s", source_date)

    row_count = df.count()
    logger.info("Bronze layer: %d rows", row_count)
    return df

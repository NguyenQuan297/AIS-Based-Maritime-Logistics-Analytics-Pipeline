"""
Vessel metadata builder: extract relatively stable vessel information.
"""

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from config.paths import PathConfig

logger = logging.getLogger("ais_pipeline.metadata_builder")


def build_vessel_metadata(spark: SparkSession, bronze_df: DataFrame) -> DataFrame:
    """
    Extract vessel metadata from bronze data.
    Takes the most recent non-null values for each vessel (by MMSI).

    Args:
        spark: SparkSession.
        bronze_df: Bronze layer DataFrame with all columns.

    Returns:
        Vessel metadata DataFrame.
    """
    logger.info("Building vessel metadata")

    metadata_cols = [
        "mmsi", "vessel_name", "imo", "call_sign",
        "vessel_type", "length", "width", "transceiver",
    ]

    available_cols = [c for c in metadata_cols if c in bronze_df.columns]
    df = bronze_df.select(*available_cols).where(F.col("mmsi").isNotNull())

    # For each vessel, pick the row with most non-null fields
    df = df.withColumn(
        "non_null_count",
        sum(F.when(F.col(c).isNotNull(), 1).otherwise(0) for c in available_cols if c != "mmsi"),
    )

    from pyspark.sql.window import Window

    window = Window.partitionBy("mmsi").orderBy(F.desc("non_null_count"))
    df = df.withColumn("rank", F.row_number().over(window))
    df = df.where(F.col("rank") == 1).drop("rank", "non_null_count")

    # Deduplicate
    df = df.dropDuplicates(["mmsi"])

    row_count = df.count()
    logger.info("Vessel metadata: %d unique vessels", row_count)

    # Write
    output_path = str(PathConfig.GOLD_METADATA_DIR)
    df.write.mode("overwrite").parquet(output_path)
    logger.info("Metadata written to %s", output_path)

    return df

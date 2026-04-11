"""
Spark session factory for the AIS pipeline.
"""

import logging

from pyspark.sql import SparkSession

from config.settings import Settings

logger = logging.getLogger("ais_pipeline.spark_session")


def get_spark_session(app_name: str = None) -> SparkSession:
    """
    Create or retrieve a SparkSession configured for the AIS pipeline.

    Args:
        app_name: Optional override for Spark application name.

    Returns:
        SparkSession instance.
    """
    app_name = app_name or Settings.SPARK_APP_NAME

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[4]")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .config("spark.sql.parquet.compression.codec", Settings.PARQUET_COMPRESSION)
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        # Reduce shuffle partitions to avoid Windows file handle exhaustion
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "4")
        # Reduce open file pressure on Windows
        .config("spark.shuffle.file.buffer", "64k")
        .config("spark.reducer.maxSizeInFlight", "24m")
        .config("spark.shuffle.sort.bypassMergeThreshold", "2")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession created: %s (master=%s)", app_name, Settings.SPARK_MASTER)

    return spark

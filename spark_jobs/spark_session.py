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
        .master(Settings.SPARK_MASTER)
        .config("spark.driver.memory", Settings.SPARK_DRIVER_MEMORY)
        .config("spark.executor.memory", Settings.SPARK_EXECUTOR_MEMORY)
        .config("spark.sql.parquet.compression.codec", Settings.PARQUET_COMPRESSION)
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession created: %s (master=%s)", app_name, Settings.SPARK_MASTER)

    return spark

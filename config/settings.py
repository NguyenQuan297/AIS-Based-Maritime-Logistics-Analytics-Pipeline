"""
Central configuration for the AIS Maritime Logistics Analytics Pipeline.
Loads environment variables from .env file.
"""

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")


class Settings:
    """Pipeline-wide settings."""

    # Spark
    SPARK_APP_NAME = "AIS_Maritime_Pipeline"
    SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")
    SPARK_DRIVER_MEMORY = os.getenv("SPARK_DRIVER_MEMORY", "4g")
    SPARK_EXECUTOR_MEMORY = os.getenv("SPARK_EXECUTOR_MEMORY", "4g")

    # PostgreSQL
    PG_HOST = os.getenv("PG_HOST", "localhost")
    PG_PORT = int(os.getenv("PG_PORT", "5432"))
    PG_DB = os.getenv("PG_DB", "ais_analytics")
    PG_USER = os.getenv("PG_USER", "postgres")
    PG_PASSWORD = os.getenv("PG_PASSWORD", "postgres")

    @classmethod
    def pg_jdbc_url(cls) -> str:
        return f"jdbc:postgresql://{cls.PG_HOST}:{cls.PG_PORT}/{cls.PG_DB}"

    @classmethod
    def pg_connection_props(cls) -> dict:
        return {
            "user": cls.PG_USER,
            "password": cls.PG_PASSWORD,
            "driver": "org.postgresql.Driver",
        }

    # AWS S3 (for cloud deployment)
    S3_BUCKET = os.getenv("S3_BUCKET", "ais-maritime-data")
    S3_REGION = os.getenv("S3_REGION", "us-east-1")

    # Data processing
    BATCH_SIZE = int(os.getenv("BATCH_SIZE", "500000"))
    PARQUET_COMPRESSION = "snappy"
    PARQUET_ROW_GROUP_SIZE = 128 * 1024 * 1024  # 128 MB

    # AIS schema - columns to keep for silver layer
    SILVER_COLUMNS = [
        "mmsi",
        "base_date_time",
        "latitude",
        "longitude",
        "sog",
        "cog",
        "heading",
        "vessel_type",
        "status",
        "draft",
        "cargo",
    ]

    METADATA_COLUMNS = [
        "mmsi",
        "vessel_name",
        "imo",
        "call_sign",
        "vessel_type",
        "length",
        "width",
        "transceiver",
    ]

    # Validation thresholds
    LAT_MIN = -90.0
    LAT_MAX = 90.0
    LON_MIN = -180.0
    LON_MAX = 180.0
    SOG_MAX = 102.3  # Max valid SOG in knots (AIS spec)
    COG_MAX = 360.0

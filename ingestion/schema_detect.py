"""
AIS data schema definition and detection.
"""

import logging

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

logger = logging.getLogger("ais_pipeline.schema_detect")

# AIS schema matching actual MarineCadastre CSV headers (already snake_case)
AIS_SCHEMA = StructType([
    StructField("mmsi", LongType(), True),
    StructField("base_date_time", StringType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("sog", DoubleType(), True),
    StructField("cog", DoubleType(), True),
    StructField("heading", DoubleType(), True),
    StructField("vessel_name", StringType(), True),
    StructField("imo", StringType(), True),
    StructField("call_sign", StringType(), True),
    StructField("vessel_type", IntegerType(), True),
    StructField("status", IntegerType(), True),
    StructField("length", DoubleType(), True),
    StructField("width", DoubleType(), True),
    StructField("draft", DoubleType(), True),
    StructField("cargo", IntegerType(), True),
    StructField("transceiver", StringType(), True),
])

# Column name mapping: raw -> normalized
# Current MarineCadastre files already use snake_case headers,
# so this mapping is identity. Kept for backward compatibility
# with older datasets that may use PascalCase headers.
COLUMN_MAPPING = {
    "MMSI": "mmsi",
    "BaseDateTime": "base_date_time",
    "LAT": "latitude",
    "LON": "longitude",
    "SOG": "sog",
    "COG": "cog",
    "Heading": "heading",
    "VesselName": "vessel_name",
    "IMO": "imo",
    "CallSign": "call_sign",
    "VesselType": "vessel_type",
    "Status": "status",
    "Length": "length",
    "Width": "width",
    "Draft": "draft",
    "Cargo": "cargo",
    "TransceiverClass": "transceiver",
}


def get_ais_schema() -> StructType:
    """Return the standard AIS schema."""
    return AIS_SCHEMA


def detect_schema_from_sample(pdf) -> dict:
    """
    Detect schema from a pandas DataFrame sample.

    Args:
        pdf: Pandas DataFrame from read_zst_sample_pandas.

    Returns:
        Dict with column names as keys and inferred types as values.
    """
    schema_info = {}
    for col in pdf.columns:
        dtype = str(pdf[col].dtype)
        null_count = int(pdf[col].isnull().sum())
        unique_count = int(pdf[col].nunique())
        schema_info[col] = {
            "dtype": dtype,
            "null_count": null_count,
            "null_rate": round(null_count / len(pdf), 4),
            "unique_count": unique_count,
            "sample_values": pdf[col].dropna().head(3).tolist(),
        }

    logger.info("Detected %d columns in sample", len(schema_info))
    return schema_info

"""
Timestamp parsing and normalization utilities.
"""

from datetime import datetime
from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType


AIS_TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss"


def parse_ais_timestamp(ts_string: str) -> Optional[datetime]:
    """Parse a single AIS timestamp string to datetime."""
    formats = [
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(ts_string.strip(), fmt)
        except (ValueError, AttributeError):
            continue
    return None


def normalize_timestamp_column(df: DataFrame, col_name: str = "BaseDateTime") -> DataFrame:
    """
    Convert AIS timestamp column to proper TimestampType in Spark DataFrame.
    Tries multiple formats to handle inconsistencies.
    """
    return df.withColumn(
        "event_time",
        F.coalesce(
            F.to_timestamp(F.col(col_name), "yyyy-MM-dd'T'HH:mm:ss"),
            F.to_timestamp(F.col(col_name), "yyyy-MM-dd HH:mm:ss"),
        ).cast(TimestampType()),
    )

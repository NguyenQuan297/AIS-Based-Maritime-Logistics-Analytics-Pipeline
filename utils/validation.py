"""
Data validation functions for AIS records.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from config.settings import Settings


def validate_coordinates(df: DataFrame) -> DataFrame:
    """Filter rows with valid latitude and longitude."""
    return df.where(
        (F.col("latitude").between(Settings.LAT_MIN, Settings.LAT_MAX))
        & (F.col("longitude").between(Settings.LON_MIN, Settings.LON_MAX))
    )


def validate_sog(df: DataFrame) -> DataFrame:
    """Filter rows with valid speed over ground. Null SOG is allowed (AIS sentinel)."""
    return df.where(
        F.col("sog").isNull()
        | ((F.col("sog") >= 0) & (F.col("sog") < Settings.SOG_MAX))
    )


def validate_cog(df: DataFrame) -> DataFrame:
    """Filter rows with valid course over ground."""
    return df.where(
        (F.col("cog") >= 0) & (F.col("cog") <= Settings.COG_MAX)
    )


def validate_mmsi(df: DataFrame) -> DataFrame:
    """Filter rows with valid MMSI (9-digit identifier)."""
    return df.where(
        F.col("mmsi").isNotNull()
        & (F.col("mmsi") > 99999999)
        & (F.col("mmsi") < 1000000000)
    )


def validate_row(df: DataFrame) -> DataFrame:
    """Apply all validation filters to a DataFrame."""
    df = validate_mmsi(df)
    df = validate_coordinates(df)
    df = validate_sog(df)
    return df


def compute_quality_report(df: DataFrame, stage: str) -> dict:
    """Compute data quality metrics for a DataFrame."""
    total = df.count()
    null_counts = {}
    for col_name in df.columns:
        null_count = df.where(F.col(col_name).isNull()).count()
        null_counts[col_name] = null_count

    return {
        "stage": stage,
        "total_rows": total,
        "null_counts": null_counts,
        "null_rates": {k: round(v / total, 4) if total > 0 else 0 for k, v in null_counts.items()},
    }

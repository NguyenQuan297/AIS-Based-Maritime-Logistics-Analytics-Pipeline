"""
Geospatial utility functions for AIS position data.
"""

import math
from typing import Tuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

EARTH_RADIUS_NM = 3440.065  # Earth radius in nautical miles


def haversine_distance(
    lat1: float, lon1: float, lat2: float, lon2: float
) -> float:
    """
    Calculate the great-circle distance between two points in nautical miles.
    """
    lat1_r, lon1_r = math.radians(lat1), math.radians(lon1)
    lat2_r, lon2_r = math.radians(lat2), math.radians(lon2)

    dlat = lat2_r - lat1_r
    dlon = lon2_r - lon1_r

    a = math.sin(dlat / 2) ** 2 + math.cos(lat1_r) * math.cos(lat2_r) * math.sin(dlon / 2) ** 2
    c = 2 * math.asin(math.sqrt(a))

    return EARTH_RADIUS_NM * c


def estimate_is_moving(sog: float, threshold: float = 0.5) -> bool:
    """Determine if a vessel is moving based on speed over ground."""
    return sog > threshold


def haversine_udf():
    """
    Return a Spark SQL expression for haversine distance between consecutive points.
    Expects columns: latitude, longitude, prev_latitude, prev_longitude.
    Returns distance in nautical miles.
    """
    return (
        F.lit(2 * EARTH_RADIUS_NM)
        * F.asin(
            F.sqrt(
                F.pow(F.sin((F.radians(F.col("latitude")) - F.radians(F.col("prev_latitude"))) / 2), 2)
                + F.cos(F.radians(F.col("prev_latitude")))
                * F.cos(F.radians(F.col("latitude")))
                * F.pow(F.sin((F.radians(F.col("longitude")) - F.radians(F.col("prev_longitude"))) / 2), 2)
            )
        )
    )


def add_distance_column(df: DataFrame) -> DataFrame:
    """
    Add distance_nm column calculated from consecutive positions.
    DataFrame must have prev_latitude and prev_longitude columns.
    """
    return df.withColumn("distance_nm", haversine_udf())

"""
Tests for data validation functions.
"""

import pytest

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, DoubleType

from utils.validation import validate_coordinates, validate_sog, validate_mmsi, validate_row


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("test_validation")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def sample_df(spark):
    schema = StructType([
        StructField("mmsi", LongType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("sog", DoubleType(), True),
    ])

    data = [
        (123456789, 29.5, -89.5, 10.0),    # Valid
        (987654321, 91.0, -89.5, 10.0),     # Invalid latitude
        (111222333, 29.5, -181.0, 10.0),    # Invalid longitude
        (444555666, 29.5, -89.5, 200.0),    # Invalid SOG
        (None, 29.5, -89.5, 10.0),          # Null MMSI
        (12345, 29.5, -89.5, 10.0),         # MMSI too short
    ]

    return spark.createDataFrame(data, schema)


class TestValidation:

    def test_validate_coordinates(self, sample_df):
        result = validate_coordinates(sample_df)
        assert result.count() == 4  # 2 invalid coords filtered

    def test_validate_sog(self, sample_df):
        result = validate_sog(sample_df)
        assert result.count() == 5  # 1 invalid SOG filtered

    def test_validate_mmsi(self, sample_df):
        result = validate_mmsi(sample_df)
        assert result.count() == 4  # Null and too-short filtered

    def test_validate_row_combined(self, sample_df):
        result = validate_row(sample_df)
        assert result.count() == 1  # Only the fully valid row

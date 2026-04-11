"""
Tests for AIS schema detection.
"""

import pytest
from pyspark.sql.types import StructType

from ingestion.schema_detect import AIS_SCHEMA, COLUMN_MAPPING, get_ais_schema, detect_schema_from_sample


class TestAISSchema:
    """Test schema definitions."""

    def test_schema_is_struct_type(self):
        assert isinstance(AIS_SCHEMA, StructType)

    def test_schema_has_expected_fields(self):
        field_names = {f.name for f in AIS_SCHEMA.fields}
        expected = {"mmsi", "base_date_time", "latitude", "longitude", "sog", "cog", "heading"}
        assert expected.issubset(field_names)

    def test_column_mapping_has_all_normalized_names(self):
        schema_fields = {f.name for f in AIS_SCHEMA.fields}
        mapping_values = set(COLUMN_MAPPING.values())
        assert schema_fields == mapping_values

    def test_get_ais_schema_returns_same(self):
        assert get_ais_schema() == AIS_SCHEMA


class TestSchemaDetection:
    """Test schema detection from sample data."""

    def test_detect_schema_from_pandas(self):
        import pandas as pd

        pdf = pd.DataFrame({
            "MMSI": [123456789, 987654321],
            "BaseDateTime": ["2025-08-15T00:00:00", "2025-08-15T00:01:00"],
            "LAT": [29.5, 30.1],
            "LON": [-89.5, -88.2],
        })

        schema_info = detect_schema_from_sample(pdf)
        assert "MMSI" in schema_info
        assert "LAT" in schema_info
        assert schema_info["MMSI"]["null_count"] == 0

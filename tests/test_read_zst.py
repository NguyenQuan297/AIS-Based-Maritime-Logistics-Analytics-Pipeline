"""
Tests for .zst file reading functionality.
"""

import pytest
from pathlib import Path

from config.paths import PathConfig
from utils.file_utils import list_zst_files, extract_date_from_filename


class TestFileDiscovery:
    """Test file discovery and parsing utilities."""

    def test_extract_date_from_valid_filename(self):
        path = Path("data/ais-2025-08-15.csv.zst")
        assert extract_date_from_filename(path) == "2025-08-15"

    def test_extract_date_from_invalid_filename(self):
        path = Path("data/random_file.csv")
        assert extract_date_from_filename(path) is None

    def test_extract_date_multiple_formats(self):
        path = Path("ais-2025-01-01.csv.zst")
        assert extract_date_from_filename(path) == "2025-01-01"

    def test_list_zst_files_returns_sorted(self):
        source_dir = PathConfig.RAW_DIR
        if source_dir.exists():
            files = list_zst_files(source_dir)
            assert files == sorted(files)

    def test_list_zst_files_empty_dir(self, tmp_path):
        files = list_zst_files(tmp_path)
        assert files == []


class TestReadZstSample:
    """Test pandas-based sample reading."""

    @pytest.fixture
    def sample_file(self):
        source_dir = PathConfig.RAW_DIR
        files = list_zst_files(source_dir)
        if not files:
            pytest.skip("No .zst files available for testing")
        return files[0]

    def test_read_sample_returns_dataframe(self, sample_file):
        from ingestion.read_zst import read_zst_sample_pandas

        df = read_zst_sample_pandas(sample_file, nrows=100)
        assert len(df) > 0
        assert len(df.columns) > 0

    def test_read_sample_has_expected_columns(self, sample_file):
        from ingestion.read_zst import read_zst_sample_pandas

        df = read_zst_sample_pandas(sample_file, nrows=100)
        expected = {"MMSI", "BaseDateTime", "LAT", "LON", "SOG", "COG"}
        assert expected.issubset(set(df.columns))

    def test_read_sample_respects_nrows(self, sample_file):
        from ingestion.read_zst import read_zst_sample_pandas

        df = read_zst_sample_pandas(sample_file, nrows=50)
        assert len(df) <= 50

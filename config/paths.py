"""
Path configuration for data lake layers.
"""

import os
from pathlib import Path


class PathConfig:
    """Manages all data path references for local and S3 modes."""

    BASE_DIR = Path(os.getenv("DATA_BASE_DIR", Path(__file__).resolve().parent.parent / "data"))

    # Data lake layers
    RAW_DIR = BASE_DIR / "raw" / "ais"
    BRONZE_DIR = BASE_DIR / "bronze"
    SILVER_DIR = BASE_DIR / "silver"
    GOLD_DIR = BASE_DIR / "gold"
    SAMPLES_DIR = BASE_DIR / "samples"

    # Gold sub-paths
    GOLD_ACTIVITY_DIR = GOLD_DIR / "vessel_daily_activity"
    GOLD_VOYAGE_DIR = GOLD_DIR / "voyage_candidates"
    GOLD_ROUTE_DIR = GOLD_DIR / "route_metrics"
    GOLD_METADATA_DIR = GOLD_DIR / "vessel_metadata"

    @classmethod
    def bronze_partition_path(cls, year: int, month: int, day: int) -> Path:
        return cls.BRONZE_DIR / f"year={year}" / f"month={month:02d}" / f"day={day:02d}"

    @classmethod
    def silver_partition_path(cls, source_date: str) -> Path:
        return cls.SILVER_DIR / f"source_date={source_date}"

    @classmethod
    def ensure_dirs(cls):
        """Create all data directories if they don't exist."""
        for d in [
            cls.RAW_DIR,
            cls.BRONZE_DIR,
            cls.SILVER_DIR,
            cls.GOLD_DIR,
            cls.SAMPLES_DIR,
            cls.GOLD_ACTIVITY_DIR,
            cls.GOLD_VOYAGE_DIR,
            cls.GOLD_ROUTE_DIR,
            cls.GOLD_METADATA_DIR,
        ]:
            d.mkdir(parents=True, exist_ok=True)

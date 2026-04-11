"""
File system utilities for discovering and managing AIS data files.
"""

import re
from pathlib import Path
from typing import List, Optional


def list_zst_files(directory: Path, pattern: str = "ais-*.csv.zst") -> List[Path]:
    """Return sorted list of .csv.zst files in a directory."""
    files = sorted(directory.glob(pattern))
    return files


def extract_date_from_filename(filepath: Path) -> Optional[str]:
    """
    Extract date string from AIS filename.
    Example: ais-2025-08-15.csv.zst -> '2025-08-15'
    """
    match = re.search(r"(\d{4}-\d{2}-\d{2})", filepath.name)
    return match.group(1) if match else None


def parse_date_parts(date_str: str) -> dict:
    """
    Parse date string into year, month, day components.
    Example: '2025-08-15' -> {'year': 2025, 'month': 8, 'day': 15}
    """
    parts = date_str.split("-")
    return {
        "year": int(parts[0]),
        "month": int(parts[1]),
        "day": int(parts[2]),
    }


def get_file_size_mb(filepath: Path) -> float:
    """Return file size in megabytes."""
    return filepath.stat().st_size / (1024 * 1024)

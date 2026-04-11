"""
Download AIS daily files from MarineCadastre.
This module provides helpers for fetching raw .csv.zst files.
"""

import logging
from pathlib import Path

import requests

logger = logging.getLogger("ais_pipeline.download")

BASE_URL = "https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2025"


def download_ais_file(date_str: str, output_dir: Path) -> Path:
    """
    Download a single AIS daily file.

    Args:
        date_str: Date in YYYY-MM-DD format.
        output_dir: Directory to save the downloaded file.

    Returns:
        Path to the downloaded file.
    """
    filename = f"ais-{date_str}.csv.zst"
    url = f"{BASE_URL}/{filename}"
    output_path = output_dir / filename

    if output_path.exists():
        logger.info("File already exists: %s", output_path)
        return output_path

    logger.info("Downloading %s ...", url)
    output_dir.mkdir(parents=True, exist_ok=True)

    response = requests.get(url, stream=True, timeout=300)
    response.raise_for_status()

    with open(output_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

    logger.info("Downloaded: %s (%.1f MB)", output_path, output_path.stat().st_size / 1e6)
    return output_path


def download_date_range(start_date: str, end_date: str, output_dir: Path) -> list:
    """
    Download AIS files for a range of dates.

    Args:
        start_date: Start date in YYYY-MM-DD format.
        end_date: End date in YYYY-MM-DD format.
        output_dir: Directory to save downloaded files.

    Returns:
        List of downloaded file paths.
    """
    from datetime import datetime, timedelta

    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    downloaded = []
    current = start
    while current <= end:
        date_str = current.strftime("%Y-%m-%d")
        try:
            path = download_ais_file(date_str, output_dir)
            downloaded.append(path)
        except Exception as e:
            logger.error("Failed to download %s: %s", date_str, e)
        current += timedelta(days=1)

    return downloaded

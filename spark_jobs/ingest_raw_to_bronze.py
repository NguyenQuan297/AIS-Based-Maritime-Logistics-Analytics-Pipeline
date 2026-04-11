"""
Spark job: Ingest raw .csv.zst files into bronze parquet layer.

Usage:
    spark-submit spark_jobs/ingest_raw_to_bronze.py [--file-limit N]
"""

import argparse
import logging
import sys

from config.logging_config import setup_logging
from config.paths import PathConfig
from ingestion.batch_ingest import ingest_all_files
from spark_jobs.spark_session import get_spark_session

logger = setup_logging()


def main():
    parser = argparse.ArgumentParser(description="Ingest raw AIS .zst files to bronze parquet")
    parser.add_argument("--file-limit", type=int, default=None, help="Max files to process")
    parser.add_argument("--source-dir", type=str, default=None, help="Override source directory")
    args = parser.parse_args()

    PathConfig.ensure_dirs()

    spark = get_spark_session("AIS_Ingest_Raw_To_Bronze")

    try:
        source_dir = PathConfig.RAW_DIR
        if args.source_dir:
            from pathlib import Path
            source_dir = Path(args.source_dir)

        results = ingest_all_files(
            spark=spark,
            source_dir=source_dir,
            file_limit=args.file_limit,
        )

        logger.info("Ingestion job complete: %d files processed", len(results))

    except Exception as e:
        logger.error("Ingestion job failed: %s", e, exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

"""
Spark job: Build voyage candidates gold table from silver data.

Usage:
    spark-submit spark_jobs/silver_to_gold_voyage.py --source-date 2025-08-15
"""

import argparse
import logging
import sys

from config.logging_config import setup_logging
from config.paths import PathConfig
from processing.voyage_candidate_builder import build_voyage_candidates
from spark_jobs.spark_session import get_spark_session

logger = setup_logging()


def main():
    parser = argparse.ArgumentParser(description="Build gold voyage candidates table")
    parser.add_argument("--source-date", type=str, required=True, help="Date to process (YYYY-MM-DD)")
    args = parser.parse_args()

    PathConfig.ensure_dirs()

    spark = get_spark_session("AIS_Silver_To_Gold_Voyage")

    try:
        silver_df = spark.read.parquet(str(PathConfig.SILVER_DIR)).where(
            f"source_date = '{args.source_date}'"
        )

        voyages_df = build_voyage_candidates(spark, silver_df)
        logger.info("Gold voyage candidates build complete for %s", args.source_date)

    except Exception as e:
        logger.error("Gold voyage build failed: %s", e, exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

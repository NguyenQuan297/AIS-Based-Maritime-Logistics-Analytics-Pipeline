"""
Spark job: Build vessel daily activity gold table from silver data.

Usage:
    spark-submit spark_jobs/silver_to_gold_activity.py --source-date 2025-08-15
"""

import argparse
import logging
import sys

from config.logging_config import setup_logging
from config.paths import PathConfig
from processing.activity_aggregator import build_daily_activity
from spark_jobs.spark_session import get_spark_session

logger = setup_logging()


def main():
    parser = argparse.ArgumentParser(description="Build gold daily activity table")
    parser.add_argument("--source-date", type=str, required=True, help="Date to process (YYYY-MM-DD)")
    args = parser.parse_args()

    PathConfig.ensure_dirs()

    spark = get_spark_session("AIS_Silver_To_Gold_Activity")

    try:
        silver_df = spark.read.parquet(str(PathConfig.SILVER_DIR)).where(
            f"source_date = '{args.source_date}'"
        )

        activity_df = build_daily_activity(spark, silver_df)
        logger.info("Gold activity build complete for %s", args.source_date)

    except Exception as e:
        logger.error("Gold activity build failed: %s", e, exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

"""
Spark job: Transform bronze data into cleaned silver layer.

Usage:
    spark-submit spark_jobs/bronze_to_silver.py --source-date 2025-08-15
"""

import argparse
import logging
import sys

from config.logging_config import setup_logging
from config.paths import PathConfig
from processing.bronze_reader import build_bronze
from processing.silver_cleaner import build_silver
from spark_jobs.spark_session import get_spark_session

logger = setup_logging()


def main():
    parser = argparse.ArgumentParser(description="Transform bronze to silver layer")
    parser.add_argument("--source-date", type=str, required=True, help="Date to process (YYYY-MM-DD)")
    args = parser.parse_args()

    PathConfig.ensure_dirs()

    spark = get_spark_session("AIS_Bronze_To_Silver")

    try:
        bronze_df = build_bronze(spark, source_date=args.source_date)
        silver_df = build_silver(spark, bronze_df, source_date=args.source_date)

        logger.info("Bronze-to-silver complete for %s", args.source_date)

    except Exception as e:
        logger.error("Bronze-to-silver failed: %s", e, exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

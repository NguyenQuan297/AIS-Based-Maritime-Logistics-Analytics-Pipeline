"""
Spark job: Build route metrics gold table from voyage candidates.

Usage:
    spark-submit spark_jobs/silver_to_gold_routes.py
"""

import argparse
import logging
import sys

from config.logging_config import setup_logging
from config.paths import PathConfig
from processing.route_metrics_builder import build_route_metrics
from spark_jobs.spark_session import get_spark_session

logger = setup_logging()


def main():
    parser = argparse.ArgumentParser(description="Build gold route metrics table")
    args = parser.parse_args()

    PathConfig.ensure_dirs()

    spark = get_spark_session("AIS_Silver_To_Gold_Routes")

    try:
        voyages_df = spark.read.parquet(str(PathConfig.GOLD_VOYAGE_DIR))
        metrics_df = build_route_metrics(spark, voyages_df)
        logger.info("Gold route metrics build complete")

    except Exception as e:
        logger.error("Gold route metrics build failed: %s", e, exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

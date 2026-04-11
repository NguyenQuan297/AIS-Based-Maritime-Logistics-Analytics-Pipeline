"""
AIS-Based Maritime Logistics Analytics Pipeline

Main entry point for running the full pipeline locally.
Executes all stages: ingestion -> bronze -> silver -> gold -> serving.

Usage:
    python main.py --source-date 2025-08-15
    python main.py --all
    python main.py --sample  (quick test with 1 file)
"""

import argparse
import sys

from config.logging_config import setup_logging
from config.paths import PathConfig
from utils.file_utils import list_zst_files, extract_date_from_filename

logger = setup_logging()


def run_pipeline(source_date: str = None, file_limit: int = None):
    """Run the full pipeline for a given date or all available dates."""
    from spark_jobs.spark_session import get_spark_session
    from ingestion.batch_ingest import ingest_all_files
    from ingestion.state_tracker import IngestionStateTracker
    from processing.bronze_reader import build_bronze
    from processing.silver_cleaner import build_silver
    from processing.metadata_builder import build_vessel_metadata
    from processing.activity_aggregator import build_daily_activity
    from processing.voyage_candidate_builder import build_voyage_candidates
    from processing.route_metrics_builder import build_route_metrics
    from storage.postgres_loader import PostgresLoader
    from quality.data_quality_checks import DataQualityChecker
    from metrics.pipeline_metrics import PipelineMetrics

    PathConfig.ensure_dirs()
    spark = get_spark_session()
    metrics = PipelineMetrics()
    quality = DataQualityChecker()
    state = IngestionStateTracker()

    try:
        # Stage 1: Ingest raw -> bronze
        with metrics.track_stage("raw_to_bronze") as stage:
            all_files = list_zst_files(PathConfig.RAW_DIR)
            if file_limit:
                all_files = all_files[:file_limit]

            pending = state.get_pending_files(all_files)
            logger.info("Files: %d total, %d pending", len(all_files), len(pending))

            results = ingest_all_files(spark, file_limit=file_limit)

            for f in pending:
                date_str = extract_date_from_filename(f)
                state.mark_ingested(f.name, source_date=date_str or "")

            stage.input_rows = len(pending)
            stage.output_rows = len(results)

        # Determine dates to process
        files = list_zst_files(PathConfig.RAW_DIR)
        if file_limit:
            files = files[:file_limit]
        dates = [extract_date_from_filename(f) for f in files]
        dates = [d for d in dates if d is not None]

        if source_date:
            dates = [source_date]

        for date_str in dates:
            # Stage 2: Bronze -> Silver
            with metrics.track_stage(f"bronze_to_silver_{date_str}") as stage:
                bronze_df = build_bronze(spark, source_date=date_str)
                stage.input_rows = bronze_df.count()

                # Quality check bronze
                bronze_report = quality.run_bronze_checks(bronze_df)
                logger.info("Bronze quality: %s", bronze_report.summary())

                silver_df = build_silver(spark, bronze_df, source_date=date_str)
                stage.output_rows = silver_df.count()

                # Quality check silver
                silver_report = quality.run_silver_checks(silver_df)
                logger.info("Silver quality: %s", silver_report.summary())

            # Stage 3: Silver -> Gold
            with metrics.track_stage(f"silver_to_gold_{date_str}") as stage:
                stage.input_rows = silver_df.count()
                activity_df = build_daily_activity(spark, silver_df)
                voyages_df = build_voyage_candidates(spark, silver_df)
                stage.output_rows = activity_df.count() + voyages_df.count()

        # Build metadata from all bronze data
        with metrics.track_stage("build_metadata") as stage:
            all_bronze = build_bronze(spark)
            stage.input_rows = all_bronze.count()
            metadata_df = build_vessel_metadata(spark, all_bronze)
            stage.output_rows = metadata_df.count()

        # Build route metrics from all voyages
        with metrics.track_stage("build_route_metrics") as stage:
            all_voyages = spark.read.parquet(str(PathConfig.GOLD_VOYAGE_DIR))
            stage.input_rows = all_voyages.count()
            route_metrics_df = build_route_metrics(spark, all_voyages)
            stage.output_rows = route_metrics_df.count()

        # Stage 4: Load to PostgreSQL
        with metrics.track_stage("load_postgres") as stage:
            try:
                loader = PostgresLoader()
                all_activity = spark.read.parquet(str(PathConfig.GOLD_ACTIVITY_DIR))
                loader.load_all_gold_tables(
                    activity_df=all_activity,
                    voyages_df=all_voyages,
                    routes_df=route_metrics_df,
                    metadata_df=metadata_df,
                )
            except Exception as e:
                logger.warning("PostgreSQL load skipped (not available): %s", e)
                stage.extra["skipped_reason"] = str(e)

        # Print pipeline summary
        summary = metrics.summary()
        logger.info("=" * 60)
        logger.info("PIPELINE COMPLETE")
        logger.info("Run ID: %s", summary["run_id"])
        logger.info("Total duration: %.2fs", summary["total_duration_seconds"])
        logger.info("Stages: %d passed, %d failed", summary["stages_passed"], summary["stages_failed"])
        logger.info("Ingestion state: %s", state.summary())
        logger.info("=" * 60)

    except Exception as e:
        logger.error("Pipeline failed: %s", e, exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()


def main():
    parser = argparse.ArgumentParser(
        description="AIS Maritime Logistics Analytics Pipeline"
    )
    parser.add_argument(
        "--source-date", type=str, default=None,
        help="Process a specific date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--all", action="store_true",
        help="Process all available dates",
    )
    parser.add_argument(
        "--sample", action="store_true",
        help="Quick test: process only 1 file",
    )
    args = parser.parse_args()

    file_limit = 1 if args.sample else None

    run_pipeline(
        source_date=args.source_date,
        file_limit=file_limit,
    )


if __name__ == "__main__":
    main()

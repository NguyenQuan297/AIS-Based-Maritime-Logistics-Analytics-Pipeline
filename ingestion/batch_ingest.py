"""
Batch ingestion: discover raw .zst files and convert to bronze parquet.
"""

import gc
import logging
import shutil
from pathlib import Path
from typing import List, Optional

from config.paths import PathConfig
from utils.file_utils import list_zst_files, extract_date_from_filename, parse_date_parts

logger = logging.getLogger("ais_pipeline.batch_ingest")


def ingest_single_file(spark, filepath: Path, output_base: Path = None) -> str:
    """
    Ingest a single .zst file into partitioned bronze parquet.

    Args:
        spark: SparkSession.
        filepath: Path to the .csv.zst file.
        output_base: Base output directory (defaults to PathConfig.BRONZE_DIR).

    Returns:
        Output path where parquet was written.
    """
    from ingestion.read_zst import read_zst_to_spark_df
    from ingestion.schema_detect import AIS_SCHEMA, COLUMN_MAPPING

    output_base = output_base or PathConfig.BRONZE_DIR

    date_str = extract_date_from_filename(filepath)
    if not date_str:
        raise ValueError(f"Cannot extract date from filename: {filepath.name}")

    date_parts = parse_date_parts(date_str)

    logger.info("Ingesting %s (date: %s)", filepath.name, date_str)

    df = read_zst_to_spark_df(spark, filepath, schema=AIS_SCHEMA)
    temp_dir = getattr(df, "_temp_dir", None)

    try:
        # Rename columns to normalized names (for older PascalCase datasets)
        from pyspark.sql import functions as F
        for old_name, new_name in COLUMN_MAPPING.items():
            if old_name in df.columns:
                df = df.withColumnRenamed(old_name, new_name)

        # Add partition columns
        df = df.withColumn("year", F.lit(date_parts["year"]))
        df = df.withColumn("month", F.lit(date_parts["month"]))
        df = df.withColumn("day", F.lit(date_parts["day"]))
        df = df.withColumn("source_date", F.lit(date_str))

        output_path = str(output_base)
        df.write.mode("overwrite").partitionBy("year", "month", "day").parquet(output_path)

        logger.info("Wrote bronze parquet for %s to %s", date_str, output_path)
        return output_path

    finally:
        # Clean up temp CSV after parquet is written
        if temp_dir and temp_dir.exists():
            shutil.rmtree(temp_dir, ignore_errors=True)
        # Release references
        del df
        gc.collect()


def ingest_all_files(
    spark,
    source_dir: Path = None,
    output_base: Path = None,
    file_limit: Optional[int] = None,
) -> List[str]:
    """
    Ingest .zst files from source directory into bronze parquet.
    Skips files that already have bronze data (by checking partition existence).

    Args:
        spark: SparkSession.
        source_dir: Directory containing .csv.zst files.
        output_base: Base output directory for bronze data.
        file_limit: Maximum number of files to process (for testing).

    Returns:
        List of output paths.
    """
    source_dir = source_dir or PathConfig.RAW_DIR
    output_base = output_base or PathConfig.BRONZE_DIR

    files = list_zst_files(source_dir)
    if file_limit:
        files = files[:file_limit]

    # Skip files whose bronze partition already exists
    pending = []
    for filepath in files:
        date_str = extract_date_from_filename(filepath)
        if date_str:
            parts = parse_date_parts(date_str)
            partition_path = output_base / f"year={parts['year']}" / f"month={parts['month']}" / f"day={parts['day']}"
            if partition_path.exists() and any(partition_path.glob("*.parquet")):
                logger.info("Skipping %s (bronze partition exists)", filepath.name)
                continue
        pending.append(filepath)

    logger.info("Found %d .zst files, %d need ingestion", len(files), len(pending))

    results = []
    for filepath in pending:
        try:
            result = ingest_single_file(spark, filepath, output_base)
            results.append(result)
        except Exception as e:
            logger.error("Failed to ingest %s: %s", filepath.name, e)

    logger.info("Ingestion complete: %d/%d files processed", len(results), len(pending))
    return results

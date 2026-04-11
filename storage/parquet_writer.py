"""
Parquet write utilities for data lake storage.
"""

import logging
from pathlib import Path
from typing import List, Optional

from pyspark.sql import DataFrame

from config.settings import Settings

logger = logging.getLogger("ais_pipeline.parquet_writer")


def write_partitioned_parquet(
    df: DataFrame,
    output_path: str,
    partition_cols: List[str],
    mode: str = "append",
    compression: str = None,
) -> None:
    """
    Write DataFrame as partitioned parquet.

    Args:
        df: Spark DataFrame to write.
        output_path: Output directory path.
        partition_cols: Columns to partition by.
        mode: Write mode (overwrite, append, etc.).
        compression: Compression codec (defaults to Settings value).
    """
    compression = compression or Settings.PARQUET_COMPRESSION

    logger.info(
        "Writing partitioned parquet to %s (partitions: %s, mode: %s)",
        output_path, partition_cols, mode,
    )

    (
        df.write
        .mode(mode)
        .option("compression", compression)
        .partitionBy(*partition_cols)
        .parquet(output_path)
    )

    logger.info("Write complete: %s", output_path)


def write_single_parquet(
    df: DataFrame,
    output_path: str,
    mode: str = "overwrite",
    coalesce: int = 1,
    compression: str = None,
) -> None:
    """
    Write DataFrame as a single (or few) parquet file(s).

    Args:
        df: Spark DataFrame.
        output_path: Output directory path.
        mode: Write mode.
        coalesce: Number of output files.
        compression: Compression codec.
    """
    compression = compression or Settings.PARQUET_COMPRESSION

    logger.info("Writing parquet to %s (coalesce=%d)", output_path, coalesce)

    (
        df.coalesce(coalesce)
        .write
        .mode(mode)
        .option("compression", compression)
        .parquet(output_path)
    )

    logger.info("Write complete: %s", output_path)

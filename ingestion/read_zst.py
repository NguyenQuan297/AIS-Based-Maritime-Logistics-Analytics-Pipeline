"""
Read .csv.zst compressed AIS files using streaming decompression.
Supports both Spark (production) and Pandas (sampling/debug).
"""

import io
import logging
from pathlib import Path

import zstandard as zstd

logger = logging.getLogger("ais_pipeline.read_zst")


def read_zst_to_spark_df(spark, filepath: Path, schema=None):
    """
    Read a .csv.zst file into a Spark DataFrame using stream decompression.

    Strategy: decompress .zst to a temporary CSV, then read with Spark.
    For very large files, this avoids loading everything into memory at once.

    Args:
        spark: SparkSession instance.
        filepath: Path to the .csv.zst file.
        schema: Optional StructType schema for the CSV.

    Returns:
        Spark DataFrame.
    """
    import tempfile
    import shutil

    logger.info("Reading .zst file: %s", filepath)

    temp_dir = Path(tempfile.mkdtemp(prefix="ais_"))
    temp_csv = temp_dir / filepath.name.replace(".csv.zst", ".csv")

    try:
        dctx = zstd.ZstdDecompressor()
        with open(filepath, "rb") as f_in, open(temp_csv, "wb") as f_out:
            dctx.copy_stream(f_in, f_out, read_size=16384, write_size=65536)

        logger.info("Decompressed to temp: %s", temp_csv)

        reader = spark.read.option("header", "true").option("inferSchema", "false")

        if schema:
            reader = reader.schema(schema)
        else:
            reader = reader.option("inferSchema", "true")

        df = reader.csv(str(temp_csv))
        # Cache to release temp file dependency
        df = df.cache()
        df.count()  # Force materialization

        logger.info("Loaded %d columns from %s", len(df.columns), filepath.name)
        return df

    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def read_zst_stream_to_spark(spark, filepath: Path, schema=None):
    """
    Alternative: read .zst via pipe into Spark using a temporary parquet intermediate.
    Better for very large files where temp CSV is too expensive.

    Args:
        spark: SparkSession instance.
        filepath: Path to the .csv.zst file.
        schema: Optional StructType schema.

    Returns:
        Spark DataFrame.
    """
    import pandas as pd
    import tempfile

    logger.info("Stream-reading .zst to parquet intermediate: %s", filepath)

    temp_dir = Path(tempfile.mkdtemp(prefix="ais_pq_"))
    batch_idx = 0

    try:
        dctx = zstd.ZstdDecompressor()
        with open(filepath, "rb") as f:
            with dctx.stream_reader(f) as reader:
                text_stream = io.TextIOWrapper(reader, encoding="utf-8", errors="ignore")
                chunk_iter = pd.read_csv(
                    text_stream,
                    sep=",",
                    chunksize=500_000,
                    low_memory=False,
                    on_bad_lines="skip",
                )

                for chunk in chunk_iter:
                    batch_path = temp_dir / f"batch_{batch_idx:04d}.parquet"
                    chunk.to_parquet(batch_path, index=False)
                    batch_idx += 1

        logger.info("Wrote %d parquet batches to %s", batch_idx, temp_dir)

        reader = spark.read
        if schema:
            reader = reader.schema(schema)
        df = reader.parquet(str(temp_dir))
        return df

    except Exception:
        import shutil
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise


def read_zst_sample_pandas(filepath: Path, nrows: int = 10000):
    """
    Read a sample of rows from a .csv.zst file using Pandas.
    For local debugging and schema inspection only.

    Args:
        filepath: Path to the .csv.zst file.
        nrows: Number of rows to read.

    Returns:
        pandas DataFrame.
    """
    import pandas as pd

    logger.info("Reading sample (%d rows) from %s", nrows, filepath)

    dctx = zstd.ZstdDecompressor()
    with open(filepath, "rb") as f:
        with dctx.stream_reader(f) as reader:
            text_stream = io.TextIOWrapper(reader, encoding="utf-8", errors="ignore")
            df = pd.read_csv(
                text_stream,
                sep=",",
                nrows=nrows,
                low_memory=False,
                on_bad_lines="skip",
            )

    logger.info("Sample loaded: %d rows, %d columns", len(df), len(df.columns))
    return df

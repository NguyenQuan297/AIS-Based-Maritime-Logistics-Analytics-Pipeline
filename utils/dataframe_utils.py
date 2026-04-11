"""
Common DataFrame operations used across pipeline stages.
"""

from typing import List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def select_and_rename(df: DataFrame, column_mapping: dict) -> DataFrame:
    """Select columns and rename them according to mapping."""
    selects = [F.col(old).alias(new) for old, new in column_mapping.items() if old in df.columns]
    return df.select(*selects)


def drop_duplicates_by_key(df: DataFrame, keys: List[str]) -> DataFrame:
    """Remove duplicates based on key columns, keeping first occurrence."""
    return df.dropDuplicates(keys)


def add_source_date_column(df: DataFrame, date_str: str) -> DataFrame:
    """Add a source_date partition column."""
    return df.withColumn("source_date", F.lit(date_str))


def count_nulls(df: DataFrame) -> dict:
    """Return null counts per column."""
    null_counts = {}
    for col_name in df.columns:
        null_counts[col_name] = df.where(F.col(col_name).isNull()).count()
    return null_counts


def get_row_count(df: DataFrame) -> int:
    """Return total row count."""
    return df.count()

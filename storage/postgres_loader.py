"""
PostgreSQL loader for serving layer tables.
"""

import logging
from typing import Optional

from pyspark.sql import DataFrame

from config.settings import Settings

logger = logging.getLogger("ais_pipeline.postgres_loader")


class PostgresLoader:
    """Load DataFrames into PostgreSQL serving tables."""

    def __init__(self):
        self.jdbc_url = Settings.pg_jdbc_url()
        self.connection_props = Settings.pg_connection_props()

    def write_table(
        self,
        df: DataFrame,
        table_name: str,
        mode: str = "overwrite",
    ) -> None:
        """
        Write a Spark DataFrame to a PostgreSQL table.

        Args:
            df: Spark DataFrame.
            table_name: Target PostgreSQL table name.
            mode: Write mode (overwrite, append, etc.).
        """
        logger.info("Loading %s into PostgreSQL (mode=%s)", table_name, mode)

        (
            df.write
            .jdbc(
                url=self.jdbc_url,
                table=table_name,
                mode=mode,
                properties=self.connection_props,
            )
        )

        logger.info("Loaded table: %s", table_name)

    def load_all_gold_tables(
        self,
        activity_df: Optional[DataFrame] = None,
        voyages_df: Optional[DataFrame] = None,
        routes_df: Optional[DataFrame] = None,
        metadata_df: Optional[DataFrame] = None,
    ) -> None:
        """Load all gold layer DataFrames into PostgreSQL."""
        tables = {
            "vessel_daily_activity": activity_df,
            "voyage_candidates": voyages_df,
            "route_metrics": routes_df,
            "vessel_metadata": metadata_df,
        }

        for table_name, df in tables.items():
            if df is not None:
                self.write_table(df, table_name)

    def read_table(self, spark, table_name: str) -> DataFrame:
        """Read a PostgreSQL table into a Spark DataFrame."""
        return (
            spark.read
            .jdbc(
                url=self.jdbc_url,
                table=table_name,
                properties=self.connection_props,
            )
        )

"""
Data expectations definitions for each pipeline layer.
Centralizes all threshold constants and validation rules.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Tuple


@dataclass
class ColumnExpectation:
    """Expectation for a single column."""

    column: str
    nullable: bool = True
    max_null_rate: float = 1.0
    value_range: Tuple[float, float] = None
    allowed_values: List = None
    unique: bool = False


@dataclass
class LayerExpectations:
    """Collection of expectations for a data layer."""

    layer: str
    min_rows: int = 0
    dedup_keys: List[str] = field(default_factory=list)
    max_duplicate_rate: float = 0.0
    columns: List[ColumnExpectation] = field(default_factory=list)


class AISExpectations:
    """Pre-defined expectations for AIS pipeline layers."""

    @staticmethod
    def bronze() -> LayerExpectations:
        return LayerExpectations(
            layer="bronze",
            min_rows=1000,
            columns=[
                ColumnExpectation(column="mmsi", nullable=False, max_null_rate=0.01),
                ColumnExpectation(column="base_date_time", nullable=False, max_null_rate=0.01),
                ColumnExpectation(column="latitude", max_null_rate=0.05, value_range=(-90.0, 90.0)),
                ColumnExpectation(column="longitude", max_null_rate=0.05, value_range=(-180.0, 180.0)),
                ColumnExpectation(column="sog", max_null_rate=0.10, value_range=(0.0, 102.3)),
            ],
        )

    @staticmethod
    def silver() -> LayerExpectations:
        return LayerExpectations(
            layer="silver",
            min_rows=1000,
            dedup_keys=["mmsi", "event_time"],
            max_duplicate_rate=0.0,
            columns=[
                ColumnExpectation(column="mmsi", nullable=False, max_null_rate=0.0),
                ColumnExpectation(column="event_time", nullable=False, max_null_rate=0.0),
                ColumnExpectation(column="latitude", nullable=False, value_range=(-90.0, 90.0)),
                ColumnExpectation(column="longitude", nullable=False, value_range=(-180.0, 180.0)),
                ColumnExpectation(column="sog", max_null_rate=0.05, value_range=(0.0, 102.3)),
                ColumnExpectation(column="cog", max_null_rate=0.10, value_range=(0.0, 360.0)),
            ],
        )

    @staticmethod
    def gold_activity() -> LayerExpectations:
        return LayerExpectations(
            layer="gold_activity",
            min_rows=100,
            dedup_keys=["mmsi", "activity_date"],
            max_duplicate_rate=0.0,
            columns=[
                ColumnExpectation(column="mmsi", nullable=False),
                ColumnExpectation(column="activity_date", nullable=False),
                ColumnExpectation(column="point_count", value_range=(1, 100000)),
                ColumnExpectation(column="avg_sog", value_range=(0.0, 102.3)),
            ],
        )

    @staticmethod
    def gold_voyage() -> LayerExpectations:
        return LayerExpectations(
            layer="gold_voyage",
            min_rows=10,
            columns=[
                ColumnExpectation(column="mmsi", nullable=False),
                ColumnExpectation(column="start_time", nullable=False),
                ColumnExpectation(column="end_time", nullable=False),
                ColumnExpectation(column="duration_hours", value_range=(0.0, 720.0)),
                ColumnExpectation(
                    column="candidate_route_type",
                    allowed_values=["stationary", "short_move", "coastal", "transit"],
                ),
            ],
        )

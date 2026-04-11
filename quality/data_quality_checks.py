"""
Data quality checks for each pipeline layer.
Runs validation rules and produces quality reports.
"""

import logging
from dataclasses import dataclass, field
from typing import Dict, List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from config.settings import Settings

logger = logging.getLogger("ais_pipeline.data_quality")


@dataclass
class QualityReport:
    """Container for quality check results."""

    stage: str
    total_rows: int = 0
    checks: List[Dict] = field(default_factory=list)
    passed: bool = True

    def add_check(self, name: str, passed: bool, detail: str = ""):
        self.checks.append({"name": name, "passed": passed, "detail": detail})
        if not passed:
            self.passed = False

    def summary(self) -> dict:
        return {
            "stage": self.stage,
            "total_rows": self.total_rows,
            "checks_run": len(self.checks),
            "checks_passed": sum(1 for c in self.checks if c["passed"]),
            "checks_failed": sum(1 for c in self.checks if not c["passed"]),
            "overall_passed": self.passed,
            "details": self.checks,
        }


class DataQualityChecker:
    """Runs data quality checks against pipeline DataFrames."""

    def check_row_count(self, df: DataFrame, min_rows: int, report: QualityReport) -> QualityReport:
        """Verify DataFrame has a minimum number of rows."""
        count = df.count()
        report.total_rows = count
        passed = count >= min_rows
        report.add_check(
            name="row_count",
            passed=passed,
            detail=f"rows={count}, min_required={min_rows}",
        )
        if not passed:
            logger.warning("Row count check FAILED: %d < %d", count, min_rows)
        return report

    def check_null_rate(
        self, df: DataFrame, column: str, max_rate: float, report: QualityReport
    ) -> QualityReport:
        """Verify column null rate is below threshold."""
        total = df.count()
        if total == 0:
            report.add_check(name=f"null_rate_{column}", passed=False, detail="empty DataFrame")
            return report

        null_count = df.where(F.col(column).isNull()).count()
        rate = null_count / total

        passed = rate <= max_rate
        report.add_check(
            name=f"null_rate_{column}",
            passed=passed,
            detail=f"null_rate={rate:.4f}, max_allowed={max_rate}",
        )
        if not passed:
            logger.warning("Null rate check FAILED for %s: %.4f > %.4f", column, rate, max_rate)
        return report

    def check_duplicate_rate(
        self, df: DataFrame, keys: List[str], max_rate: float, report: QualityReport
    ) -> QualityReport:
        """Verify duplicate rate on key columns is below threshold."""
        total = df.count()
        if total == 0:
            report.add_check(name="duplicate_rate", passed=False, detail="empty DataFrame")
            return report

        unique = df.dropDuplicates(keys).count()
        dup_rate = 1 - (unique / total)

        passed = dup_rate <= max_rate
        report.add_check(
            name=f"duplicate_rate_{'_'.join(keys)}",
            passed=passed,
            detail=f"dup_rate={dup_rate:.4f}, max_allowed={max_rate}",
        )
        if not passed:
            logger.warning("Duplicate rate check FAILED: %.4f > %.4f", dup_rate, max_rate)
        return report

    def check_value_range(
        self, df: DataFrame, column: str, min_val: float, max_val: float, report: QualityReport
    ) -> QualityReport:
        """Verify all values in column fall within expected range."""
        out_of_range = df.where(
            (F.col(column) < min_val) | (F.col(column) > max_val)
        ).count()

        passed = out_of_range == 0
        report.add_check(
            name=f"range_{column}",
            passed=passed,
            detail=f"out_of_range={out_of_range}, expected=[{min_val}, {max_val}]",
        )
        if not passed:
            logger.warning("Range check FAILED for %s: %d values out of [%s, %s]", column, out_of_range, min_val, max_val)
        return report

    def check_completeness(
        self, df: DataFrame, required_columns: List[str], report: QualityReport
    ) -> QualityReport:
        """Verify required columns exist in DataFrame."""
        missing = [c for c in required_columns if c not in df.columns]
        passed = len(missing) == 0
        report.add_check(
            name="schema_completeness",
            passed=passed,
            detail=f"missing_columns={missing}" if missing else "all required columns present",
        )
        return report

    def run_bronze_checks(self, df: DataFrame) -> QualityReport:
        """Run all bronze layer quality checks."""
        report = QualityReport(stage="bronze")
        self.check_row_count(df, min_rows=1000, report=report)
        self.check_completeness(df, ["mmsi", "base_date_time", "latitude", "longitude", "sog"], report=report)
        self.check_null_rate(df, "mmsi", max_rate=0.01, report=report)
        self.check_null_rate(df, "latitude", max_rate=0.05, report=report)
        logger.info("Bronze quality: %s", "PASSED" if report.passed else "FAILED")
        return report

    def run_silver_checks(self, df: DataFrame) -> QualityReport:
        """Run all silver layer quality checks."""
        report = QualityReport(stage="silver")
        self.check_row_count(df, min_rows=1000, report=report)
        self.check_null_rate(df, "mmsi", max_rate=0.0, report=report)
        self.check_null_rate(df, "event_time", max_rate=0.0, report=report)
        self.check_duplicate_rate(df, ["mmsi", "event_time"], max_rate=0.0, report=report)
        self.check_value_range(df, "latitude", Settings.LAT_MIN, Settings.LAT_MAX, report=report)
        self.check_value_range(df, "longitude", Settings.LON_MIN, Settings.LON_MAX, report=report)
        self.check_value_range(df, "sog", 0, Settings.SOG_MAX, report=report)
        logger.info("Silver quality: %s", "PASSED" if report.passed else "FAILED")
        return report

    def run_gold_activity_checks(self, df: DataFrame) -> QualityReport:
        """Run gold activity layer quality checks."""
        report = QualityReport(stage="gold_activity")
        self.check_row_count(df, min_rows=100, report=report)
        self.check_null_rate(df, "mmsi", max_rate=0.0, report=report)
        self.check_null_rate(df, "activity_date", max_rate=0.0, report=report)
        self.check_value_range(df, "point_count", 1, 100000, report=report)
        logger.info("Gold activity quality: %s", "PASSED" if report.passed else "FAILED")
        return report

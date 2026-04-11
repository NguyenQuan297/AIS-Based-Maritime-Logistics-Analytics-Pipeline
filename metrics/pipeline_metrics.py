"""
Pipeline metrics tracking: records timing, row counts, and stage statistics
for observability and debugging.
"""

import json
import logging
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger("ais_pipeline.metrics")

DEFAULT_METRICS_FILE = Path(__file__).resolve().parent.parent / "data" / ".pipeline_metrics.json"


@dataclass
class StageMetric:
    """Metrics for a single pipeline stage execution."""

    stage: str
    started_at: str = ""
    completed_at: str = ""
    duration_seconds: float = 0.0
    input_rows: int = 0
    output_rows: int = 0
    status: str = "pending"
    error: str = ""
    extra: Dict = field(default_factory=dict)


class PipelineMetrics:
    """
    Collects and persists pipeline execution metrics.
    Provides timing context manager and row count tracking.
    """

    def __init__(self, run_id: str = None, metrics_file: Path = None):
        self.run_id = run_id or datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        self.metrics_file = metrics_file or DEFAULT_METRICS_FILE
        self.stages: List[StageMetric] = []
        self.run_started_at = datetime.utcnow().isoformat()

    @contextmanager
    def track_stage(self, stage_name: str):
        """
        Context manager to track a pipeline stage.

        Usage:
            with metrics.track_stage("bronze_to_silver") as stage:
                # ... do work ...
                stage.input_rows = 1000000
                stage.output_rows = 950000
        """
        metric = StageMetric(
            stage=stage_name,
            started_at=datetime.utcnow().isoformat(),
            status="running",
        )
        self.stages.append(metric)
        start = time.time()

        logger.info("[METRICS] Stage started: %s", stage_name)

        try:
            yield metric
            metric.status = "success"
        except Exception as e:
            metric.status = "failed"
            metric.error = str(e)
            logger.error("[METRICS] Stage failed: %s - %s", stage_name, e)
            raise
        finally:
            metric.duration_seconds = round(time.time() - start, 2)
            metric.completed_at = datetime.utcnow().isoformat()
            logger.info(
                "[METRICS] Stage completed: %s (%.2fs, in=%d, out=%d, status=%s)",
                stage_name,
                metric.duration_seconds,
                metric.input_rows,
                metric.output_rows,
                metric.status,
            )
            self._save()

    def _save(self):
        """Persist metrics to disk."""
        self.metrics_file.parent.mkdir(parents=True, exist_ok=True)

        # Load existing runs
        all_runs = {}
        if self.metrics_file.exists():
            with open(self.metrics_file, "r") as f:
                all_runs = json.load(f)

        # Update current run
        all_runs[self.run_id] = {
            "run_id": self.run_id,
            "started_at": self.run_started_at,
            "stages": [
                {
                    "stage": s.stage,
                    "started_at": s.started_at,
                    "completed_at": s.completed_at,
                    "duration_seconds": s.duration_seconds,
                    "input_rows": s.input_rows,
                    "output_rows": s.output_rows,
                    "status": s.status,
                    "error": s.error,
                    "extra": s.extra,
                }
                for s in self.stages
            ],
        }

        with open(self.metrics_file, "w") as f:
            json.dump(all_runs, f, indent=2)

    def summary(self) -> dict:
        """Return summary of current pipeline run."""
        total_duration = sum(s.duration_seconds for s in self.stages)
        return {
            "run_id": self.run_id,
            "started_at": self.run_started_at,
            "total_stages": len(self.stages),
            "total_duration_seconds": round(total_duration, 2),
            "stages_passed": sum(1 for s in self.stages if s.status == "success"),
            "stages_failed": sum(1 for s in self.stages if s.status == "failed"),
            "stages": [
                {"stage": s.stage, "duration": s.duration_seconds, "status": s.status}
                for s in self.stages
            ],
        }

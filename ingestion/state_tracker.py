"""
Ingestion state tracker: tracks which files have been ingested
to avoid reprocessing and enable idempotent pipeline runs.
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger("ais_pipeline.state_tracker")

DEFAULT_STATE_FILE = Path(__file__).resolve().parent.parent / "data" / ".ingestion_state.json"


class IngestionStateTracker:
    """
    Tracks file ingestion state using a local JSON file.
    Records which files have been ingested, when, and their row counts.
    """

    def __init__(self, state_file: Path = None):
        self.state_file = state_file or DEFAULT_STATE_FILE
        self.state: Dict = self._load_state()

    def _load_state(self) -> dict:
        """Load state from disk."""
        if self.state_file.exists():
            with open(self.state_file, "r") as f:
                return json.load(f)
        return {"ingested_files": {}, "last_run": None}

    def _save_state(self):
        """Persist state to disk."""
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        with open(self.state_file, "w") as f:
            json.dump(self.state, f, indent=2, default=str)

    def is_ingested(self, filename: str) -> bool:
        """Check if a file has already been ingested."""
        return filename in self.state["ingested_files"]

    def mark_ingested(
        self, filename: str, row_count: int = 0, source_date: str = ""
    ):
        """Mark a file as successfully ingested."""
        self.state["ingested_files"][filename] = {
            "ingested_at": datetime.utcnow().isoformat(),
            "row_count": row_count,
            "source_date": source_date,
            "status": "success",
        }
        self.state["last_run"] = datetime.utcnow().isoformat()
        self._save_state()
        logger.info("Marked as ingested: %s (%d rows)", filename, row_count)

    def mark_failed(self, filename: str, error: str):
        """Mark a file ingestion as failed."""
        self.state["ingested_files"][filename] = {
            "ingested_at": datetime.utcnow().isoformat(),
            "row_count": 0,
            "status": "failed",
            "error": error,
        }
        self._save_state()
        logger.warning("Marked as failed: %s (%s)", filename, error)

    def get_pending_files(self, all_files: List[Path]) -> List[Path]:
        """
        Filter list of files to only those not yet successfully ingested.

        Args:
            all_files: List of all available .zst file paths.

        Returns:
            List of files that need ingestion.
        """
        pending = []
        for f in all_files:
            entry = self.state["ingested_files"].get(f.name)
            if entry is None or entry.get("status") != "success":
                pending.append(f)
        return pending

    def get_ingested_dates(self) -> List[str]:
        """Return list of successfully ingested source dates."""
        dates = []
        for filename, info in self.state["ingested_files"].items():
            if info.get("status") == "success" and info.get("source_date"):
                dates.append(info["source_date"])
        return sorted(dates)

    def reset(self):
        """Clear all state (for full reprocessing)."""
        self.state = {"ingested_files": {}, "last_run": None}
        self._save_state()
        logger.info("Ingestion state reset")

    def summary(self) -> dict:
        """Return a summary of ingestion state."""
        files = self.state["ingested_files"]
        return {
            "total_tracked": len(files),
            "successful": sum(1 for f in files.values() if f.get("status") == "success"),
            "failed": sum(1 for f in files.values() if f.get("status") == "failed"),
            "total_rows": sum(f.get("row_count", 0) for f in files.values()),
            "last_run": self.state.get("last_run"),
        }

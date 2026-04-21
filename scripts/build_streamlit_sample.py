"""
Build a small silver sample suitable for Streamlit Community Cloud (1GB RAM).

Samples ~60K rows per source_date partition (~500K total), keeping only the
columns the dashboard needs. Output size is ~25-30MB which is safe to commit.

Usage:
    python scripts/build_streamlit_sample.py
"""

import re
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parent.parent
SILVER_DIR = ROOT / "data" / "silver"
SAMPLE_DIR = ROOT / "data" / "silver_sample"
ROWS_PER_DAY = 60_000
COLUMNS = ["mmsi", "latitude", "longitude", "sog", "vessel_type", "event_time"]


def main() -> None:
    dataset = ds.dataset(str(SILVER_DIR), partitioning="hive")
    fragments = list(dataset.get_fragments())
    if not fragments:
        raise SystemExit(f"No silver fragments found at {SILVER_DIR}")

    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    by_date: dict[str, list[pd.DataFrame]] = {}
    for frag in fragments:
        expr = str(frag.partition_expression)
        match = re.search(r"(\d{4}-\d{2}-\d{2})", expr)
        if not match:
            raise RuntimeError(f"Cannot extract date from partition expr: {expr!r}")
        source_date = match.group(1)
        pdf = frag.to_table(columns=COLUMNS).to_pandas()
        by_date.setdefault(source_date, []).append(pdf)

    total = 0
    for source_date, frames in sorted(by_date.items()):
        day_df = pd.concat(frames, ignore_index=True)
        if len(day_df) > ROWS_PER_DAY:
            day_df = day_df.sample(n=ROWS_PER_DAY, random_state=42)
        day_df["source_date"] = source_date
        out_path = SAMPLE_DIR / f"source_date={source_date}"
        out_path.mkdir(parents=True, exist_ok=True)
        table = pa.Table.from_pandas(day_df.drop(columns=["source_date"]), preserve_index=False)
        pq.write_table(table, out_path / "part.parquet", compression="snappy")
        total += len(day_df)
        print(f"  {source_date}: {len(day_df):,} rows -> {out_path}")

    size_mb = sum(p.stat().st_size for p in SAMPLE_DIR.rglob("*.parquet")) / 1_048_576
    print(f"\nTotal: {total:,} rows, {size_mb:.1f} MB at {SAMPLE_DIR}")


if __name__ == "__main__":
    main()

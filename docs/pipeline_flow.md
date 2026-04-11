# Pipeline Flow

## Execution Order

```
1. discover_raw_files    -> Find .csv.zst files in data/raw/ais/
2. ingest_to_bronze      -> Stream decompress, normalize, write partitioned parquet
3. bronze_to_silver      -> Clean, type, validate, deduplicate
4. build_vessel_metadata -> Extract unique vessel info from bronze
5. build_daily_activity  -> Aggregate per-vessel per-day stats from silver
6. build_voyage_candidates -> Segment voyages via time-gap logic from silver
7. build_route_metrics   -> Aggregate route-level stats from voyage candidates
8. load_postgres         -> Write gold tables to PostgreSQL serving layer
9. run_quality_checks    -> Validate row counts, null rates, data ranges
```

## Partition Strategy

| Layer | Partition Keys | Example Path |
|-------|---------------|--------------|
| Bronze | year, month, day | `data/bronze/year=2025/month=08/day=15/` |
| Silver | source_date | `data/silver/source_date=2025-08-15/` |
| Gold Activity | activity_date | `data/gold/vessel_daily_activity/activity_date=2025-08-15/` |
| Gold Routes | metric_date | `data/gold/route_metrics/metric_date=2025-08-15/` |

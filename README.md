# AIS-Based Maritime Logistics Analytics Pipeline

End-to-end data engineering project that turns ~8.7M raw AIS vessel-position records per day into analytics-ready tables via a medallion (Bronze/Silver/Gold) architecture, served through a live interactive Streamlit dashboard backed by Amazon S3.

## Live Demo

**[ais-based-maritime-logistics-analytics-pipeline-6dcp6ccp4ygg3l.streamlit.app](https://ais-based-maritime-logistics-analytics-pipeline-6dcp6ccp4ygg3l.streamlit.app/)**

Deployed on Streamlit Community Cloud, fetching Parquet from `s3://ais-maritime-quan` (us-east-2). First load takes ~6s; cached thereafter.

## Dashboard Preview

### KPI Cards + Speed Distribution + Route Breakdown
![KPI and Charts](docs/images/01_kpi_charts.png)

### Vessel Position Heatmap (80K points, US waters)
![Heatmap](docs/images/02_heatmap.png)

### Route Metrics + Top Active Vessels
![Route Tables](docs/images/03_route_tables.png)

### Voyage Origin-Destination Arc Map (transit=red, coastal=cyan)
![Voyage Arcs](docs/images/04_voyage_arcs.png)

---

## Goals

This project was built to demonstrate, in one cohesive repo, the skills a data engineer uses day-to-day:

- **Ingestion** of compressed, schema-evolving sources at scale (Zstandard-encoded CSV, ~250 MB/day compressed, ~1.5 GB uncompressed).
- **Medallion architecture** (Bronze/Silver/Gold) with explicit quality boundaries between layers.
- **Batch processing with PySpark** — partitioning, shuffle tuning, deterministic aggregations, idempotent writes.
- **Orchestration** — a scheduled Airflow DAG with staged dependencies, plus metrics / quality gates per stage.
- **Serving** — partitioned Parquet on S3, queryable via Amazon Athena and exportable to PostgreSQL.
- **Production-grade dashboard** — Streamlit with pydeck maps, deployed publicly via Streamlit Community Cloud, reading from S3 via `pyarrow.fs.S3FileSystem` with IAM-scoped credentials.

---

## Architecture

```
                     Daily .csv.zst from MarineCadastre
                                    │
                                    │  stream-decompress (zstandard)
                                    │  schema enforcement
                                    ▼
┌──────────────────────────────────────────────────────────────┐
│                       BRONZE LAYER                           │
│  Raw normalized Parquet · partition year/month/day · 2.1 GB  │
└──────────────────────────────────────┬───────────────────────┘
                                       │ timestamp normalize
                                       │ type cast + null-island filter
                                       │ SOG 102.3 sentinel → null
                                       │ deduplicate (mmsi, event_time)
                                       ▼
┌──────────────────────────────────────────────────────────────┐
│                       SILVER LAYER                           │
│  Cleaned positions · partition source_date · 68.8M rows · 1.1 GB │
└────────┬───────────────────┬───────────────────┬─────────────┘
         │                   │                   │
         ▼                   ▼                   ▼
  metadata_builder   activity_aggregator   voyage_candidate_builder
  (rebuild from         (daily stats         (time-gap segmentation,
   bronze, pick          per mmsi)            min_by/max_by for
   most complete                              deterministic start/end)
   row per mmsi)             │                       │
         │                   │                       │
         └───────────────────┴──────┬────────────────┘
                                    │
                                    ▼
                             route_metrics_builder
                         (aggregate across voyages)
                                    │
                                    ▼
┌──────────────────────────────────────────────────────────────┐
│                        GOLD LAYER                            │
│   vessel_metadata · vessel_daily_activity                    │
│   voyage_candidates · route_metrics                          │
│   ~15 MB total                                               │
└─────────┬──────────────────────────────────────┬─────────────┘
          │                                      │
          │ aws s3 sync                          │ Spark JDBC load
          ▼                                      ▼
┌────────────────────┐                   ┌────────────────────┐
│  Amazon S3         │                   │  PostgreSQL        │
│  gold/             │                   │  (optional serving │
│  silver_sample/    │                   │   via docker-      │
│  (partitioned)     │                   │   compose)         │
└─────────┬──────────┘                   └────────────────────┘
          │
          │ pyarrow.fs.S3FileSystem (read-only IAM key)
          ▼
┌──────────────────────────────────────────────────────────────┐
│       Streamlit Community Cloud + pydeck + Carto tiles       │
│       (heatmap · scatter · voyage arcs · vessel lookup)      │
└──────────────────────────────────────────────────────────────┘
```

Parallel path: Amazon Athena can query the same S3 Gold/Silver via Glue Data Catalog — SQL files in [sql/athena/](sql/athena/) are ready to drop into the Athena console once a Glue Crawler has been run.

---

## Tech Stack

| Layer | Component | Notes |
|-------|-----------|-------|
| Ingestion | **zstandard** | Stream decompress `.csv.zst` without full extraction |
| Processing | **PySpark 3.5** | Local `[4]` master, 4 GB driver, dynamic partition overwrite |
| Storage | **Apache Parquet** | Snappy compression, hive-style partitioning |
| Cloud storage | **Amazon S3** (us-east-2) | 28 MB uploaded (gold + silver_sample); free tier |
| Access control | **AWS IAM** | Split users: `ais-dashboard-reader` (GetObject+ListBucket) and `ais-admin` (uploads) |
| Orchestration | **Apache Airflow** 2.8 | 9-task DAG, `@daily` schedule, via `docker-compose` |
| Serving DB | **PostgreSQL** 16 | Optional, via `docker-compose`; Spark JDBC loader |
| Ad-hoc SQL | **Amazon Athena** | DDL in `sql/athena/`, reads directly from S3 Parquet |
| Dashboard | **Streamlit** + **pydeck** | 1 GB RAM budget on Streamlit Community Cloud |
| Map tiles | **Carto Dark-Matter** | Token-less alternative to Mapbox, via `basemaps.cartocdn.com` |
| Quality | Custom checker | Null rate, range, duplicate, row-count checks per layer |
| Metrics | JSON tracker | Stage duration + row counts per run in `data/.pipeline_metrics.json` |
| Testing | **pytest** | Unit tests for read_zst, schema, validation, geo utils |

---

## Data

**Source**: [MarineCadastre AIS](https://marinecadastre.gov/ais/) — public AIS vessel broadcasts for US waters.

| Property | Value |
|----------|-------|
| Format | `.csv.zst` (Zstandard-compressed CSV, UTF-8) |
| Compressed size | ~250 MB/day |
| Uncompressed | ~1.5 GB/day |
| Rows | ~8.7M position reports/day |
| Dataset used | 8 files, 2025-08-15 → 2025-08-22 (~70M rows total) |
| Columns | `mmsi, base_date_time, latitude, longitude, sog, cog, heading, vessel_name, imo, call_sign, vessel_type, status, length, width, draft, cargo, transceiver` |

---

## Pipeline Stages

### Stage 1 — Raw → Bronze
[ingestion/batch_ingest.py](ingestion/batch_ingest.py), [ingestion/read_zst.py](ingestion/read_zst.py)

- Discover `.csv.zst` files via [ingestion/state_tracker.py](ingestion/state_tracker.py) (skips already-ingested files).
- Stream-decompress with `zstandard.ZstdDecompressor`, enforce [ingestion/schema_detect.py](ingestion/schema_detect.py) `AIS_SCHEMA`.
- Write partitioned Parquet: `bronze/year=YYYY/month=MM/day=DD/`.

### Stage 2 — Bronze → Silver
[processing/silver_cleaner.py](processing/silver_cleaner.py)

1. Normalize `base_date_time` → `event_time` (TimestampType).
2. Cast numeric columns to correct types.
3. Null-out SOG `102.3` AIS "not available" sentinel.
4. Drop null-island records (`lat == 0 AND lon == 0`).
5. Validate coordinates + MMSI range + non-negative SOG ([utils/validation.py](utils/validation.py)).
6. Deduplicate on `(mmsi, event_time)`.
7. Write `silver/source_date=YYYY-MM-DD/` with `overwrite` + `partitionOverwriteMode=dynamic` → safe reruns.

### Stage 3 — Silver → Gold
Four derived tables:

| Output | Builder | Logic |
|--------|---------|-------|
| `vessel_metadata` | [processing/metadata_builder.py](processing/metadata_builder.py) | Rank rows per MMSI by non-null count, keep the most complete |
| `vessel_daily_activity` | [processing/activity_aggregator.py](processing/activity_aggregator.py) | Group by (mmsi, date): point_count, avg/max/min SOG, first/last seen |
| `voyage_candidates` | [processing/voyage_candidate_builder.py](processing/voyage_candidate_builder.py) | Time-gap segmentation (>4h = new voyage); `min_by/max_by(event_time)` for deterministic start/end coordinates; classify as stationary / short_move / coastal / transit |
| `route_metrics` | [processing/route_metrics_builder.py](processing/route_metrics_builder.py) | Aggregate voyage stats by route type and date |

### Stage 4 — Serving + Dashboard
- `aws s3 sync data/gold data/silver_sample → s3://ais-maritime-quan/` for Streamlit Cloud to read
- (Optional) `storage/postgres_loader.py` pushes Gold tables to PostgreSQL via JDBC
- [app.py](app.py) auto-detects data source: S3 (secret `AIS_S3_BUCKET` set) → full local silver → committed sample

---

## Data Model

```
┌──────────────────┐      ┌───────────────────────────┐
│ vessel_metadata  │      │ vessel_daily_activity     │
├──────────────────┤      ├───────────────────────────┤
│ mmsi (PK)        │<──── │ mmsi                      │
│ vessel_name      │      │ activity_date             │
│ imo              │      │ point_count               │
│ call_sign        │      │ avg_sog / max / min       │
│ vessel_type      │      │ first_seen / last_seen    │
│ length / width   │      └───────────────────────────┘
│ transceiver      │
└────────┬─────────┘      ┌───────────────────────────┐
         │                │ voyage_candidates         │
         │                ├───────────────────────────┤
         └──────────────> │ mmsi                      │
                          │ start_time / end_time     │
                          │ start_lat / start_lon     │
                          │ end_lat / end_lon         │
                          │ duration_hours            │
                          │ avg_sog                   │
                          │ candidate_route_type      │
                          │ point_count               │
                          └──────────────┬────────────┘
                                         │
                                         ▼
                          ┌───────────────────────────┐
                          │ route_metrics             │
                          ├───────────────────────────┤
                          │ route_id (PK)             │
                          │ metric_date               │
                          │ candidate_route_type      │
                          │ vessel_count              │
                          │ avg_duration_hours        │
                          │ avg_speed                 │
                          │ point_count               │
                          └───────────────────────────┘
```

---

## Orchestration (Airflow DAG)

[orchestration/airflow_dags/ais_pipeline_dag.py](orchestration/airflow_dags/ais_pipeline_dag.py) — daily schedule, 9 tasks:

```
discover_raw_files
        │
        ▼
ingest_to_bronze
        │
        ▼
bronze_to_silver
        │
   ┌────┼──────────────────┐
   │    │                  │
   ▼    ▼                  ▼
build_    build_daily_    build_voyage_
metadata  activity        candidates
   │        │                  │
   │        │                  ▼
   │        │           build_route_metrics
   │        │                  │
   └────┬───┴──────────────────┘
        │
        ▼
  load_postgres
        │
        ▼
 run_quality_checks
```

Quality gates from [quality/data_quality_checks.py](quality/data_quality_checks.py) run after each layer; a fail raises and the DAG stops.

---

## Project Structure

```
.
├── app.py                              # Streamlit dashboard (S3-aware)
├── main.py                             # Full-pipeline entry point
├── config/                             # settings, paths, logging, .env loader
├── ingestion/                          # read_zst, schema_detect, batch_ingest, state_tracker
├── processing/                         # bronze_reader, silver_cleaner, 4 gold builders
├── spark_jobs/                         # Standalone spark-submit entry points per stage
├── storage/
│   └── postgres_loader.py              # JDBC loader for gold → RDS/local Postgres
├── quality/
│   └── data_quality_checks.py          # Null / range / dup / row-count checks
├── metrics/
│   └── pipeline_metrics.py             # Stage timing + row counts
├── orchestration/airflow_dags/         # Daily Airflow DAG
├── sql/
│   ├── athena/                         # External table DDL + example queries
│   └── postgres/                       # Serving schema + indexes
├── scripts/
│   └── build_streamlit_sample.py       # Silver-sample rebuilder for Streamlit Cloud
├── tests/                              # pytest unit tests
├── utils/                              # file, time, geo (haversine), validation
├── docs/                               # architecture.md, data_dictionary.md, images
├── docker-compose.yml                  # Postgres + Spark master/worker + Airflow
├── requirements.txt                    # Dashboard-only (used by Streamlit Cloud)
├── requirements-pipeline.txt           # Full pipeline deps
└── data/
    ├── raw/ais/                        # Source .csv.zst (not committed)
    ├── bronze/                         # Partitioned raw parquet (not committed)
    ├── silver/                         # Cleaned positions (not committed)
    ├── silver_sample/                  # 480K-row sample — committed for demo
    └── gold/                           # 4 analytics tables — committed for demo
```

---

## Running Locally

### Prerequisites
- Python 3.9+ (tested on 3.9)
- Java 11+ (for PySpark)
- 8 GB RAM recommended for full 8-day run

### Setup

```bash
# Clone
git clone https://github.com/NguyenQuan297/AIS-Based-Maritime-Logistics-Analytics-Pipeline.git
cd AIS-Based-Maritime-Logistics-Analytics-Pipeline

# Install full pipeline deps
pip install -r requirements-pipeline.txt

# (Or, dashboard-only — no Java/Spark needed)
pip install -r requirements.txt

# Copy env template
cp .env.example .env   # then edit as needed (PG_*, S3_* optional)
```

### Run the pipeline

```bash
# Drop MarineCadastre .csv.zst files in data/raw/ais/

# Quick smoke test (1 file)
python main.py --sample

# Full run (all available dates)
python main.py --all

# Single date
python main.py --source-date 2025-08-15
```

Full run on 8 files finishes in ~12 minutes on a 16 GB laptop: Bronze in 2m50s, Silver+Gold per day ~45s, final metadata+route build ~45s.

### Launch the dashboard locally

```bash
streamlit run app.py
# → http://localhost:8501
```

The app auto-detects `data/silver/` (full, ~1 GB) if present, otherwise falls back to `data/silver_sample/`. Set `AIS_S3_BUCKET` to read from S3 instead.

### Run Spark jobs individually

```bash
spark-submit spark_jobs/ingest_raw_to_bronze.py --file-limit 1
spark-submit spark_jobs/bronze_to_silver.py --source-date 2025-08-15
spark-submit spark_jobs/silver_to_gold_activity.py --source-date 2025-08-15
spark-submit spark_jobs/silver_to_gold_voyage.py --source-date 2025-08-15
spark-submit spark_jobs/silver_to_gold_routes.py
```

### Full stack in Docker (Postgres + Airflow + Spark)

```bash
docker-compose up -d
# Airflow UI:    http://localhost:8081  (admin / admin)
# Postgres:      localhost:5432         (postgres / postgres)
# Spark master:  http://localhost:8080
```

### Tests

```bash
pytest tests/ -v
```

---

## License

MIT — see [LICENSE](LICENSE) if present, otherwise free to fork and adapt.

## Credits

Built by [NguyenQuan297](https://github.com/NguyenQuan297). AIS data courtesy of [MarineCadastre](https://marinecadastre.gov/ais/).

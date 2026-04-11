-- Athena external tables for AIS data lake on S3
-- These tables point to partitioned parquet data in S3

-- Silver layer: cleaned vessel positions
CREATE EXTERNAL TABLE IF NOT EXISTS ais_silver.vessel_positions (
    mmsi            BIGINT,
    event_time      TIMESTAMP,
    latitude        DOUBLE,
    longitude       DOUBLE,
    sog             DOUBLE,
    cog             DOUBLE,
    heading         DOUBLE,
    vessel_type     INT,
    status          INT,
    draft           DOUBLE,
    cargo           INT
)
PARTITIONED BY (source_date STRING)
STORED AS PARQUET
LOCATION 's3://ais-maritime-data/silver/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- Gold layer: vessel daily activity
CREATE EXTERNAL TABLE IF NOT EXISTS ais_gold.vessel_daily_activity (
    mmsi            BIGINT,
    point_count     BIGINT,
    avg_sog         DOUBLE,
    max_sog         DOUBLE,
    min_sog         DOUBLE,
    first_seen_time TIMESTAMP,
    last_seen_time  TIMESTAMP
)
PARTITIONED BY (activity_date DATE)
STORED AS PARQUET
LOCATION 's3://ais-maritime-data/gold/vessel_daily_activity/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- Gold layer: voyage candidates
CREATE EXTERNAL TABLE IF NOT EXISTS ais_gold.voyage_candidates (
    mmsi                BIGINT,
    start_time          TIMESTAMP,
    end_time            TIMESTAMP,
    start_longitude     DOUBLE,
    start_latitude      DOUBLE,
    end_longitude       DOUBLE,
    end_latitude        DOUBLE,
    point_count         BIGINT,
    avg_sog             DOUBLE,
    duration_hours      DOUBLE,
    candidate_route_type STRING
)
STORED AS PARQUET
LOCATION 's3://ais-maritime-data/gold/voyage_candidates/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- Gold layer: route metrics
CREATE EXTERNAL TABLE IF NOT EXISTS ais_gold.route_metrics (
    route_id            STRING,
    candidate_route_type STRING,
    vessel_count        BIGINT,
    avg_duration_hours  DOUBLE,
    avg_speed           DOUBLE,
    point_count         BIGINT
)
PARTITIONED BY (metric_date DATE)
STORED AS PARQUET
LOCATION 's3://ais-maritime-data/gold/route_metrics/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- Gold layer: vessel metadata
CREATE EXTERNAL TABLE IF NOT EXISTS ais_gold.vessel_metadata (
    mmsi            BIGINT,
    vessel_name     STRING,
    imo             STRING,
    call_sign       STRING,
    vessel_type     INT,
    length          DOUBLE,
    width           DOUBLE,
    transceiver     STRING
)
STORED AS PARQUET
LOCATION 's3://ais-maritime-data/gold/vessel_metadata/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- Repair partitions after data load
MSCK REPAIR TABLE ais_silver.vessel_positions;
MSCK REPAIR TABLE ais_gold.vessel_daily_activity;
MSCK REPAIR TABLE ais_gold.route_metrics;

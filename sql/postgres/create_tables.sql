-- PostgreSQL serving layer tables
-- These tables are loaded from gold parquet by the pipeline

CREATE TABLE IF NOT EXISTS vessel_metadata (
    mmsi            BIGINT PRIMARY KEY,
    vessel_name     VARCHAR(255),
    imo             VARCHAR(50),
    call_sign       VARCHAR(50),
    vessel_type     INTEGER,
    length          DOUBLE PRECISION,
    width           DOUBLE PRECISION,
    transceiver     VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS vessel_daily_activity (
    mmsi            BIGINT NOT NULL,
    activity_date   DATE NOT NULL,
    point_count     BIGINT,
    avg_sog         DOUBLE PRECISION,
    max_sog         DOUBLE PRECISION,
    min_sog         DOUBLE PRECISION,
    first_seen_time TIMESTAMP,
    last_seen_time  TIMESTAMP,
    PRIMARY KEY (mmsi, activity_date)
);

CREATE TABLE IF NOT EXISTS voyage_candidates (
    id                      SERIAL PRIMARY KEY,
    mmsi                    BIGINT NOT NULL,
    start_time              TIMESTAMP,
    end_time                TIMESTAMP,
    start_longitude         DOUBLE PRECISION,
    start_latitude          DOUBLE PRECISION,
    end_longitude           DOUBLE PRECISION,
    end_latitude            DOUBLE PRECISION,
    point_count             BIGINT,
    avg_sog                 DOUBLE PRECISION,
    duration_hours          DOUBLE PRECISION,
    candidate_route_type    VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS route_metrics (
    route_id                VARCHAR(64) PRIMARY KEY,
    metric_date             DATE,
    candidate_route_type    VARCHAR(50),
    vessel_count            BIGINT,
    avg_duration_hours      DOUBLE PRECISION,
    avg_speed               DOUBLE PRECISION,
    point_count             BIGINT
);

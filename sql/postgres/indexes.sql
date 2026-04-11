-- Performance indexes for PostgreSQL serving layer

-- vessel_metadata
CREATE INDEX IF NOT EXISTS idx_metadata_vessel_type ON vessel_metadata (vessel_type);
CREATE INDEX IF NOT EXISTS idx_metadata_vessel_name ON vessel_metadata (vessel_name);

-- vessel_daily_activity
CREATE INDEX IF NOT EXISTS idx_activity_date ON vessel_daily_activity (activity_date);
CREATE INDEX IF NOT EXISTS idx_activity_mmsi ON vessel_daily_activity (mmsi);
CREATE INDEX IF NOT EXISTS idx_activity_mmsi_date ON vessel_daily_activity (mmsi, activity_date);

-- voyage_candidates
CREATE INDEX IF NOT EXISTS idx_voyage_mmsi ON voyage_candidates (mmsi);
CREATE INDEX IF NOT EXISTS idx_voyage_start_time ON voyage_candidates (start_time);
CREATE INDEX IF NOT EXISTS idx_voyage_route_type ON voyage_candidates (candidate_route_type);

-- route_metrics
CREATE INDEX IF NOT EXISTS idx_route_metric_date ON route_metrics (metric_date);
CREATE INDEX IF NOT EXISTS idx_route_type ON route_metrics (candidate_route_type);

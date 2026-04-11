-- Query: Vessel daily activity summary
-- Use this to analyze vessel activity patterns

-- Top 20 most active vessels by point count on a given date
SELECT
    a.mmsi,
    m.vessel_name,
    m.vessel_type,
    a.activity_date,
    a.point_count,
    a.avg_sog,
    a.max_sog,
    a.first_seen_time,
    a.last_seen_time
FROM ais_gold.vessel_daily_activity a
LEFT JOIN ais_gold.vessel_metadata m ON a.mmsi = m.mmsi
WHERE a.activity_date = DATE '2025-08-15'
ORDER BY a.point_count DESC
LIMIT 20;

-- Average daily positions per vessel type
SELECT
    m.vessel_type,
    COUNT(DISTINCT a.mmsi) AS vessel_count,
    ROUND(AVG(a.point_count), 0) AS avg_daily_points,
    ROUND(AVG(a.avg_sog), 2) AS avg_speed
FROM ais_gold.vessel_daily_activity a
LEFT JOIN ais_gold.vessel_metadata m ON a.mmsi = m.mmsi
GROUP BY m.vessel_type
ORDER BY vessel_count DESC;

-- Vessels with unusual speed patterns (potential anomalies)
SELECT
    a.mmsi,
    m.vessel_name,
    a.activity_date,
    a.avg_sog,
    a.max_sog,
    a.point_count
FROM ais_gold.vessel_daily_activity a
LEFT JOIN ais_gold.vessel_metadata m ON a.mmsi = m.mmsi
WHERE a.max_sog > 30
ORDER BY a.max_sog DESC
LIMIT 50;

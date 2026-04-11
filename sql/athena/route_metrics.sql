-- Query: Route metrics analysis

-- Daily route type distribution
SELECT
    metric_date,
    candidate_route_type,
    vessel_count,
    avg_duration_hours,
    avg_speed,
    point_count
FROM ais_gold.route_metrics
ORDER BY metric_date, vessel_count DESC;

-- Transit routes: average duration and speed trends
SELECT
    metric_date,
    vessel_count,
    avg_duration_hours,
    avg_speed
FROM ais_gold.route_metrics
WHERE candidate_route_type = 'transit'
ORDER BY metric_date;

-- Route type breakdown across all dates
SELECT
    candidate_route_type,
    COUNT(*) AS day_count,
    ROUND(AVG(vessel_count), 0) AS avg_daily_vessels,
    ROUND(AVG(avg_duration_hours), 2) AS avg_duration,
    ROUND(AVG(avg_speed), 2) AS avg_speed
FROM ais_gold.route_metrics
GROUP BY candidate_route_type
ORDER BY avg_daily_vessels DESC;

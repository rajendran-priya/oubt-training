SET search_path TO analytics;
--ANALYZE;

-- Sample analytical queries to validate the model
-- 1) Trip count by vendor and pickup borough
SELECT vd.vendor_name, pz.borough, COUNT(*) AS trips
FROM trip_fact f
JOIN vendor_dim vd ON f.vendor_sk = vd.vendor_sk
JOIN zone_dim pz ON f.pickup_zone_sk = pz.zone_sk
GROUP BY 1,2
ORDER BY trips DESC
LIMIT 20;

-- 2) Daily trips and revenue
SELECT DATE_TRUNC('day', pickup_ts) AS day, COUNT(*) AS trips, SUM(total_amount) AS revenue
FROM trip_fact
GROUP BY 1
ORDER BY day
LIMIT 30;

-- 3) Top pickup-dropoff flows
SELECT pz.zone_name AS pickup_zone, dz.zone_name AS dropoff_zone, COUNT(*) AS trips
FROM trip_fact f
JOIN zone_dim pz ON f.pickup_zone_sk = pz.zone_sk
JOIN zone_dim dz ON f.dropoff_zone_sk = dz.zone_sk
GROUP BY 1,2
ORDER BY trips DESC
LIMIT 20;


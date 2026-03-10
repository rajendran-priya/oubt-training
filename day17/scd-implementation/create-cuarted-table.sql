-- Redshift curated table build with basic quality checks
-- Assumes data already in analytics schema: stg_trip, zone_dim_scd, vendor_dim_scd (is_current rows)
-- Adjust thresholds/filters as needed.

SET search_path TO day17_SCD;

DROP TABLE IF EXISTS trip_curated;
CREATE TABLE trip_curated
DISTSTYLE KEY
DISTKEY (pickup_zone_sk)
SORTKEY (pickup_ts, vendor_sk) AS
SELECT
    f.vendor_sk,
    f.pickup_zone_sk,
    f.dropoff_zone_sk,
    f.pickup_ts,
    f.dropoff_ts,
    TRUNC(f.pickup_ts) AS pickup_date,
    TRUNC(f.dropoff_ts) AS dropoff_date,
    f.passenger_count,
    f.trip_distance_miles,
    f.rate_code_id,
    f.payment_type,
    f.fare_amount,
    f.tip_amount,
    f.tolls_amount,
    f.total_amount,
    f.store_and_fwd_flag,
    -- Quality flags
    CASE WHEN f.total_amount > 0 THEN 1 ELSE 0 END AS q_total_ok,
    CASE WHEN f.fare_amount > 0 THEN 1 ELSE 0 END AS q_fare_ok,
    --CASE WHEN f.passenger_count > 0 THEN 1 ELSE 0 END AS q_passenger_ok,
    CASE WHEN f.pickup_ts <= f.dropoff_ts  THEN 1 ELSE 0 END AS q_time_order_ok
FROM trip_fact f
JOIN vendor_dim_scd vd ON vd.vendor_sk = f.vendor_sk 
JOIN zone_dim_scd pz ON pz.zone_sk = f.pickup_zone_sk 
JOIN zone_dim_scd dz ON dz.zone_sk = f.dropoff_zone_sk 
WHERE
    -- hard filters (rejects)
    f.total_amount > 0
    AND f.fare_amount > 0
    --AND (f.passenger_count > 0)
    AND (f.dropoff_ts IS NULL OR f.pickup_ts <= f.dropoff_ts);

-- Optional: rejected records for auditing
DROP TABLE IF EXISTS trip_curated_rejects;
CREATE TABLE trip_curated_rejects AS
SELECT f.*,
    CASE WHEN NOT (f.fare_amount > 0) THEN 'fare_out_of_range' END AS reason_fare,
    --CASE WHEN NOT (f.passenger_count > 0) THEN 'passenger_out_of_range' END AS reason_passenger,
    CASE WHEN NOT (f.pickup_ts <= f.dropoff_ts) THEN 'pickup_after_dropoff' END AS reason_time
FROM trip_fact f
LEFT JOIN vendor_dim_scd vd ON vd.vendor_sk = f.vendor_sk 
LEFT JOIN zone_dim_scd pz ON pz.zone_sk = f.pickup_zone_sk 
LEFT JOIN zone_dim_scd dz ON dz.zone_sk = f.dropoff_zone_sk 
WHERE
    NOT (
        f.fare_amount > 0 AND
        --f.passenger_count > 0 AND
        (f.dropoff_ts IS NULL OR f.pickup_ts <= f.dropoff_ts)
    );



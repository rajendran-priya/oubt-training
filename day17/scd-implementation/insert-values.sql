SET search_path TO day17_SCD;
-- Merge pattern for zone_dim_scd
-- 1) Close existing rows that changed

UPDATE zone_dim_scd d
SET expire_ts = SYSDATE, is_current = FALSE
FROM stg_zone_delta s
WHERE d.zone_id = s.zone_id
  AND d.is_current = TRUE
  AND (COALESCE(d.zone_name,'') <> COALESCE(s.zone_name,'')
    OR COALESCE(d.borough,'') <> COALESCE(s.borough,'')
    OR COALESCE(d.service_zone,'') <> COALESCE(s.service_zone,''));

-- 2) Insert new current rows
INSERT INTO zone_dim_scd (zone_id, borough, zone_name, service_zone, version, is_current, effective_ts, expire_ts)
SELECT s.zone_id,
       s.borough,
       s.zone_name,
       s.service_zone,
       COALESCE((SELECT MAX(d.version) + 1 FROM zone_dim_scd d WHERE d.zone_id = s.zone_id), 1) AS version,
       TRUE,
       SYSDATE,
       '9999-12-31 23:59:59'
FROM stg_zone_delta s;

-- Merge pattern for vendor_dim_scd
UPDATE vendor_dim_scd d
SET expire_ts = SYSDATE, is_current = FALSE
FROM stg_vendor_delta s
WHERE d.vendor_id = s.vendor_id
  AND d.is_current = TRUE
    AND (COALESCE(d.vendor_name,'') <> COALESCE(s.vendor_name,''));

INSERT INTO vendor_dim_scd (vendor_id, vendor_name, version, is_current, effective_ts, expire_ts)
SELECT s.vendor_id,
             s.vendor_name,
             COALESCE((SELECT MAX(d.version) + 1 FROM vendor_dim_scd d WHERE d.vendor_id = s.vendor_id), 1) AS version,
             TRUE,
             SYSDATE,
             '9999-12-31 23:59:59'
FROM stg_vendor_delta s;

SET search_path TO day17_SCD;
-- Load fact table from staging + current SCD dims
INSERT INTO trip_fact (
    vendor_sk,
    pickup_zone_sk,
    dropoff_zone_sk,
    pickup_ts,
    dropoff_ts,
    passenger_count,
    trip_distance_miles,
    rate_code_id,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    congestion_surcharge,
    total_amount,
    store_and_fwd_flag
)
SELECT
    vd.vendor_sk,
    pz.zone_sk AS pickup_zone_sk,
    dz.zone_sk AS dropoff_zone_sk,
    t.tpep_pickup_datetime,
    t.tpep_dropoff_datetime,
    CAST(t.passenger_count AS SMALLINT),
    CAST(t.trip_distance AS DECIMAL(10,2)),
    CAST(t.ratecodeid AS SMALLINT),
    CAST(t.payment_type AS SMALLINT),
    CAST(t.fare_amount AS DECIMAL(10,2)),
    CAST(t.extra AS DECIMAL(10,2)),
    CAST(t.mta_tax AS DECIMAL(10,2)),
    CAST(t.tip_amount AS DECIMAL(10,2)),
    CAST(t.tolls_amount AS DECIMAL(10,2)),
    CAST(t.improvement_surcharge AS DECIMAL(10,2)),
    CAST(t.congestion_surcharge AS DECIMAL(10,2)),
    CAST(t.total_amount AS DECIMAL(10,2)),
    t.store_and_fwd_flag
FROM stg_trip t
JOIN vendor_dim_scd vd
  ON vd.vendor_id = t.vendorid AND vd.is_current = TRUE
JOIN zone_dim_scd pz
  ON pz.zone_id = t.pulocationid AND pz.is_current = TRUE
JOIN zone_dim_scd dz
  ON dz.zone_id = t.dolocationid AND dz.is_current = TRUE;
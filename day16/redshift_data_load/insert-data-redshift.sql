SET search_path TO analytics;
-- Refresh dims
DELETE FROM zone_dim WHERE is_current = TRUE;
INSERT INTO zone_dim (zone_id, borough, zone_name, service_zone)
SELECT DISTINCT locationid, borough, zone, service_zone
FROM stg_zone;

DELETE FROM vendor_dim WHERE is_current = TRUE;
INSERT INTO vendor_dim (vendor_id, vendor_name, source_system, effective_ts, expire_ts)
SELECT DISTINCT vendor_id,
       vendor_name,
       'unknown' AS source_system,
       SYSDATE   AS effective_ts,
       '9999-12-31'::timestamp AS expire_ts
FROM stg_vendor;

-- Load fact
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
JOIN vendor_dim vd
  ON vd.vendor_id = t.vendorid AND vd.is_current = TRUE
JOIN zone_dim pz
  ON pz.zone_id = t.pulocationid AND pz.is_current = TRUE
JOIN zone_dim dz
  ON dz.zone_id = t.dolocationid AND dz.is_current = TRUE;
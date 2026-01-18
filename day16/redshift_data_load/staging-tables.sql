SET search_path TO analytics;


-- Staging tables for raw loads
CREATE TABLE IF NOT EXISTS stg_zone (
    locationid      INT ENCODE az64,
    borough         VARCHAR(50) ENCODE lzo,
    zone            VARCHAR(100) ENCODE lzo,
    service_zone    VARCHAR(50) ENCODE lzo
)
DISTSTYLE ALL;

CREATE TABLE IF NOT EXISTS stg_vendor (
    vendor_id       INT ENCODE az64,
    vendor_name     VARCHAR(80) ENCODE lzo
)
DISTSTYLE ALL;



CREATE TABLE IF NOT EXISTS stg_trip (
    vendorid             INT ENCODE az64,
    tpep_pickup_datetime TIMESTAMP ENCODE az64,
    tpep_dropoff_datetime TIMESTAMP ENCODE az64,
    passenger_count      BIGINT ENCODE az64,
    trip_distance        DOUBLE PRECISION ENCODE RAW,
    ratecodeid           BIGINT ENCODE az64,
    store_and_fwd_flag   VARCHAR(1) ENCODE zstd,
    pulocationid         INT ENCODE az64,
    dolocationid         INT ENCODE az64,
    payment_type         BIGINT ENCODE az64,
    fare_amount          DOUBLE PRECISION ENCODE RAW,
    extra                DOUBLE PRECISION ENCODE RAW,
    mta_tax              DOUBLE PRECISION ENCODE RAW,
    tip_amount           DOUBLE PRECISION ENCODE RAW,
    tolls_amount         DOUBLE PRECISION ENCODE RAW,
    improvement_surcharge DOUBLE PRECISION ENCODE RAW,
    total_amount         DOUBLE PRECISION ENCODE RAW,
    congestion_surcharge DOUBLE PRECISION ENCODE RAW,
    airport_fee          DOUBLE PRECISION ENCODE RAW,
    cbd_congestion_fee   DOUBLE PRECISION ENCODE RAW
)
DISTSTYLE AUTO;
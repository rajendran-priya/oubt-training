-- Schema setup
CREATE SCHEMA IF NOT EXISTS analytics;
SET search_path TO analytics;
-- Conformed dimensions
CREATE TABLE IF NOT EXISTS zone_dim (
    zone_sk         INT IDENTITY(1,1),
    zone_id         INT NOT NULL,
    borough         VARCHAR(50),
    zone_name       VARCHAR(100),
    service_zone    VARCHAR(50),
    mdm_source      VARCHAR(30) DEFAULT 'taxi_zones_golden',
    is_current      BOOLEAN DEFAULT TRUE,
    effective_ts    TIMESTAMP DEFAULT SYSDATE,
    expire_ts       TIMESTAMP DEFAULT '9999-12-31 23:59:59'
)
DISTSTYLE ALL
SORTKEY (zone_id);

CREATE TABLE IF NOT EXISTS vendor_dim (
    vendor_sk       INT IDENTITY(1,1),
    vendor_id       INT NOT NULL,
    vendor_name     VARCHAR(80),
    source_system   VARCHAR(50),
    is_current      BOOLEAN DEFAULT TRUE,
    effective_ts    TIMESTAMP DEFAULT SYSDATE,
    expire_ts       TIMESTAMP DEFAULT '9999-12-31 23:59:59'
)
DISTSTYLE ALL
SORTKEY (vendor_id);

-- Optional calendar helper
CREATE TABLE IF NOT EXISTS date_dim (
    date_sk         INT IDENTITY(1,1),
    full_date       DATE NOT NULL,
    year            SMALLINT,
    quarter         SMALLINT,
    month           SMALLINT,
    day             SMALLINT,
    day_of_week     SMALLINT,
    week_of_year    SMALLINT
)
DISTSTYLE ALL
SORTKEY (full_date);

-- Fact table
CREATE TABLE IF NOT EXISTS trip_fact (
    trip_sk             BIGINT IDENTITY(1,1),
    vendor_sk           INT NOT NULL,
    pickup_zone_sk      INT NOT NULL,
    dropoff_zone_sk     INT NOT NULL,
    pickup_ts           TIMESTAMP NOT NULL,
    dropoff_ts          TIMESTAMP,
    passenger_count     SMALLINT,
    trip_distance_miles DECIMAL(10,2),
    rate_code_id        SMALLINT,
    payment_type        SMALLINT,
    fare_amount         DECIMAL(10,2),
    extra               DECIMAL(10,2),
    mta_tax             DECIMAL(10,2),
    tip_amount          DECIMAL(10,2),
    tolls_amount        DECIMAL(10,2),
    improvement_surcharge DECIMAL(10,2),
    congestion_surcharge  DECIMAL(10,2),
    total_amount        DECIMAL(10,2),
    store_and_fwd_flag  VARCHAR(1),
    load_ts             TIMESTAMP DEFAULT SYSDATE
)
DISTSTYLE KEY
DISTKEY (pickup_zone_sk)
SORTKEY (pickup_ts, vendor_sk);
CREATE SCHEMA IF NOT EXISTS day20_final_demo;
CREATE TABLE IF NOT EXISTS day20_final_demo.taxi_gold_enriched (
  vendorid                INTEGER,
  tpep_pickup_datetime     TIMESTAMP,
  tpep_dropoff_datetime    TIMESTAMP,
  passenger_count          BIGINT,
  trip_distance            DOUBLE PRECISION,
  ratecodeid               BIGINT,
  store_and_fwd_flag       VARCHAR(10),
  pulocationid             INTEGER,
  dolocationid             INTEGER,
  payment_type             BIGINT,
  fare_amount              DECIMAL(10,2),
  extra                    DOUBLE PRECISION,
  mta_tax                  DOUBLE PRECISION,
  tip_amount               DECIMAL(10,2),
  tolls_amount             DOUBLE PRECISION,
  improvement_surcharge    DOUBLE PRECISION,
  total_amount             DECIMAL(10,2),
  congestion_surcharge     DOUBLE PRECISION,
  airport_fee              DOUBLE PRECISION,
  cbd_congestion_fee       DOUBLE PRECISION,
  trip_duration_min        DOUBLE PRECISION,
  trip_revenue             DECIMAL(11,2),
  zone_id_lookup           VARCHAR(50),
  pickup_zone              VARCHAR(256),
  service_zone             VARCHAR(256),
  pickup_borough           VARCHAR(100)
)
DISTSTYLE AUTO;

CREATE TABLE IF NOT EXISTS day20_final_demo.dim_payment_type (
  payment_type_id  VARCHAR(50),
  description      VARCHAR(256),
  row_hash         VARCHAR(128),
  processed_at     TIMESTAMP,
  source_file      VARCHAR(512),
  created_by       VARCHAR(100),
  approved_by      VARCHAR(100),
  updated_by       VARCHAR(100),
  version          INTEGER,
  is_current       BOOLEAN,
  valid_from       TIMESTAMP,
  valid_to         TIMESTAMP,
  created_at       TIMESTAMP,
  hash_value       VARCHAR(128)
)
DISTSTYLE AUTO;

CREATE TABLE IF NOT EXISTS day20_final_demo.dim_ratecode (
  ratecode_id      VARCHAR(50),
  description      VARCHAR(256),
  row_hash         VARCHAR(128),
  processed_at     TIMESTAMP,
  source_file      VARCHAR(512),
  created_by       VARCHAR(100),
  approved_by      VARCHAR(100),
  updated_by       VARCHAR(100),
  version          INTEGER,
  is_current       BOOLEAN,
  valid_from       TIMESTAMP,
  valid_to         TIMESTAMP,
  created_at       TIMESTAMP,
  hash_value       VARCHAR(128)
)
DISTSTYLE AUTO;

CREATE TABLE IF NOT EXISTS day20_final_demo.dim_taxi_zone (
  locationid       VARCHAR(50),
  borough          VARCHAR(100),
  zone             VARCHAR(256),
  service_zone     VARCHAR(256),
  row_hash         VARCHAR(128),
  processed_at     TIMESTAMP,
  source_file      VARCHAR(512),
  created_by       VARCHAR(100),
  approved_by      VARCHAR(100),
  updated_by       VARCHAR(100),
  version          INTEGER,
  is_current       BOOLEAN,
  valid_from       TIMESTAMP,
  valid_to         TIMESTAMP,
  created_at       TIMESTAMP,
  hash_value       VARCHAR(128)
)
DISTSTYLE AUTO;

CREATE TABLE IF NOT EXISTS day20_final_demo.dim_vendor (
  vendor_id        VARCHAR(50),
  name             VARCHAR(256),
  row_hash         VARCHAR(128),
  processed_at     TIMESTAMP,
  source_file      VARCHAR(512),
  created_by       VARCHAR(100),
  approved_by      VARCHAR(100),
  updated_by       VARCHAR(100),
  version          INTEGER,
  is_current       BOOLEAN,
  valid_from       TIMESTAMP,
  valid_to         TIMESTAMP,
  created_at       TIMESTAMP,
  hash_value       VARCHAR(128)
)
DISTSTYLE AUTO;
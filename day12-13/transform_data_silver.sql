

/* =========================================================

* AUTHOR: Priya Rajendran

* DATE: 2026-01-11

* PROJECT: NYC Taxi Analytics (Hands-on)

* LAYER: SILVER (Standardization)

* PURPOSE: Clean and Deduplicate raw data into a structured table.

* =========================================================

*/

--DROP TABLE IF EXISTS "day12-sql-transformations"."silver_yellow_taxi";
--DROP TABLE IF EXISTS `day12-sql-transformations`.`silver_yellow_taxi`

CREATE TABLE "day12-sql-transformations"."silver_yellow_taxi" 
WITH (
  format = 'PARQUET',
  external_location = 's3://day12-sql-transformations/transformed_silver/',
  parquet_compression = 'SNAPPY'
) AS

SELECT DISTINCT--------------------------------removes duplicates
CAST(vendorid AS INT) AS vendor_id,
CAST(tpep_pickup_datetime AS TIMESTAMP) AS pickup_time,
CAST(tpep_dropoff_datetime AS TIMESTAMP) AS dropoff_time,
CAST(passenger_count AS INT) AS passenger_count,
CAST(trip_distance AS DOUBLE) AS trip_distance,
pulocationid AS pickup_zone,
dolocationid AS dropoff_zone,
-- Financial Surcharges
CAST(ratecodeid AS INT) AS rate_code_id,
CAST(payment_type AS INT) AS payment_type,
CAST(fare_amount AS DECIMAL(10,2)) AS fare_amount,
CAST(tip_amount AS DECIMAL(10,2)) AS tip_amount,
CAST(tolls_amount AS DECIMAL(10,2)) AS tolls_amount,
CAST(congestion_surcharge AS DECIMAL(10,2)) AS congestion_surcharge, 
CAST(airport_fee AS DECIMAL(10,2)) AS airport_fee, 
CAST(cbd_congestion_fee AS DECIMAL(10,2)) AS cbd_congestion_fee, 
CAST(total_amount AS DECIMAL(10,2)) AS total_paid

FROM

"AwsDataCatalog"."day12-sql-transformations"."raw"

WHERE

-- Handling Nulls (dropna logic)
tpep_pickup_datetime IS NOT NULL
AND tpep_dropoff_datetime IS NOT NULL
AND pulocationid IS NOT NULL
AND dolocationid IS NOT NULL
-- Business Logic Filters
AND passenger_count > 0
AND trip_distance > 0
AND fare_amount > 0
AND tpep_pickup_datetime < tpep_dropoff_datetime

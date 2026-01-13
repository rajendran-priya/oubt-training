# Transformations Documentation

This document summarizes the data transformations performed by the SQL scripts in `day12-13`.

Files
- `day12-13/transform_data_silver.sql` — SILVER layer: standardization, type-casting, filtering, de-duplication, and write to Parquet in S3.
- `day12-13/enrich_data_gold.sql` — GOLD layer: enrichment (joins to master data), time bucketing, aggregations and write to Parquet in S3.

## Summary: transform_data_silver.sql (Silver)

Purpose
- Clean and standardize raw taxi data and produce a deduplicated, typed `silver_yellow_taxi` table stored as Parquet in `s3://day12-sql-transformations/transformed_silver/`.

Inputs
- Source: `AwsDataCatalog.day12-sql-transformations.raw` (raw ingest table).

Main steps
- SELECT DISTINCT to remove duplicate rows.
- Type casting for fields: vendor, timestamps, counts, distances, numeric financial fields (DECIMAL, DOUBLE, INT, TIMESTAMP).
- Column renames/normalization: e.g. `tpep_pickup_datetime` -> `pickup_time`, `pulocationid` -> `pickup_zone`.
- Business logic filters (WHERE):
  - Non-null requirement: pickup/dropoff datetimes and pickup/dropoff zone ids must be present.
  - Positive values: `passenger_count > 0`, `trip_distance > 0`, `fare_amount > 0`.
  - Temporal sanity: `tpep_pickup_datetime < tpep_dropoff_datetime`.

Output
- Table: `day12-sql-transformations.silver_yellow_taxi` (Parquet, external_location `s3://day12-sql-transformations/transformed_silver/`).
- Key fields produced: `vendor_id`, `pickup_time`, `dropoff_time`, `passenger_count`, `trip_distance`, `pickup_zone`, `dropoff_zone`, `rate_code_id`, `payment_type`, `fare_amount`, `tip_amount`, `tolls_amount`, `congestion_surcharge`, `airport_fee`, `cbd_congestion_fee`, `total_paid`.

Assumptions & Notes
- `SELECT DISTINCT` is used for de-duplication; if dedupe keys should be specific, update the query to use `ROW_NUMBER()` windowing.
- Monetary fields are cast to DECIMAL(10,2) for consistent precision.
- The table is written as Parquet with SNAPPY compression.

## Summary: enrich_data_gold.sql (Gold)

Purpose
- Enrich the standardized silver data with zone/borough metadata and compute business metrics aggregated by pickup/dropoff borough and shift.

Inputs
- `day12-sql-transformations.silver_yellow_taxi` (silver output).
- `day12-sql-transformations.master_data` — contains zone/location metadata (e.g., `locationid`, `borough`, `zone`).

Main steps
- Join silver data to `master_data` twice:
  - `z_pu` join on `pickup_zone = locationid` to obtain `pickup_borough` and `pickup_zone`.
  - `z_do` join on `dropoff_zone = locationid` to obtain `dropoff_borough` and `dropoff_zone`.
- Compute derived fields:
  - `duration_min`: difference between pickup and dropoff in minutes.
  - `shift_type`: time-of-day bucket using pickup hour:
    - `Morning Rush`: 06:00–10:59
    - `Evening Rush`: 16:00–20:59
    - `Overnight`: 23:00–04:59
    - `Mid-Day`: otherwise
- Aggregation:
  - Group by `pickup_borough`, `dropoff_borough`, and `shift_type`.
  - Compute `total_trips`, `total_revenue` (sum of `total_paid`), `avg_duration_min`, `avg_tip`, and `rev_per_minute` (sum(total_paid) / sum(duration_min), null-safe).

Output
- Physical Parquet table at `s3://day12-sql-transformations/enriched_gold/` as `enriched_taxi_trips_gold_table`.

Assumptions & Notes
- The `shift_type` logic uses hour boundaries; adjust ranges if you want inclusive/exclusive semantics or different buckets.
- `rev_per_minute` uses `NULLIF` to avoid division by zero; rows where duration sums to zero yield NULL.
- The query currently uses left joins so missing master data will yield NULL boroughs; consider inner joins if you wish to drop unmatched records.

## Suggested Tests / Validation
- Row counts: compare raw → silver → gold counts to verify expected reductions.
- Spot-check types: validate TIMESTAMP casts and numeric precision in silver table.
- Permissions: ensure the GitHub Actions role used has `s3:PutObject` and `s3:ListBucket` for the target prefixes.
- Sample data check: run `SELECT * FROM silver_yellow_taxi LIMIT 10` and `SELECT * FROM enriched_taxi_trips_gold_table LIMIT 10`.

## Where to find the files
- `day12-13/transform_data_silver.sql`
- `day12-13/enrich_data_gold.sql`

---
Generated on 2026-01-12

# Governance: Dimension Derivation Logic

Purpose: document how conformed dimensions are derived from golden records to ensure transparency and reuse across data marts.

## zone_dim derivation (from taxi_zones golden records)
- **Source**: S3 object `taxi_zone_lookup.csv` produced by MDM as the golden record set; staged into `stg_zone`.
- **Keys**: `locationid` (business key) retained as `zone_id`; surrogate `zone_sk` generated in `zone_dim`.
- **Attributes**: `borough`, `zone` -> `zone_name`, `service_zone` copied without transformation.
- **Quality rules**: reject rows where `locationid` is null; trim whitespace on string fields if needed.
- **SCD policy**: Type 1 for now (overwrite current rows). For Type 2, add `is_current`, `effective_ts`, `expire_ts` updates on change detection.
- **Distribution**: `DISTSTYLE ALL` because the table is small and widely joined.
- **Sort**: `SORTKEY(zone_id)` for lookup efficiency.

## vendor_dim derivation (from vendor master golden records)
- **Source**: Vendor master CSV from MDM (includes `vendor_id`, `vendor_name`, `effective_ts`, `expire_ts`, `source_system`); staged into `stg_vendor`.
- **Keys**: `vendor_id` retained as business key; surrogate `vendor_sk` generated.
- **Attributes**: propagate names and lifecycle timestamps. Null `effective_ts` defaults to load time; null `expire_ts` defaults to open-ended.
- **Quality rules**: enforce non-null `vendor_id`; optionally deduplicate by `(vendor_id, effective_ts)` keeping the latest.
- **SCD policy**: Type 2 ready. Current load overwrites (`DELETE` + `INSERT`). For full Type 2, detect changes and close previous rows.
- **Distribution**: `DISTSTYLE ALL` to speed joins; table is small.
- **Sort**: `SORTKEY(vendor_id)`.

## Relationship to facts
- `trip_fact` joins to both dimensions by surrogate keys; foreign keys resolved during load using current dimension records.
- Any orphan trips (missing dimension matches) should be captured in an exception table before loading facts; treat as data quality issues feeding MDM stewardship.

## Stewardship and publishing
- MDM team publishes golden record files to S3 with versioned prefixes; analytics pipelines pull the latest approved snapshot.
- Conformed dimensions are the governance artifacts shared across data marts; downstream teams should not re-key or rename attributes without change control.

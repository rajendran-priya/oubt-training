# Day 16: Amazon Redshift and Dimensional Modeling

This folder contains practical steps and starter SQL to provision Redshift, model NYC taxi data with conformed dimensions, load from S3, and optimize for analytics. Adjust placeholders (cluster identifiers, ARNs, bucket names, regions) to your environment.

## Prerequisites
- AWS CLI configured with permissions to create Redshift, IAM roles, and S3 access.
- `psql` installed for SQL execution against Redshift.
- S3 bucket with NYC taxi data (trips), taxi zone golden records, and vendor master data (CSV with headers).
- Network access from your client to the Redshift cluster (VPC security group inbound on port 5439).

## High-level steps
1) **Provision Redshift cluster**
   - Create an IAM role with `AmazonS3ReadOnlyAccess` and trust policy for Redshift. Note the role ARN.
   - Create a Redshift cluster (ra3.xlplus/ra3.4xlarge preferred) with database name `dev`, master user, and attach the IAM role.
   - Configure security group to allow your IP on port 5439.

2) **Set up schema and tables**
   - Connect with `psql`: `psql -h <endpoint> -p 5439 -U <user> -d dev`.
   - Run the DDL in `redshift_ddl.sql` to create schemas, staging tables, conformed dimensions (`zone_dim`, `vendor_dim`), and `trip_fact`.

3) **Load data from S3**
   - Update placeholders in `copy_commands.sql` for your S3 paths and IAM role ARN.
   - Execute the file in Redshift: `\i copy_commands.sql`.
   - This stages raw data, derives dimensions from golden records, and populates the fact table.

4) **Optimize**
   - Run statistics and vacuum as needed: `ANALYZE; VACUUM;` (for large deletes/updates on columnar tables, run `VACUUM SORT ONLY`).
   - Validate distribution: co-locate joins on `pickup_zone_id` and `vendor_id` for common queries.

5) **Redshift Spectrum (S3 external access)**
   - If you need to query raw S3 data without loading, use the external schema snippet in `redshift_ddl.sql` (configure Glue catalog and role with `AmazonAthenaFullAccess` or scoped permissions).

6) **Analytics and verification**
   - Use the sample analytical queries in `copy_commands.sql` (bottom section) to validate counts, joins, and performance.

## Dimensional model (star schema)
- `zone_dim`: derived from taxi zone golden records; small, shared across marts; `DISTSTYLE ALL` for fast joins.
- `vendor_dim`: derived from vendor golden records; small; `DISTSTYLE ALL`.
- `trip_fact`: grain = one completed trip; `DISTSTYLE KEY` on `pickup_zone_id`; `SORTKEY` on `(pickup_ts, vendor_id)` for time/range filters and vendor grouping.
- Optional helpers: `date_dim` included for calendar attributes; `staging` tables hold raw ingests before conformance.

## MDM vs analytics modeling
- **Golden record (MDM)**: authoritative, normalized, stewarded; single source of truth for each entity.
- **Dimension (analytics)**: denormalized, SCD-tracked, query-optimized representation derived from golden records.
- **Relationship**: dimensions are published extracts of golden records; stewardship changes flow into dimensions via controlled pipelines.

### Conformed dimensions as governance artifacts
- Owned/published by MDM; consumed by analytics teams across marts.
- Enforce consistent keys/definitions for zones and vendors; include surrogate keys and business keys.

### Master data effectiveness metrics
- Duplicate rate: percent of potential duplicates detected.
- Match confidence distribution: volume by confidence band to focus stewarding effort.
- Orphan rate: percent of transactions referencing non-existent master data.
- Stewardship velocity: average time from proposed to approved golden records.
- Golden record completeness: percent of required attributes populated.
- Cross-reference accuracy: correctness of external ID mappings.

## Files
- `redshift_ddl.sql`: schema, staging tables, dimensions, fact, and Spectrum external schema example.
- `copy_commands.sql`: COPY commands, dimension derivations, fact loads, and sample analytical queries.
- `governance_derivations.md`: derivation logic for `zone_dim` and `vendor_dim` as governance artifacts.

SET search_path TO day17_SCD;



-- Clean staging tables
TRUNCATE stg_zone_delta;
TRUNCATE stg_vendor_delta;
TRUNCATE stg_trip;

-- Load golden records for zones
COPY stg_zone_delta
FROM 's3://day16-amazon-redshift/master-data/taxi_zone_lookup.csv'
IAM_ROLE 'arn:aws:iam::614302797935:role/service-role/AmazonRedshift-CommandsAccessRole-20260116T223605'
REGION 'us-east-1'
CSV IGNOREHEADER 1
ACCEPTINVCHARS;

-- Load golden records for vendors
COPY stg_vendor_delta
FROM 's3://day16-amazon-redshift/master-data/vendor.csv'
IAM_ROLE 'arn:aws:iam::614302797935:role/service-role/AmazonRedshift-CommandsAccessRole-20260116T223605'
REGION 'us-east-1'
CSV IGNOREHEADER 1
ACCEPTINVCHARS;

-- Load trip facts (raw)
COPY stg_trip
FROM 's3://day16-amazon-redshift/raw-data/yellow_tripdata_2025-08.parquet'
IAM_ROLE 'arn:aws:iam::614302797935:role/service-role/AmazonRedshift-CommandsAccessRole-20260116T223605'
REGION 'us-east-1'
FORMAT AS PARQUET-- Helps if the column has specific encoding issues
FILLRECORD
ACCEPTINVCHARS;
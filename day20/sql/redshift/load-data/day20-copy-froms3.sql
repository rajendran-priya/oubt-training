TRUNCATE TABLE day20_final_demo.dim_vendor;
COPY day20_final_demo.dim_vendor
FROM 's3://day20-demo-oubt/redshift_exports/dim_vendor/'
IAM_ROLE 'arn:aws:iam::614302797935:role/service-role/AmazonRedshift-CommandsAccessRole-20260116T223605'
REGION 'us-east-1'
FORMAT AS PARQUET;

TRUNCATE TABLE day20_final_demo.dim_payment_type;
COPY day20_final_demo.dim_payment_type
FROM 's3://day20-demo-oubt/redshift_exports/dim_payment_type/'
IAM_ROLE 'arn:aws:iam::614302797935:role/service-role/AmazonRedshift-CommandsAccessRole-20260116T223605'
REGION 'us-east-1'
FORMAT AS PARQUET;

TRUNCATE TABLE day20_final_demo.dim_ratecode;
COPY day20_final_demo.dim_ratecode
FROM 's3://day20-demo-oubt/redshift_exports/dim_ratecode/'
IAM_ROLE 'arn:aws:iam::614302797935:role/service-role/AmazonRedshift-CommandsAccessRole-20260116T223605'
REGION 'us-east-1'
FORMAT AS PARQUET;

TRUNCATE TABLE day20_final_demo.dim_taxi_zone;
COPY day20_final_demo.dim_taxi_zone
FROM 's3://day20-demo-oubt/redshift_exports/dim_taxi_zone/'
IAM_ROLE 'arn:aws:iam::614302797935:role/service-role/AmazonRedshift-CommandsAccessRole-20260116T223605'
REGION 'us-east-1'
FORMAT AS PARQUET;

TRUNCATE TABLE day20_final_demo.taxi_gold_enriched;
COPY day20_final_demo.taxi_gold_enriched
FROM 's3://day20-demo-oubt/redshift_exports/fact_taxi_gold_enriched/'
IAM_ROLE 'arn:aws:iam::614302797935:role/service-role/AmazonRedshift-CommandsAccessRole-20260116T223605'
REGION 'us-east-1'
FORMAT AS PARQUET;
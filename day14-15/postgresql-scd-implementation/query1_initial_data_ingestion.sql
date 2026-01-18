-- 1. Add a new customer to staging
TRUNCATE staging_customer;
INSERT INTO staging_customer (customer_id, name, city) 
VALUES ('C101', 'Alice Johnson', 'New York');

-- 2. Run the SCD Merge (Creates Version 1)
CALL scd2_merge('ingest_service', 'Initial migration');

-- 3. Verify: You should see 1 active record with version_number 1
SELECT * FROM dim_customer ;
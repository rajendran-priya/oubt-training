-- This empties the tables and resets the auto-incrementing identity keys (serial)
TRUNCATE TABLE dim_customer RESTART IDENTITY;
TRUNCATE TABLE scd_version_audit RESTART IDENTITY;
TRUNCATE TABLE staging_customer;
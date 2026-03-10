-- 1. Rollback C101 back to Version 1
-- Note: '2' here is the record_id (customer_key), '1' is the target version
CALL rollback_version(2, 1, 'Input error, reverting to previous city');

-- 2. Verify: You should now see THREE rows
-- Version 1: Expired (Old NYC)
-- Version 2: Expired (Bad London)
-- Version 3: Current (New NYC created by rollback)
SELECT * FROM dim_customer ;
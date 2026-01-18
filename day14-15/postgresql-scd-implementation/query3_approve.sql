-- 1. Find the customer_key for the 'London' version (it's likely key 2)
-- 2. Approve it
CALL approve_version(2, 'Manager_Bob', 'Proof of address verified');

-- 3. Verify the flags
SELECT *
FROM dim_customer ;
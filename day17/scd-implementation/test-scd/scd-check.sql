SET search_path TO day17_SCD;
-- 1) Stage the change (already done)
/*TRUNCATE stg_vendor_delta;
INSERT INTO stg_vendor_delta (vendor_id, vendor_name)
VALUES (7, 'Helix-Renamed');
*/
/*SELECT * FROM vendor_dim_scd;
-- Close current version (v2)
/*UPDATE vendor_dim_scd
SET expire_ts = SYSDATE, is_current = FALSE
WHERE vendor_id = 7 AND is_current = TRUE;

-- Reopen prior version (v1) as current
UPDATE vendor_dim_scd
SET expire_ts = '9999-12-31 23:59:59',
    is_current = TRUE
WHERE vendor_id = 7
  AND version = 1;  -- the version you want to restore
  */

/*

BEGIN;

-- 1) Capture the version you want to restore
CREATE TEMP TABLE tmp_vendor_restore AS
SELECT vendor_id, vendor_name
FROM vendor_dim_scd
WHERE vendor_id = 7 AND version = 1;  -- version to restore

-- 2) Close the current row
UPDATE vendor_dim_scd
SET expire_ts = SYSDATE, is_current = FALSE
WHERE vendor_id = 7 AND is_current = TRUE;

-- 3) Insert a new version that matches the saved attributes
INSERT INTO vendor_dim_scd (vendor_id, vendor_name, version, is_current, effective_ts, expire_ts)
SELECT t.vendor_id,
       t.vendor_name,
       (SELECT COALESCE(MAX(version), 0) + 1 FROM vendor_dim_scd d WHERE d.vendor_id = t.vendor_id) AS version,
       TRUE,
       SYSDATE,
       '9999-12-31 23:59:59'
FROM tmp_vendor_restore t;

DROP TABLE tmp_vendor_restore;

COMMIT;
*/

SELECT * FROM vendor_dim_scd;
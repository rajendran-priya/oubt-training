-- scd_postgres_procs.sql
-- Implements SCD Type 2 with metadata, stored procedures for merge, approve, rollback, and audit

-----------------------------------------------------------
-- 1. SCHEMA SETUP
-----------------------------------------------------------

CREATE TABLE IF NOT EXISTS dim_customer (
    customer_key serial PRIMARY KEY,
    customer_id text NOT NULL,
    name text,
    city text,
    version_number integer NOT NULL,
    change_reason text,
    created_by text,
    approved boolean DEFAULT false,
    approved_by text,
    approval_reason text,
    version_created_at timestamptz NOT NULL DEFAULT now(),
    effective_from timestamptz NOT NULL DEFAULT now(),
    effective_to timestamptz DEFAULT '9999-12-31 23:59:59',
    is_current boolean NOT NULL DEFAULT true
);

CREATE INDEX IF NOT EXISTS idx_dim_customer_custid ON dim_customer(customer_id);

CREATE TABLE IF NOT EXISTS scd_version_audit (
    audit_id serial PRIMARY KEY,
    customer_id text,
    customer_key int,
    version_number int,
    action text,
    actor text,
    reason text,
    action_ts timestamptz DEFAULT now()
);

-- Staging table for incoming batch data
CREATE TABLE IF NOT EXISTS staging_customer (
    customer_id text,
    name text,
    city text
);

-----------------------------------------------------------
-- 2. CORE SCD PROCEDURES
-----------------------------------------------------------

-- Procedure: scd2_merge
-- Main engine for processing Type 2 logic
CREATE OR REPLACE PROCEDURE scd2_merge(p_created_by text, p_change_reason text)
LANGUAGE plpgsql
AS $$
DECLARE
    rec RECORD;
    max_ver integer;
    new_key integer;
BEGIN
    FOR rec IN SELECT * FROM staging_customer LOOP
        
        -- Check if record exists and is current
        IF EXISTS (SELECT 1 FROM dim_customer d WHERE d.customer_id = rec.customer_id AND d.is_current = true) THEN
            
            -- Detect attribute changes
            IF EXISTS (
                SELECT 1 FROM dim_customer d
                WHERE d.customer_id = rec.customer_id AND d.is_current = true
                AND (d.name IS DISTINCT FROM rec.name OR d.city IS DISTINCT FROM rec.city)
            ) THEN
                -- A: Expire old record
                UPDATE dim_customer
                SET is_current = false,
                    effective_to = now()
                WHERE customer_id = rec.customer_id AND is_current = true;

                -- B: Get next version number
                SELECT COALESCE(MAX(version_number),0) + 1 INTO max_ver 
                FROM dim_customer WHERE customer_id = rec.customer_id;

                -- C: Insert new version
                INSERT INTO dim_customer (customer_id, name, city, version_number, change_reason, created_by, approved, is_current)
                VALUES (rec.customer_id, rec.name, rec.city, max_ver, p_change_reason, p_created_by, false, true)
                RETURNING customer_key INTO new_key;

                INSERT INTO scd_version_audit (customer_id, customer_key, version_number, action, actor, reason)
                VALUES (rec.customer_id, new_key, max_ver, 'insert_new_version', p_created_by, p_change_reason);
            END IF;

        ELSE
            -- New business key: Insert initial version (v1)
            INSERT INTO dim_customer (customer_id, name, city, version_number, change_reason, created_by, approved, is_current)
            VALUES (rec.customer_id, rec.name, rec.city, 1, p_change_reason, p_created_by, false, true)
            RETURNING customer_key INTO new_key;

            INSERT INTO scd_version_audit (customer_id, customer_key, version_number, action, actor, reason)
            VALUES (rec.customer_id, new_key, 1, 'insert_initial_version', p_created_by, p_change_reason);
        END IF;

    END LOOP;
 --   COMMIT; -- Save all changes in the batch
END;
$$;

-----------------------------------------------------------
-- 3. VERSION MANAGEMENT PROCEDURES
-----------------------------------------------------------

-- Procedure: approve_version
CREATE OR REPLACE PROCEDURE approve_version(p_record_id int, p_approver text, p_reason text)
LANGUAGE plpgsql
AS $$
DECLARE
    v_customer_id text;
    v_version int;
BEGIN
    SELECT customer_id, version_number INTO v_customer_id, v_version 
    FROM dim_customer WHERE customer_key = p_record_id;
    
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Record id % not found', p_record_id;
    END IF;

    UPDATE dim_customer
    SET approved = true,
        approved_by = p_approver,
        approval_reason = p_reason
    WHERE customer_key = p_record_id;

    INSERT INTO scd_version_audit (customer_id, customer_key, version_number, action, actor, reason)
    VALUES (v_customer_id, p_record_id, v_version, 'approve_version', p_approver, p_reason);
    
   -- COMMIT;
END;
$$;

-- Procedure: rollback_version
CREATE OR REPLACE PROCEDURE rollback_version(p_record_id int, p_target_version int, p_reason text)
LANGUAGE plpgsql
AS $$
DECLARE
    v_customer_id text;
    v_new_ver int;
    v_new_key int;
BEGIN
    -- Identify the customer_id based on the provided record_id
    SELECT customer_id INTO v_customer_id FROM dim_customer WHERE customer_key = p_record_id;
    
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Record id % not found', p_record_id;
    END IF;

    -- Verify target version exists
    IF NOT EXISTS (SELECT 1 FROM dim_customer WHERE customer_id = v_customer_id AND version_number = p_target_version) THEN
        RAISE EXCEPTION 'Target version % for customer % not found', p_target_version, v_customer_id;
    END IF;

    -- 1. Expire current version
    UPDATE dim_customer SET is_current = false, effective_to = now() 
    WHERE customer_id = v_customer_id AND is_current = true;

    -- 2. Determine the next version number for the rollback entry
    SELECT MAX(version_number) + 1 INTO v_new_ver FROM dim_customer WHERE customer_id = v_customer_id;

    -- 3. Copy target version data into a NEW current record (maintains audit trail)
    INSERT INTO dim_customer (customer_id, name, city, version_number, change_reason, created_by, is_current)
    SELECT customer_id, name, city, v_new_ver, 'Rollback to v' || p_target_version, 'system_rollback', true
    FROM dim_customer 
    WHERE customer_id = v_customer_id AND version_number = p_target_version
    RETURNING customer_key INTO v_new_key;

    -- 4. Audit the action
    INSERT INTO scd_version_audit (customer_id, customer_key, version_number, action, actor, reason)
    VALUES (v_customer_id, v_new_key, v_new_ver, 'rollback', current_user, p_reason);
    
   -- COMMIT;
END;
$$;

-----------------------------------------------------------
-- 4. AUDIT FUNCTION (Kept as Function to allow SELECT)
-----------------------------------------------------------

CREATE OR REPLACE FUNCTION audit_version_history(p_record_id int, p_start timestamptz DEFAULT NULL, p_end timestamptz DEFAULT NULL)
RETURNS TABLE(audit_id int, customer_id text, customer_key int, version_number int, action text, actor text, reason text, action_ts timestamptz) 
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT a.audit_id, a.customer_id, a.customer_key, a.version_number, a.action, a.actor, a.reason, a.action_ts
    FROM scd_version_audit a
    WHERE a.customer_id = (SELECT customer_id FROM dim_customer WHERE customer_key = p_record_id)
      AND (p_start IS NULL OR a.action_ts >= p_start)
      AND (p_end IS NULL OR a.action_ts <= p_end)
    ORDER BY a.action_ts DESC;
END;
$$;
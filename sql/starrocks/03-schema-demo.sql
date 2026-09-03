-- ================================================================
-- StarRocks: Schema Evolution Demo (Pit 2)
-- ================================================================
-- Shows the "Fragment must be equal to partition column count" error
-- that StarRocks throws when Paimon tables have cross-schema partitions
-- ================================================================

SET CATALOG paimon_catalog;

SELECT '===== Schema Evolution + StarRocks (Pit 2) =====' AS report;

-- ================================================================
-- Scenario: Paimon table has partitions with different schemas
-- ================================================================

-- Check current schema
SELECT 'Step 1: Current table schema' AS info;
SHOW CREATE TABLE paimon_db.orders;

-- If we had run ALTER TABLE ADD COLUMN on Paimon (via Flink),
-- old partitions would have 2 columns, new partitions 3 columns.
-- StarRocks would throw:
--   ERROR: Fragment must be equal to partition column count
--
-- Root cause: StarRocks reads manifest, finds different schema-ids
-- across partitions, and cannot generate a unified query plan.

-- ================================================================
-- Workaround 1: Use reserved ext_field columns (our approach)
-- ================================================================
SELECT 'Step 2: Reserved field approach (no ALTER needed)' AS info;

SELECT
    order_id,
    amount,
    ext_field1 AS customer_level,
    ext_field2 AS channel
FROM paimon_db.orders
LIMIT 10;

-- This works because ext_field1-5 are in the ORIGINAL schema
-- No ALTER TABLE = no schema-id mismatch = StarRocks is happy

-- ================================================================
-- Workaround 2: If ALTER was already done, rewrite history
-- ================================================================
SELECT 'Step 3: If ALTER was done, must rewrite old partitions' AS info;
-- In Flink SQL:
-- INSERT OVERWRITE paimon_db.orders PARTITION (dt = '2026-09-01')
-- SELECT order_id, amount, dt, NULL AS customer_level, NULL AS channel, ...;

-- After rewrite, all partitions have same schema → StarRocks works

-- ================================================================
-- Workaround 3: Query only new partitions (temporary fix)
-- ================================================================
SELECT 'Step 4: Temporary fix - query only new partitions' AS info;

SELECT
    order_id,
    amount,
    dt
FROM paimon_db.orders
WHERE dt = '2026-09-02'
LIMIT 10;
-- Only querying one partition avoids cross-schema issue temporarily

-- ================================================================
-- 11-checkpoint-demo.sql: Checkpoint Recovery Demo (Pit 2)
-- ================================================================
-- Shows: schema.evolution.enabled config
-- Shows: how checkpoint recovery with old schema causes errors
-- ================================================================

USE CATALOG paimon;

SELECT '===== Checkpoint Recovery Demo (Pit 2) =====' AS report;

-- ================================================================
-- Table option: schema.evolution.enabled
-- ================================================================
-- Without this, Flink read jobs cache schema at startup.
-- After ALTER TABLE, the read job uses old schema -> error.
--
-- Solution: set in table options:
--   'schema.evolution.enabled' = 'true'
--
-- Or restart the read job (without checkpoint) to get latest schema.

-- ================================================================
-- Simulate: query with Time Travel (read old snapshot)
-- ================================================================
SELECT 'Time Travel: read historical snapshot' AS info;

-- Query a specific snapshot (replace snapshot-id with actual)
-- SELECT * FROM paimon_db.orders /*+ OPTIONS('scan.snapshot-id' = '1') */;

-- ================================================================
-- Verify: schema versions
-- ================================================================
SELECT 'Current schema versions in table' AS info;

-- Show all snapshots (uncomment to run)
-- SELECT * FROM paimon_db.orders$snapshots;
-- SELECT * FROM paimon_db.orders$schemas;

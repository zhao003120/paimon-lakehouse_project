-- ================================================================
-- 10-schema-evolution.sql: Schema Evolution Demo (Pit 2)
-- ================================================================
-- Demonstrates: ALTER ADD COLUMN -> cross-schema query issue
-- Shows the "reserved field" workaround
-- ================================================================

USE CATALOG paimon;

-- ================================================================
-- Demo: Why we use reserved fields instead of ALTER TABLE
-- ================================================================

-- Step 1: Simulate adding a new column (the WRONG way)
SELECT '===== Schema Evolution Demo (Pit 2) =====' AS report;
SELECT 'Step 1: This is what happens with ALTER TABLE ADD COLUMN' AS info;

-- Uncomment to test (will cause issues with StarRocks cross-schema reads):
-- ALTER TABLE paimon_db.orders ADD COLUMN customer_level STRING;

-- Step 2: The workaround - use reserved ext_field1
SELECT 'Step 2: We use ext_field1 instead (reserved field)' AS info;
SELECT
    order_id,
    amount,
    ext_field1 AS customer_level
FROM paimon_db.orders
LIMIT 5;

-- Step 3: Show the problem with old partitions
SELECT 'Step 3: Old partitions still have NULL in new columns' AS info;
SELECT
    dt,
    COUNT(*) AS orders,
    COUNT(ext_field1) AS has_customer_level,
    COUNT(*) - COUNT(ext_field1) AS missing_count
FROM paimon_db.orders
GROUP BY dt
ORDER BY dt;

-- Step 4: The reserved field approach
SELECT 'Step 4: Reserved field approach - no ALTER needed' AS info;
SELECT '
  Instead of:  ALTER TABLE orders ADD COLUMN customer_level STRING;
  We use:      ext_field1 (already in schema) to store customer_level
  
  Pros: No schema-id mismatch, no history rewrite
  Cons: Need to know field semantics from documentation
' AS explanation;

-- ================================================================
-- StarRocks: Benchmark - StarRocks vs Flink SQL direct read
-- ================================================================
-- Same queries, compare execution time
-- StarRocks advantages: Data Cache + CBO + Vectorized Engine
-- ================================================================

SET CATALOG paimon_catalog;

-- ================================================================
-- Query 1: COUNT(*) - full scan
-- ================================================================
SELECT '===== Benchmark 1: COUNT(*) =====' AS test;

SELECT COUNT(*) AS total_rows
FROM paimon_db.orders;

-- Expected: StarRocks ~1.2s vs Flink ~7-12s
-- StarRocks uses Data Cache on second run: ~0.2s

-- ================================================================
-- Query 2: GROUP BY + TOP 10 - aggregation
-- ================================================================
SELECT '===== Benchmark 2: TOP 10 by amount =====' AS test;

SELECT
    dt,
    COUNT(*)           AS order_count,
    CAST(SUM(amount) AS DECIMAL(18,2)) AS total_amount,
    CAST(AVG(amount) AS DECIMAL(18,2)) AS avg_amount
FROM dwd.dwd_order_detail
WHERE is_valid = TRUE
GROUP BY dt
ORDER BY total_amount DESC
LIMIT 10;

-- Expected: StarRocks ~2.4s vs Flink ~15-25s
-- CBO pushes filter down to Parquet reader, skips non-matching row groups

-- ================================================================
-- Query 3: Multi-table JOIN - CBO advantage
-- ================================================================
SELECT '===== Benchmark 3: JOIN with aggregation =====' AS test;

SELECT
    c.customer_level,
    COUNT(*)                                    AS order_count,
    CAST(SUM(o.amount) AS DECIMAL(18,2))        AS total_amount,
    CAST(AVG(o.amount) AS DECIMAL(18,2))        AS avg_amount
FROM dwd.dwd_order_detail o
JOIN ads.ads_customer_rank c
    ON o.dt = c.dt AND o.customer_level = c.customer_level
WHERE o.is_valid = TRUE
GROUP BY c.customer_level
ORDER BY total_amount DESC;

-- Expected: StarRocks ~3.0s vs Flink ~15-25s
-- CBO selects Broadcast JOIN (small table broadcast) instead of Shuffle JOIN

-- ================================================================
-- Query 4: Data Cache effect (run twice, second time faster)
-- ================================================================
SELECT '===== Benchmark 4: Data Cache (run 1) =====' AS test;

SELECT dt, COUNT(*) AS cnt FROM paimon_db.orders GROUP BY dt;

SELECT '===== Benchmark 4: Data Cache (run 2 - should be faster) =====' AS test;

SELECT dt, COUNT(*) AS cnt FROM paimon_db.orders GROUP BY dt;

-- First run: reads from MinIO S3 (~1s)
-- Second run: reads from local Data Cache (~0.1s)

-- ================================================================
-- Show Data Cache stats
-- ================================================================
-- SELECT '===== Data Cache Stats =====' AS test;
-- SHOW CACHE FOR paimon_catalog.paimon_db.orders;

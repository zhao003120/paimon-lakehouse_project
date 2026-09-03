-- ================================================================
-- StarRocks: Report Queries (same logic as Flink 09-report.sql)
-- ================================================================
-- Run AFTER Flink pipeline has written data to Paimon
-- StarRocks reads Paimon tables via external catalog
-- ================================================================

SET CATALOG paimon_catalog;

-- ================================================================
-- Report 1: KPI Dashboard
-- ================================================================
SELECT '===== KPI Dashboard (via StarRocks) =====' AS report;

SELECT
    dt              AS `date`,
    total_orders    AS orders,
    total_amount    AS amount,
    avg_amount      AS avg_price,
    vip_count       AS vip_orders,
    vip_ratio       AS vip_pct,
    refund_count    AS refunds,
    refund_rate     AS refund_pct
FROM ads.ads_order_kpi
ORDER BY dt;

-- ================================================================
-- Report 2: Customer Level Ranking
-- ================================================================
SELECT '===== Customer Level Ranking (via StarRocks) =====' AS report;

SELECT
    dt              AS `date`,
    customer_level,
    order_count     AS orders,
    total_amount    AS amount,
    avg_amount      AS avg_price,
    amount_rank     AS `rank`
FROM ads.ads_customer_rank
ORDER BY dt, amount_rank;

-- ================================================================
-- Report 3: Channel Analysis
-- ================================================================
SELECT '===== Channel Analysis (via StarRocks) =====' AS report;

SELECT
    dt              AS `date`,
    channel,
    order_count     AS orders,
    total_amount    AS amount,
    avg_amount      AS avg_price,
    refund_rate     AS refund_pct
FROM ads.ads_channel_stat
ORDER BY dt, channel;

-- ================================================================
-- Report 4: Daily Trend
-- ================================================================
SELECT '===== Daily Trend (via StarRocks) =====' AS report;

SELECT
    dt          AS `date`,
    order_count AS orders,
    total_amount AS amount,
    avg_amount  AS avg_price
FROM dws.dws_order_daily
ORDER BY dt;

-- ================================================================
-- Report 5: Dirty Data Traceability (Pit 3)
-- ================================================================
SELECT '===== Dirty Data Traceability (via StarRocks) =====' AS report;

SELECT
    reject_reason,
    COUNT(*)     AS dirty_count,
    MIN(amount)  AS min_amount,
    MAX(amount)  AS max_amount
FROM paimon_db.dirty_orders
GROUP BY reject_reason
ORDER BY dirty_count DESC;

-- ================================================================
-- Report 6: Full-chain Reconciliation
-- ================================================================
SELECT '===== Full-chain Reconciliation (via StarRocks) =====' AS report;

SELECT 'ODS' AS `layer`, 'paimon_db.orders' AS table_name, CAST(SUM(amount) AS DECIMAL(18,2)) AS total_amount
FROM paimon_db.orders
UNION ALL
SELECT 'DWD', 'dwd.dwd_order_detail', CAST(SUM(amount) AS DECIMAL(18,2))
FROM dwd.dwd_order_detail
UNION ALL
SELECT 'DWS', 'dws.dws_order_daily', CAST(SUM(total_amount) AS DECIMAL(18,2))
FROM dws.dws_order_daily
UNION ALL
SELECT 'ADS', 'ads.ads_order_kpi', CAST(SUM(total_amount) AS DECIMAL(18,2))
FROM ads.ads_order_kpi;

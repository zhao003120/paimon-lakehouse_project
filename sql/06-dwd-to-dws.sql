-- ================================================================
-- 06-dwd-to-dws.sql: DWD -> DWS Aggregation
-- ================================================================

USE CATALOG paimon;

-- Daily aggregation
INSERT INTO dws.dws_order_daily
SELECT
    dt,
    COUNT(*)           AS order_count,
    CAST(SUM(amount) AS DECIMAL(18,2))  AS total_amount,
    CAST(AVG(amount) AS DECIMAL(18,2))  AS avg_amount,
    0                  AS refund_count,
    CAST(0.0 AS DECIMAL(10,4)) AS refund_rate
FROM dwd.dwd_order_detail
WHERE is_valid = TRUE
GROUP BY dt;

-- Weekly aggregation (2026-09-01 ~ 2026-09-07)
INSERT INTO dws.dws_order_weekly
SELECT
    '2026-09-01'       AS week_start,
    '2026-09-07'       AS week_end,
    COUNT(*)           AS order_count,
    CAST(SUM(amount) AS DECIMAL(18,2))  AS total_amount,
    CAST(AVG(amount) AS DECIMAL(18,2))  AS avg_amount
FROM dwd.dwd_order_detail
WHERE dt BETWEEN '2026-09-01' AND '2026-09-07'
  AND is_valid = TRUE;

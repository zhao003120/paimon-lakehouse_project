-- ================================================================
-- 08-dws-to-ads.sql: DWS -> ADS Application layer
-- ================================================================

USE CATALOG paimon;
SET 'table.exec.adaptive-parallelism.enabled' = 'false';

-- ================================================================
-- ADS: KPI dashboard
-- ================================================================
INSERT INTO ads.ads_order_kpi
SELECT
    d.dt,
    d.order_count                                          AS total_orders,
    d.total_amount                                          AS total_amount,
    d.avg_amount                                            AS avg_amount,
    COUNT(CASE WHEN c.customer_level = 'VIP' THEN 1 END)   AS vip_count,
    CAST(
        COUNT(CASE WHEN c.customer_level = 'VIP' THEN 1 END) * 1.0
        / d.order_count AS DECIMAL(10,4)
    )                                                       AS vip_ratio,
    d.refund_count                                          AS refund_count,
    d.refund_rate                                           AS refund_rate
FROM dws.dws_order_daily d
LEFT JOIN dwd.dwd_order_detail c
    ON c.dt = d.dt
GROUP BY
    d.dt, d.order_count, d.total_amount,
    d.avg_amount, d.refund_count, d.refund_rate;

-- ================================================================
-- ADS: Customer level ranking
-- ================================================================
INSERT INTO ads.ads_customer_rank
SELECT
    dt,
    customer_level,
    COUNT(*)                                           AS order_count,
    CAST(SUM(amount) AS DECIMAL(18,2))                 AS total_amount,
    CAST(AVG(amount) AS DECIMAL(18,2))                 AS avg_amount,
    ROW_NUMBER() OVER (PARTITION BY dt ORDER BY SUM(amount) DESC) AS amount_rank
FROM dwd.dwd_order_detail
WHERE is_valid = TRUE
GROUP BY dt, customer_level;

-- ================================================================
-- ADS: Channel statistics
-- ================================================================
INSERT INTO ads.ads_channel_stat
SELECT
    dt,
    channel,
    COUNT(*)                                           AS order_count,
    CAST(SUM(amount) AS DECIMAL(18,2))                 AS total_amount,
    CAST(AVG(amount) AS DECIMAL(18,2))                 AS avg_amount,
    CAST(0.0 AS DECIMAL(10,4))                         AS refund_rate
FROM dwd.dwd_order_detail
WHERE is_valid = TRUE
GROUP BY dt, channel;

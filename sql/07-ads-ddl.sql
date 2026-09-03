-- ================================================================
-- 07-ads-ddl.sql: ADS Layer - Application tables
-- ================================================================

USE CATALOG paimon;

-- ADS: KPI dashboard (one row per day)
CREATE TABLE IF NOT EXISTS ads.ads_order_kpi (
    dt              STRING,
    total_orders    BIGINT,
    total_amount    DECIMAL(18,2),
    avg_amount      DECIMAL(18,2),
    vip_count       BIGINT,
    vip_ratio       DECIMAL(10,4),
    refund_count    BIGINT,
    refund_rate     DECIMAL(10,4),
    PRIMARY KEY (dt) NOT ENFORCED
) WITH (
    'connector' = 'paimon',
    'bucket' = '2',
    'file.format' = 'parquet',
    'sink.parallelism' = '1'
);

-- ADS: customer level ranking
CREATE TABLE IF NOT EXISTS ads.ads_customer_rank (
    dt              STRING,
    customer_level  STRING,
    order_count     BIGINT,
    total_amount    DECIMAL(18,2),
    avg_amount      DECIMAL(18,2),
    amount_rank     INT,
    PRIMARY KEY (dt, customer_level) NOT ENFORCED
) WITH (
    'connector' = 'paimon',
    'bucket' = '4',
    'file.format' = 'parquet',
    'sink.parallelism' = '1'
);

-- ADS: channel statistics
CREATE TABLE IF NOT EXISTS ads.ads_channel_stat (
    dt              STRING,
    channel         STRING,
    order_count     BIGINT,
    total_amount    DECIMAL(18,2),
    avg_amount      DECIMAL(18,2),
    refund_rate     DECIMAL(10,4),
    PRIMARY KEY (dt, channel) NOT ENFORCED
) WITH (
    'connector' = 'paimon',
    'bucket' = '4',
    'file.format' = 'parquet',
    'sink.parallelism' = '1'
);

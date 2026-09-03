-- ================================================================
-- 05-dws-ddl.sql: DWS Layer - Daily & Weekly aggregation
-- ================================================================

USE CATALOG paimon;

-- DWS: daily aggregation
CREATE TABLE IF NOT EXISTS dws.dws_order_daily (
    dt              STRING,
    order_count     BIGINT,
    total_amount    DECIMAL(18,2),
    avg_amount      DECIMAL(18,2),
    refund_count    BIGINT,
    refund_rate     DECIMAL(10,4),
    PRIMARY KEY (dt) NOT ENFORCED
) WITH (
    'connector' = 'paimon',
    'bucket' = '4',
    'file.format' = 'parquet',
    'sink.parallelism' = '1'
);

-- DWS: weekly aggregation
CREATE TABLE IF NOT EXISTS dws.dws_order_weekly (
    week_start      STRING,
    week_end        STRING,
    order_count     BIGINT,
    total_amount    DECIMAL(18,2),
    avg_amount      DECIMAL(18,2),
    PRIMARY KEY (week_start) NOT ENFORCED
) WITH (
    'connector' = 'paimon',
    'bucket' = '2',
    'file.format' = 'parquet',
    'sink.parallelism' = '1'
);

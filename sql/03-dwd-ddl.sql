-- ================================================================
-- 03-dwd-ddl.sql: DWD Layer - Detail wide table
-- ================================================================
-- ODS ext_field1 = customer_level
-- ODS ext_field2 = channel
-- ODS ext_field3 = order_type
-- ================================================================

USE CATALOG paimon;

CREATE TABLE IF NOT EXISTS dwd.dwd_order_detail (
    order_id        STRING,
    amount          DECIMAL(12,2),
    dt              STRING,
    customer_level  STRING,
    channel         STRING,
    order_type      STRING,
    is_valid        BOOLEAN,
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'connector' = 'paimon',
    'bucket' = '8',
    'partition' = 'dt',
    'file.format' = 'parquet'
);

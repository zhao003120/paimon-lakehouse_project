-- ================================================================
-- 01-ods-ddl.sql: ODS Layer - Raw data tables
-- ================================================================
-- Pit 1 fix: FULL-COMPACTION + 3min checkpoint (table options)
-- Pit 2 fix: Reserved extension fields (ext_field1-5)
-- Pit 3 fix: dirty_orders table for rejected data
-- ================================================================

USE CATALOG paimon;

-- ================================================================
-- ODS: orders table (with Pit 1 & Pit 2 fixes)
-- ================================================================
CREATE TABLE IF NOT EXISTS paimon_db.orders (
    order_id        STRING,
    amount          DECIMAL(12,2),
    dt              STRING,
    -- Pit 2 fix: reserved extension fields (avoid ALTER TABLE)
    ext_field1      STRING,
    ext_field2      STRING,
    ext_field3      STRING,
    ext_field4      STRING,
    ext_field5      STRING,
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'connector' = 'paimon',
    'bucket' = '8',
    'changelog-producer' = 'full-compaction',
    'full-compaction.delta-commits' = '10',
    'partition' = 'dt',
    'file.format' = 'parquet'
);

-- ================================================================
-- ODS: dirty_orders table (Pit 3 fix: trace rejected data)
-- ================================================================
CREATE TABLE IF NOT EXISTS paimon_db.dirty_orders (
    order_id        STRING,
    amount          STRING,
    dt              STRING,
    reject_reason   STRING,
    raw_data        STRING,
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'connector' = 'paimon',
    'bucket' = '4',
    'partition' = 'dt',
    'file.format' = 'parquet'
);

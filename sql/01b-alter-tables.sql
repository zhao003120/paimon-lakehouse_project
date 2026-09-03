-- ================================================================
-- 01b-alter-tables.sql: Update table options for existing tables
-- ================================================================
-- Run this if tables were created before sink.parallelism was added
-- CREATE TABLE IF NOT EXISTS skips if table exists, so we need ALTER TABLE
-- ================================================================

USE CATALOG paimon;

-- ODS
ALTER TABLE paimon_db.orders SET ('sink.parallelism' = '2');
ALTER TABLE paimon_db.dirty_orders SET ('sink.parallelism' = '2');

-- DWD
ALTER TABLE dwd.dwd_order_detail SET ('sink.parallelism' = '2');

-- DWS
ALTER TABLE dws.dws_order_daily SET ('sink.parallelism' = '2');
ALTER TABLE dws.dws_order_weekly SET ('sink.parallelism' = '2');

-- ADS
ALTER TABLE ads.ads_order_kpi SET ('sink.parallelism' = '2');
ALTER TABLE ads.ads_customer_rank SET ('sink.parallelism' = '2');
ALTER TABLE ads.ads_channel_stat SET ('sink.parallelism' = '2');

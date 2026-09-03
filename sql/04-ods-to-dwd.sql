-- ================================================================
-- 04-ods-to-dwd.sql: ODS -> DWD ETL
-- ================================================================
-- JSON parse (ext_field extraction) + dimension enrichment
-- ================================================================

USE CATALOG paimon;
SET 'table.exec.adaptive-parallelism.enabled' = 'false';

-- Full refresh: overwrite mode (idempotent, safe to re-run)
INSERT OVERWRITE dwd.dwd_order_detail
SELECT
    order_id,
    amount,
    dt,
    ext_field1 AS customer_level,
    ext_field2 AS channel,
    ext_field3 AS order_type,
    TRUE AS is_valid
FROM paimon_db.orders
WHERE amount > 0
  AND amount < 999999999;

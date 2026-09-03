-- ================================================================
-- 02-mock-data.sql: Insert mock data into ODS layer
-- ================================================================
-- Includes: normal orders + dirty data (Pit 3 demo)
-- Data covers 2 days: 2026-09-01 and 2026-09-02
-- ================================================================

USE CATALOG paimon;

-- ================================================================
-- Normal orders - Day 1: 2026-09-01 (5 orders)
-- ================================================================
INSERT OVERWRITE paimon_db.orders (order_id, amount, dt, ext_field1, ext_field2, ext_field3, ext_field4, ext_field5) VALUES
    ('ORD001', 500.00,  '2026-09-01', 'VIP',       'APP',      'order_type_A', '', ''),
    ('ORD002', 150.50,  '2026-09-01', 'NORMAL',    'WEB',      'order_type_B', '', ''),
    ('ORD003', 300.00,  '2026-09-01', 'VIP',       'APP',      'order_type_A', '', ''),
    ('ORD004', 200.00,  '2026-09-01', 'NORMAL',    'WEB',      'order_type_B', '', ''),
    ('ORD005', 50.00,   '2026-09-01', 'NEW',       'MINI_APP', 'order_type_C', '', '');

-- ================================================================
-- Normal orders - Day 2: 2026-09-02 (6 orders)
-- ================================================================
INSERT OVERWRITE paimon_db.orders (order_id, amount, dt, ext_field1, ext_field2, ext_field3, ext_field4, ext_field5) VALUES
    ('ORD006', 1000.00, '2026-09-02', 'VIP',       'APP',      'order_type_A', '', ''),
    ('ORD007', 900.00,  '2026-09-02', 'VIP',       'APP',      'order_type_A', '', ''),
    ('ORD008', 400.00,  '2026-09-02', 'NORMAL',    'WEB',      'order_type_B', '', ''),
    ('ORD009', 350.00,  '2026-09-02', 'NORMAL',    'WEB',      'order_type_B', '', ''),
    ('ORD010', 150.00,  '2026-09-02', 'NEW',       'MINI_APP', 'order_type_C', '', ''),
    ('ORD011', 100.00,  '2026-09-02', 'NEW',       'MINI_APP', 'order_type_C', '', '');

-- ================================================================
-- Dirty data - Pit 3 demo: negative, abnormal, null amounts
-- ================================================================
INSERT OVERWRITE paimon_db.dirty_orders (order_id, amount, dt, reject_reason, raw_data) VALUES
    ('DIRTY001', '-500.00',     '2026-09-01', 'NEGATIVE_AMOUNT',  '{"order_id":"DIRTY001","amount":-500.00}'),
    ('DIRTY002', '-3268000.00', '2026-09-01', 'NEGATIVE_AMOUNT',  '{"order_id":"DIRTY002","amount":-3268000.00}'),
    ('DIRTY003', '99999999.00', '2026-09-02', 'ABNORMAL_AMOUNT', '{"order_id":"DIRTY003","amount":99999999.00}'),
    ('DIRTY004', 'null',        '2026-09-02', 'NULL_AMOUNT',     '{"order_id":"DIRTY004","amount":null}');

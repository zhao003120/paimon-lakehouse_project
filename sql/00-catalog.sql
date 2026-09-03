-- ================================================================
-- 00-catalog.sql: Create Paimon Catalog with MinIO S3
-- ================================================================

CREATE CATALOG paimon WITH (
    'type' = 'paimon',
    'warehouse' = 's3://paimon/warehouse',
    's3.endpoint' = 'http://minio:9000',
    's3.access-key' = 'admin',
    's3.secret-key' = 'admin123',
    's3.path-style-access' = 'true'
);

USE CATALOG paimon;

-- Create databases for each warehouse layer
CREATE DATABASE IF NOT EXISTS paimon_db;
CREATE DATABASE IF NOT EXISTS dwd;
CREATE DATABASE IF NOT EXISTS dws;
CREATE DATABASE IF NOT EXISTS ads;

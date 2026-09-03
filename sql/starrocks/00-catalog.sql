-- ================================================================
-- StarRocks: Create Paimon External Catalog
-- ================================================================
-- This connects StarRocks to Paimon tables on MinIO S3
-- After this, StarRocks can query all Paimon tables directly
-- ================================================================

-- Create Paimon catalog (pointing to MinIO S3)
CREATE EXTERNAL CATALOG paimon_catalog
PROPERTIES (
    "type" = "paimon",
    "paimon.catalog.type" = "filesystem",
    "paimon.catalog.warehouse" = "s3://paimon/warehouse",
    "aws.s3.endpoint" = "http://minio:9000",
    "aws.s3.access_key" = "admin",
    "aws.s3.secret_key" = "admin123",
    "aws.s3.use_path_style" = "true"
);

-- Verify catalog
SHOW CATALOGS;

-- Switch to Paimon catalog
SET CATALOG paimon_catalog;

-- List databases
SHOW DATABASES;

-- Enable Data Cache (StarRocks local SSD caching for Paimon files)
-- This is the key performance advantage over Flink SQL direct read
SET GLOBAL enable_scan_datacache = true;
SET GLOBAL enable_populate_datacache = true;

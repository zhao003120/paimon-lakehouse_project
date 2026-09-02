package com.example.paimon.common;

/**
 * Paimon 相关配置常量。
 *
 * 对应文章中的三个坑：
 *   坑一: 小文件问题       → FULL-COMPACTION + checkpoint 间隔
 *   坑二: Schema Evolution  → 预留字段 + schema.evolution.enabled
 *   坑三: 数据质量          → Flink 层过滤 + 脏数据记录
 */
public class PaimonConfig {

    // ==================== MinIO 存储配置 ====================

    /**
     * MinIO 服务地址。
     * 优先读环境变量 S3_ENDPOINT（Docker 内用 http://minio:9000），默认 localhost:9000。
     */
    public static final String MINIO_ENDPOINT =
        System.getenv().getOrDefault("S3_ENDPOINT", "http://localhost:9000");

    /** MinIO 访问密钥 */
    public static final String MINIO_ACCESS_KEY =
        System.getenv().getOrDefault("S3_ACCESS_KEY", "admin");

    /** MinIO 秘密密钥 */
    public static final String MINIO_SECRET_KEY =
        System.getenv().getOrDefault("S3_SECRET_KEY", "admin123");

    /** MinIO Bucket 名称 */
    public static final String MINIO_BUCKET = "paimon";

    /** Paimon Warehouse 路径（MinIO S3 兼容存储） */
    public static final String WAREHOUSE_PATH = "s3a://" + MINIO_BUCKET + "/warehouse";

    /** Checkpoint 存储路径（MinIO） */
    public static final String CHECKPOINT_PATH = "s3a://" + MINIO_BUCKET + "/checkpoints";

    /** Savepoint 存储路径（MinIO） */
    public static final String SAVEPOINT_PATH = "s3a://" + MINIO_BUCKET + "/savepoints";

    // ==================== 表配置（坑一：小文件问题） ====================

    /**
     * 坑一修复后的建表参数。
     *
     * 修改前（错误配置）:
     *   bucket = 4
     *   checkpoint = 30s          ← 太频繁，一天 2880 次提交
     *   无 FULL-COMPACTION         ← append 模式不会合小文件
     *
     * 修改后（正确配置）:
     *   bucket = 8                 ← 并发的 2-4 倍
     *   changelog-producer = FULL-COMPACTION
     *   full-compaction.delta-commits = 10
     *   checkpoint 间隔 = 3min
     */
    public static final String CREATE_ORDERS_DDL = String.join("\n",
        "CREATE TABLE IF NOT EXISTS paimon_db.orders (",
        "    order_id        STRING,",
        "    amount          DECIMAL(12,2),",
        "    dt              STRING,",
        "    -- 预留扩展字段（坑二的解决方案）",
        "    ext_field1      STRING,",
        "    ext_field2      STRING,",
        "    ext_field3      STRING,",
        "    ext_field4      STRING,",
        "    ext_field5      STRING,",
        "    PRIMARY KEY (order_id) NOT ENFORCED",
        ") WITH (",
        "    'connector' = 'paimon',",
        "    'bucket' = '8',",
        "    'write-only' = 'false',",
        "    'changelog-producer' = 'full-compaction',",
        "    'full-compaction.delta-commits' = '10',",
        "    'partition' = 'dt',",
        "    'file.format' = 'parquet'",
        ")"
    );

    /**
     * 脏数据表（坑三的解决方案）。
     * 不符合规则的数据写入此表，方便追溯。
     */
    public static final String CREATE_DIRTY_DATA_DDL = String.join("\n",
        "CREATE TABLE IF NOT EXISTS paimon_db.dirty_orders (",
        "    order_id        STRING,",
        "    amount          STRING,",
        "    dt              STRING,",
        "    reject_reason   STRING,",
        "    raw_data        STRING,",
        "    PRIMARY KEY (order_id) NOT ENFORCED",
        ") WITH (",
        "    'connector' = 'paimon',",
        "    'bucket' = '4',",
        "    'partition' = 'dt',",
        "    'file.format' = 'parquet'",
        ")"
    );

    // ==================== Checkpoint 配置 ====================

    /** Checkpoint 间隔：3 分钟（坑一修复：从 30s 改为 3min） */
    public static final long CHECKPOINT_INTERVAL_MS = 180_000L;

    /** Checkpoint 最小间隔 */
    public static final long MIN_PAUSE_BETWEEN_CHECKPOINTS_MS = 30_000L;

    /** Checkpoint 超时时间 */
    public static final long CHECKPOINT_TIMEOUT_MS = 600_000L;

    /** 最大并发 Checkpoint 数 */
    public static final int MAX_CONCURRENT_CHECKPOINTS = 1;

    // ==================== Kafka 配置 ====================

    public static final String KAFKA_BROKERS = "localhost:9092";
    public static final String KAFKA_TOPIC = "orders";
    public static final String KAFKA_GROUP = "paimon-writer";

    // ==================== 数据校验规则（坑三） ====================

    /** 金额上限：超过此值视为异常 */
    public static final String MAX_AMOUNT = "999999999";

    /** 金额下限：负数视为脏数据 */
    public static final String MIN_AMOUNT = "0";

    // ==================== Catalog SQL（含 MinIO S3 配置） ====================

    /**
     * 生成 Paimon Catalog 创建 SQL，包含 MinIO S3 连接参数。
     *
     * 各 Job 统一调用此方法，确保 S3 配置一致。
     */
    public static String getCreateCatalogSql() {
        return String.join("\n",
            "CREATE CATALOG paimon WITH (",
            "    'type' = 'paimon',",
            "    'warehouse' = '" + WAREHOUSE_PATH + "',",
            "    's3.endpoint' = '" + MINIO_ENDPOINT + "',",
            "    's3.access-key' = '" + MINIO_ACCESS_KEY + "',",
            "    's3.secret-key' = '" + MINIO_SECRET_KEY + "',",
            "    's3.path-style-access' = 'true'",
            ")"
        );
    }
}

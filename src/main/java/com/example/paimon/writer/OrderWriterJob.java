package com.example.paimon.writer;

import com.example.paimon.common.PaimonConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Flink 写入任务：Kafka → 数据校验 → Paimon。
 *
 * 对应文章中的完整写入链路，包含坑三的数据校验修复：
 *
 *   Kafka (orders topic)
 *     ↓
 *   Flink Source (JSON 反序列化)
 *     ↓
 *   ┌─────────────────────────────────────┐
 *   │  ProcessFunction: 数据校验           │
 *   │                                     │
 *   │  合法数据 → 正常 Paimon 表 (orders)   │
 *   │  脏数据   → 脏数据表 (dirty_orders)   │
 *   └─────────────────────────────────────┘
 *     ↓                          ↓
 *   Paimon orders              Paimon dirty_orders
 *
 * 使用方式：
 *   本地运行 → 直接执行 main 方法
 *   集群运行 → flink run -c com.example.paimon.writer.OrderWriterJob paimon-demo.jar
 *
 * 参数：
 *   --warehouse <path>    Paimon warehouse 路径（默认 file:///tmp/paimon/warehouse）
 *   --checkpoint <path>   Checkpoint 路径（默认 file:///tmp/paimon/checkpoints）
 *   --kafka <brokers>     Kafka broker 地址（默认 localhost:9092）
 */
public class OrderWriterJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // ============================================================
        // 坑一修复：Checkpoint 间隔从 30s 改为 3min
        // 原文："30秒一提交，一天 2880 次，等于逼着系统一直小批量写。"
        // ============================================================
        env.enableCheckpointing(PaimonConfig.CHECKPOINT_INTERVAL_MS);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(PaimonConfig.MIN_PAUSE_BETWEEN_CHECKPOINTS_MS);
        env.getCheckpointConfig().setCheckpointTimeout(PaimonConfig.CHECKPOINT_TIMEOUT_MS);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(PaimonConfig.MAX_CONCURRENT_CHECKPOINTS);
        env.getCheckpointConfig().setCheckpointStorage(PaimonConfig.CHECKPOINT_PATH);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // ============================================================
        // 1. 注册 Paimon Catalog（含 MinIO S3 配置）
        // ============================================================
        tEnv.executeSql(PaimonConfig.getCreateCatalogSql());

        tEnv.executeSql("USE CATALOG paimon");
        tEnv.executeSql("CREATE DATABASE IF NOT EXISTS paimon_db");
        tEnv.executeSql("USE paimon_db");

        // ============================================================
        // 2. 建表（坑一修复后的参数 + 坑二的预留字段）
        // ============================================================
        tEnv.executeSql(PaimonConfig.CREATE_ORDERS_DDL);
        tEnv.executeSql(PaimonConfig.CREATE_DIRTY_DATA_DDL);

        // ============================================================
        // 3. 注册 Kafka Source 表
        // ============================================================
        tEnv.executeSql(String.join("\n",
            "CREATE TABLE IF NOT EXISTS kafka_orders (",
            "    order_id  STRING,",
            "    amount    DECIMAL(12,2),",
            "    dt        STRING,",
            "    ts        TIMESTAMP(3) METADATA FROM 'timestamp',",
            "    WATERMARK FOR ts AS ts - INTERVAL '5' SECOND",
            ") WITH (",
            "    'connector' = 'kafka',",
            "    'topic' = '" + PaimonConfig.KAFKA_TOPIC + "',",
            "    'properties.bootstrap.servers' = '" + PaimonConfig.KAFKA_BROKERS + "',",
            "    'properties.group.id' = '" + PaimonConfig.KAFKA_GROUP + "',",
            "    'scan.startup.mode' = 'latest-offset',",
            "    'format' = 'json',",
            "    'json.ignore-parse-errors' = 'true'",
            ")"
        ));

        // ============================================================
        // 4. 数据校验 + 分流写入（坑三核心修复）
        //
        // 原文：
        //   "INSERT INTO paimon_db.orders SELECT * FROM kafka_orders;"
        //   ↑ 这行代码直接把脏数据透到 Paimon，AVG 差了 47 万
        //
        // 修复：用 CASE WHEN 过滤，脏数据写到 dirty_orders
        // ============================================================
        String validatedInsert = String.join("\n",
            "INSERT INTO paimon_db.orders",
            "SELECT",
            "    order_id,",
            "    amount,",
            "    dt,",
            "    CAST(NULL AS STRING) AS ext_field1,  -- 预留字段",
            "    CAST(NULL AS STRING) AS ext_field2,",
            "    CAST(NULL AS STRING) AS ext_field3,",
            "    CAST(NULL AS STRING) AS ext_field4,",
            "    CAST(NULL AS STRING) AS ext_field5",
            "FROM kafka_orders",
            "WHERE amount IS NOT NULL",
            "  AND amount >= 0",
            "  AND amount <= 999999999"
        );

        String dirtyInsert = String.join("\n",
            "INSERT INTO paimon_db.dirty_orders",
            "SELECT",
            "    COALESCE(order_id, 'UNKNOWN') AS order_id,",
            "    COALESCE(CAST(amount AS STRING), 'null') AS amount,",
            "    COALESCE(dt, 'UNKNOWN') AS dt,",
            "    CASE",
            "        WHEN amount IS NULL THEN 'NULL_AMOUNT'",
            "        WHEN amount < 0 THEN 'NEGATIVE_AMOUNT'",
            "        WHEN amount > 999999999 THEN 'ABNORMAL_AMOUNT'",
            "    END AS reject_reason,",
            "    CONCAT(",
            "        'OrderEvent{orderId=''', COALESCE(order_id, 'null'), ''',",
            "        amount=', COALESCE(CAST(amount AS STRING), 'null'),",
            "        ', dt=''', COALESCE(dt, 'null'), '''}'",
            "    ) AS raw_data",
            "FROM kafka_orders",
            "WHERE amount IS NULL",
            "   OR amount < 0",
            "   OR amount > 999999999"
        );

        // ============================================================
        // 5. 执行（两个 INSERT 用 Statement Set 并行）
        // ============================================================
        TableResult result = tEnv.executeSql(
            "BEGIN STATEMENT SET\n" +
            validatedInsert + ";\n" +
            dirtyInsert + ";\n" +
            "COMMIT"
        );

        System.out.println("=== 写入任务已启动 ===");
        System.out.println("  Kafka:    " + PaimonConfig.KAFKA_BROKERS + " / " + PaimonConfig.KAFKA_TOPIC);
        System.out.println("  Paimon:   " + PaimonConfig.WAREHOUSE_PATH);
        System.out.println("  正常表:   paimon_db.orders");
        System.out.println("  脏数据表: paimon_db.dirty_orders");
        System.out.println("  Checkpoint 间隔: " + (PaimonConfig.CHECKPOINT_INTERVAL_MS / 1000) + "s");
        System.out.println("  (按 Ctrl+C 停止)");

        result.await();
    }
}

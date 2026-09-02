package com.example.paimon.schema;

import com.example.paimon.common.PaimonConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Schema Evolution 完整演示。
 *
 * 对应文章坑二："埋在三个月后"。
 *
 * 演示内容：
 *   1. 建表（初始 Schema: 2列）
 *   2. 写入旧数据（schema-0）
 *   3. ALTER TABLE ADD COLUMN（schema-1）
 *   4. 写入新数据（schema-1）
 *   5. 全量查询 → 旧分区缺列补 NULL，新分区有值
 *   6. 模拟 StarRocks 报错场景
 *
 * 坑二原文：
 *   "三月分区是旧 Schema，新写入是加了字段之后的新 Schema，
 *    类型对不上。Paimon 不是 Hive，它的 Schema Evolution 不给你
 *    平滑加列，历史分区要全量重写。"
 *
 * 解决方案对比：
 *   方案A: ALTER TABLE ADD COLUMN → 历史数据需全量重写（代价大）
 *   方案B: 预留 STRING 字段 → 塞 JSON（文章采用）
 *   方案C: schema.evolution.enabled=true → Flink 读侧自动适配
 */
public class SchemaEvolutionDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // ============================================================
        // 1. 注册 Catalog（含 MinIO S3 配置）
        // ============================================================
        tEnv.executeSql(PaimonConfig.getCreateCatalogSql());

        tEnv.executeSql("USE CATALOG paimon");
        tEnv.executeSql("CREATE DATABASE IF NOT EXISTS paimon_db");
        tEnv.executeSql("USE paimon_db");

        // ============================================================
        // 2. 建表（初始 Schema，模拟文章中的场景）
        //
        // 注意：这个表演示用，不带预留字段
        // 实际生产中应该用 PaimonConfig.CREATE_ORDERS_DDL（带预留字段）
        // ============================================================
        System.out.println("=== 步骤1: 建表（初始 Schema） ===");
        System.out.println("  Schema-0: [order_id, amount, dt]");
        System.out.println();

        tEnv.executeSql(String.join("\n",
            "CREATE TABLE IF NOT EXISTS schema_demo (",
            "    order_id   STRING,",
            "    amount     DECIMAL(12,2),",
            "    dt         STRING,",
            "    PRIMARY KEY (order_id) NOT ENFORCED",
            ") WITH (",
            "    'connector' = 'paimon',",
            "    'bucket' = '4',",
            "    'partition' = 'dt',",
            "    'file.format' = 'parquet'",
            ")"
        ));

        // ============================================================
        // 3. 写入旧数据（Schema-0 时期）
        // ============================================================
        System.out.println("=== 步骤2: 写入旧数据（Schema-0） ===");

        tEnv.executeSql(String.join("\n",
            "INSERT INTO schema_demo VALUES",
            "    ('order-001', 100.00, '2026-06-01'),",
            "    ('order-002', 200.50, '2026-06-01'),",
            "    ('order-003', 300.00, '2026-06-02'),",
            "    ('order-004', 150.00, '2026-06-02')"
        )).await();

        System.out.println("  写入 4 条旧数据 (6月分区)");
        System.out.println();

        // ============================================================
        // 4. 查询旧数据
        // ============================================================
        System.out.println("=== 步骤3: 查询旧数据 ===");
        tEnv.executeSql("SELECT * FROM schema_demo ORDER BY order_id").print();
        System.out.println();

        // ============================================================
        // 5. Schema Evolution: ALTER TABLE ADD COLUMN
        //
        // 文章原文：
        //   "ALTER TABLE orders ADD COLUMN customer_level STRING;"
        //   "下午三点四十，报表全挂。"
        // ============================================================
        System.out.println("=== 步骤4: Schema 变更 (ALTER TABLE ADD COLUMN) ===");
        System.out.println("  Schema-1: [order_id, amount, dt, customer_level]");
        System.out.println();

        tEnv.executeSql("ALTER TABLE schema_demo ADD COLUMN customer_level STRING");

        // ============================================================
        // 6. 写入新数据（Schema-1 时期）
        // ============================================================
        System.out.println("=== 步骤5: 写入新数据（Schema-1） ===");

        tEnv.executeSql(String.join("\n",
            "INSERT INTO schema_demo VALUES",
            "    ('order-005', 500.00, '2026-09-01', 'VIP'),",
            "    ('order-006', 600.00, '2026-09-01', 'NORMAL'),",
            "    ('order-007', 700.00, '2026-09-02', 'VIP')"
        )).await();

        System.out.println("  写入 3 条新数据 (9月分区, 带 customer_level)");
        System.out.println();

        // ============================================================
        // 7. 跨 Schema 版本查询
        //
        // Paimon 直读：旧分区 customer_level = NULL，新分区有值
        // StarRocks 外表：可能报 "Fragment must be equal to partition column count"
        // ============================================================
        System.out.println("=== 步骤6: 跨 Schema 版本全量查询 ===");
        System.out.println("  Paimon 直读：旧分区 customer_level = NULL ✅");
        System.out.println("  StarRocks 外表：可能报 Fragment 错 ❌");
        System.out.println();

        tEnv.executeSql(
            "SELECT order_id, amount, dt, customer_level FROM schema_demo ORDER BY dt, order_id"
        ).print();
        System.out.println();

        // ============================================================
        // 8. 对比：预留字段方案（文章采用的方案）
        //
        // 文章原文：
        //   "预留 5 个 STRING 字段当扩展位这个方案，
        //    我们当场就定了：不动历史数据，5 分钟搞定，
        //    业务方有什么幺蛾子都往里塞 JSON。"
        // ============================================================
        System.out.println("=== 步骤7: 预留字段方案演示 ===");
        System.out.println("  不改 Schema 结构，往预留字段塞 JSON");
        System.out.println();

        // 往预留字段写 JSON（模拟文章中的方案）
        tEnv.executeSql(String.join("\n",
            "INSERT INTO paimon_db.orders",
            "SELECT",
            "    'order-ext-001',",
            "    999.00,",
            "    '2026-09-02',",
            "    '{\"customer_level\":\"VIP\",\"region\":\"HK\"}',  -- ext_field1 塞 JSON",
            "    CAST(NULL AS STRING),",
            "    CAST(NULL AS STRING),",
            "    CAST(NULL AS STRING),",
            "    CAST(NULL AS STRING)"
        )).await();

        System.out.println("  写入 1 条数据，customer_level 信息塞在 ext_field1 的 JSON 里");
        System.out.println();

        tEnv.executeSql(
            "SELECT order_id, amount, dt, ext_field1 FROM paimon_db.orders WHERE ext_field1 IS NOT NULL"
        ).print();
        System.out.println();

        // ============================================================
        // 9. 总结
        // ============================================================
        System.out.println("============================================================");
        System.out.println("  Schema Evolution 方案对比");
        System.out.println("============================================================");
        System.out.println();
        System.out.println("┌─────────────────┬──────────────────────────────────────────┐");
        System.out.println("│ 方案            │ 优劣                                     │");
        System.out.println("├─────────────────┼──────────────────────────────────────────┤");
        System.out.println("│ ALTER ADD COLUMN│ 新数据OK, 旧分区缺列补NULL               │");
        System.out.println("│                 │ StarRocks 跨版本查询会报 Fragment 错    │");
        System.out.println("│                 │ 历史分区要全量重写(代价大)               │");
        System.out.println("├─────────────────┼──────────────────────────────────────────┤");
        System.out.println("│ 预留 STRING 字段│ 不改 Schema 结构, 5分钟搞定              │");
        System.out.println("│ (文章采用)      │ 查询端要拆 JSON, 性能差                  │");
        System.out.println("│                 │ 但生产稳定, 再没炸过                     │");
        System.out.println("├─────────────────┼──────────────────────────────────────────┤");
        System.out.println("│ schema.evolution│ Flink 读侧自动适配新 Schema              │");
        System.out.println("│ .enabled=true   │ 需要 Flink 1.18+ + Paimon 0.8+          │");
        System.out.println("│                 │ 从 checkpoint 恢复时仍可能有问题         │");
        System.out.println("└─────────────────┴──────────────────────────────────────────┘");
    }
}

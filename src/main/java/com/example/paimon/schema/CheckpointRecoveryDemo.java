package com.example.paimon.schema;

import com.example.paimon.common.PaimonConfig;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Checkpoint 恢复 + Schema 不一致 演示。
 *
 * 对应我们讨论的核心问题：
 *   "读侧 Flink 任务用旧 schema-id 重启，读新数据"
 *
 * 演示流程：
 *
 *   阶段1: 正常运行
 *     → Flink 读侧任务启动，拿 schema-0
 *     → 做 checkpoint（记录 schema-0）
 *     → 正常消费数据
 *
 *   阶段2: Schema 变更
 *     → ALTER TABLE ADD COLUMN → schema-1
 *     → 新数据按 schema-1 写入
 *
 *   阶段3: 从 Checkpoint 恢复
 *     → Flink 从 checkpoint 恢复，缓存的是 schema-0
 *     → 追增量时发现 data 文件是 schema-1
 *     → ❌ IllegalStateException
 *
 *   阶段4: 修复
 *     → 方案A: 不从 checkpoint 恢复，全新启动
 *     → 方案B: 开启 schema.evolution.enabled=true
 *
 * 注意：这个演示需要手动分步执行，不能一次跑完。
 * 每个阶段用独立的 main 方法或手动控制。
 */
public class CheckpointRecoveryDemo {

    /**
     * 阶段1: 启动读侧任务（正常消费）
     *
     * 这一步模拟文章中 Flink 读侧任务正常运行：
     *   - 从 Paimon 流式读取
     *   - 定期做 checkpoint
     *   - checkpoint 里缓存了当前 schema-0
     */
    public static void phase1_normalRun() throws Exception {
        System.out.println("=== 阶段1: 正常运行 ===");
        System.out.println("  Flink 读侧任务启动，拿到 schema-0");
        System.out.println("  开始流式消费 Paimon 数据");
        System.out.println("  Checkpoint 定期保存（含 schema-0）");
        System.out.println();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10_000); // 10秒一次 checkpoint（演示用，生产 3min）
        env.getCheckpointConfig().setCheckpointStorage(PaimonConfig.CHECKPOINT_PATH);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.executeSql(PaimonConfig.getCreateCatalogSql());
        tEnv.executeSql("USE CATALOG paimon");
        tEnv.executeSql("USE paimon_db");

        // 流式读取（schema.evolution.enabled = false，默认）
        // 此时拿到的 schema 是 schema-0
        TableResult result = tEnv.executeSql(String.join("\n",
            "SELECT order_id, amount, dt",
            "FROM schema_demo /*+ OPTIONS(",
            "    'scan.mode' = 'latest',",
            "    'schema.evolution.enabled' = 'false'",
            ") */"
        ));

        result.print();
    }

    /**
     * 阶段2: Schema 变更 + 写入新数据
     *
     * 这一步模拟文章中的操作：
     *   下午3点: "小唐找我加「客户等级」字段"
     *   "ALTER TABLE orders ADD COLUMN customer_level STRING;"
     *   下午3点40: "报表全挂"
     */
    public static void phase2_schemaChange() throws Exception {
        System.out.println("=== 阶段2: Schema 变更 ===");
        System.out.println("  下午3点: 小唐找你加「客户等级」字段");
        System.out.println("  执行: ALTER TABLE schema_demo ADD COLUMN customer_level STRING");
        System.out.println("  Schema-0 → Schema-1");
        System.out.println();
        System.out.println("  新数据按 schema-1 写入（带 customer_level）");
        System.out.println();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.executeSql(PaimonConfig.getCreateCatalogSql());
        tEnv.executeSql("USE CATALOG paimon");
        tEnv.executeSql("USE paimon_db");

        // Schema 变更
        tEnv.executeSql("ALTER TABLE schema_demo ADD COLUMN customer_level STRING");
        System.out.println("  ✅ ALTER TABLE 成功，schema-1 已生成");
        System.out.println("  ⚠️ 此时正在运行的 Flink 读侧任务不知道 Schema 变了！");
        System.out.println("     (Schema 变更不产生 snapshot，Flink 不感知)");
        System.out.println();

        // 写入新数据
        tEnv.executeSql(String.join("\n",
            "INSERT INTO schema_demo VALUES",
            "    ('order-new-001', 500.00, '2026-09-01', 'VIP'),",
            "    ('order-new-002', 600.00, '2026-09-01', 'NORMAL')"
        )).await();
        System.out.println("  ✅ 新数据已写入 (schema-1, 3列)");
        System.out.println();
        System.out.println("  此时 Flink 读侧任务（阶段1还在跑）：");
        System.out.println("    → 发现新 snapshot（知道有新数据）");
        System.out.println("    → 但内存中缓存的是 schema-0（2列）");
        System.out.println("    → 尝试用 schema-0 解析 schema-1 的数据文件");
        System.out.println("    → ❌ 可能抛出 IllegalStateException");
    }

    /**
     * 阶段3: 从 Checkpoint 恢复 → 报错
     *
     * 这一步演示从 checkpoint 恢复时的报错场景：
     *   1. Flink 任务挂了
     *   2. 从 checkpoint 恢复
     *   3. checkpoint 里缓存的是 schema-0
     *   4. 追增量时发现新数据是 schema-1
     *   5. ❌ 报错
     */
    public static void phase3_recoveryWithError() throws Exception {
        System.out.println("=== 阶段3: 从 Checkpoint 恢复 → 报错 ===");
        System.out.println();
        System.out.println("  场景: Flink 读侧任务挂了，从 checkpoint 恢复");
        System.out.println();
        System.out.println("  ┌──────────────────────────────────────────────────┐");
        System.out.println("  │ Checkpoint 恢复过程:                              │");
        System.out.println("  │                                                   │");
        System.out.println("  │  1. 从 checkpoint 恢复状态                         │");
        System.out.println("  │     → 缓存的 schema 还是 schema-0 (2列)           │");
        System.out.println("  │                                                   │");
        System.out.println("  │  2. 查最新 snapshot                               │");
        System.out.println("  │     → 发现 snapshot-3 (有新数据)                  │");
        System.out.println("  │                                                   │");
        System.out.println("  │  3. 计算 snapshot-2 → snapshot-3 的增量           │");
        System.out.println("  │     → 增量 = data-new.orc (schema-1, 3列)         │");
        System.out.println("  │                                                   │");
        System.out.println("  │  4. 用 schema-0 去读 schema-1 的文件              │");
        System.out.println("  │     → ❌ Column 'customer_level' does not exist   │");
        System.out.println("  │        in table schema.                           │");
        System.out.println("  │        Current schema id = 0, required schema id = 1│");
        System.out.println("  └──────────────────────────────────────────────────┘");
        System.out.println();
        System.out.println("  注意: 历史文件不会报错（checkpoint 保证已消费）");
        System.out.println("  报错发生在追增量的那一刻——增量恰好是 schema-1 写的");
        System.out.println();

        // 尝试实际恢复
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10_000);

        // 模拟从 checkpoint 恢复
        // 实际生产中: flink run -s <checkpoint-path> -c ... paimon-demo.jar
        env.setRestartStrategy(RestartStrategies.noRestart());

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.executeSql(PaimonConfig.getCreateCatalogSql());
        tEnv.executeSql("USE CATALOG paimon");
        tEnv.executeSql("USE paimon_db");

        try {
            tEnv.executeSql(String.join("\n",
                "SELECT order_id, amount, dt",
                "FROM schema_demo /*+ OPTIONS(",
                "    'scan.mode' = 'latest',",
                "    'schema.evolution.enabled' = 'false'",
                ") */"
            )).print();
        } catch (Exception e) {
            System.err.println("  ❌ 报错: " + e.getMessage());
            System.err.println();
            System.err.println("  原因: 读侧用旧 schema-0 解析新 schema-1 的数据文件");
        }
    }

    /**
     * 阶段4: 修复方案
     *
     * 方案A: 不从 checkpoint 恢复，全新启动
     *   → 重新从 Catalog 拿最新 schema-1
     *   → 正常读取
     *
     * 方案B: 开启 schema.evolution.enabled = true
     *   → 每次读新 snapshot 时自动拉最新 schema
     *   → 正常读取
     */
    public static void phase4_fix() throws Exception {
        System.out.println("=== 阶段4: 修复方案 ===");
        System.out.println();

        // 方案A
        System.out.println("--- 方案A: 不从 checkpoint 恢复，全新启动 ---");
        System.out.println("  1. 停掉当前 Flink 读侧任务");
        System.out.println("  2. 不使用 -s 参数恢复，直接全新启动");
        System.out.println("  3. Flink 从 Catalog 拿到最新 schema-1");
        System.out.println("  4. 正常读取 ✅");
        System.out.println("  缺点: 可能丢失消费位点，需要重新定位");
        System.out.println();

        // 方案B
        System.out.println("--- 方案B: 开启 schema.evolution.enabled = true ---");
        System.out.println("  1. 在读取 SQL 的 OPTIONS 中加上:");
        System.out.println("     'schema.evolution.enabled' = 'true'");
        System.out.println("  2. Flink 每次读新 snapshot 时，主动从 Catalog 拉最新 schema");
        System.out.println("  3. 即使从 checkpoint 恢复，也能自动适配新 Schema ✅");
        System.out.println("  要求: Flink 1.18+ / Paimon 0.8+");
        System.out.println();

        // 方案C
        System.out.println("--- 方案C: 预留字段（文章采用） ---");
        System.out.println("  1. 建表时预留 5 个 STRING 字段 (ext_field1 ~ ext_field5)");
        System.out.println("  2. 业务变更时不改 Schema，往 ext_field 塞 JSON");
        System.out.println("  3. 不存在 Schema 版本不一致的问题 ✅");
        System.out.println("  缺点: 查询端要拆 JSON，性能差");
        System.out.println();

        // 实际演示方案B
        System.out.println("--- 演示: 方案B 实际运行 ---");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.executeSql(PaimonConfig.getCreateCatalogSql());
        tEnv.executeSql("USE CATALOG paimon");
        tEnv.executeSql("USE paimon_db");

        try {
            System.out.println("  执行查询 (schema.evolution.enabled = true):");
            tEnv.executeSql(String.join("\n",
                "SELECT order_id, amount, dt, customer_level",
                "FROM schema_demo /*+ OPTIONS(",
                "    'scan.mode' = 'from-snapshot-full',",
                "    'schema.evolution.enabled' = 'true'",
                ") */"
            )).print();
            System.out.println("  ✅ 查询成功！");
        } catch (Exception e) {
            System.err.println("  ❌ 仍然报错: " + e.getMessage());
            System.err.println("  可能是 Connector 版本不支持，请升级到 Flink 1.18+ / Paimon 0.8+");
        }
    }

    /**
     * 完整演示入口。
     * 注意：实际执行时建议分步运行各阶段方法。
     */
    public static void main(String[] args) throws Exception {
        System.out.println("############################################################");
        System.out.println("#  Checkpoint 恢复 + Schema 不一致 完整演示");
        System.out.println("#");
        System.out.println("#  对应讨论: 读侧 Flink 任务用旧 schema-id 重启，读新数据");
        System.out.println("############################################################");
        System.out.println();

        // 演示各阶段的说明（实际执行需分步手动控制）
        System.out.println("本演示包含 4 个阶段，建议分步执行：");
        System.out.println();
        System.out.println("  阶段1: phase1_normalRun()");
        System.out.println("    → 启动读侧任务，正常消费，做 checkpoint");
        System.out.println();
        System.out.println("  阶段2: phase2_schemaChange()");
        System.out.println("    → ALTER TABLE ADD COLUMN，写入新 Schema 数据");
        System.out.println();
        System.out.println("  阶段3: phase3_recoveryWithError()");
        System.out.println("    → 从 checkpoint 恢复 → 报错");
        System.out.println();
        System.out.println("  阶段4: phase4_fix()");
        System.out.println("    → 三种修复方案演示");
        System.out.println();
        System.out.println("实际执行步骤:");
        System.out.println("  1. 先运行 SchemaEvolutionDemo 建表并写入数据");
        System.out.println("  2. 运行 phase1_normalRun() 启动读侧任务");
        System.out.println("  3. 运行 phase2_schemaChange() 做 Schema 变更");
        System.out.println("  4. 运行 phase3_recoveryWithError() 模拟恢复报错");
        System.out.println("  5. 运行 phase4_fix() 演示修复方案");
    }
}

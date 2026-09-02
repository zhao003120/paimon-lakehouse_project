package com.example.paimon.reader;

import com.example.paimon.common.PaimonConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Flink 流式读取任务：Paimon → 控制台打印。
 *
 * 核心演示：
 *   - 流式消费 Paimon 变更（changelog 模式）
 *   - schema.evolution.enabled 的作用
 *
 * 两种模式对比：
 *
 *   模式A: schema.evolution.enabled = false（默认）
 *     → 启动时拿一次 schema，之后一直用缓存的
 *     → 表结构变了 → 可能报 IllegalStateException
 *
 *   模式B: schema.evolution.enabled = true
 *     → 每次读新 snapshot 时，主动从 Catalog 拉最新 schema
 *     → 表结构变了 → 自动切换 → 正常
 *
 * 使用方式：
 *   本地运行 → 直接执行 main 方法
 *   集群运行 → flink run -c com.example.paimon.reader.OrderReaderJob paimon-demo.jar
 *
 * 参数：
 *   --warehouse <path>    Paimon warehouse 路径
 *   --evolution <true|false>  是否开启 schema evolution（默认 false）
 */
public class OrderReaderJob {

    /**
     * @param args args[0] = warehouse path (可选)
     *            args[1] = schema.evolution.enabled (可选, "true" / "false")
     */
    public static void main(String[] args) throws Exception {
        String warehouse = args.length > 0 ? args[0] : PaimonConfig.WAREHOUSE_PATH;
        boolean schemaEvolution = args.length > 1 && "true".equalsIgnoreCase(args[1]);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(PaimonConfig.CHECKPOINT_INTERVAL_MS);
        env.getCheckpointConfig().setCheckpointStorage(PaimonConfig.CHECKPOINT_PATH);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // ============================================================
        // 1. 注册 Paimon Catalog（含 MinIO S3 配置）
        // ============================================================
        if (args.length > 0) {
            // 自定义 warehouse（兼容旧参数）
            tEnv.executeSql(String.join("\n",
                "CREATE CATALOG paimon WITH (",
                "    'type' = 'paimon',",
                "    'warehouse' = '" + warehouse + "',",
                "    's3.endpoint' = '" + PaimonConfig.MINIO_ENDPOINT + "',",
                "    's3.access-key' = '" + PaimonConfig.MINIO_ACCESS_KEY + "',",
                "    's3.secret-key' = '" + PaimonConfig.MINIO_SECRET_KEY + "',",
                "    's3.path-style-access' = 'true'",
                ")"
            ));
        } else {
            tEnv.executeSql(PaimonConfig.getCreateCatalogSql());
        }

        tEnv.executeSql("USE CATALOG paimon");
        tEnv.executeSql("USE paimon_db");

        // ============================================================
        // 2. 流式读取 Paimon 表
        //
        // 关键参数：
        //   scan.mode = 'from-snapshot'      → 从指定 snapshot 开始
        //   scan.mode = 'latest'             → 只读新数据（默认）
        //   scan.mode = 'from-snapshot-full' → 全量 + 增量
        //
        // schema.evolution.enabled:
        //   false → Flink 缓存启动时的 schema，之后不变
        //   true  → 每次读新 snapshot 时重新拉取最新 schema
        // ============================================================
        String readMode = schemaEvolution ? "from-snapshot-full" : "latest";

        System.out.println("=== 流式读取任务启动 ===");
        System.out.println("  Paimon:        " + warehouse);
        System.out.println("  表:            paimon_db.orders");
        System.out.println("  读取模式:      " + readMode);
        System.out.println("  Schema演进:    " + (schemaEvolution ? "开启 ✅" : "关闭 ❌（可能报错）"));
        System.out.println("  (按 Ctrl+C 停止)");
        System.out.println();

        // ============================================================
        // 3. 执行流式查询并打印
        //
        // 如果 schema.evolution.enabled=false 且此时表结构已变更，
        // 执行到这里可能会抛出：
        //   java.lang.IllegalStateException:
        //   Column 'customer_level' does not exist in table schema.
        //   Current schema id = 0, required schema id = 1
        // ============================================================

        String sql = String.join("\n",
            "SELECT order_id, amount, dt,",
            "       ext_field1, ext_field2, ext_field3, ext_field4, ext_field5",
            "FROM orders /*+ OPTIONS(",
            "    'scan.mode' = '" + readMode + "',",
            "    'schema.evolution.enabled' = '" + schemaEvolution + "'",
            ") */"
        );

        TableResult result = tEnv.executeSql(sql);

        // 打印结果（流式模式会持续输出）
        try {
            result.print();
        } catch (Exception e) {
            System.err.println("=== 读取失败 ===");
            System.err.println("错误信息: " + e.getMessage());
            System.err.println();
            System.err.println("可能原因:");
            System.err.println("  1. 表 Schema 已变更但 schema.evolution.enabled=false");
            System.err.println("     → 试试用 --evolution true 重新运行");
            System.err.println("  2. 从 checkpoint 恢复时缓存了旧 schema");
            System.err.println("     → 不从 checkpoint 恢复，全新启动");
            System.err.println("  3. 表不存在");
            System.err.println("     → 先运行 OrderWriterJob 创建表并写入数据");
        }
    }
}

package com.example.paimon.reader;

import com.example.paimon.common.PaimonConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Flink 批式查询任务：一次性查询 Paimon 表数据。
 *
 * 对比文章中的场景：
 *   - Flink Batch 直读 Paimon → 慢（6-10倍于 StarRocks）
 *   - 但 Schema Evolution 兼容性好（旧分区缺列自动补 NULL）
 *
 * 演示内容：
 *   1. 全量查询
 *   2. 按分区查询（跨新旧 Schema 版本）
 *   3. 聚合查询（AVG，对应坑三）
 *   4. Time Travel 查询（读取历史 snapshot）
 *   5. 脏数据统计
 */
public class BatchQueryJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // ============================================================
        // 1. 注册 Paimon Catalog（含 MinIO S3 配置）
        // ============================================================
        tEnv.executeSql(PaimonConfig.getCreateCatalogSql());

        tEnv.executeSql("USE CATALOG paimon");
        tEnv.executeSql("USE paimon_db");

        System.out.println("============================================================");
        System.out.println("  Paimon 批式查询演示");
        System.out.println("============================================================\n");

        // ============================================================
        // 查询1: 全量数据
        // ============================================================
        System.out.println("--- 1. 全量数据 ---");
        tEnv.executeSql("SELECT * FROM orders").print();

        // ============================================================
        // 查询2: 按分区查询（跨 Schema 版本）
        //
        // 场景：Schema 变更后，旧分区缺列 → Paimon 自动补 NULL
        // 对比：StarRocks 跨 Schema 版本查询 → 直接报 Fragment 错
        // ============================================================
        System.out.println("\n--- 2. 跨分区查询（新旧 Schema 混合） ---");
        System.out.println("    旧分区: ext_field1 = NULL");
        System.out.println("    新分区: ext_field1 可能有值");
        tEnv.executeSql(
            "SELECT order_id, amount, dt, ext_field1 FROM orders ORDER BY dt"
        ).print();

        // ============================================================
        // 查询3: 聚合查询（坑三：AVG 被脏数据带歪）
        //
        // 坑三原文：
        //   "AVG(amount) 本来 1000 上下，硬给拉到 1500。"
        //   "最多的一回差了 47 万。"
        //
        // 修复后：脏数据已被过滤到 dirty_orders 表
        // ============================================================
        System.out.println("\n--- 3. 聚合查询（已过滤脏数据） ---");
        tEnv.executeSql(String.join("\n",
            "SELECT",
            "    dt,",
            "    COUNT(*)          AS order_count,",
            "    SUM(amount)       AS total_amount,",
            "    AVG(amount)       AS avg_amount,",
            "    MIN(amount)       AS min_amount,",
            "    MAX(amount)       AS max_amount",
            "FROM orders",
            "GROUP BY dt",
            "ORDER BY dt"
        )).print();

        // ============================================================
        // 查询4: 脏数据统计
        // ============================================================
        System.out.println("\n--- 4. 脏数据统计 ---");
        System.out.println("    （被拦截的脏数据，写入 dirty_orders 表）");
        tEnv.executeSql(String.join("\n",
            "SELECT",
            "    reject_reason,",
            "    COUNT(*)          AS dirty_count,",
            "    MIN(amount)       AS min_amount,",
            "    MAX(amount)       AS max_amount",
            "FROM dirty_orders",
            "GROUP BY reject_reason"
        )).print();

        // ============================================================
        // 查询5: Time Travel（读取历史 snapshot）
        //
        // Paimon 支持 time travel，可以读某个 snapshot 时的数据
        // ============================================================
        System.out.println("\n--- 5. Time Travel 查询 ---");
        System.out.println("    读取第一个 snapshot 的数据");
        tEnv.executeSql(
            "SELECT * FROM orders /*+ OPTIONS('scan.snapshot-id' = '1') */"
        ).print();

        // ============================================================
        // 查询6: Schema 历史
        // ============================================================
        System.out.println("\n--- 6. 表的 Schema 历史 ---");
        tEnv.executeSql(
            "SHOW CREATE TABLE orders"
        ).print();

        System.out.println("\n============================================================");
        System.out.println("  查询完成");
        System.out.println("============================================================");
    }
}

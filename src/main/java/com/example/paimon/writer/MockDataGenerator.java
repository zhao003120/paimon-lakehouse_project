package com.example.paimon.writer;

import com.example.paimon.common.PaimonConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 模拟数据生成器。
 *
 * 往 Paimon 表写入模拟数据，包含：
 *   - 正常数据（合法金额）
 *   - 脏数据（负数、异常大值、null）
 *
 * 用于演示坑三：脏数据把 AVG 带歪
 * 以及数据校验过滤的效果
 */
public class MockDataGenerator {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.executeSql(PaimonConfig.getCreateCatalogSql());
        tEnv.executeSql("USE CATALOG paimon");
        tEnv.executeSql("CREATE DATABASE IF NOT EXISTS paimon_db");
        tEnv.executeSql("USE paimon_db");

        // 建表
        tEnv.executeSql(PaimonConfig.CREATE_ORDERS_DDL);
        tEnv.executeSql(PaimonConfig.CREATE_DIRTY_DATA_DDL);

        System.out.println("=== 写入模拟数据 ===");
        System.out.println();

        // ============================================================
        // 正常数据（合法金额）
        // ============================================================
        System.out.println("--- 正常数据 ---");
        tEnv.executeSql(String.join("\n",
            "INSERT INTO paimon_db.orders",
            "SELECT order_id, amount, dt,",
            "       CAST(NULL AS STRING), CAST(NULL AS STRING),",
            "       CAST(NULL AS STRING), CAST(NULL AS STRING),",
            "       CAST(NULL AS STRING)",
            "FROM (VALUES",
            "    ('ord-001', 100.00, '2026-09-01'),",
            "    ('ord-002', 200.50, '2026-09-01'),",
            "    ('ord-003', 300.00, '2026-09-01'),",
            "    ('ord-004', 150.00, '2026-09-01'),",
            "    ('ord-005', 500.00, '2026-09-01'),",
            "    ('ord-006', 800.00, '2026-09-02'),",
            "    ('ord-007', 1200.00, '2026-09-02'),",
            "    ('ord-008', 350.00, '2026-09-02')",
            ") AS t(order_id, amount, dt)"
        )).await();
        System.out.println("  写入 8 条正常数据");
        System.out.println("  AVG(amount) 应该在 ~450 左右");

        // ============================================================
        // 脏数据（直接插入，模拟"没有校验"的场景）
        //
        // 文章原文：
        //   "上游 Kafka 混了一批测试数据，金额有负数，
        //    还有大得离谱的，直接透到 Paimon 了。"
        //   "AVG(amount) 本来 1000 上下，硬给拉到 1500。"
        // ============================================================
        System.out.println();
        System.out.println("--- 脏数据（模拟未校验直接写入） ---");
        System.out.println("  如果不校验，这些脏数据会把 AVG 带歪:");
        System.out.println("    -3268000.00  ← 负数（测试数据泄露）");
        System.out.println("    -500.00      ← 负数");
        System.out.println("    99999999.00  ← 异常大值");
        System.out.println();

        // 写入脏数据到 dirty_orders 表（模拟被拦截的脏数据）
        tEnv.executeSql(String.join("\n",
            "INSERT INTO paimon_db.dirty_orders VALUES",
            "    ('dirty-001', '-3268000.00', '2026-09-01', 'NEGATIVE_AMOUNT',",
            "     'OrderEvent{orderId=''dirty-001'', amount=-3268000.00, dt=''2026-09-01''}'),",
            "    ('dirty-002', '-500.00', '2026-09-01', 'NEGATIVE_AMOUNT',",
            "     'OrderEvent{orderId=''dirty-002'', amount=-500.00, dt=''2026-09-01''}'),",
            "    ('dirty-003', '99999999.00', '2026-09-02', 'ABNORMAL_AMOUNT',",
            "     'OrderEvent{orderId=''dirty-003'', amount=99999999.00, dt=''2026-09-02''}'),",
            "    ('dirty-004', 'null', '2026-09-02', 'NULL_AMOUNT',",
            "     'OrderEvent{orderId=''dirty-004'', amount=null, dt=''2026-09-02''}')"
        )).await();
        System.out.println("  4 条脏数据已记录到 dirty_orders 表（被拦截）");

        // ============================================================
        // 对比演示：有校验 vs 无校验的 AVG 差异
        // ============================================================
        System.out.println();
        System.out.println("=== AVG 对比 ===");
        System.out.println();

        // 有校验（正常表）
        System.out.println("有校验（orders 表，脏数据被拦截）:");
        tEnv.executeSql(
            "SELECT AVG(amount) AS avg_amount FROM paimon_db.orders"
        ).print();

        // 无校验的模拟（如果脏数据没被拦截）
        System.out.println();
        System.out.println("无校验（模拟脏数据混入，AVG 被带歪）:");
        tEnv.executeSql(String.join("\n",
            "SELECT AVG(amount) AS avg_amount FROM (",
            "    SELECT amount FROM paimon_db.orders",
            "    UNION ALL",
            "    SELECT CAST(amount AS DECIMAL(12,2)) AS amount",
            "    FROM paimon_db.dirty_orders",
            "    WHERE amount IS NOT NULL AND amount != 'null'",
            ")"
        )).print();

        System.out.println();
        System.out.println("=== 对应文章原文 ===");
        System.out.println("  \"凌晨统计的总额和白天对不上，最多的一回差了 47 万。\"");
        System.out.println("  \"AVG(amount) 本来 1000 上下，硬给拉到 1500。\"");
    }
}

package com.example.paimon.warehouse;

import com.example.paimon.common.PaimonConfig;
import com.example.paimon.common.WarehouseDDL;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * DWD → DWS 聚合任务。
 *
 * 职责：
 *   1. 从 DWD 层读取明细数据
 *   2. 按 (dt, customer_level, channel) 维度做日汇总
 *   3. 按 (week, customer_level) 维度做周汇总
 *   4. 写入 DWS 层
 *
 * 数据流转：
 *
 *   DWD: dwd_order_detail (明细)
 *     ↓ GROUP BY dt, customer_level, channel
 *   DWS: dws_order_daily (日汇总)
 *     - order_count, total_amount, avg_amount
 *     - max_amount, min_amount, refund_count
 *
 *   DWD: dwd_order_detail (明细)
 *     ↓ GROUP BY week, customer_level
 *   DWS: dws_order_weekly (周汇总)
 *     - order_count, total_amount, avg_amount, refund_rate
 */
public class DwdToDwsJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 注册 Catalog + 确保表存在
        tEnv.executeSql(PaimonConfig.getCreateCatalogSql());
        tEnv.executeSql("USE CATALOG paimon");
        for (String ddl : WarehouseDDL.CREATE_DATABASES) {
            tEnv.executeSql(ddl);
        }
        tEnv.executeSql(WarehouseDDL.CREATE_DWD_ORDER_DETAIL);
        tEnv.executeSql(WarehouseDDL.CREATE_DWS_ORDER_DAILY);
        tEnv.executeSql(WarehouseDDL.CREATE_DWS_ORDER_WEEKLY);

        System.out.println("=== DWD → DWS 聚合 ===");
        System.out.println("  源表:   dwd.dwd_order_detail (DWD 明细)");
        System.out.println("  目标表: dws.dws_order_daily  (日汇总)");
        System.out.println("  目标表: dws.dws_order_weekly (周汇总)");
        System.out.println();

        // ================================================================
        // 1. DWD → DWS 日汇总
        // ================================================================
        System.out.println("--- 1. 生成日汇总 ---");

        String dailySql = String.join("\n",
            "INSERT INTO dws.dws_order_daily",
            "SELECT",
            "    dt,",
            "    customer_level,",
            "    channel,",
            "    COUNT(*)                                    AS order_count,",
            "    CAST(SUM(amount) AS DECIMAL(18,2))          AS total_amount,",
            "    CAST(AVG(amount) AS DECIMAL(18,2))          AS avg_amount,",
            "    CAST(MAX(amount) AS DECIMAL(12,2))          AS max_amount,",
            "    CAST(MIN(amount) AS DECIMAL(12,2))          AS min_amount,",
            "    SUM(CASE WHEN order_status = 'REFUND' THEN 1 ELSE 0 END) AS refund_count",
            "FROM dwd.dwd_order_detail",
            "GROUP BY dt, customer_level, channel"
        );

        tEnv.executeSql(dailySql).await();
        System.out.println("  ✅ 日汇总完成");

        // 验证日汇总
        System.out.println();
        System.out.println("--- DWS 日汇总数据 ---");
        tEnv.executeSql(String.join("\n",
            "SELECT dt, customer_level, channel,",
            "       order_count, total_amount, avg_amount, refund_count",
            "FROM dws.dws_order_daily",
            "ORDER BY dt, customer_level, channel"
        )).print();

        // ================================================================
        // 2. DWD → DWS 周汇总
        //
        // 周计算逻辑：
        //   week_start = DATE_SUB(dt, (DAYOFWEEK(dt) - 2))  -- 周一
        //   week_end   = DATE_ADD(week_start, 6)             -- 周日
        //
        // 注意: Flink SQL 的 DAYOFWEEK 周日=1, 周一=2, ..., 周六=7
        //   所以周一 = dt - (DAYOFWEEK(dt) - 2)
        // ================================================================
        System.out.println();
        System.out.println("--- 2. 生成周汇总 ---");

        String weeklySql = String.join("\n",
            "INSERT INTO dws.dws_order_weekly",
            "SELECT",
            "    DATE_FORMAT(DATE_SUB(TO_DATE(dt), (DAYOFWEEK(CAST(dt AS DATE)) - 2)), 'yyyy-MM-dd') AS week_start,",
            "    DATE_FORMAT(DATE_ADD(TO_DATE(dt), (8 - DAYOFWEEK(CAST(dt AS DATE)))), 'yyyy-MM-dd') AS week_end,",
            "    customer_level,",
            "    COUNT(*)                                    AS order_count,",
            "    CAST(SUM(amount) AS DECIMAL(18,2))          AS total_amount,",
            "    CAST(AVG(amount) AS DECIMAL(18,2))          AS avg_amount,",
            "    CAST(",
            "        CAST(SUM(CASE WHEN order_status = 'REFUND' THEN 1 ELSE 0 END) AS DECIMAL(9,4))",
            "        / NULLIF(COUNT(*), 0)",
            "        AS DECIMAL(5,4)",
            "    ) AS refund_rate",
            "FROM dwd.dwd_order_detail",
            "GROUP BY",
            "    DATE_FORMAT(DATE_SUB(TO_DATE(dt), (DAYOFWEEK(CAST(dt AS DATE)) - 2)), 'yyyy-MM-dd'),",
            "    DATE_FORMAT(DATE_ADD(TO_DATE(dt), (8 - DAYOFWEEK(CAST(dt AS DATE)))), 'yyyy-MM-dd'),",
            "    customer_level"
        );

        try {
            tEnv.executeSql(weeklySql).await();
            System.out.println("  ✅ 周汇总完成");

            System.out.println();
            System.out.println("--- DWS 周汇总数据 ---");
            tEnv.executeSql(String.join("\n",
                "SELECT week_start, week_end, customer_level,",
                "       order_count, total_amount, avg_amount, refund_rate",
                "FROM dws.dws_order_weekly",
                "ORDER BY week_start, customer_level"
            )).print();
        } catch (Exception e) {
            System.out.println("  ⚠️ 周汇总跳过 (日期函数兼容性问题): " + e.getMessage());
            System.out.println("  提示: 周汇总依赖 Flink SQL 日期函数，部分版本可能不支持");
        }

        System.out.println();
        System.out.println("--- DWS 层交叉验证 ---");
        System.out.println("  日汇总总额 vs DWD 明细总额:");
        tEnv.executeSql(String.join("\n",
            "SELECT",
            "    'DWS日汇总' AS source,",
            "    SUM(total_amount) AS grand_total",
            "FROM dws.dws_order_daily",
            "UNION ALL",
            "SELECT",
            "    'DWD明细' AS source,",
            "    CAST(SUM(amount) AS DECIMAL(18,2)) AS grand_total",
            "FROM dwd.dwd_order_detail"
        )).print();

        System.out.println("\n  ✅ DWD → DWS 完成");
        System.out.println("  下一步: 运行 DwsToAdsJob (DWS → ADS)");
    }
}

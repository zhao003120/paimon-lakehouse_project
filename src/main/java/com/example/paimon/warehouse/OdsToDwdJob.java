package com.example.paimon.warehouse;

import com.example.paimon.common.PaimonConfig;
import com.example.paimon.common.WarehouseDDL;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * ODS → DWD 数据加工任务。
 *
 * 职责：
 *   1. 从 ODS 层 (paimon_db.orders) 读取数据
 *   2. 从 ext_field1 的 JSON 中解析出 customer_level / channel / region
 *   3. 补全订单状态
 *   4. 写入 DWD 层 (dwd.dwd_order_detail)
 *
 * 数据流转：
 *
 *   ODS: orders
 *     order_id, amount, dt, ext_field1(JSON)
 *                          ↓ 解析 JSON
 *   DWD: dwd_order_detail
 *     order_id, amount, dt, customer_level, channel, region, order_status, create_time
 *
 * JSON 格式示例 (ext_field1):
 *   {"customer_level":"VIP","channel":"APP","region":"HK"}
 *
 * 如果 ext_field1 为 NULL（旧数据），默认值：
 *   customer_level = 'NORMAL'
 *   channel = 'WEB'
 *   region = 'UNKNOWN'
 */
public class OdsToDwdJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(PaimonConfig.CHECKPOINT_INTERVAL_MS);
        env.getCheckpointConfig().setCheckpointStorage(PaimonConfig.CHECKPOINT_PATH);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 注册 Catalog
        tEnv.executeSql(PaimonConfig.getCreateCatalogSql());
        tEnv.executeSql("USE CATALOG paimon");

        // 确保库表存在
        for (String ddl : WarehouseDDL.CREATE_DATABASES) {
            tEnv.executeSql(ddl);
        }
        tEnv.executeSql(PaimonConfig.CREATE_ORDERS_DDL);
        tEnv.executeSql(WarehouseDDL.CREATE_DWD_ORDER_DETAIL);

        System.out.println("=== ODS → DWD 数据加工 ===");
        System.out.println("  源表:   paimon_db.orders (ODS)");
        System.out.println("  目标表: dwd.dwd_order_detail (DWD)");
        System.out.println("  操作:   解析 JSON + 维度补全 + 状态标记");
        System.out.println();

        // ================================================================
        // ODS → DWD 加工 SQL
        //
        // 核心逻辑：
        //   1. JSON 解析：从 ext_field1 提取 customer_level / channel / region
        //   2. 默认值：旧数据无 ext_field1 → 默认 NORMAL / WEB / UNKNOWN
        //   3. 订单状态：金额 > 1000 标记为 SUCCESS，否则 PENDING
        //   4. create_time：当前时间
        // ================================================================
        String etlSql = String.join("\n",
            "INSERT INTO dwd.dwd_order_detail",
            "SELECT",
            "    order_id,",
            "    amount,",
            "    dt,",
            "    -- 从 ext_field1 JSON 解析客户等级，默认 NORMAL",
            "    COALESCE(",
            "        JSON_VALUE(ext_field1, '$.customer_level'),",
            "        'NORMAL'",
            "    ) AS customer_level,",
            "    -- 从 ext_field1 JSON 解析渠道，默认 WEB",
            "    COALESCE(",
            "        JSON_VALUE(ext_field1, '$.channel'),",
            "        'WEB'",
            "    ) AS channel,",
            "    -- 从 ext_field1 JSON 解析地区，默认 UNKNOWN",
            "    COALESCE(",
            "        JSON_VALUE(ext_field1, '$.region'),",
            "        'UNKNOWN'",
            "    ) AS region,",
            "    -- 订单状态: 大额标记 SUCCESS，小额 PENDING",
            "    CASE WHEN amount > 1000 THEN 'SUCCESS'",
            "         ELSE 'PENDING'",
            "    END AS order_status,",
            "    CURRENT_TIMESTAMP AS create_time",
            "FROM paimon_db.orders"
        );

        System.out.println("  执行 ETL...");
        tEnv.executeSql(etlSql).await();

        // 验证结果
        System.out.println();
        System.out.println("--- DWD 层数据 ---");
        tEnv.executeSql(
            "SELECT * FROM dwd.dwd_order_detail ORDER BY dt, order_id"
        ).print();

        System.out.println();
        System.out.println("--- DWD 层统计 ---");
        tEnv.executeSql(String.join("\n",
            "SELECT",
            "    customer_level,",
            "    channel,",
            "    COUNT(*)        AS cnt,",
            "    SUM(amount)     AS total,",
            "    AVG(amount)     AS avg_amt",
            "FROM dwd.dwd_order_detail",
            "GROUP BY customer_level, channel",
            "ORDER BY customer_level, channel"
        )).print();

        System.out.println("\n  ✅ ODS → DWD 完成");
        System.out.println("  下一步: 运行 DwdToDwsJob (DWD → DWS)");
    }
}

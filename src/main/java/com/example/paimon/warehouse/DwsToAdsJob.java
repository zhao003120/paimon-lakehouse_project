package com.example.paimon.warehouse;

import com.example.paimon.common.PaimonConfig;
import com.example.paimon.common.WarehouseDDL;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * DWS → ADS 应用层任务。
 *
 * 职责：
 *   1. 从 DWS 日汇总表生成订单核心 KPI 大盘
 *   2. 从 DWS 日汇总表生成客户等级排行
 *   3. 从 DWS 日汇总表生成渠道分析
 *   4. 写入 ADS 层
 *
 * 数据流转：
 *
 *   DWS: dws_order_daily
 *     ↓ GROUP BY dt (全渠道汇总)
 *   ADS: ads_order_kpi
 *     - total_orders, total_amount, avg_amount
 *     - vip_count, vip_ratio, refund_count, refund_rate
 *
 *   DWS: dws_order_daily
 *     ↓ GROUP BY dt, customer_level
 *   ADS: ads_customer_rank
 *     - order_count, total_amount, avg_amount, amount_rank
 *
 *   DWS: dws_order_daily
 *     ↓ GROUP BY dt, channel
 *   ADS: ads_channel_stat
 *     - order_count, total_amount, refund_count, refund_rate
 */
public class DwsToAdsJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 注册 Catalog + 确保表存在
        tEnv.executeSql(PaimonConfig.getCreateCatalogSql());
        tEnv.executeSql("USE CATALOG paimon");
        for (String ddl : WarehouseDDL.CREATE_DATABASES) {
            tEnv.executeSql(ddl);
        }
        tEnv.executeSql(WarehouseDDL.CREATE_DWS_ORDER_DAILY);
        tEnv.executeSql(WarehouseDDL.CREATE_ADS_ORDER_KPI);
        tEnv.executeSql(WarehouseDDL.CREATE_ADS_CUSTOMER_RANK);
        tEnv.executeSql(WarehouseDDL.CREATE_ADS_CHANNEL_STAT);

        System.out.println("=== DWS → ADS 应用层加工 ===");
        System.out.println("  源表:   dws.dws_order_daily (DWS 日汇总)");
        System.out.println("  目标:   ads.ads_order_kpi / ads_customer_rank / ads_channel_stat");
        System.out.println();

        // ================================================================
        // 1. ADS 订单核心 KPI 大盘
        //
        // 按日汇总全渠道、全等级的核心指标
        // ================================================================
        System.out.println("--- 1. 生成订单核心 KPI ---");

        String kpiSql = String.join("\n",
            "INSERT INTO ads.ads_order_kpi",
            "SELECT",
            "    dt,",
            "    SUM(order_count)                               AS total_orders,",
            "    CAST(SUM(total_amount) AS DECIMAL(18,2))        AS total_amount,",
            "    CAST(",
            "        SUM(total_amount) / NULLIF(SUM(order_count), 0)",
            "        AS DECIMAL(18,2)",
            "    )                                               AS avg_amount,",
            "    SUM(CASE WHEN customer_level = 'VIP' THEN order_count ELSE 0 END) AS vip_count,",
            "    CAST(",
            "        CAST(SUM(CASE WHEN customer_level = 'VIP' THEN order_count ELSE 0 END) AS DECIMAL(9,4))",
            "        / NULLIF(SUM(order_count), 0)",
            "        AS DECIMAL(5,4)",
            "    )                                               AS vip_ratio,",
            "    SUM(refund_count)                               AS refund_count,",
            "    CAST(",
            "        CAST(SUM(refund_count) AS DECIMAL(9,4))",
            "        / NULLIF(SUM(order_count), 0)",
            "        AS DECIMAL(5,4)",
            "    )                                               AS refund_rate,",
            "    CURRENT_TIMESTAMP                                AS update_time",
            "FROM dws.dws_order_daily",
            "GROUP BY dt"
        );

        tEnv.executeSql(kpiSql).await();
        System.out.println("  ✅ KPI 大盘完成");

        // ================================================================
        // 2. ADS 客户等级排行
        // ================================================================
        System.out.println("\n--- 2. 生成客户等级排行 ---");

        String rankSql = String.join("\n",
            "INSERT INTO ads.ads_customer_rank",
            "SELECT",
            "    dt,",
            "    customer_level,",
            "    SUM(order_count)                        AS order_count,",
            "    CAST(SUM(total_amount) AS DECIMAL(18,2)) AS total_amount,",
            "    CAST(",
            "        SUM(total_amount) / NULLIF(SUM(order_count), 0)",
            "        AS DECIMAL(18,2)",
            "    )                                        AS avg_amount,",
            "    ROW_NUMBER() OVER (PARTITION BY dt ORDER BY SUM(total_amount) DESC) AS amount_rank",
            "FROM dws.dws_order_daily",
            "GROUP BY dt, customer_level"
        );

        tEnv.executeSql(rankSql).await();
        System.out.println("  ✅ 客户等级排行完成");

        // ================================================================
        // 3. ADS 渠道分析
        // ================================================================
        System.out.println("\n--- 3. 生成渠道分析 ---");

        String channelSql = String.join("\n",
            "INSERT INTO ads.ads_channel_stat",
            "SELECT",
            "    dt,",
            "    channel,",
            "    SUM(order_count)                         AS order_count,",
            "    CAST(SUM(total_amount) AS DECIMAL(18,2))  AS total_amount,",
            "    CAST(",
            "        SUM(total_amount) / NULLIF(SUM(order_count), 0)",
            "        AS DECIMAL(18,2)",
            "    )                                         AS avg_amount,",
            "    SUM(refund_count)                         AS refund_count,",
            "    CAST(",
            "        CAST(SUM(refund_count) AS DECIMAL(9,4))",
            "        / NULLIF(SUM(order_count), 0)",
            "        AS DECIMAL(5,4)",
            "    )                                         AS refund_rate",
            "FROM dws.dws_order_daily",
            "GROUP BY dt, channel"
        );

        tEnv.executeSql(channelSql).await();
        System.out.println("  ✅ 渠道分析完成");

        // 验证结果
        System.out.println();
        System.out.println("--- ADS 层数据预览 ---");

        System.out.println("\n  [订单核心 KPI]");
        tEnv.executeSql(String.join("\n",
            "SELECT dt, total_orders, total_amount, avg_amount,",
            "       vip_count, vip_ratio, refund_count, refund_rate",
            "FROM ads.ads_order_kpi",
            "ORDER BY dt"
        )).print();

        System.out.println("\n  [客户等级排行]");
        tEnv.executeSql(String.join("\n",
            "SELECT dt, customer_level, order_count, total_amount, avg_amount, amount_rank",
            "FROM ads.ads_customer_rank",
            "ORDER BY dt, amount_rank"
        )).print();

        System.out.println("\n  [渠道分析]");
        tEnv.executeSql(String.join("\n",
            "SELECT dt, channel, order_count, total_amount, avg_amount, refund_rate",
            "FROM ads.ads_channel_stat",
            "ORDER BY dt, channel"
        )).print();

        System.out.println("\n  ✅ DWS → ADS 完成");
        System.out.println("  下一步: 运行 ReportJob (报表查询)");
    }
}

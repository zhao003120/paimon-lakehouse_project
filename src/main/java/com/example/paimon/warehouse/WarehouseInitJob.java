package com.example.paimon.warehouse;

import com.example.paimon.common.PaimonConfig;
import com.example.paimon.common.WarehouseDDL;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 数仓初始化任务。
 *
 * 执行内容：
 *   1. 注册 Paimon Catalog (MinIO S3)
 *   2. 创建所有数据库: paimon_db(ODS) / dwd / dws / ads
 *   3. 创建所有表: ODS + DWD + DWS + ADS
 *
 * 运行顺序：这是数仓流程的第一步
 */
public class WarehouseInitJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        System.out.println("============================================================");
        System.out.println("  数仓初始化 - 建库建表");
        System.out.println("  存储引擎: Paimon @ MinIO (" + PaimonConfig.MINIO_ENDPOINT + ")");
        System.out.println("============================================================\n");

        // 1. 注册 Catalog
        tEnv.executeSql(PaimonConfig.getCreateCatalogSql());
        tEnv.executeSql("USE CATALOG paimon");

        // 2. 建库
        System.out.println("--- 创建数据库 ---");
        for (String ddl : WarehouseDDL.CREATE_DATABASES) {
            System.out.println("  " + ddl);
            tEnv.executeSql(ddl);
        }

        // 3. ODS 层建表（来自 PaimonConfig）
        System.out.println("\n--- ODS 层建表 ---");
        tEnv.executeSql("USE paimon_db");
        tEnv.executeSql(PaimonConfig.CREATE_ORDERS_DDL);
        System.out.println("  ✅ paimon_db.orders (ODS 订单表)");
        tEnv.executeSql(PaimonConfig.CREATE_DIRTY_DATA_DDL);
        System.out.println("  ✅ paimon_db.dirty_orders (ODS 脏数据表)");

        // 4. DWD / DWS / ADS 层建表
        System.out.println("\n--- DWD / DWS / ADS 层建表 ---");
        for (String ddl : WarehouseDDL.CREATE_TABLES) {
            tEnv.executeSql(ddl);
            // 提取表名
            String tableName = ddl.split("\n")[0].replaceAll(".*CREATE TABLE IF NOT EXISTS (\\S+).*", "$1");
            System.out.println("  ✅ " + tableName);
        }

        System.out.println("\n============================================================");
        System.out.println("  初始化完成！表结构概览:");
        System.out.println("============================================================");
        System.out.println();
        System.out.println("  ODS 层 (paimon_db):");
        System.out.println("    ├─ orders           — 订单原始数据 (含校验过滤)");
        System.out.println("    └─ dirty_orders     — 脏数据 (追溯用)");
        System.out.println();
        System.out.println("  DWD 层 (dwd):");
        System.out.println("    └─ dwd_order_detail — 订单明细宽表 (补维度)");
        System.out.println();
        System.out.println("  DWS 层 (dws):");
        System.out.println("    ├─ dws_order_daily  — 按日汇总");
        System.out.println("    └─ dws_order_weekly — 按周汇总");
        System.out.println();
        System.out.println("  ADS 层 (ads):");
        System.out.println("    ├─ ads_order_kpi    — 订单核心 KPI");
        System.out.println("    ├─ ads_customer_rank— 客户等级排行");
        System.out.println("    └─ ads_channel_stat — 渠道分析");
        System.out.println();
        System.out.println("  下一步:");
        System.out.println("    1. 运行 MockDataGenerator 生成 ODS 数据");
        System.out.println("    2. 运行 OdsToDwdJob  (ODS → DWD)");
        System.out.println("    3. 运行 DwdToDwsJob  (DWD → DWS)");
        System.out.println("    4. 运行 DwsToAdsJob  (DWS → ADS)");
        System.out.println("    5. 运行 ReportJob    (报表查询)");
    }
}

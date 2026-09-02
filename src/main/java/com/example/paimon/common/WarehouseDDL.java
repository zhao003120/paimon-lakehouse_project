package com.example.paimon.common;

/**
 * 数仓分层建表 DDL。
 *
 * 完整数仓架构：
 *
 *   Kafka (原始数据)
 *     ↓
 *   ┌─────────────────────────────────────────────────────────┐
 *   │ ODS 层 (操作数据层)                                      │
 *   │   orders          — 订单明细 (原始, 含校验过滤)           │
 *   │   dirty_orders    — 脏数据 (被拦截, 追溯用)               │
 *   └──────────────────────┬──────────────────────────────────┘
 *                          ↓
 *   ┌─────────────────────────────────────────────────────────┐
 *   │ DWD 层 (明细数据层)                                      │
 *   │   dwd_order_detail — 订单明细宽表 (补维度, 标准化)        │
 *   └──────────────────────┬──────────────────────────────────┘
 *                          ↓
 *   ┌─────────────────────────────────────────────────────────┐
 *   │ DWS 层 (汇总数据层)                                      │
 *   │   dws_order_daily  — 按日汇总 (客户/渠道/等级维度)       │
 *   │   dws_order_weekly — 按周汇总                            │
 *   └──────────────────────┬──────────────────────────────────┘
 *                          ↓
 *   ┌─────────────────────────────────────────────────────────┐
 *   │ ADS 层 (应用数据层)                                      │
 *   │   ads_order_kpi    — 订单核心 KPI (实时大盘)             │
 *   │   ads_customer_rank — 客户等级排行                       │
 *   │   ads_channel_stat  — 渠道分析                           │
 *   └──────────────────────┬──────────────────────────────────┘
 *                          ↓
 *                       BI 报表
 */
public class WarehouseDDL {

    // ================================================================
    // ODS 层（已有，在 PaimonConfig 中定义）
    // ================================================================

    // paimon_db.orders        — 见 PaimonConfig.CREATE_ORDERS_DDL
    // paimon_db.dirty_orders  — 见 PaimonConfig.CREATE_DIRTY_DATA_DDL

    // ================================================================
    // DWD 层（明细数据层）
    // ================================================================

    /**
     * DWD 订单明细宽表。
     *
     * 从 ODS 层清洗 + 补维度后写入：
     *   - 金额标准化（统一币种）
     *   - 渠道维度补全（从 ext_field1 JSON 解析）
     *   - 客户等级补全（从 ext_field1 JSON 解析）
     *   - 订单状态标记
     */
    public static final String CREATE_DWD_ORDER_DETAIL = String.join("\n",
        "CREATE TABLE IF NOT EXISTS dwd.dwd_order_detail (",
        "    order_id          STRING,",
        "    amount            DECIMAL(12,2),",
        "    dt                STRING,",
        "    customer_level    STRING,    -- 客户等级: VIP/NORMAL/NEW",
        "    channel           STRING,    -- 渠道: APP/WEB/MINI_APP",
        "    region            STRING,    -- 地区: HK/MO/TW/CN",
        "    order_status      STRING,    -- 状态: SUCCESS/REFUND/PENDING",
        "    create_time       TIMESTAMP(3),",
        "    PRIMARY KEY (order_id) NOT ENFORCED",
        ") WITH (",
        "    'connector' = 'paimon',",
        "    'bucket' = '8',",
        "    'write-only' = 'false',",
        "    'changelog-producer' = 'full-compaction',",
        "    'full-compaction.delta-commits' = '10',",
        "    'partition' = 'dt',",
        "    'file.format' = 'parquet'",
        ")"
    );

    // ================================================================
    // DWS 层（汇总数据层）
    // ================================================================

    /**
     * DWS 按日汇总表。
     *
     * 按 (dt, customer_level, channel) 维度汇总：
     *   - 订单数
     *   - 总金额
     *   - 平均金额
     *   - 最大/最小金额
     *   - 退款数
     */
    public static final String CREATE_DWS_ORDER_DAILY = String.join("\n",
        "CREATE TABLE IF NOT EXISTS dws.dws_order_daily (",
        "    dt                STRING,",
        "    customer_level    STRING,",
        "    channel           STRING,",
        "    order_count       BIGINT,",
        "    total_amount      DECIMAL(18,2),",
        "    avg_amount        DECIMAL(18,2),",
        "    max_amount        DECIMAL(12,2),",
        "    min_amount        DECIMAL(12,2),",
        "    refund_count      BIGINT,",
        "    PRIMARY KEY (dt, customer_level, channel) NOT ENFORCED",
        ") WITH (",
        "    'connector' = 'paimon',",
        "    'bucket' = '4',",
        "    'write-only' = 'false',",
        "    'changelog-producer' = 'full-compaction',",
        "    'full-compaction.delta-commits' = '10',",
        "    'partition' = 'dt',",
        "    'file.format' = 'parquet'",
        ")"
    );

    /**
     * DWS 按周汇总表。
     */
    public static final String CREATE_DWS_ORDER_WEEKLY = String.join("\n",
        "CREATE TABLE IF NOT EXISTS dws.dws_order_weekly (",
        "    week_start        STRING,    -- 周一日期",
        "    week_end          STRING,    -- 周日日期",
        "    customer_level    STRING,",
        "    order_count       BIGINT,",
        "    total_amount      DECIMAL(18,2),",
        "    avg_amount        DECIMAL(18,2),",
        "    refund_rate       DECIMAL(5,4),  -- 退款率",
        "    PRIMARY KEY (week_start, customer_level) NOT ENFORCED",
        ") WITH (",
        "    'connector' = 'paimon',",
        "    'bucket' = '4',",
        "    'partition' = 'week_start',",
        "    'file.format' = 'parquet'",
        ")"
    );

    // ================================================================
    // ADS 层（应用数据层）
    // ================================================================

    /**
     * ADS 订单核心 KPI 表（实时大盘）。
     *
     * 供 BI Dashboard 使用：
     *   - 当日订单总数
     *   - 当日总金额
     *   - 当日均单价
     *   - VIP 客户占比
     *   - 退款率
     */
    public static final String CREATE_ADS_ORDER_KPI = String.join("\n",
        "CREATE TABLE IF NOT EXISTS ads.ads_order_kpi (",
        "    dt                STRING,",
        "    total_orders      BIGINT,",
        "    total_amount      DECIMAL(18,2),",
        "    avg_amount        DECIMAL(18,2),",
        "    vip_count         BIGINT,",
        "    vip_ratio         DECIMAL(5,4),    -- VIP 占比",
        "    refund_count      BIGINT,",
        "    refund_rate       DECIMAL(5,4),    -- 退款率",
        "    update_time       TIMESTAMP(3),",
        "    PRIMARY KEY (dt) NOT ENFORCED",
        ") WITH (",
        "    'connector' = 'paimon',",
        "    'bucket' = '2',",
        "    'write-only' = 'false',",
        "    'changelog-producer' = 'full-compaction',",
        "    'full-compaction.delta-commits' = '5',",
        "    'partition' = 'dt',",
        "    'file.format' = 'parquet'",
        ")"
    );

    /**
     * ADS 客户等级排行表。
     */
    public static final String CREATE_ADS_CUSTOMER_RANK = String.join("\n",
        "CREATE TABLE IF NOT EXISTS ads.ads_customer_rank (",
        "    dt                STRING,",
        "    customer_level    STRING,",
        "    order_count       BIGINT,",
        "    total_amount      DECIMAL(18,2),",
        "    avg_amount        DECIMAL(18,2),",
        "    amount_rank       INT,          -- 金额排名",
        "    PRIMARY KEY (dt, customer_level) NOT ENFORCED",
        ") WITH (",
        "    'connector' = 'paimon',",
        "    'bucket' = '2',",
        "    'partition' = 'dt',",
        "    'file.format' = 'parquet'",
        ")"
    );

    /**
     * ADS 渠道分析表。
     */
    public static final String CREATE_ADS_CHANNEL_STAT = String.join("\n",
        "CREATE TABLE IF NOT EXISTS ads.ads_channel_stat (",
        "    dt                STRING,",
        "    channel           STRING,",
        "    order_count       BIGINT,",
        "    total_amount      DECIMAL(18,2),",
        "    avg_amount        DECIMAL(18,2),",
        "    refund_count      BIGINT,",
        "    refund_rate       DECIMAL(5,4),",
        "    PRIMARY KEY (dt, channel) NOT ENFORCED",
        ") WITH (",
        "    'connector' = 'paimon',",
        "    'bucket' = '2',",
        "    'partition' = 'dt',",
        "    'file.format' = 'parquet'",
        ")"
    );

    // ================================================================
    // 批量建库建表
    // ================================================================

    /** 所有建库语句 */
    public static final String[] CREATE_DATABASES = {
        "CREATE DATABASE IF NOT EXISTS paimon_db",  // ODS
        "CREATE DATABASE IF NOT EXISTS dwd",
        "CREATE DATABASE IF NOT EXISTS dws",
        "CREATE DATABASE IF NOT EXISTS ads"
    };

    /** 所有建表语句（按依赖顺序） */
    public static final String[] CREATE_TABLES = {
        // ODS（已在 PaimonConfig 中定义，这里不重复）
        // DWD
        CREATE_DWD_ORDER_DETAIL,
        // DWS
        CREATE_DWS_ORDER_DAILY,
        CREATE_DWS_ORDER_WEEKLY,
        // ADS
        CREATE_ADS_ORDER_KPI,
        CREATE_ADS_CUSTOMER_RANK,
        CREATE_ADS_CHANNEL_STAT
    };
}

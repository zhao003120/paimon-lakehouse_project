package com.example.paimon.warehouse;

import com.example.paimon.common.PaimonConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.io.FileWriter;
import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * 报表数据生成器。
 *
 * 从 Paimon ADS/DWS/ODS 层查询真实数据，生成 data.js 文件，
 * dashboard.html 加载 data.js 后自动渲染报表。
 *
 * 流程：
 *   1. 查询 ads.ads_order_kpi      → KPI 卡片
 *   2. 查询 dws.dws_order_daily    → 日趋势图
 *   3. 查询 ads.ads_customer_rank  → 客户排行
 *   4. 查询 ads.ads_channel_stat   → 渠道分析
 *   5. 查询 paimon_db.dirty_orders → 脏数据追溯
 *   6. 查询四层金额                → 全链路对账
 *   7. 汇总成 JSON，写入 report/data.js
 *
 * 运行后打开 report/dashboard.html 即可看到真实数据报表。
 */
public class DashboardGenerator {

    /** 输出目录 */
    private static final String REPORT_DIR = "report";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.executeSql(PaimonConfig.getCreateCatalogSql());
        tEnv.executeSql("USE CATALOG paimon");

        System.out.println("=== 报表数据生成 ===");
        System.out.println("  从 Paimon 查询数据 → 生成 data.js → dashboard.html 自动渲染");
        System.out.println();

        StringBuilder json = new StringBuilder();
        json.append("window.paimonData = {\n");

        // ================================================================
        // 1. KPI 大盘
        // ================================================================
        System.out.print("  [1/6] 查询 KPI 大盘...");
        try {
            TableResult result = tEnv.executeSql(String.join("\n",
                "SELECT",
                "    dt,",
                "    total_orders,",
                "    total_amount,",
                "    avg_amount,",
                "    vip_count,",
                "    vip_ratio,",
                "    refund_count,",
                "    refund_rate",
                "FROM ads.ads_order_kpi",
                "ORDER BY dt"
            ));

            List<String> kpiRows = new ArrayList<>();
            long latestOrders = 0;
            double latestAmount = 0, latestAvg = 0;
            long latestVip = 0, latestRefund = 0;
            double latestVipRatio = 0, latestRefundRate = 0;

            for (Row row : (Iterable<Row>) result::collect) {
                String dt = String.valueOf(row.getField(0));
                long orders = ((Number) row.getField(1)).longValue();
                double amount = ((Number) row.getField(2)).doubleValue();
                double avg = row.getField(3) != null ? ((Number) row.getField(3)).doubleValue() : 0;
                long vip = row.getField(4) != null ? ((Number) row.getField(4)).longValue() : 0;
                double vipRatio = row.getField(5) != null ? ((Number) row.getField(5)).doubleValue() : 0;
                long refund = row.getField(6) != null ? ((Number) row.getField(6)).longValue() : 0;
                double refundRate = row.getField(7) != null ? ((Number) row.getField(7)).doubleValue() : 0;

                // 取最新一天作为 KPI 卡片
                latestOrders = orders;
                latestAmount = amount;
                latestAvg = avg;
                latestVip = vip;
                latestVipRatio = vipRatio;
                latestRefund = refund;
                latestRefundRate = refundRate;

                kpiRows.add(String.format(
                    "    {dt:'%s', orders:%d, amount:%.2f, avg:%.2f, vip:%d, vipRatio:%.4f, refund:%d, refundRate:%.4f}",
                    dt, orders, amount, avg, vip, vipRatio, refund, refundRate
                ));
            }

            json.append("  kpi: {\n");
            json.append(String.format("    totalOrders: %d,\n", latestOrders));
            json.append(String.format("    totalAmount: %.2f,\n", latestAmount));
            json.append(String.format("    avgAmount: %.2f,\n", latestAvg));
            json.append(String.format("    vipCount: %d,\n", latestVip));
            json.append(String.format("    vipRatio: %.4f,\n", latestVipRatio));
            json.append(String.format("    refundCount: %d,\n", latestRefund));
            json.append(String.format("    refundRate: %.4f,\n", latestRefundRate));
            json.append("    daily: [\n").append(String.join(",\n", kpiRows)).append("\n    ]\n");
            json.append("  },\n");
            System.out.println(" OK (" + kpiRows.size() + " 行)");
        } catch (Exception e) {
            System.out.println(" 跳过 (" + e.getMessage() + ")");
            json.append("  kpi: {totalOrders:0,totalAmount:0,avgAmount:0,vipCount:0,vipRatio:0,refundCount:0,refundRate:0,daily:[]},\n");
        }

        // ================================================================
        // 2. 日趋势 (DWS)
        // ================================================================
        System.out.print("  [2/6] 查询日趋势...");
        try {
            TableResult result = tEnv.executeSql(String.join("\n",
                "SELECT",
                "    dt,",
                "    SUM(order_count)  AS orders,",
                "    SUM(total_amount) AS amount,",
                "    AVG(avg_amount)   AS avg_amt",
                "FROM dws.dws_order_daily",
                "GROUP BY dt",
                "ORDER BY dt"
            ));

            List<String> dates = new ArrayList<>();
            List<Long> orders = new ArrayList<>();
            List<Double> amounts = new ArrayList<>();
            List<Double> avgs = new ArrayList<>();

            for (Row row : (Iterable<Row>) result::collect) {
                dates.add("'" + String.valueOf(row.getField(0)) + "'");
                orders.add(((Number) row.getField(1)).longValue());
                amounts.add(((Number) row.getField(2)).doubleValue());
                avgs.add(row.getField(3) != null ? ((Number) row.getField(3)).doubleValue() : 0);
            }

            json.append("  trend: {\n");
            json.append("    dates: [").append(String.join(",", dates)).append("],\n");
            json.append("    orders: [").append(orders.stream().map(String::valueOf).reduce((a,b)->a+","+b).orElse("")).append("],\n");
            json.append("    amounts: [").append(amounts.stream().map(v->String.format("%.2f",v)).reduce((a,b)->a+","+b).orElse("")).append("],\n");
            json.append("    avgs: [").append(avgs.stream().map(v->String.format("%.2f",v)).reduce((a,b)->a+","+b).orElse("")).append("]\n");
            json.append("  },\n");
            System.out.println(" OK (" + dates.size() + " 天)");
        } catch (Exception e) {
            System.out.println(" 跳过 (" + e.getMessage() + ")");
            json.append("  trend: {dates:[],orders:[],amounts:[],avgs:[]},\n");
        }

        // ================================================================
        // 3. 客户等级排行 (ADS)
        // ================================================================
        System.out.print("  [3/6] 查询客户等级排行...");
        try {
            TableResult result = tEnv.executeSql(String.join("\n",
                "SELECT dt, customer_level, order_count, total_amount, avg_amount, amount_rank",
                "FROM ads.ads_customer_rank",
                "ORDER BY dt, amount_rank"
            ));

            List<String> rows = new ArrayList<>();
            for (Row row : (Iterable<Row>) result::collect) {
                rows.add(String.format(
                    "    {dt:'%s', level:'%s', count:%d, total:%.2f, avg:%.2f, rank:%d}",
                    String.valueOf(row.getField(0)),
                    String.valueOf(row.getField(1)),
                    ((Number) row.getField(2)).longValue(),
                    ((Number) row.getField(3)).doubleValue(),
                    row.getField(4) != null ? ((Number) row.getField(4)).doubleValue() : 0,
                    ((Number) row.getField(5)).intValue()
                ));
            }
            json.append("  customerRank: [\n").append(String.join(",\n", rows)).append("\n  ],\n");
            System.out.println(" OK (" + rows.size() + " 行)");
        } catch (Exception e) {
            System.out.println(" 跳过 (" + e.getMessage() + ")");
            json.append("  customerRank: [],\n");
        }

        // ================================================================
        // 4. 渠道分析 (ADS)
        // ================================================================
        System.out.print("  [4/6] 查询渠道分析...");
        try {
            TableResult result = tEnv.executeSql(String.join("\n",
                "SELECT dt, channel, order_count, total_amount, avg_amount, refund_rate",
                "FROM ads.ads_channel_stat",
                "ORDER BY dt, channel"
            ));

            List<String> rows = new ArrayList<>();
            for (Row row : (Iterable<Row>) result::collect) {
                rows.add(String.format(
                    "    {dt:'%s', channel:'%s', count:%d, total:%.2f, avg:%.2f, refundRate:%.4f}",
                    String.valueOf(row.getField(0)),
                    String.valueOf(row.getField(1)),
                    ((Number) row.getField(2)).longValue(),
                    ((Number) row.getField(3)).doubleValue(),
                    row.getField(4) != null ? ((Number) row.getField(4)).doubleValue() : 0,
                    row.getField(5) != null ? ((Number) row.getField(5)).doubleValue() : 0
                ));
            }
            json.append("  channelStat: [\n").append(String.join(",\n", rows)).append("\n  ],\n");
            System.out.println(" OK (" + rows.size() + " 行)");
        } catch (Exception e) {
            System.out.println(" 跳过 (" + e.getMessage() + ")");
            json.append("  channelStat: [],\n");
        }

        // ================================================================
        // 5. 脏数据追溯 (ODS)
        // ================================================================
        System.out.print("  [5/6] 查询脏数据...");
        try {
            TableResult result = tEnv.executeSql(String.join("\n",
                "SELECT reject_reason, COUNT(*) AS cnt,",
                "       MIN(amount) AS min_amt, MAX(amount) AS max_amt",
                "FROM paimon_db.dirty_orders",
                "GROUP BY reject_reason",
                "ORDER BY cnt DESC"
            ));

            List<String> rows = new ArrayList<>();
            int totalDirty = 0;
            for (Row row : (Iterable<Row>) result::collect) {
                int count = ((Number) row.getField(1)).intValue();
                totalDirty += count;
                rows.add(String.format(
                    "    {reason:'%s', count:%d, min:'%s', max:'%s'}",
                    String.valueOf(row.getField(0)),
                    count,
                    String.valueOf(row.getField(2)),
                    String.valueOf(row.getField(3))
                ));
            }
            json.append("  dirty: {\n");
            json.append(String.format("    totalCount: %d,\n", totalDirty));
            json.append("    items: [\n").append(String.join(",\n", rows)).append("\n    ]\n");
            json.append("  },\n");
            System.out.println(" OK (" + totalDirty + " 条脏数据)");
        } catch (Exception e) {
            System.out.println(" 跳过 (" + e.getMessage() + ")");
            json.append("  dirty: {totalCount:0,items:[]},\n");
        }

        // ================================================================
        // 6. 全链路对账
        // ================================================================
        System.out.print("  [6/6] 全链路对账...");
        try {
            String[] layers = {
                "SELECT 'ODS' AS layer, 'paimon_db.orders' AS tbl, CAST(SUM(amount) AS DECIMAL(18,2)) AS amt FROM paimon_db.orders",
                "SELECT 'DWD' AS layer, 'dwd.dwd_order_detail' AS tbl, CAST(SUM(amount) AS DECIMAL(18,2)) AS amt FROM dwd.dwd_order_detail",
                "SELECT 'DWS' AS layer, 'dws.dws_order_daily' AS tbl, CAST(SUM(total_amount) AS DECIMAL(18,2)) AS amt FROM dws.dws_order_daily",
                "SELECT 'ADS' AS layer, 'ads.ads_order_kpi' AS tbl, CAST(SUM(total_amount) AS DECIMAL(18,2)) AS amt FROM ads.ads_order_kpi"
            };
            String[] colors = {"#4f8cff", "#00d68f", "#a855f7", "#22d3ee"};

            List<String> rows = new ArrayList<>();
            for (int i = 0; i < layers.length; i++) {
                try {
                    TableResult r = tEnv.executeSql(layers[i]);
                    for (Row row : (Iterable<Row>) r::collect) {
                        rows.add(String.format(
                            "    {layer:'%s', table:'%s', amount:%.2f, color:'#%s'}",
                            String.valueOf(row.getField(0)),
                            String.valueOf(row.getField(1)),
                            ((Number) row.getField(2)).doubleValue(),
                            colors[i]
                        ));
                        break;
                    }
                } catch (Exception ignored) {}
            }
            json.append("  reconcile: [\n").append(String.join(",\n", rows)).append("\n  ]\n");
            System.out.println(" OK (" + rows.size() + " 层)");
        } catch (Exception e) {
            System.out.println(" 跳过 (" + e.getMessage() + ")");
            json.append("  reconcile: []\n");
        }

        json.append("};\n");

        // ================================================================
        // 写入 data.js
        // ================================================================
        File reportDir = new File(REPORT_DIR);
        if (!reportDir.exists()) {
            reportDir.mkdirs();
        }

        String dataJsPath = REPORT_DIR + File.separator + "data.js";
        try (FileWriter writer = new FileWriter(dataJsPath)) {
            writer.write(json.toString());
        }

        System.out.println();
        System.out.println("=== 报表数据已生成 ===");
        System.out.println("  文件: " + new File(dataJsPath).getAbsolutePath());
        System.out.println();
        System.out.println("  打开报表:");
        System.out.println("    浏览器打开 report/dashboard.html");
        System.out.println("    (dashboard.html 会自动加载 data.js 渲染真实数据)");
    }
}

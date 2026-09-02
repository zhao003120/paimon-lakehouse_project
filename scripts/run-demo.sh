#!/bin/bash
# ================================================================
# Paimon Lakehouse 运行脚本 (Bash)
# ================================================================

set -e

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
JAR_PATH="${PROJECT_DIR}/target/paimon-lakehouse_project-1.0-SNAPSHOT.jar"
FLINK_LIB="${FLINK_HOME:-/opt/flink}/lib"

ACTION="${1:-all}"

echo ""
echo "========================================================"
echo "  Paimon Lakehouse Project"
echo "  三个坑 + 完整数仓 (ODS/DWD/DWS/ADS)"
echo "  存储: MinIO S3 兼容"
echo "========================================================"
echo ""

build() {
    echo "=== 编译项目 ==="
    cd "${PROJECT_DIR}"
    mvn clean package -DskipTests
    echo "编译成功: ${JAR_PATH}"
}

run_java() {
    local main_class="$1"
    local title="$2"
    local subtitle="$3"
    echo "=== ${title} ==="
    [ -n "$subtitle" ] && echo "  ${subtitle}"
    echo ""
    java -cp "${JAR_PATH}:${FLINK_LIB}/*" "${main_class}"
}

# --- 原有功能 ---
run_mock()        { run_java "com.example.paimon.writer.MockDataGenerator"        "生成模拟数据"        "正常数据 + 脏数据 + AVG 对比"; }
run_batch()       { run_java "com.example.paimon.reader.BatchQueryJob"            "批式查询演示"        "全量/跨Schema/聚合/Time Travel"; }
run_schema()      { run_java "com.example.paimon.schema.SchemaEvolutionDemo"      "Schema Evolution (坑二)" "ALTER ADD COLUMN → 跨 Schema 查询"; }
run_checkpoint()  { run_java "com.example.paimon.schema.CheckpointRecoveryDemo"  "Checkpoint 恢复演示" "从旧 schema 恢复 → 报错 → 修复"; }
run_writer()      { run_java "com.example.paimon.writer.OrderWriterJob"           "写入任务 (Kafka→Flink→Paimon)" "坑一+坑三修复"; }
run_reader()      {
    local evo="${1:-false}"
    echo "=== 流式读取 (schema.evolution=${evo}) ==="
    java -cp "${JAR_PATH}:${FLINK_LIB}/*" com.example.paimon.reader.OrderReaderJob "" "${evo}"
}

# --- 数仓流程 ---
run_init()    { run_java "com.example.paimon.warehouse.WarehouseInitJob"  "数仓初始化"       "ODS/DWD/DWS/ADS 四层建库建表"; }
run_ods_dwd() { run_java "com.example.paimon.warehouse.OdsToDwdJob"       "ODS → DWD"       "JSON解析 + 维度补全"; }
run_dwd_dws() { run_java "com.example.paimon.warehouse.DwdToDwsJob"       "DWD → DWS"       "日汇总 + 周汇总"; }
run_dws_ads() { run_java "com.example.paimon.warehouse.DwsToAdsJob"       "DWS → ADS"       "KPI大盘 + 客户排行 + 渠道分析"; }
run_report()  { run_java "com.example.paimon.warehouse.ReportJob"         "报表查询"         "BI 指标展示"; }

run_warehouse() {
    echo "=== 完整数仓流程 (ODS → DWD → DWS → ADS → 报表) ==="
    echo ""
    run_mock
    echo ""
    run_init
    echo ""
    run_ods_dwd
    echo ""
    run_dwd_dws
    echo ""
    run_dws_ads
    echo ""
    run_report
}

case "${ACTION}" in
    build)          build ;;
    mock)           run_mock ;;
    batch)          run_batch ;;
    schema)         run_schema ;;
    checkpoint)     run_checkpoint ;;
    write)          run_writer ;;
    read)           run_reader "false" ;;
    read-evolution) run_reader "true" ;;
    init)           run_init ;;
    ods-dwd)        run_ods_dwd ;;
    dwd-dws)        run_dwd_dws ;;
    dws-ads)        run_dws_ads ;;
    report)         run_report ;;
    warehouse)      run_warehouse ;;
    all)
        build
        echo ""
        echo "数仓流程 (推荐顺序):"
        echo "  1. ./run-demo.sh mock       # 生成 ODS 模拟数据"
        echo "  2. ./run-demo.sh init       # 数仓初始化 (建库建表)"
        echo "  3. ./run-demo.sh ods-dwd    # ODS → DWD 加工"
        echo "  4. ./run-demo.sh dwd-dws    # DWD → DWS 聚合"
        echo "  5. ./run-demo.sh dws-ads    # DWS → ADS 应用层"
        echo "  6. ./run-demo.sh report     # 报表查询"
        echo ""
        echo "一键运行完整数仓:"
        echo "  ./run-demo.sh warehouse    # ODS→DWD→DWS→ADS→报表"
        echo ""
        echo "三个坑演示:"
        echo "  ./run-demo.sh schema       # Schema Evolution (坑二)"
        echo "  ./run-demo.sh checkpoint   # Checkpoint 恢复报错"
        echo "  ./run-demo.sh batch        # 批式查询"
        echo "  ./run-demo.sh write        # 启动 Kafka 写入"
        echo "  ./run-demo.sh read         # 流式读取 (无evolution)"
        echo "  ./run-demo.sh read-evolution # 流式读取 (有evolution)"
        ;;
    *)
        echo "Usage: $0 {build|mock|batch|schema|checkpoint|write|read|read-evolution|init|ods-dwd|dwd-dws|dws-ads|report|warehouse|all}"
        exit 1
        ;;
esac

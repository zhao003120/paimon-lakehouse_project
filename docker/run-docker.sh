#!/bin/bash
# ================================================================
# Paimon Lakehouse Docker Runner
# Run inside the Flink jobmanager container
# ================================================================

set -e

APP_DIR="/app"
JAR_PATH="$APP_DIR/target/paimon-lakehouse_project-1.0-SNAPSHOT.jar"
FLINK_LIB="/opt/flink/lib"

# Color output
RED='\033[0;31m'
GREEN='\033[0;32m'
CYAN='\033[0;36m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${CYAN}"
echo "========================================================"
echo "  Paimon Lakehouse Project (Docker)"
echo "  Flink 1.18 + Paimon 0.8 + MinIO S3"
echo "========================================================"
echo -e "${NC}"

# ================================================================
# Build
# ================================================================
build() {
    echo -e "${CYAN}=== Build Project ===${NC}"
    cd $APP_DIR
    mvn clean package -DskipTests
    echo -e "${GREEN}Build OK: $JAR_PATH${NC}"
}

# ================================================================
# Run Java class
# ================================================================
run_java() {
    local main_class=$1
    local title=$2
    local subtitle=$3
    echo -e "${CYAN}=== $title ===${NC}"
    if [ -n "$subtitle" ]; then
        echo -e "  ${YELLOW}$subtitle${NC}"
    fi
    echo ""
    java -cp "$JAR_PATH:$FLINK_LIB/*" "$main_class"
}

# ================================================================
# Warehouse pipeline
# ================================================================
run_mock()        { run_java "com.example.paimon.writer.MockDataGenerator"        "Generate Mock Data"            "Normal + Dirty + AVG compare"; }
run_init()        { run_java "com.example.paimon.warehouse.WarehouseInitJob"      "Warehouse Init"                "ODS / DWD / DWS / ADS"; }
run_ods_dwd()     { run_java "com.example.paimon.warehouse.OdsToDwdJob"           "ODS -> DWD"                    "JSON parse + Dimension"; }
run_dwd_dws()     { run_java "com.example.paimon.warehouse.DwdToDwsJob"           "DWD -> DWS"                    "Daily + Weekly Aggregate"; }
run_dws_ads()     { run_java "com.example.paimon.warehouse.DwsToAdsJob"           "DWS -> ADS"                    "KPI + Customer Rank + Channel"; }
run_report()      { run_java "com.example.paimon.warehouse.ReportJob"             "Report Query"                  "KPI / Rank / Channel / Trend / Reconcile"; }
run_dashboard()   {
    run_java "com.example.paimon.warehouse.DashboardGenerator" "Generate HTML Dashboard" "Query real data -> data.js -> dashboard.html"
    echo -e "${GREEN}Dashboard generated: /app/report/dashboard.html${NC}"
}

run_warehouse() {
    echo -e "${CYAN}=== Full Pipeline (ODS -> DWD -> DWS -> ADS -> Dashboard) ===${NC}"
    run_mock
    run_init
    run_ods_dwd
    run_dwd_dws
    run_dws_ads
    run_dashboard
}

# ================================================================
# Demo functions
# ================================================================
run_schema()      { run_java "com.example.paimon.schema.SchemaEvolutionDemo"      "Schema Evolution Demo (Pit 2)"  "ALTER ADD COLUMN -> Cross Schema Query"; }
run_checkpoint()  { run_java "com.example.paimon.schema.CheckpointRecoveryDemo"  "Checkpoint Recovery Demo"      "Old schema -> Error -> Fix"; }
run_batch()       { run_java "com.example.paimon.reader.BatchQueryJob"            "Batch Query Demo"              "Full / CrossSchema / Aggregate"; }

# ================================================================
# Main
# ================================================================
ACTION=${1:-help}

case $ACTION in
    build)
        build
        ;;
    mock)
        run_mock
        ;;
    init)
        run_init
        ;;
    ods-dwd)
        run_ods_dwd
        ;;
    dwd-dws)
        run_dwd_dws
        ;;
    dws-ads)
        run_dws_ads
        ;;
    report)
        run_report
        ;;
    dashboard)
        run_dashboard
        ;;
    warehouse)
        build
        run_warehouse
        ;;
    schema)
        run_schema
        ;;
    checkpoint)
        run_checkpoint
        ;;
    batch)
        run_batch
        ;;
    *)
        echo "Usage: $0 {build|mock|init|ods-dwd|dwd-dws|dws-ads|report|dashboard|warehouse|schema|checkpoint|batch}"
        echo ""
        echo "Warehouse pipeline:"
        echo "  build       - Build the project with Maven"
        echo "  mock        - Generate mock data (ODS)"
        echo "  init        - Create databases and tables"
        echo "  ods-dwd     - ODS -> DWD"
        echo "  dwd-dws     - DWD -> DWS"
        echo "  dws-ads     - DWS -> ADS"
        echo "  report      - Query report"
        echo "  dashboard   - Generate HTML dashboard"
        echo "  warehouse   - Full pipeline (build + all steps)"
        echo ""
        echo "Pitfall demos:"
        echo "  schema      - Schema Evolution (Pit 2)"
        echo "  checkpoint  - Checkpoint Recovery"
        echo "  batch       - Batch Query"
        ;;
esac

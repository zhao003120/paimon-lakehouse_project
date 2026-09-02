#!/bin/bash
# ================================================================
# run-sql.sh: Run SQL files via Flink SQL Client (inside Docker)
# ================================================================
# Usage:
#   bash run-sql.sh all         # Run entire pipeline
#   bash run-sql.sh ddl          # Create all tables
#   bash run-sql.sh mock         # Insert mock data
#   bash run-sql.sh etl          # Run ETL pipeline
#   bash run-sql.sh report       # Query reports
#   bash run-sql.sh warehouse    # Full pipeline + report
# ================================================================

set -e

SQL_DIR="/app/sql"
FLINK_HOME="/opt/flink"
SQL_CLIENT="$FLINK_HOME/bin/sql-client.sh"

# ================================================================
# Run a single SQL file
# ================================================================
run_sql() {
    local file=$1
    local name=$(basename "$file" .sql)
    echo -e "\033[0;36m=== Running: $name ===\033[0m"
    $SQL_CLIENT embedded \
        -l "$FLINK_HOME/lib" \
        -f "$file" \
        -e "$SQL_DIR/sql-defaults.yaml" \
        2>&1 | head -100
    echo -e "\033[0;32m  Done: $name\033[0m"
}

# ================================================================
# Run multiple SQL files
# ================================================================
run_files() {
    for file in "$@"; do
        run_sql "$SQL_DIR/$file"
    done
}

# ================================================================
# Actions
# ================================================================
ACTION=${1:-help}

case $ACTION in
    ddl)
        echo -e "\033[0;36m=== Create All Tables ===\033[0m"
        run_files 00-catalog.sql 01-ods-ddl.sql 03-dwd-ddl.sql 05-dws-ddl.sql 07-ads-ddl.sql
        ;;
    mock)
        echo -e "\033[0;36m=== Insert Mock Data ===\033[0m"
        run_files 00-catalog.sql 02-mock-data.sql
        ;;
    etl)
        echo -e "\033[0;36m=== Run ETL Pipeline ===\033[0m"
        run_files 00-catalog.sql 04-ods-to-dwd.sql 06-dwd-to-dws.sql 08-dws-to-ads.sql
        ;;
    report)
        echo -e "\033[0;36m=== Query Reports ===\033[0m"
        run_files 00-catalog.sql 09-report.sql
        ;;
    schema-demo)
        echo -e "\033[0;36m=== Schema Evolution Demo ===\033[0m"
        run_files 00-catalog.sql 10-schema-evolution.sql
        ;;
    checkpoint-demo)
        echo -e "\033[0;36m=== Checkpoint Recovery Demo ===\033[0m"
        run_files 00-catalog.sql 11-checkpoint-demo.sql
        ;;
    warehouse)
        echo -e "\033[0;36m"
        echo "========================================================"
        echo "  Full Warehouse Pipeline (SQL)"
        echo "  ODS -> DWD -> DWS -> ADS -> Report"
        echo "========================================================"
        echo -e "\033[0m"
        run_files \
            00-catalog.sql \
            01-ods-ddl.sql \
            02-mock-data.sql \
            03-dwd-ddl.sql \
            04-ods-to-dwd.sql \
            05-dws-ddl.sql \
            06-dwd-to-dws.sql \
            07-ads-ddl.sql \
            08-dws-to-ads.sql \
            09-report.sql
        echo -e "\033[0;32m"
        echo "  Pipeline complete!"
        echo "  Dashboard: open /app/report/dashboard.html"
        echo -e "\033[0m"
        ;;
    *)
        echo "Usage: bash run-sql.sh {ddl|mock|etl|report|warehouse|schema-demo|checkpoint-demo}"
        echo ""
        echo "Commands:"
        echo "  ddl             Create all tables (ODS/DWD/DWS/ADS)"
        echo "  mock            Insert mock data"
        echo "  etl             Run ETL (ODS->DWD->DWS->ADS)"
        echo "  report          Query all reports"
        echo "  warehouse       Full pipeline (DDL + mock + ETL + report)"
        echo "  schema-demo     Schema Evolution demo (Pit 2)"
        echo "  checkpoint-demo Checkpoint Recovery demo"
        ;;
esac

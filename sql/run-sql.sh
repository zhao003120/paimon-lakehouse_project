#!/bin/bash
# ================================================================
# run-sql.sh: Run SQL files via Flink SQL Client or StarRocks
# ================================================================
# Catalog is created inline via 00-catalog.sql (auto-injected)
# Batch mode is set at cluster level in flink-conf.yaml
# ================================================================
# Usage:
#   bash run-sql.sh warehouse    # Full Flink pipeline
#   bash run-sql.sh ddl          # Create all Paimon tables
#   bash run-sql.sh mock         # Insert mock data
#   bash run-sql.sh etl          # Run ETL pipeline
#   bash run-sql.sh report       # Query reports via Flink
#   bash run-sql.sh warehouse+sr # Full Flink pipeline + StarRocks queries
#   bash run-sql.sh sr-report    # Query reports via StarRocks
#   bash run-sql.sh sr-benchmark # Benchmark via StarRocks
# ================================================================

# NO set -e: we want to continue even if one SQL statement fails
# set -e

SQL_DIR="/app/sql"
FLINK_HOME="/opt/flink"
SQL_CLIENT="$FLINK_HOME/bin/sql-client.sh"

# ================================================================
# Flink SQL runner: concatenate files into one temp file
# 00-catalog.sql is always included first
# Uses -f flag which WAITS for each INSERT to complete before next
# ================================================================
run_flink_sql() {
    local files=("$@")
    local names=""
    local tmp_file="/tmp/flink-combined-$(date +%s).sql"
    
    # Build combined SQL file
    > "$tmp_file"
    
    # Always start with catalog creation
    local catalog_file="$SQL_DIR/00-catalog.sql"
    if [ -f "$catalog_file" ]; then
        cat "$catalog_file" >> "$tmp_file"
        echo "" >> "$tmp_file"
    fi
    
    for file in "${files[@]}"; do
        local f="$SQL_DIR/$file"
        if [ -f "$f" ]; then
            names="$names $(basename "$file" .sql)"
            echo "-- ================================================================" >> "$tmp_file"
            echo "-- File: $file" >> "$tmp_file"
            echo "-- ================================================================" >> "$tmp_file"
            cat "$f" >> "$tmp_file"
            echo "" >> "$tmp_file"
        else
            echo "WARN: File not found: $f"
        fi
    done
    
    echo -e "\033[0;36m=== Flink SQL:$names ===\033[0m"
    echo "       Combined file: $tmp_file ($(wc -l < "$tmp_file") lines)"
    
    # Execute with -f flag
    # -f mode: reads file, executes each statement, WAITS for INSERT to complete
    # Batch mode comes from flink-conf.yaml (execution.runtime-mode: BATCH)
    $SQL_CLIENT embedded \
        -l "$FLINK_HOME/lib" \
        -f "$tmp_file" \
        2>&1
    
    local exit_code=$?
    rm -f "$tmp_file"
    
    if [ $exit_code -ne 0 ]; then
        echo -e "\033[0;33m  Completed with warnings (exit code: $exit_code)\033[0m"
    else
        echo -e "\033[0;32m  Done:$names\033[0m"
    fi
    return 0
}

# ================================================================
# StarRocks runner
# ================================================================
run_starrocks() {
    bash "$SQL_DIR/starrocks/run-starrocks.sh" "$@"
}

# ================================================================
# Actions
# ================================================================
ACTION=${1:-help}

case $ACTION in
    # --- Flink pipeline (catalog auto-included) ---
    ddl)
        echo -e "\033[0;36m=== Create All Tables ===\033[0m"
        run_flink_sql 01-ods-ddl.sql 01b-alter-tables.sql 03-dwd-ddl.sql 05-dws-ddl.sql 07-ads-ddl.sql
        ;;
    mock)
        echo -e "\033[0;36m=== Insert Mock Data ===\033[0m"
        run_flink_sql 02-mock-data.sql
        ;;
    etl)
        echo -e "\033[0;36m=== Run ETL Pipeline ===\033[0m"
        run_flink_sql 04-ods-to-dwd.sql 06-dwd-to-dws.sql 08-dws-to-ads.sql
        ;;
    report)
        echo -e "\033[0;36m=== Query Reports (Flink) ===\033[0m"
        run_flink_sql 09-report.sql
        ;;
    schema-demo)
        echo -e "\033[0;36m=== Schema Evolution Demo ===\033[0m"
        run_flink_sql 10-schema-evolution.sql
        ;;
    checkpoint-demo)
        echo -e "\033[0;36m=== Checkpoint Recovery Demo ===\033[0m"
        run_flink_sql 11-checkpoint-demo.sql
        ;;
    warehouse)
        echo -e "\033[0;36m"
        echo "========================================================"
        echo "  Full Warehouse Pipeline (Flink SQL)"
        echo "  ODS -> DWD -> DWS -> ADS -> Report"
        echo "  Batch mode: cluster-level (flink-conf.yaml)"
        echo "  Execution: -f flag (waits for each INSERT)"
        echo "========================================================"
        echo -e "\033[0m"
        run_flink_sql \
            01-ods-ddl.sql \
            01b-alter-tables.sql \
            02-mock-data.sql \
            03-dwd-ddl.sql \
            04-ods-to-dwd.sql \
            05-dws-ddl.sql \
            06-dwd-to-dws.sql \
            07-ads-ddl.sql \
            08-dws-to-ads.sql \
            09-report.sql
        echo -e "\033[0;32m  Flink pipeline complete!\033[0m"
        ;;

    # --- StarRocks ---
    sr-init)
        run_starrocks init
        ;;
    sr-catalog)
        run_starrocks catalog
        ;;
    sr-report)
        run_starrocks report
        ;;
    sr-benchmark)
        run_starrocks benchmark
        ;;
    sr-schema)
        run_starrocks schema
        ;;
    sr-all)
        run_starrocks all
        ;;
    sr-shell)
        run_starrocks shell
        ;;

    # --- Combined: Flink write + StarRocks read ---
    warehouse+sr)
        echo -e "\033[0;36m"
        echo "========================================================"
        echo "  Full Pipeline: Flink (write) + StarRocks (read)"
        echo "========================================================"
        echo -e "\033[0m"
        run_flink_sql \
            01-ods-ddl.sql \
            01b-alter-tables.sql \
            02-mock-data.sql \
            03-dwd-ddl.sql \
            04-ods-to-dwd.sql \
            05-dws-ddl.sql \
            06-dwd-to-dws.sql \
            07-ads-ddl.sql \
            08-dws-to-ads.sql
        echo -e "\033[0;32m  Flink ETL complete, now querying via StarRocks...\033[0m"
        run_starrocks catalog
        run_starrocks report
        run_starrocks benchmark
        echo -e "\033[0;32m"
        echo "  Complete! Flink wrote data, StarRocks queried it."
        echo -e "\033[0m"
        ;;

    *)
        echo "Usage: bash run-sql.sh {command}"
        echo ""
        echo "Flink pipeline (catalog auto-created, batch mode from flink-conf.yaml):"
        echo "  warehouse       Full Flink pipeline (DDL + mock + ETL + report)"
        echo "  ddl             Create all Paimon tables"
        echo "  mock            Insert mock data"
        echo "  etl             Run ETL (ODS->DWD->DWS->ADS)"
        echo "  report          Query reports via Flink"
        echo "  schema-demo     Schema Evolution demo (Pit 2)"
        echo "  checkpoint-demo Checkpoint Recovery demo"
        echo ""
        echo "StarRocks (OLAP query):"
        echo "  sr-init         Initialize StarRocks cluster"
        echo "  sr-catalog      Create Paimon external catalog"
        echo "  sr-report       Query reports via StarRocks (MPP + Data Cache)"
        echo "  sr-benchmark    Benchmark StarRocks vs Flink performance"
        echo "  sr-schema       Schema evolution demo via StarRocks"
        echo "  sr-all          StarRocks: catalog + report + benchmark"
        echo "  sr-shell        Open StarRocks MySQL shell"
        echo ""
        echo "Combined:"
        echo "  warehouse+sr    Flink ETL + StarRocks query (recommended)"
        ;;
esac

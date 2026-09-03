#!/bin/bash
# ================================================================
# run-starrocks.sh: Run StarRocks SQL via MySQL client
# ================================================================
# Usage:
#   bash run-starrocks.sh catalog    # Create Paimon external catalog
#   bash run-starrocks.sh report     # Query reports via StarRocks
#   bash run-starrocks.sh benchmark  # Run benchmark queries
#   bash run-starrocks.sh schema     # Schema evolution demo
#   bash run-starrocks.sh all        # Catalog + report
# ================================================================

set -e

SQL_DIR="/app/sql/starrocks"
SR_HOST="starrocks-fe"
SR_PORT="9030"
SR_USER="root"
SR_PASSWORD=""

# MySQL client command (StarRocks uses MySQL protocol)
MYSQL_CMD="mysql -h ${SR_HOST} -P ${SR_PORT} -u ${SR_USER}"

# Wait for StarRocks FE to be ready
wait_for_starrocks() {
    echo -e "\033[0;33mWaiting for StarRocks FE...\033[0m"
    for i in $(seq 1 30); do
        if $MYSQL_CMD -e "SELECT 1" >/dev/null 2>&1; then
            echo -e "\033[0;32mStarRocks is ready!\033[0m"
            return 0
        fi
        echo "  Attempt $i/30..."
        sleep 5
    done
    echo -e "\033[0;31mERROR: StarRocks not ready after 30 attempts\033[0m"
    exit 1
}

# Run a SQL file
run_sql() {
    local file=$1
    local name=$(basename "$file" .sql)
    echo -e "\033[0;36m=== StarRocks: $name ===\033[0m"
    $MYSQL_CMD --default-character-set=utf8mb4 < "$SQL_DIR/$file" 2>&1
    echo -e "\033[0;32m  Done: $name\033[0m"
}

# ================================================================
# Actions
# ================================================================
ACTION=${1:-help}

case $ACTION in
    init)
        # Add BE to FE cluster (first time only)
        wait_for_starrocks
        echo -e "\033[0;36m=== Initializing StarRocks cluster ===\033[0m"
        BE_IP=$(getent hosts starrocks-be | awk '{print $1}')
        echo "Adding BE: $BE_IP"
        $MYSQL_CMD -e "ALTER SYSTEM ADD BACKEND '$BE_IP:9050';" 2>/dev/null || true
        echo -e "\033[0;32m  BE added. Checking cluster status...\033[0m"
        sleep 5
        $MYSQL_CMD -e "SHOW BACKENDS\G"
        ;;
    catalog)
        wait_for_starrocks
        run_sql 00-catalog.sql
        ;;
    report)
        wait_for_starrocks
        run_sql 00-catalog.sql
        run_sql 01-report.sql
        ;;
    benchmark)
        wait_for_starrocks
        run_sql 00-catalog.sql
        run_sql 02-benchmark.sql
        ;;
    schema)
        wait_for_starrocks
        run_sql 00-catalog.sql
        run_sql 03-schema-demo.sql
        ;;
    all)
        wait_for_starrocks
        echo -e "\033[0;36m"
        echo "========================================================"
        echo "  StarRocks + Paimon: Full Query Pipeline"
        echo "  Catalog -> Report -> Benchmark"
        echo "========================================================"
        echo -e "\033[0m"
        run_sql 00-catalog.sql
        run_sql 01-report.sql
        run_sql 02-benchmark.sql
        echo -e "\033[0;32m"
        echo "  StarRocks queries complete!"
        echo -e "\033[0m"
        ;;
    shell)
        wait_for_starrocks
        $MYSQL_CMD --default-character-set=utf8mb4
        ;;
    *)
        echo "Usage: bash run-starrocks.sh {init|catalog|report|benchmark|schema|all|shell}"
        echo ""
        echo "Commands:"
        echo "  init       Initialize StarRocks cluster (add BE to FE)"
        echo "  catalog    Create Paimon external catalog"
        echo "  report     Query all 6 reports via StarRocks"
        echo "  benchmark  Run benchmark queries (StarRocks vs Flink)"
        echo "  schema     Schema evolution demo (Pit 2)"
        echo "  all        Catalog + report + benchmark"
        echo "  shell      Open StarRocks MySQL shell"
        ;;
esac

#!/bin/bash
# ================================================================
# Paimon Lakehouse Docker Runner (SQL-only, Flink + StarRocks)
# ================================================================

set -e

CYAN='\033[0;36m'
GREEN='\033[0;32m'
NC='\033[0m'

echo -e "${CYAN}"
echo "========================================================"
echo "  Paimon Lakehouse Project (SQL Mode)"
echo "  Flink 1.18 + Paimon 0.8 + MinIO S3 + StarRocks 3.3"
echo "========================================================"
echo -e "${NC}"

ACTION=${1:-help}

case $ACTION in
    # --- Flink pipeline ---
    warehouse)
        bash /app/sql/run-sql.sh warehouse
        ;;
    ddl)
        bash /app/sql/run-sql.sh ddl
        ;;
    mock)
        bash /app/sql/run-sql.sh mock
        ;;
    etl)
        bash /app/sql/run-sql.sh etl
        ;;
    report)
        bash /app/sql/run-sql.sh report
        ;;
    schema-demo)
        bash /app/sql/run-sql.sh schema-demo
        ;;
    checkpoint-demo)
        bash /app/sql/run-sql.sh checkpoint-demo
        ;;

    # --- StarRocks ---
    sr-init)
        bash /app/sql/run-sql.sh sr-init
        ;;
    sr-catalog)
        bash /app/sql/run-sql.sh sr-catalog
        ;;
    sr-report)
        bash /app/sql/run-sql.sh sr-report
        ;;
    sr-benchmark)
        bash /app/sql/run-sql.sh sr-benchmark
        ;;
    sr-schema)
        bash /app/sql/run-sql.sh sr-schema
        ;;
    sr-all)
        bash /app/sql/run-sql.sh sr-all
        ;;
    sr-shell)
        bash /app/sql/run-sql.sh sr-shell
        ;;

    # --- Combined ---
    warehouse+sr)
        bash /app/sql/run-sql.sh warehouse+sr
        ;;

    shell)
        exec bash
        ;;

    *)
        echo "Usage: bash run-docker.sh {command}"
        echo ""
        echo "Flink pipeline (write to Paimon):"
        echo "  warehouse       Full Flink pipeline (DDL + mock + ETL + report)"
        echo "  ddl             Create all Paimon tables"
        echo "  mock            Insert mock data"
        echo "  etl             Run ETL (ODS->DWD->DWS->ADS)"
        echo "  report          Query reports via Flink SQL"
        echo "  schema-demo     Schema Evolution demo (Pit 2)"
        echo "  checkpoint-demo Checkpoint Recovery demo"
        echo ""
        echo "StarRocks (OLAP query, MPP + Data Cache):"
        echo "  sr-init         Initialize StarRocks cluster (add BE)"
        echo "  sr-catalog      Create Paimon external catalog"
        echo "  sr-report       Query 6 reports via StarRocks"
        echo "  sr-benchmark    Benchmark StarRocks vs Flink"
        echo "  sr-schema       Schema evolution demo via StarRocks"
        echo "  sr-all          StarRocks: catalog + report + benchmark"
        echo "  sr-shell        Open StarRocks MySQL shell"
        echo ""
        echo "Combined (recommended):"
        echo "  warehouse+sr    Flink ETL + StarRocks query"
        echo ""
        echo "  shell           Open bash shell"
        ;;
esac

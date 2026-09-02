#!/bin/bash
# ================================================================
# Paimon Lakehouse Docker Runner (SQL-only version)
# ================================================================

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
CYAN='\033[0;36m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${CYAN}"
echo "========================================================"
echo "  Paimon Lakehouse Project (SQL Mode)"
echo "  Flink 1.18 + Paimon 0.8 + MinIO S3"
echo "========================================================"
echo -e "${NC}"

ACTION=${1:-help}

case $ACTION in
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
    shell)
        exec bash
        ;;
    *)
        echo "Usage: bash run-docker.sh {warehouse|ddl|mock|etl|report|schema-demo|checkpoint-demo|shell}"
        echo ""
        echo "Commands:"
        echo "  warehouse       Full SQL pipeline (DDL + mock + ETL + report)"
        echo "  ddl             Create all tables"
        echo "  mock            Insert mock data"
        echo "  etl             Run ETL (ODS->DWD->DWS->ADS)"
        echo "  report          Query all reports"
        echo "  schema-demo     Schema Evolution demo (Pit 2)"
        echo "  checkpoint-demo Checkpoint Recovery demo"
        echo "  shell           Open bash shell"
        ;;
esac

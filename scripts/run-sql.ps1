# ================================================================
# run-sql.ps1: Run SQL pipeline via Docker (Windows PowerShell)
# ================================================================
# Usage:
#   .\scripts\run-sql.ps1 up          # Start MinIO + Flink
#   .\scripts\run-sql.ps1 warehouse   # Full SQL pipeline
#   .\scripts\run-sql.ps1 ddl         # Create tables only
#   .\scripts\run-sql.ps1 mock        # Insert mock data
#   .\scripts\run-sql.ps1 etl         # Run ETL
#   .\scripts\run-sql.ps1 report      # Query reports
#   .\scripts\run-sql.ps1 down        # Stop services
# ================================================================

param(
    [Parameter(Position=0)]
    [string]$Action = "warehouse"
)

$PROJECT_DIR = Split-Path -Parent $PSScriptRoot
$COMPOSE_FILE = Join-Path $PROJECT_DIR "docker\docker-compose.yml"

function Ensure-Running {
    $status = docker compose -f $COMPOSE_FILE ps --format json 2>$null | Out-String
    if ($status -notmatch "running") {
        Write-Host "Starting Docker services..." -ForegroundColor Cyan
        docker compose -f $COMPOSE_FILE up -d
        Start-Sleep -Seconds 5
    }
}

function Run-Sql-In-Docker($cmd) {
    Write-Host "=== Running SQL: $cmd ===" -ForegroundColor Cyan
    docker compose -f $COMPOSE_FILE exec jobmanager bash -c "bash /app/sql/run-sql.sh $cmd"
}

function Up-Services {
    Write-Host "=== Starting Docker (MinIO + Flink) ===" -ForegroundColor Cyan
    docker compose -f $COMPOSE_FILE up -d
    Write-Host ""
    Write-Host "MinIO Console: http://localhost:9001 (admin / admin123)" -ForegroundColor Green
    Write-Host "Flink Web UI:  http://localhost:8081" -ForegroundColor Green
}

function Down-Services {
    Write-Host "=== Stopping Docker ===" -ForegroundColor Cyan
    docker compose -f $COMPOSE_FILE down
}

# ================================================================
# Main
# ================================================================

Write-Host ""
Write-Host "========================================================" -ForegroundColor Cyan
Write-Host "  Paimon Lakehouse SQL Runner (Docker)" -ForegroundColor Cyan
Write-Host "  Pure SQL - No Java compilation needed" -ForegroundColor Cyan
Write-Host "========================================================" -ForegroundColor Cyan
Write-Host ""

switch ($Action) {
    "up"          { Up-Services }
    "down"        { Down-Services }
    "warehouse"   {
        Ensure-Running
        Run-Sql-In-Docker "warehouse"
        Write-Host ""
        Write-Host "Open report\dashboard.html in browser" -ForegroundColor Green
    }
    "ddl"         { Ensure-Running; Run-Sql-In-Docker "ddl" }
    "mock"        { Ensure-Running; Run-Sql-In-Docker "mock" }
    "etl"         { Ensure-Running; Run-Sql-In-Docker "etl" }
    "report"      { Ensure-Running; Run-Sql-In-Docker "report" }
    "schema-demo" { Ensure-Running; Run-Sql-In-Docker "schema-demo" }
    "checkpoint-demo" { Ensure-Running; Run-Sql-In-Docker "checkpoint-demo" }
    default {
        Write-Host "Usage: .\run-sql.ps1 {up|warehouse|ddl|mock|etl|report|schema-demo|checkpoint-demo|down}" -ForegroundColor Yellow
        Write-Host ""
        Write-Host "Quick start:" -ForegroundColor Green
        Write-Host "  .\run-sql.ps1 up          # Start MinIO + Flink" -ForegroundColor White
        Write-Host "  .\run-sql.ps1 warehouse   # Run full SQL pipeline" -ForegroundColor White
        Write-Host "  .\run-sql.ps1 report      # Query all reports" -ForegroundColor White
        Write-Host "  .\run-sql.ps1 down        # Stop services" -ForegroundColor White
    }
}

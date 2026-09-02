# ================================================================
# run-docker.ps1: Run SQL pipeline via Docker (Windows PowerShell)
# ================================================================
# Pure SQL version - no Java compilation needed
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
    Write-Host "=== Running: $cmd ===" -ForegroundColor Cyan
    docker compose -f $COMPOSE_FILE exec jobmanager bash -c "bash /app/sql/run-sql.sh $cmd"
}

Write-Host ""
Write-Host "========================================================" -ForegroundColor Cyan
Write-Host "  Paimon Lakehouse (SQL Mode)" -ForegroundColor Cyan
Write-Host "  Flink 1.18 + Paimon 0.8 + MinIO S3" -ForegroundColor Cyan
Write-Host "========================================================" -ForegroundColor Cyan
Write-Host ""

switch ($Action) {
    "up"          {
        Write-Host "=== Starting Docker ===" -ForegroundColor Cyan
        docker compose -f $COMPOSE_FILE up -d
        Write-Host "MinIO: http://localhost:9001 (admin/admin123)" -ForegroundColor Green
        Write-Host "Flink: http://localhost:8081" -ForegroundColor Green
    }
    "down"        { docker compose -f $COMPOSE_FILE down }
    "warehouse"   { Ensure-Running; Run-Sql-In-Docker "warehouse" }
    "ddl"         { Ensure-Running; Run-Sql-In-Docker "ddl" }
    "mock"        { Ensure-Running; Run-Sql-In-Docker "mock" }
    "etl"         { Ensure-Running; Run-Sql-In-Docker "etl" }
    "report"      { Ensure-Running; Run-Sql-In-Docker "report" }
    "schema-demo" { Ensure-Running; Run-Sql-In-Docker "schema-demo" }
    "checkpoint-demo" { Ensure-Running; Run-Sql-In-Docker "checkpoint-demo" }
    default {
        Write-Host "Usage: .\run-docker.ps1 {up|warehouse|ddl|mock|etl|report|schema-demo|checkpoint-demo|down}" -ForegroundColor Yellow
    }
}

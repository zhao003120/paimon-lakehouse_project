# ================================================================
# run-docker.ps1: Run SQL pipeline via Docker (Windows PowerShell)
# ================================================================
# Flink (write) + StarRocks (read) - pure SQL, no Java
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
        Write-Host "Waiting for services to initialize..." -ForegroundColor Yellow
        Start-Sleep -Seconds 15
    }
}

function Run-In-Docker($cmd) {
    Write-Host "=== Running: $cmd ===" -ForegroundColor Cyan
    docker compose -f $COMPOSE_FILE exec jobmanager bash -c "bash /app/sql/run-sql.sh $cmd"
}

Write-Host ""
Write-Host "========================================================" -ForegroundColor Cyan
Write-Host "  Paimon Lakehouse (SQL Mode)" -ForegroundColor Cyan
Write-Host "  Flink 1.18 + Paimon 0.8 + MinIO S3 + StarRocks 3.3" -ForegroundColor Cyan
Write-Host "========================================================" -ForegroundColor Cyan
Write-Host ""

switch -Wildcard ($Action) {
    "up"          {
        Write-Host "=== Starting Docker (MinIO + Flink + StarRocks) ===" -ForegroundColor Cyan
        docker compose -f $COMPOSE_FILE up -d
        Write-Host ""
        Write-Host "MinIO:      http://localhost:9001 (admin/admin123)" -ForegroundColor Green
        Write-Host "Flink:      http://localhost:8081" -ForegroundColor Green
        Write-Host "StarRocks:  http://localhost:8030  (MySQL: localhost:9030 root/empty)" -ForegroundColor Green
    }
    "down"        { docker compose -f $COMPOSE_FILE down }
    "warehouse+sr" { Ensure-Running; Run-In-Docker "warehouse+sr" }

    # Flink
    "warehouse"   { Ensure-Running; Run-In-Docker "warehouse" }
    "ddl"         { Ensure-Running; Run-In-Docker "ddl" }
    "mock"        { Ensure-Running; Run-In-Docker "mock" }
    "etl"         { Ensure-Running; Run-In-Docker "etl" }
    "report"      { Ensure-Running; Run-In-Docker "report" }
    "schema-demo" { Ensure-Running; Run-In-Docker "schema-demo" }
    "checkpoint-demo" { Ensure-Running; Run-In-Docker "checkpoint-demo" }

    # StarRocks
    "sr-init"     { Ensure-Running; Run-In-Docker "sr-init" }
    "sr-catalog"  { Ensure-Running; Run-In-Docker "sr-catalog" }
    "sr-report"   { Ensure-Running; Run-In-Docker "sr-report" }
    "sr-benchmark" { Ensure-Running; Run-In-Docker "sr-benchmark" }
    "sr-schema"   { Ensure-Running; Run-In-Docker "sr-schema" }
    "sr-all"      { Ensure-Running; Run-In-Docker "sr-all" }
    "sr-shell"    { Ensure-Running; Run-In-Docker "sr-shell" }

    default {
        Write-Host "Usage: .\run-docker.ps1 {command}" -ForegroundColor Yellow
        Write-Host ""
        Write-Host "Quick start:" -ForegroundColor Green
        Write-Host "  .\run-docker.ps1 up            # Start all services" -ForegroundColor White
        Write-Host "  .\run-docker.ps1 warehouse     # Flink: full pipeline" -ForegroundColor White
        Write-Host "  .\run-docker.ps1 sr-init       # StarRocks: init cluster" -ForegroundColor White
        Write-Host "  .\run-docker.ps1 sr-report     # StarRocks: query reports" -ForegroundColor White
        Write-Host "  .\run-docker.ps1 warehouse+sr  # Flink ETL + StarRocks query" -ForegroundColor White
        Write-Host "  .\run-docker.ps1 down          # Stop all services" -ForegroundColor White
        Write-Host ""
        Write-Host "Services:" -ForegroundColor Cyan
        Write-Host "  MinIO:      http://localhost:9001 (admin/admin123)" -ForegroundColor Gray
        Write-Host "  Flink:      http://localhost:8081" -ForegroundColor Gray
        Write-Host "  StarRocks:  http://localhost:8030  (MySQL: localhost:9030)" -ForegroundColor Gray
    }
}

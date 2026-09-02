# ================================================================
# Paimon Lakehouse Docker Runner (Windows PowerShell)
# ================================================================
# Usage:
#   .\scripts\run-docker.ps1 up          # Start MinIO + Flink
#   .\scripts\run-docker.ps1 build       # Build project in Docker
#   .\scripts\run-docker.ps1 warehouse   # Full pipeline
#   .\scripts\run-docker.ps1 dashboard   # Generate HTML dashboard
#   .\scripts\run-docker.ps1 down        # Stop & cleanup
#   .\scripts\run-docker.ps1 logs        # View logs
#   .\scripts\run-docker.ps1 shell       # Open shell in jobmanager
# ================================================================

param(
    [Parameter(Position=0)]
    [string]$Action = "up"
)

$PROJECT_DIR = Split-Path -Parent $PSScriptRoot
$COMPOSE_FILE = Join-Path $PROJECT_DIR "docker\docker-compose.yml"

function Up-Services {
    Write-Host "=== Starting Docker services (MinIO + Flink) ===" -ForegroundColor Cyan
    docker compose -f $COMPOSE_FILE up -d
    if ($LASTEXITCODE -eq 0) {
        Write-Host ""
        Write-Host "Services started:" -ForegroundColor Green
        Write-Host "  MinIO Console:  http://localhost:9001  (admin / admin123)" -ForegroundColor White
        Write-Host "  Flink Web UI:   http://localhost:8081" -ForegroundColor White
        Write-Host ""
        Write-Host "Next: .\scripts\run-docker.ps1 build" -ForegroundColor Yellow
    } else {
        Write-Host "Failed to start. Is Docker Desktop running?" -ForegroundColor Red
    }
}

function Build-In-Docker {
    Write-Host "=== Building project in Docker ===" -ForegroundColor Cyan
    docker compose -f $COMPOSE_FILE exec jobmanager bash -c "cd /app && mvn clean package -DskipTests"
    if ($LASTEXITCODE -eq 0) {
        Write-Host "Build OK" -ForegroundColor Green
    } else {
        Write-Host "Build FAILED" -ForegroundColor Red
    }
}

function Run-In-Docker($cmd) {
    Write-Host "=== Running: $cmd ===" -ForegroundColor Cyan
    docker compose -f $COMPOSE_FILE exec jobmanager bash -c "cd /app && bash docker/run-docker.sh $cmd"
}

function Open-Shell {
    Write-Host "=== Opening shell in jobmanager ===" -ForegroundColor Cyan
    docker compose -f $COMPOSE_FILE exec jobmanager bash
}

function Down-Services {
    Write-Host "=== Stopping Docker services ===" -ForegroundColor Cyan
    docker compose -f $COMPOSE_FILE down
    Write-Host "Stopped." -ForegroundColor Green
}

function Show-Logs {
    docker compose -f $COMPOSE_FILE logs -f
}

# ================================================================
# Main
# ================================================================

Write-Host ""
Write-Host "========================================================" -ForegroundColor Cyan
Write-Host "  Paimon Lakehouse Docker Runner" -ForegroundColor Cyan
Write-Host "  Flink 1.18 + Paimon 0.8 + MinIO S3" -ForegroundColor Cyan
Write-Host "========================================================" -ForegroundColor Cyan
Write-Host ""

switch ($Action) {
    "up"          { Up-Services }
    "build"       { Build-In-Docker }
    "mock"        { Run-In-Docker "mock" }
    "init"        { Run-In-Docker "init" }
    "ods-dwd"     { Run-In-Docker "ods-dwd" }
    "dwd-dws"     { Run-In-Docker "dwd-dws" }
    "dws-ads"     { Run-In-Docker "dws-ads" }
    "report"      { Run-In-Docker "report" }
    "dashboard"   { Run-In-Docker "dashboard" }
    "warehouse"   {
        Up-Services
        Write-Host ""
        Start-Sleep -Seconds 3
        Build-In-Docker
        Write-Host ""
        Run-In-Docker "warehouse"
        Write-Host ""
        Write-Host "Dashboard: open report\dashboard.html in browser" -ForegroundColor Green
    }
    "schema"      { Run-In-Docker "schema" }
    "checkpoint"  { Run-In-Docker "checkpoint" }
    "batch"       { Run-In-Docker "batch" }
    "shell"       { Open-Shell }
    "logs"        { Show-Logs }
    "down"        { Down-Services }
    default {
        Write-Host "Usage: .\run-docker.ps1 {up|build|warehouse|mock|init|ods-dwd|dwd-dws|dws-ads|report|dashboard|schema|checkpoint|batch|shell|logs|down}" -ForegroundColor Yellow
        Write-Host ""
        Write-Host "Quick start:" -ForegroundColor Green
        Write-Host "  .\run-docker.ps1 up          # Start MinIO + Flink" -ForegroundColor White
        Write-Host "  .\run-docker.ps1 build       # Build project" -ForegroundColor White
        Write-Host "  .\run-docker.ps1 warehouse   # Full pipeline + dashboard" -ForegroundColor White
        Write-Host "  .\run-docker.ps1 down        # Stop & cleanup" -ForegroundColor White
    }
}

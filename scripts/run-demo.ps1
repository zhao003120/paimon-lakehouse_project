# ================================================================
# Paimon Lakehouse Run Script (PowerShell)
# ================================================================

param(
    [Parameter(Position=0)]
    [ValidateSet(
        "build", "write", "read", "read-evolution", "schema", "checkpoint",
        "mock", "batch",
        "init", "ods-dwd", "dwd-dws", "dws-ads", "report", "dashboard", "warehouse",
        "all"
    )]
    [string]$Action = "all"
)

$PROJECT_DIR = Split-Path -Parent $PSScriptRoot
$JAR_NAME = "paimon-lakehouse_project-1.0-SNAPSHOT.jar"
$JAR_PATH = Join-Path $PROJECT_DIR "target\$JAR_NAME"
$FLINK_LIB = "${env:FLINK_HOME}\lib"

function Build-Project {
    Write-Host "=== Build Project ===" -ForegroundColor Cyan
    Push-Location $PROJECT_DIR
    mvn clean package -DskipTests
    if ($LASTEXITCODE -eq 0) {
        Write-Host "Build OK: $JAR_PATH" -ForegroundColor Green
    } else {
        Write-Host "Build FAILED" -ForegroundColor Red
        Pop-Location
        exit 1
    }
    Pop-Location
}

function Run-Java($mainClass, $title, $subtitle) {
    Write-Host "=== $title ===" -ForegroundColor Cyan
    if ($subtitle) { Write-Host "  $subtitle" -ForegroundColor Yellow }
    Write-Host ""
    java -cp "$JAR_PATH;$FLINK_LIB\*" $mainClass
}

# ================================================================
# Three Pitfalls Demo
# ================================================================

function Run-Writer {
    Run-Java "com.example.paimon.writer.OrderWriterJob" `
        "Start Writer (Kafka -> Flink -> Paimon)" `
        "Pit 1: FULL-COMPACTION + 3min checkpoint / Pit 3: Data Validation"
}

function Run-Reader([bool]$Evolution = $false) {
    $evo = if ($Evolution) { "ON" } else { "OFF" }
    Write-Host "=== Start Reader (Schema Evolution: $evo) ===" -ForegroundColor Cyan
    if ($Evolution) {
        java -cp "$JAR_PATH;$FLINK_LIB\*" com.example.paimon.reader.OrderReaderJob "" "true"
    } else {
        java -cp "$JAR_PATH;$FLINK_LIB\*" com.example.paimon.reader.OrderReaderJob
    }
}

function Run-SchemaDemo      { Run-Java "com.example.paimon.schema.SchemaEvolutionDemo"      "Schema Evolution Demo (Pit 2)" "ALTER ADD COLUMN -> Cross Schema Query" }
function Run-CheckpointDemo  { Run-Java "com.example.paimon.schema.CheckpointRecoveryDemo"  "Checkpoint Recovery Demo"      "Old schema recovery -> Error -> Fix" }
function Run-MockData        { Run-Java "com.example.paimon.writer.MockDataGenerator"        "Generate Mock Data"            "Normal + Dirty + AVG compare" }
function Run-BatchQuery      { Run-Java "com.example.paimon.reader.BatchQueryJob"            "Batch Query Demo"              "Full/CrossSchema/Aggregate/TimeTravel" }

# ================================================================
# Warehouse Pipeline
# ================================================================

function Run-WarehouseInit {
    Run-Java "com.example.paimon.warehouse.WarehouseInitJob" `
        "Warehouse Init (Create DB & Tables)" `
        "ODS / DWD / DWS / ADS"
}

function Run-OdsToDwd {
    Run-Java "com.example.paimon.warehouse.OdsToDwdJob" `
        "ODS -> DWD" `
        "JSON parse + Dimension + Status"
}

function Run-DwdToDws {
    Run-Java "com.example.paimon.warehouse.DwdToDwsJob" `
        "DWD -> DWS" `
        "Daily + Weekly Aggregate"
}

function Run-DwsToAds {
    Run-Java "com.example.paimon.warehouse.DwsToAdsJob" `
        "DWS -> ADS" `
        "KPI + Customer Rank + Channel"
}

function Run-Report {
    Run-Java "com.example.paimon.warehouse.ReportJob" `
        "Report Query (BI)" `
        "KPI / Rank / Channel / Trend / Reconcile"
}

function Run-Dashboard {
    Run-Java "com.example.paimon.warehouse.DashboardGenerator" `
        "Generate HTML Dashboard (from Paimon ADS)" `
        "Query real data -> data.js -> dashboard.html"
    Write-Host ""
    Write-Host "Dashboard generated, open in browser:" -ForegroundColor Green
    Write-Host "  $PROJECT_DIR\report\dashboard.html" -ForegroundColor White
    Write-Host ""
    Write-Host "Auto open:" -ForegroundColor Yellow
    Write-Host "  Start-Process '$PROJECT_DIR\report\dashboard.html'" -ForegroundColor White
}

function Run-Warehouse {
    Write-Host "=== Full Warehouse Pipeline (ODS -> DWD -> DWS -> ADS -> Dashboard) ===" -ForegroundColor Magenta
    Write-Host ""
    Run-MockData
    Write-Host ""
    Run-WarehouseInit
    Write-Host ""
    Run-OdsToDwd
    Write-Host ""
    Run-DwdToDws
    Write-Host ""
    Run-DwsToAds
    Write-Host ""
    Run-Dashboard
}

# ================================================================
# Main
# ================================================================

Write-Host ""
Write-Host "========================================================" -ForegroundColor Cyan
Write-Host "  Paimon Lakehouse Project" -ForegroundColor Cyan
Write-Host "  3 Pitfalls + Full Warehouse (ODS/DWD/DWS/ADS)" -ForegroundColor Cyan
Write-Host "  Storage: MinIO S3" -ForegroundColor Cyan
Write-Host "========================================================" -ForegroundColor Cyan
Write-Host ""

if ($Action -eq "build" -or $Action -eq "all") {
    Build-Project
}

if ($Action -eq "all") {
    Write-Host ""
    Write-Host "Warehouse Pipeline (recommended order):" -ForegroundColor Green
    Write-Host "  1. .\run-demo.ps1 mock       # Generate ODS mock data" -ForegroundColor White
    Write-Host "  2. .\run-demo.ps1 init       # Warehouse init" -ForegroundColor White
    Write-Host "  3. .\run-demo.ps1 ods-dwd    # ODS -> DWD" -ForegroundColor White
    Write-Host "  4. .\run-demo.ps1 dwd-dws    # DWD -> DWS" -ForegroundColor White
    Write-Host "  5. .\run-demo.ps1 dws-ads    # DWS -> ADS" -ForegroundColor White
    Write-Host "  6. .\run-demo.ps1 report     # Report query" -ForegroundColor White
    Write-Host "  7. .\run-demo.ps1 dashboard  # HTML dashboard" -ForegroundColor White
    Write-Host ""
    Write-Host "One-click full pipeline:" -ForegroundColor Green
    Write-Host "  .\run-demo.ps1 warehouse    # ODS->DWD->DWS->ADS->Dashboard" -ForegroundColor White
    Write-Host ""
    Write-Host "Pitfall demos:" -ForegroundColor Green
    Write-Host "  .\run-demo.ps1 schema       # Schema Evolution (Pit 2)" -ForegroundColor White
    Write-Host "  .\run-demo.ps1 checkpoint   # Checkpoint Recovery" -ForegroundColor White
    Write-Host "  .\run-demo.ps1 batch        # Batch Query" -ForegroundColor White
    Write-Host "  .\run-demo.ps1 write        # Kafka Writer" -ForegroundColor White
    Write-Host "  .\run-demo.ps1 read         # Stream Reader (no evolution)" -ForegroundColor White
    Write-Host "  .\run-demo.ps1 read-evolution # Stream Reader (with evolution)" -ForegroundColor White
    Write-Host ""
    exit 0
}

switch ($Action) {
    "write"             { Run-Writer }
    "read"              { Run-Reader $false }
    "read-evolution"    { Run-Reader $true }
    "schema"            { Run-SchemaDemo }
    "checkpoint"        { Run-CheckpointDemo }
    "mock"              { Run-MockData }
    "batch"             { Run-BatchQuery }
    "init"              { Run-WarehouseInit }
    "ods-dwd"           { Run-OdsToDwd }
    "dwd-dws"           { Run-DwdToDws }
    "dws-ads"           { Run-DwsToAds }
    "report"            { Run-Report }
    "dashboard"         { Run-Dashboard }
    "warehouse"         { Run-Warehouse }
}

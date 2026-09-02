# ================================================================
# MinIO 一键启动脚本 (PowerShell)
#
# 启动 MinIO + 自动创建 paimon bucket
# ================================================================

param(
    [string]$DataPath = "C:\minio\data",
    [int]$Port = 9000,
    [int]$ConsolePort = 9001,
    [string]$AccessKey = "admin",
    [string]$SecretKey = "admin123"
)

$ErrorActionPreference = "Stop"

Write-Host ""
Write-Host "========================================================" -ForegroundColor Cyan
Write-Host "  MinIO 启动脚本 (Paimon Demo)" -ForegroundColor Cyan
Write-Host "========================================================" -ForegroundColor Cyan
Write-Host ""

# ---------------------------------- 检查 Docker ----------------------------------
$dockerAvailable = $false
try {
    $dockerVersion = docker --version 2>$null
    if ($dockerVersion) {
        $dockerAvailable = $true
        Write-Host "[OK] Docker 检测到: $dockerVersion" -ForegroundColor Green
    }
} catch {
    Write-Host "[INFO] Docker 不可用，将使用本地 MinIO 二进制" -ForegroundColor Yellow
}

# ---------------------------------- 方式1: Docker ----------------------------------
if ($dockerAvailable) {
    Write-Host ""
    Write-Host "方式: Docker 启动" -ForegroundColor Cyan
    Write-Host "  API 端口:    $Port" -ForegroundColor White
    Write-Host "  控制台端口:  $ConsolePort" -ForegroundColor White
    Write-Host "  数据目录:    $DataPath" -ForegroundColor White
    Write-Host "  Access Key:  $AccessKey" -ForegroundColor White
    Write-Host "  Secret Key:  $SecretKey" -ForegroundColor White
    Write-Host ""

    # 创建数据目录
    if (-not (Test-Path $DataPath)) {
        New-Item -ItemType Directory -Force -Path $DataPath | Out-Null
        Write-Host "[OK] 创建数据目录: $DataPath" -ForegroundColor Green
    }

    # 停止旧容器（如果有）
    docker stop paimon-minio 2>$null | Out-Null
    docker rm paimon-minio 2>$null | Out-Null

    # 启动 MinIO
    docker run -d --name paimon-minio `
        -p "${Port}:9000" `
        -p "${ConsolePort}:9001" `
        -e "MINIO_ROOT_USER=$AccessKey" `
        -e "MINIO_ROOT_PASSWORD=$SecretKey" `
        -v "${DataPath}:/data" `
        minio/minio server /data --console-address ":9001"

    if ($LASTEXITCODE -ne 0) {
        Write-Host "[ERROR] Docker 启动 MinIO 失败" -ForegroundColor Red
        exit 1
    }

    Write-Host "[OK] MinIO 容器已启动" -ForegroundColor Green
    Start-Sleep -Seconds 3

    # 创建 bucket
    Write-Host ""
    Write-Host "创建 paimon bucket..." -ForegroundColor Cyan
    docker exec paimon-minio mc alias set local http://localhost:9000 $AccessKey $SecretKey 2>$null
    docker exec paimon-minio mc mb local/paimon --ignore-existing 2>$null
    docker exec paimon-minio mc anonymous set readwrite local/paimon 2>$null

    Write-Host "[OK] Bucket 'paimon' 已创建" -ForegroundColor Green

} else {
    # ---------------------------------- 方式2: 本地二进制 ----------------------------------
    Write-Host "方式: 本地二进制启动（需要 minio.exe）" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "如果未安装 MinIO，请执行以下步骤:" -ForegroundColor Yellow
    Write-Host "  1. 下载: https://min.io/download#/windows" -ForegroundColor White
    Write-Host "  2. 放到 C:\minio\minio.exe" -ForegroundColor White
    Write-Host "  3. 重新运行此脚本" -ForegroundColor White
    Write-Host ""
    Write-Host "或使用 Docker:" -ForegroundColor Yellow
    Write-Host "  docker run -d --name paimon-minio -p 9000:9000 -p 9001:9001" -ForegroundColor White
    Write-Host "    -e MINIO_ROOT_USER=admin -e MINIO_ROOT_PASSWORD=admin123" -ForegroundColor White
    Write-Host "    -v C:\minio\data:/data minio/minio server /data --console-address ':9001'" -ForegroundColor White
    Write-Host ""

    $minioExe = "C:\minio\minio.exe"
    if (-not (Test-Path $minioExe)) {
        Write-Host "[ERROR] 未找到 $minioExe" -ForegroundColor Red
        Write-Host "请先下载 MinIO 或安装 Docker" -ForegroundColor Yellow
        exit 1
    }

    if (-not (Test-Path $DataPath)) {
        New-Item -ItemType Directory -Force -Path $DataPath | Out-Null
    }

    Write-Host "启动 MinIO..." -ForegroundColor Cyan
    Start-Process -FilePath $minioExe -ArgumentList "server", $DataPath, "--console-address", ":$ConsolePort" -NoNewWindow

    Start-Sleep -Seconds 3

    # 用 mc 创建 bucket
    $mcExe = "C:\minio\mc.exe"
    if (Test-Path $mcExe) {
        & $mcExe alias set local "http://localhost:$Port" $AccessKey $SecretKey 2>$null
        & $mcExe mb "local/paimon" --ignore-existing 2>$null
        Write-Host "[OK] Bucket 'paimon' 已创建" -ForegroundColor Green
    } else {
        Write-Host "[WARN] 未找到 mc.exe，请手动创建 bucket" -ForegroundColor Yellow
        Write-Host "  控制台: http://localhost:$ConsolePort" -ForegroundColor White
        Write-Host "  账号: $AccessKey / $SecretKey" -ForegroundColor White
    }
}

# ---------------------------------- 输出信息 ----------------------------------
Write-Host ""
Write-Host "========================================================" -ForegroundColor Cyan
Write-Host "  MinIO 已启动" -ForegroundColor Green
Write-Host "========================================================" -ForegroundColor Cyan
Write-Host "  API 地址:     http://localhost:$Port" -ForegroundColor White
Write-Host "  控制台地址:   http://localhost:$ConsolePort" -ForegroundColor White
Write-Host "  Access Key:   $AccessKey" -ForegroundColor White
Write-Host "  Secret Key:   $SecretKey" -ForegroundColor White
Write-Host "  Bucket:       paimon" -ForegroundColor White
Write-Host ""
Write-Host "  Paimon Warehouse: s3a://paimon/warehouse" -ForegroundColor Green
Write-Host ""
Write-Host "  下一步:" -ForegroundColor Cyan
Write-Host "    .\run-demo.ps1 build    # 编译" -ForegroundColor White
Write-Host "    .\run-demo.ps1 mock     # 生成模拟数据" -ForegroundColor White
Write-Host "    .\run-demo.ps1 batch    # 查询数据" -ForegroundColor White
Write-Host "========================================================" -ForegroundColor Cyan

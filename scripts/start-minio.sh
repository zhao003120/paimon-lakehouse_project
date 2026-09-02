#!/bin/bash
# ================================================================
# MinIO 一键启动脚本 (Bash)
# ================================================================

set -e

DATA_PATH="${1:-/tmp/minio-data}"
PORT=9000
CONSOLE_PORT=9001
ACCESS_KEY="admin"
SECRET_KEY="admin123"

echo ""
echo "========================================================"
echo "  MinIO 启动脚本 (Paimon Demo)"
echo "========================================================"
echo ""

# ---------------------------------- 检查 Docker ----------------------------------
if command -v docker &> /dev/null; then
    echo "方式: Docker 启动"
    echo "  API 端口:    $PORT"
    echo "  控制台端口:  $CONSOLE_PORT"
    echo "  数据目录:    $DATA_PATH"
    echo ""

    mkdir -p "$DATA_PATH"

    # 停止旧容器
    docker stop paimon-minio 2>/dev/null || true
    docker rm paimon-minio 2>/dev/null || true

    # 启动 MinIO
    docker run -d --name paimon-minio \
        -p "${PORT}:9000" \
        -p "${CONSOLE_PORT}:9001" \
        -e "MINIO_ROOT_USER=${ACCESS_KEY}" \
        -e "MINIO_ROOT_PASSWORD=${SECRET_KEY}" \
        -v "${DATA_PATH}:/data" \
        minio/minio server /data --console-address ":9001"

    echo "[OK] MinIO 容器已启动"
    sleep 3

    # 创建 bucket
    echo "创建 paimon bucket..."
    docker exec paimon-minio mc alias set local http://localhost:9000 "$ACCESS_KEY" "$SECRET_KEY" 2>/dev/null || true
    docker exec paimon-minio mc mb local/paimon --ignore-existing 2>/dev/null || true
    docker exec paimon-minio mc anonymous set readwrite local/paimon 2>/dev/null || true

    echo "[OK] Bucket 'paimon' 已创建"

else
    # ---------------------------------- 本地二进制 ----------------------------------
    echo "方式: 本地二进制启动"
    echo ""

    if ! command -v minio &> /dev/null; then
        echo "[ERROR] 未找到 minio 命令"
        echo "安装方式:"
        echo "  wget https://dl.min.io/server/minio/release/linux-amd64/minio"
        echo "  chmod +x minio && sudo mv minio /usr/local/bin/"
        echo ""
        echo "或使用 Docker:"
        echo "  docker run -d --name paimon-minio -p 9000:9000 -p 9001:9001"
        echo "    -e MINIO_ROOT_USER=admin -e MINIO_ROOT_PASSWORD=admin123"
        echo "    -v /tmp/minio-data:/data minio/minio server /data --console-address ':9001'"
        exit 1
    fi

    mkdir -p "$DATA_PATH"

    nohup minio server "$DATA_PATH" --console-address ":${CONSOLE_PORT}" > /tmp/minio.log 2>&1 &
    echo "[OK] MinIO 已启动 (PID: $!)"
    sleep 3

    # 创建 bucket
    if command -v mc &> /dev/null; then
        mc alias set local "http://localhost:${PORT}" "$ACCESS_KEY" "$SECRET_KEY" 2>/dev/null || true
        mc mb "local/paimon" --ignore-existing 2>/dev/null || true
        echo "[OK] Bucket 'paimon' 已创建"
    else
        echo "[WARN] 未找到 mc 命令，请通过控制台手动创建 bucket"
        echo "  控制台: http://localhost:${CONSOLE_PORT}"
    fi
fi

# ---------------------------------- 输出信息 ----------------------------------
echo ""
echo "========================================================"
echo "  MinIO 已启动"
echo "========================================================"
echo "  API 地址:     http://localhost:${PORT}"
echo "  控制台地址:   http://localhost:${CONSOLE_PORT}"
echo "  Access Key:   ${ACCESS_KEY}"
echo "  Secret Key:   ${SECRET_KEY}"
echo "  Bucket:       paimon"
echo ""
echo "  Paimon Warehouse: s3a://paimon/warehouse"
echo ""
echo "  下一步:"
echo "    ./run-demo.sh build    # 编译"
echo "    ./run-demo.sh mock     # 生成模拟数据"
echo "    ./run-demo.sh batch    # 查询数据"
echo "========================================================"

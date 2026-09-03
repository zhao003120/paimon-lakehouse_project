#!/bin/bash
# StarRocks BE startup script
# Appends priority_networks to existing be.conf (doesn't overwrite)

set -e

BE_HOME=/opt/starrocks/be
BE_CONF=$BE_HOME/conf/be.conf

# Only add priority_networks if not already present
if ! grep -q "priority_networks" "$BE_CONF"; then
    echo "" >> "$BE_CONF"
    echo "priority_networks = 10.0.0.0/8;172.16.0.0/12;192.168.0.0/16;127.0.0.1/32" >> "$BE_CONF"
fi

# Wait for FE to be ready
echo "Waiting for FE at starrocks-fe:9020..."
for i in $(seq 1 60); do
    if bash -c "echo > /dev/tcp/starrocks-fe/9020" 2>/dev/null; then
        echo "FE is ready!"
        break
    fi
    echo "  Attempt $i/60..."
    sleep 5
done

# Start BE in foreground
exec $BE_HOME/bin/start_be.sh

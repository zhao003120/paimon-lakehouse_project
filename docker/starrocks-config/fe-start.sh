#!/bin/bash
# StarRocks FE startup script
# Appends priority_networks to existing fe.conf (doesn't overwrite)

set -e

FE_HOME=/opt/starrocks/fe
FE_CONF=$FE_HOME/conf/fe.conf

# Only add priority_networks if not already present
if ! grep -q "priority_networks" "$FE_CONF"; then
    echo "" >> "$FE_CONF"
    echo "priority_networks = 10.0.0.0/8;172.16.0.0/12;192.168.0.0/16;127.0.0.1/32" >> "$FE_CONF"
fi

# Start FE in foreground (not daemon mode)
# This keeps the container running
exec $FE_HOME/bin/start_fe.sh

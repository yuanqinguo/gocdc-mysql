#!/bin/bash
# 通用 node1 重启（可选参数 1=先重建二进制）
set -u
cd /mnt/d/workers/worker/qingfly/gocdc-mysql/.verify
if [ "${1:-}" = "build" ]; then
  (cd /mnt/d/workers/worker/qingfly/gocdc-mysql && /usr/local/go/bin/go build -o .verify/gocdc ./cmd/gocdc) && echo BINARY-REBUILT
fi
PID=$(ps aux | grep '[.]/gocdc' | awk '{print $2}' | head -1)
if [ -n "$PID" ]; then kill -TERM "$PID"; sleep 3; fi
WEB_PASSWORD=demo123 nohup ./gocdc -config-file config-node1.yml > node1.log 2>&1 &
sleep 6
NEWPID=$(ps aux | grep '[.]/gocdc' | awk '{print $2}' | head -1)
echo "$NEWPID" > node1.pid
echo "RUNNING pid=$NEWPID"
curl -s --noproxy '*' localhost:9595/metrics | grep -E '^gocdc_master_position_lag_bytes|^gocdc_leader_state' | head -3

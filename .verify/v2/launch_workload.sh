#!/bin/bash
# 在 WSL 内以脱离会话的方式启动持续负载（运行一次即可，重复执行会先停旧负载）
set -u
BASE=/mnt/d/course/gocdc-mysql/.verify/v2
DURATION=${1:-604800}
INTERVAL=${2:-3}

for p in $(pgrep -f 'bash .*demo_workload\.sh' 2>/dev/null); do
  kill "$p" 2>/dev/null
done
sleep 1

setsid nohup bash "$BASE/demo_workload.sh" "$DURATION" "$INTERVAL" \
  >/dev/null 2>&1 </dev/null &
echo "workload launched: duration=${DURATION}s interval=${INTERVAL}s"
sleep 2
pgrep -af 'bash .*demo_workload\.sh' 2>/dev/null | sed 's/^/  running /'
tail -n 2 /tmp/demo-workload.log 2>/dev/null || true

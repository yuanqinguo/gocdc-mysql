#!/bin/bash
# WSL 全量回归：单测 -race + 集成（testcontainers）。日志落 .verify/wsl_full_test.log
set -uo pipefail
cd /mnt/d/workers/worker/qingfly/gocdc-mysql
LOG=/mnt/d/workers/worker/qingfly/gocdc-mysql/.verify/wsl_full_test.log
# 代理泄漏会杀 grpc/testcontainers（lib.sh 铁律）
unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY all_proxy ALL_PROXY no_proxy NO_PROXY 2>/dev/null || true
GO=/usr/local/go/bin/go
{
echo "=== [$(date '+%F %T')] WSL 单测 -race ==="
$GO test -race -count=1 ./... 2>&1 | grep -v "no test files"
echo "UNIT_RC=${PIPESTATUS[0]}"
echo "=== [$(date '+%F %T')] WSL 集成 -tags=integration ==="
$GO test -tags=integration -count=1 ./... 2>&1 | grep -v "no test files"
echo "INTEG_RC=${PIPESTATUS[0]}"
echo "=== [$(date '+%F %T')] done ==="
} 2>&1 | tee "$LOG"

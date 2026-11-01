#!/bin/bash
set -uo pipefail
cd /mnt/d/workers/worker/qingfly/gocdc-mysql
unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY all_proxy ALL_PROXY no_proxy NO_PROXY 2>/dev/null || true
GO=/usr/local/go/bin/go
{
echo "=== api audit ==="
$GO test -tags=integration -count=1 -run 'TestIntegrationAudit' -v ./internal/api 2>&1 | grep -E "RUN|PASS|FAIL|ok " 
echo "API_RC=${PIPESTATUS[0]}"
echo "=== stock marker GC（完整输出）==="
$GO test -tags=integration -count=1 -run 'TestIntegrationMarkerGC' -v ./internal/stock 2>&1 | tail -30
echo "STOCK_RC=${PIPESTATUS[0]}"
echo "=== ddl expired ==="
$GO test -tags=integration -count=1 -run 'TestIntegrationDDLExpiredDuringPause' ./internal/pipeline 2>&1 | tail -4
echo "DDL_RC=${PIPESTATUS[0]}"
} 2>&1 | tee /mnt/d/workers/worker/qingfly/gocdc-mysql/.verify/wsl_fix_test.log

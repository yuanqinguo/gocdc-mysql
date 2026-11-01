#!/bin/bash
# L1 WSL 侧：go test -race -count=1 ./...（强制真实执行，禁缓存）
REPO="${GOCDC_REPO:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"; cd "$REPO" || exit 1
/usr/local/go/bin/go test -race -count=1 ./... 2>&1
echo "L1_WSL_RACE_EXIT=$?"

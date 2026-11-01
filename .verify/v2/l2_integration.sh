#!/bin/bash
# L2：go test -tags=integration ./...（testcontainers，分钟级）
REPO="${GOCDC_REPO:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"; cd "$REPO" || exit 1
unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY all_proxy ALL_PROXY
export NO_PROXY="*"; export no_proxy="*"
# testcontainers 镜像默认取本地已有（可用环境变量覆盖）
export MYSQL_IMAGE="${MYSQL_IMAGE:-mysql:8.0}"
export GOCDC_TEST_ETCD_IMAGE="${GOCDC_TEST_ETCD_IMAGE:-docker.cnb.cool/qingflyfei/edu.image.base:etcd-3.5.16}"
/usr/local/go/bin/go test -tags=integration -count=1 -timeout=30m ./... 2>&1
echo "L2_EXIT=$?"

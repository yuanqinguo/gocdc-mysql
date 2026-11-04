#!/bin/bash
# 重启演示双节点（保留持续负载/容器/数据/etcd 状态）——更新前端或产品代码后使用。
# 前置：Windows 侧已 npm --prefix web run build（产物进 internal/api/dist）。
# 流程：重编 .verify/gocdc（嵌入最新前端）→ 精确杀节点 → start_demo.sh 重新拉起。
# 注意：本脚本不 source lib.sh（避免 flock 与 start_demo 冲突），杀进程规则与
# lib.killall_gocdc 完全一致：仅按 argv 第二段匹配 /gocdc$ 或 gocdcbin$，严禁宽匹配。
set -u
V2="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="${GOCDC_REPO:-$(cd "$V2/../.." && pwd)}"

echo "════ 重编译二进制（嵌入最新前端 dist）"
(cd "$REPO" && /usr/local/go/bin/go build -o .verify/gocdc ./cmd/gocdc) || { echo "❌ 编译失败"; exit 1; }
"$REPO/.verify/gocdc" -version

echo "════ 停旧节点（负载继续跑）"
for p in $(ps -eo pid,args | awk '$2 ~ /\/gocdc$/ || $2 ~ /gocdcbin$/ {print $1}'); do
  kill -9 "$p" 2>/dev/null
done
sleep 2

bash "$V2/start_demo.sh"

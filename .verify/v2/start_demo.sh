#!/bin/bash
# 演示环境启动（在 00_reset.sh 之后执行）：把一个集群的双节点拉起来。
# 形态遵循 v2 套件基线：配置正文在配置中心 cfg-etcd(12380)，节点零参数 + 引导桩。
#   node1: metrics :9595, web :8060
#   node2: metrics :9596, web :8061（热备，web 可看 follower 视角）
# 凭据：admin / demo123（HTTP Basic）
set -u
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

echo "════ 写入配置正文（/gocdc/verify/config ← body_main.yml）"
ccput verify "$V2/body_main.yml" || { echo "❌ 配置写入失败"; exit 1; }

echo "════ 启动 node1（web :8060 / metrics :9595）"
stub_start node1 verify :9595 :8060
echo "════ 启动 node2（web :8061 / metrics :9596）"
stub_start node2 verify :9596 :8061

for p in 9595 9596; do
  if wait_ready "$p" 120; then
    echo "  ✅ metrics :$p /healthz ok"
  else
    echo "  ❌ metrics :$p 未就绪，最近日志："
    tail -n 40 /tmp/v2/node*.log 2>/dev/null
    exit 1
  fi
done

echo "════ 集群状态"
for wp in 8060 8061; do
  echo "-- web :$wp role=$(roleof $wp)"
done

echo "════ 访问入口（Windows 侧）"
echo "  Web 管理台 : http://localhost:8060   （admin / demo123）"
echo "  热备节点   : http://localhost:8061   （同一套凭据，role=follower）"
echo "  指标       : http://localhost:9595/metrics"
echo "  源库 MySQL : 127.0.0.1:13306 (root/123456)"
echo "  目标库MySQL: 127.0.0.1:13307 (root/123456)"
echo "  日志       : /tmp/v2/node1.log /tmp/v2/node2.log（WSL 内）"

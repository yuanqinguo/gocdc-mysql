#!/bin/bash
# 停止演示环境：先停持续负载，再停 gocdc 节点（容器保留，数据/etcd 状态保留）。
# 注意：不能 source lib.sh——运行中的节点继承了启动脚本的 flock fd（stub_start
# 后台化时 fd 泄漏给 gocdcbin），先抢锁必被拒；必须先杀进程（已实证一次）。
set -u
echo "════ 停止持续负载"
for p in $(pgrep -f 'bash .*demo_workload\.sh' 2>/dev/null); do
  kill "$p" 2>/dev/null && echo "  killed workload pid=$p"
done
sleep 1

echo "════ 停止 gocdc 节点"
for p in $(ps -eo pid,args | awk '$2 ~ /\/gocdc$/ || $2 ~ /gocdcbin$/ {print $1}'); do
  kill -9 "$p" 2>/dev/null && echo "  killed node pid=$p"
done
sleep 2

W=$(pgrep -cf 'bash .*demo_workload\.sh' 2>/dev/null || true)
N=$(ps -eo args | awk '$1 ~ /\/gocdc$/ || $1 ~ /gocdcbin$/' | wc -l)
echo "════ 结果：负载残留=${W:-0} 节点残留=$N（容器未动，MySQL 数据保留）"
[ "${W:-0}" = "0" ] && [ "$N" = "0" ] && echo "✅ 演示环境已停止"

#!/bin/bash
# dbg_lag_race.sh — B2-B5 FAIL 定位：lag gauge 3s 采样周期 vs 断言首探竞态
# 机制假设：workload 落库后立刻读 /metrics，gauge 仍是上一 tick（workload 前、
# 确已追平的 0）→ 假「追平」→ 行数断言过早。若 gauge 在 >2 个周期（6s）内
# 持续 0 而行数仍缺 → 才是产品缺陷（计算错误/不刷新）。
source "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/lib.sh"

wipe_all
ccput verify "$V2/body_main.yml"
stub_start node1 verify :9595 :8060
wait_ready 9595 60 || { echo "FATAL 启动失败"; exit 1; }
# stock 收敛（行数口径）
for i in $(seq 1 40); do
  D=$($DST -N -e "SELECT COUNT(*) FROM rmqtest.msg" 2>/dev/null)
  [ "$D" = "0" ] && S0=$($SRC -N -e "SELECT COUNT(*) FROM rmqtest.msg" 2>/dev/null) && [ "$S0" = "0" ] && break
  sleep 2
done
# 静默确证：连续两次采样（间隔≥4s，跨刷新周期）lag 均 0 → gauge 是「新鲜 0」
L1=$(lagof 9595); sleep 4; L2=$(lagof 9595)
echo "[quiesce] lag1=$L1 lag2=$L2（双 0 = 新鲜追平态）"

# 写入 3000 行（消费需要 >1s，制造可观测积压窗口）
$SRC -e "INSERT INTO shop.orders (user_id,amount,remark) SELECT 9,n,CONCAT('dbg-lag-',n) FROM (WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM s WHERE n<3000) SELECT n FROM s) t" 2>/dev/null
T0=$(date +%s%3N)
echo "[t=ms] lag dst_rows"
for i in $(seq 1 40); do
  NOW=$(( $(date +%s%3N) - T0 ))
  L=$(lagof 9595)
  D=$($DST -N -e "SELECT COUNT(*) FROM shop.orders WHERE remark LIKE 'dbg-lag-%'" 2>/dev/null)
  echo "t=${NOW}ms lag=$L dst=$D/3000"
  [ "$D" = "3000" ] && [ "$L" = "0" ] && break
  sleep 0.3
done
killall_gocdc
echo "DBG DONE"

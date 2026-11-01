#!/bin/bash
# v2 组40：高可用集群（D1-D5）——共享配置中心正文（/gocdc/verify/config 一份，
# 端口经 ${WEB_ADDR}/${METRICS_ADDR} 环境变量区分，node.id/server_id 全派生）
# D1 竞选单 leader / D2 follower 三态 / D3 kill -9 接管 / D4 旧 leader 回归 / D5 resign
set -u
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

wipe_all
ccput verify "$V2/body_main.yml"

grp "D1 双节点竞选恰一 leader（单 Binlog Dump）"
stub_start node1 verify :9595 :8060
sleep 6
stub_start node2 verify :9596 :8061
L=""
for i in $(seq 1 20); do L=$(leader_port); [ -n "$L" ] && break; sleep 2; done
R1=$(roleof 8060); R2=$(roleof 8061); DC=$(dumpconns)
echo "  node1=$R1 node2=$R2 leader=$L dump=$DC"
[ -n "$L" ] && [ "$DC" = "1" ] && ok "D1 竞选单 leader 单 Dump（leader=: $L）" || bad "D1 ($R1/$R2 dump=$DC)"

grp "D2 follower 三断言（role 三态 / readyz not ready / leader_state=0）"
FP=$([ "$L" = "8060" ] && echo 8061 || echo 8060)
FMET=$([ "$FP" = "8060" ] && echo 9595 || echo 9596)
FR=""
for i in $(seq 1 15); do FR=$(roleof $FP); [ -n "$FR" ] && break; sleep 1; done
FREADY=$(curl -s --noproxy '*' localhost:$FMET/readyz)
FLEAD=$(curl -s --noproxy '*' localhost:$FMET/metrics | grep -c '^gocdc_leader_state 1')
echo "  follower role=$FR readyz=[$FREADY] leader_state1=$FLEAD"
[ "$FR" = "follower" ] && echo "$FREADY" | grep -qi "not ready" && [ "$FLEAD" = "0" ] \
  && ok "D2 follower 三断言全过" || bad "D2 (role=$FR ready=$FREADY lead=$FLEAD)"

grp "D3 kill -9 leader → ≤20s 接管 + 窗口收敛"
$SRC -e "INSERT INTO shop.new_table (val) VALUES ('d3-w1')" 2>/dev/null
LPID=$(pickpid $([ "$L" = "8060" ] && echo node1 || echo node2))
T0=$(date +%s)
kill -9 "$LPID"
$SRC -e "INSERT INTO shop.new_table (val) VALUES ('d3-w2')" 2>/dev/null
NL=""; T1=""
for i in $(seq 1 25); do
  NL=$(leader_port)
  [ -n "$NL" ] && [ "$NL" != "$L" ] && { T1=$(date +%s); break; }
  sleep 1
done
sleep 10   # 旧进程死连接需被 MySQL 回收，dump 断言在窗口后取值
DC=$(dumpconns)
W1=$($DST -N -e "SELECT COUNT(*) FROM shop.new_table WHERE val='d3-w1'" 2>/dev/null)
W2=$($DST -N -e "SELECT COUNT(*) FROM shop.new_table WHERE val='d3-w2'" 2>/dev/null)
DT=$( [ -n "$T1" ] && echo $((T1-T0)) || echo -1 )
echo "  接管=${DT}s 新leader=$NL dump(10s后)=$DC 窗口 w1=$W1 w2=$W2"
if [ -n "$T1" ] && [ -n "$NL" ] && [ "$DT" -le 20 ] && [ "$DC" = "1" ] && [ "$W1" = "1" ] && [ "$W2" = "1" ]; then
  ok "D3 接管 ${DT}s + 窗口前后数据收敛"
else
  bad "D3 (dt=$DT nl=$NL dump=$DC w1=$W1 w2=$W2)"
fi

grp "D4 被杀节点回归降 follower（仍单 Dump）"
[ "$L" = "8060" ] && stub_start node1 verify :9595 :8060 || stub_start node2 verify :9596 :8061
sleep 12
R1=$(roleof 8060); R2=$(roleof 8061); CL=$(leader_port); DC=$(dumpconns)
echo "  node1=$R1 node2=$R2 leader=$CL dump=$DC"
[ "$CL" = "$NL" ] && [ "$DC" = "1" ] && ok "D4 旧 leader 回归降 follower 仍单 Dump" || bad "D4 ($R1/$R2 leader=$CL dump=$DC)"

grp "D5 resign 优雅让位 ≤5s + 双进程存活"
LPID=$(pickpid $([ "$CL" = "8060" ] && echo node1 || echo node2))
T0=$(date +%s)
curl -s --noproxy '*' -u admin:demo123 -X POST "localhost:$CL/api/v1/leader/resign?confirm=true" >/dev/null
NL2=""; T1=""
for i in $(seq 1 20); do
  NL2=$(leader_port)
  [ -n "$NL2" ] && [ "$NL2" != "$CL" ] && { T1=$(date +%s); break; }
  sleep 0.5
done
sleep 3
A1=$(pickpid node1); A2=$(pickpid node2); DC=$(dumpconns)
echo "  切换=$((T1-T0))s 新leader=$NL2 dump=$DC node1存活=$A1 node2存活=$A2"
if [ -n "$T1" ] && [ -n "$NL2" ] && [ "$((T1-T0))" -le 5 ] && [ "$DC" = "1" ] && [ -n "$A1" ] && [ -n "$A2" ]; then
  ok "D5 resign $((T1-T0))s 双进程存活"
else
  bad "D5 (t=$T1 nl=$NL2 dump=$DC a1=$A1 a2=$A2)"
fi

killall_gocdc
summary

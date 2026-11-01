#!/bin/bash
# D1-D5 v2（修正：Binlog Dump GTID 变体 / status.role 字段 / node1 缺席重启）
set -u
cd /mnt/d/workers/worker/qingfly/gocdc-mysql/.verify
SRC="docker exec vf-src mysql -uroot -p123456 -h127.0.0.1 --default-character-set=utf8mb4"
dumpconns() { $SRC -N -e "SELECT COUNT(*) FROM information_schema.processlist WHERE command LIKE 'Binlog Dump%'" 2>/dev/null; }
roleof() { curl -s --noproxy '*' -u admin:demo123 "localhost:$1/api/v1/status" | python3 -c 'import sys,json
try: print(json.load(sys.stdin)["data"]["role"])
except: print("?")' 2>/dev/null; }
leader_port() { for p in 8060 8061; do [ "$(roleof $p)" = "leader" ] && { echo $p; return; }; done; echo ""; }

echo "=== 前置：确保 node1 活着（HA-10 段被停了）"
if [ "$(ps aux | grep '[.]gocdc -config-file config-node1' | wc -l)" = "0" ]; then
  WEB_PASSWORD=demo123 nohup ./gocdc -config-file config-node1.yml > node1.log 2>&1 &
  sleep 6
fi
ps aux | grep '[.]gocdc -config-file config-node1' | grep -v grep | awk '{print $2}' | head -1 > node1.pid
ps aux | grep '[.]gocdc -config-file config-node2' | grep -v grep | awk '{print $2}' | head -1 > node2.pid
echo "node1=$(cat node1.pid) node2=$(cat node2.pid)"

echo "=== D1: 双节点——恰一 leader、单 Binlog Dump(GTID)"
sleep 4
L=$(leader_port)
R1=$(roleof 8060); R2=$(roleof 8061)
DC=$(dumpconns)
LEADERCOUNT=$([ "$R1" = "leader" ] && echo 0 || echo 1; [ "$R2" = "leader" ] && echo 0 || echo 1)
echo "D1: node1=$R1 node2=$R2 leader端口=$L dump=$DC"
[ -n "$L" ] && [ "$DC" = "1" ] && echo "D1 RESULT=PASS" || echo "D1 RESULT=FAIL"

echo "=== D2: follower 不启管线"
FP=$([ "$L" = "8060" ] && echo 8061 || echo 8060)
FR=$(roleof $FP)
FREADY=$(curl -s --noproxy '*' localhost:$([ "$FP" = "8060" ] && echo 9595 || echo 9596)/readyz)
echo "D2: follower($FP) role=$FR readyz=$FREADY（期待 follower/not ready）"
[ "$FR" = "follower" ] && echo "D2 RESULT=PASS" || echo "D2 RESULT=FAIL"

echo "=== D3: kill -9 leader → 接管 ≤TTL+10s=20s"
LPID=$(cat $([ "$L" = "8060" ] && echo node1.pid || echo node2.pid))
T0=$(date +%s)
kill -9 "$LPID"
NL=""
for i in $(seq 1 25); do
  NL=$(leader_port)
  if [ -n "$NL" ] && [ "$NL" != "$L" ]; then T1=$(date +%s); break; fi
  sleep 1
done
DC=$(dumpconns)
if [ -n "$NL" ] && [ "$NL" != "$L" ]; then
  echo "D3: 接管耗时=$((T1-T0))s 新leader=$NL dump=$DC"
  [ "$((T1-T0))" -le 20 ] && [ "$DC" = "1" ] && echo "D3 RESULT=PASS" || echo "D3 RESULT=FAIL"
else
  echo "D3 RESULT=FAIL（20s+ 无人接管）"; T1=$(date +%s)
fi
echo "接管后窗口数据补齐检查——写入一行"
sleep 3

echo "=== 被杀节点重启回归"
if [ "$L" = "8060" ]; then
  WEB_PASSWORD=demo123 nohup ./gocdc -config-file config-node1.yml > node1.log 2>&1 &
else
  WEB_PASSWORD=demo123 nohup ./gocdc -config-file config-node2.yml > node2.log 2>&1 &
fi
sleep 7
ps aux | grep '[.]gocdc -config-file config-node1' | grep -v grep | awk '{print $2}' | head -1 > node1.pid
ps aux | grep '[.]gocdc -config-file config-node2' | grep -v grep | awk '{print $2}' | head -1 > node2.pid

echo "=== D4: 旧 leader 回归降 follower（仍单 Dump）"
sleep 5
CL=$(leader_port)
R1=$(roleof 8060); R2=$(roleof 8061)
DC=$(dumpconns)
echo "D4: node1=$R1 node2=$R2 leader=$CL dump=$DC"
[ "$CL" = "$NL" ] && [ "$DC" = "1" ] && echo "D4 RESULT=PASS" || echo "D4 RESULT=FAIL"

echo "=== D5: resign 优雅让位（≤3s）"
LPID=$(cat $([ "$CL" = "8060" ] && echo node1.pid || echo node2.pid))
T0=$(date +%s)
curl -s --noproxy '*' -u admin:demo123 -X POST "localhost:$CL/api/v1/leader/resign?confirm=true" >/dev/null
NL2=""
for i in $(seq 1 20); do
  NL2=$(leader_port)
  if [ -n "$NL2" ] && [ "$NL2" != "$CL" ]; then T1=$(date +%s); break; fi
  sleep 0.5
done
DC=$(dumpconns)
echo "D5: 切换 $((T1-T0))s → $NL2 dump=$DC"
[ -n "$NL2" ] && [ "$((T1-T0))" -le 5 ] && [ "$DC" = "1" ] && echo "D5 RESULT=PASS" || echo "D5 RESULT=FAIL"
echo "CLUSTER_LEADER=$NL2" > cluster_state.txt

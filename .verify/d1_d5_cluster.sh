#!/bin/bash
# D1-D5: 双节点竞选/follower 不启管线/kill -9 接管/旧 leader 回归/resign
set -u
cd /mnt/d/workers/worker/qingfly/gocdc-mysql/.verify
SRC="docker exec vf-src mysql -uroot -p123456 -h127.0.0.1 --default-character-set=utf8mb4"
DST="docker exec vf-dst mysql -uroot -p123456 -h127.0.0.1 --default-character-set=utf8mb4"
api() { curl -s --noproxy '*' -u admin:demo123 "localhost:806$1/api/v1$2" ${3:+-X $3}; }
dumpconns() { $SRC -N -e "SELECT COUNT(*) FROM information_schema.processlist WHERE command='Binlog Dump'" 2>/dev/null; }
leader_port() { for p in 8060 8061; do R=$(curl -s --noproxy '*' -u admin:demo123 localhost:$p/api/v1/overview 2>/dev/null; curl -s --noproxy '*' -u admin:demo123 localhost:$p/api/v1/node 2>/dev/null); L=$(curl -s --noproxy '*' -u admin:demo123 localhost:$p/api/v1/status 2>/dev/null | python3 -c 'import sys,json
try: d=json.load(sys.stdin)["data"]; print(d.get("role") or d.get("leader"))
except: print("")' 2>/dev/null); [ "$L" = "leader" ] && { echo $p; return; }; done; echo ""; }

echo "=== 启动 node2（node1 已在跑）"
WEB_PASSWORD=demo123 nohup ./gocdc -config-file config-node2.yml > node2.log 2>&1 &
sleep 8
N2=$(ps aux | grep '[.]gocdc -config-file config-node2' | awk '{print $2}' | head -1)
echo "$N2" > node2.pid
echo "node2 pid=$N2"

echo "=== D1: 双节点竞选——恰一 leader、单 Binlog Dump"
for i in $(seq 1 15); do
  L=$(leader_port)
  [ -n "$L" ] && break
  sleep 2
done
DC=$(dumpconns)
echo "D1: leader 在端口=$L BinlogDump连接数=$DC（期待1）"
[ -n "$L" ] && [ "$DC" = "1" ] && echo "D1 RESULT=PASS" || echo "D1 RESULT=FAIL"

echo "=== D2: follower 不启管线（role/无位点写）"
FP=$([ "$L" = "8060" ] && echo 8061 || echo 8060)
ROLE=$(curl -s --noproxy '*' -u admin:demo123 localhost:$FP/api/v1/status | python3 -c 'import sys,json
try: d=json.load(sys.stdin)["data"]; print(d.get("role") or d.get("leader"))
except: print("?")' 2>/dev/null)
echo "D2: follower($FP) role=$ROLE（期待 follower）"
[ "$ROLE" = "follower" ] && echo "D2 RESULT=PASS" || echo "D2 RESULT=FAIL"

echo "=== D3: kill -9 leader → 接管 ≤TTL+10s（TTL=10s → ≤20s）"
LPID=$([ "$L" = "8060" ] && cat node1.pid || cat node2.pid)
T0=$(date +%s)
kill -9 "$LPID" && echo "D3: kill -9 leader pid=$LPID port=$L"
for i in $(seq 1 30); do
  NL=$(leader_port)
  if [ -n "$NL" ] && [ "$NL" != "$L" ]; then
    T1=$(date +%s); echo "D3: 新 leader=$NL 耗时=$((T1-T0))s"; break
  fi
  sleep 1
done
DC=$(dumpconns)
$([ -n "$NL" ] && [ "$NL" != "$L" ] && [ "$((T1-T0))" -le 20 ] ) && [ "$DC" = "1" ] && echo "D3 RESULT=PASS" || echo "D3 RESULT=FAIL（耗时 $((T1-T0))s dump=$DC）"
# 被杀节点重启回归
if [ "$L" = "8060" ]; then
  WEB_PASSWORD=demo123 nohup ./gocdc -config-file config-node1.yml > node1.log 2>&1 &
  sleep 6; ps aux | grep '[.]gocdc -config-file config-node1' | awk '{print $2}' | head -1 > node1.pid
else
  WEB_PASSWORD=demo123 nohup ./gocdc -config-file config-node2.yml > node2.log 2>&1 &
  sleep 6; ps aux | grep '[.]gocdc -config-file config-node2' | awk '{print $2}' | head -1 > node2.pid
fi
sleep 5

echo "=== D4: 旧 leader 回归降 follower（仍单 Dump）"
sleep 4
DC=$(dumpconns)
CL=$(leader_port)
echo "D4: leader=$CL dump=$DC（期待同端口/1）"
[ "$CL" = "$NL" ] && [ "$DC" = "1" ] && echo "D4 RESULT=PASS" || echo "D4 RESULT=FAIL"

echo "=== D5: resign 优雅让位（0.6~3s 切换）"
LPID=$(ps aux | grep "[.]gocdc -config-file config-node$([ "$CL" = "8060" ] && echo 1 || echo 2)" | awk '{print $2}' | head -1)
T0=$(date +%s)
api "${CL#806}" "/leader/resign?confirm=true" POST >/dev/null
for i in $(seq 1 20); do
  NL2=$(leader_port)
  if [ -n "$NL2" ] && [ "$NL2" != "$CL" ]; then T1=$(date +%s); break; fi
  sleep 0.5
done
echo "D5: resign 切换耗时=$((T1-T0))s 新leader=$NL2"
DC=$(dumpconns)
[ -n "$NL2" ] && [ "$((T1-T0))" -le 3 ] && [ "$DC" = "1" ] && echo "D5 RESULT=PASS" || echo "D5 RESULT=FAIL"
echo "当前 leader=$NL2（后续脚本以此为准）" > cluster_state.txt
cat cluster_state.txt

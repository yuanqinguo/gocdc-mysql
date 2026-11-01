#!/bin/bash
# D 系列干净重跑：双节点从零启动 → D2/D3/D4/D5（含 resign 后双进程存活断言）
set -u
cd /mnt/d/workers/worker/qingfly/gocdc-mysql/.verify
SRC="docker exec vf-src mysql -uroot -p123456 -h127.0.0.1 --default-character-set=utf8mb4"
DST="docker exec vf-dst mysql -uroot -p123456 -h127.0.0.1 --default-character-set=utf8mb4"
dumpconns() { $SRC -N -e "SELECT COUNT(*) FROM information_schema.processlist WHERE command LIKE 'Binlog Dump%'" 2>/dev/null; }
roleof() { curl -s --noproxy '*' -u admin:demo123 "localhost:$1/api/v1/status" | python3 -c 'import sys,json
try: print(json.load(sys.stdin)["data"]["role"])
except: print("?")' 2>/dev/null; }
leader_port() { for p in 8060 8061; do [ "$(roleof $p)" = "leader" ] && { echo $p; return; }; done; echo ""; }
pickpid() { ps -eo pid,args | grep "config-node$1.yml" | grep -v grep | awk '{print $1}' | head -1; }

echo "=== 清场：杀全部 gocdc"
for p in $(ps -eo pid,args | grep '[g]ocdc' | awk '{print $1}'); do kill -9 "$p" 2>/dev/null; done
sleep 2
ps -eo pid,args | grep '[g]ocdc' | wc -l

echo "=== 起 node1 → 等 leader → 起 node2"
WEB_PASSWORD=demo123 nohup ./gocdc -config-file config-node1.yml > node1.log 2>&1 &
sleep 8
WEB_PASSWORD=demo123 nohup ./gocdc -config-file config-node2.yml > node2.log 2>&1 &
sleep 8
echo "node1=$(pickpid 1) node2=$(pickpid 2)"
for i in $(seq 1 10); do L=$(leader_port); [ -n "$L" ] && break; sleep 2; done
R1=$(roleof 8060); R2=$(roleof 8061)
echo "D1复验: node1=$R1 node2=$R2 leader=$L dump=$(dumpconns)"
[ -n "$L" ] && [ "$(dumpconns)" = "1" ] && echo "D1 RESULT=PASS" || echo "D1 RESULT=FAIL"

echo "=== D2: follower 三断言"
FP=$([ "$L" = "8060" ] && echo 8061 || echo 8060)
FR=$(roleof $FP)
FPORT=$([ "$FP" = "8060" ] && echo 9595 || echo 9596)
FREADY=$(curl -s --noproxy '*' localhost:$FPORT/readyz)
FLEAD=$(curl -s --noproxy '*' localhost:$FPORT/metrics | grep -c '^gocdc_leader_state 1')
echo "D2: role=$FR readyz=[$FREADY] leader_state1=$FLEAD"
[ "$FR" = "follower" ] && [ "$FLEAD" = "0" ] && echo "D2 RESULT=PASS" || echo "D2 RESULT=FAIL"

echo "=== D3: kill -9 leader → ≤20s 接管 + 窗口数据收敛"
$SRC -e "INSERT INTO shop.new_table (val) VALUES ('d3-w1')" 2>/dev/null
LPID=$(pickpid $([ "$L" = "8060" ] && echo 1 || echo 2))
T0=$(date +%s)
kill -9 "$LPID"
$SRC -e "INSERT INTO shop.new_table (val) VALUES ('d3-w2')" 2>/dev/null
NL=""
for i in $(seq 1 25); do
  NL=$(leader_port)
  [ -n "$NL" ] && [ "$NL" != "$L" ] && { T1=$(date +%s); break; }
  sleep 1
done
DC=$(dumpconns); sleep 8
W1=$($DST -N -e "SELECT COUNT(*) FROM shop.new_table WHERE val='d3-w1'" 2>/dev/null)
W2=$($DST -N -e "SELECT COUNT(*) FROM shop.new_table WHERE val='d3-w2'" 2>/dev/null)
echo "D3: 接管=$((T1-T0))s 新leader=$NL dump=$DC 窗口 w1=$W1 w2=$W2"
if [ -n "${T1:-}" ] && [ -n "$NL" ] && [ "$((T1-T0))" -le 20 ] && [ "$DC" = "1" ] && [ "$W1" = "1" ] && [ "$W2" = "1" ]; then
  echo "D3 RESULT=PASS"
else
  echo "D3 RESULT=FAIL"
fi

echo "=== 被杀节点回归 + D4 复验"
if [ "$L" = "8060" ]; then
  WEB_PASSWORD=demo123 setsid nohup ./gocdc -config-file config-node1.yml > node1.log 2>&1 &
else
  WEB_PASSWORD=demo123 setsid nohup ./gocdc -config-file config-node2.yml > node2.log 2>&1 &
fi
sleep 10
R1=$(roleof 8060); R2=$(roleof 8061)
CL=$(leader_port)
echo "D4: node1=$R1 node2=$R2 leader=$CL dump=$(dumpconns)"
[ "$CL" = "$NL" ] && [ "$(dumpconns)" = "1" ] && echo "D4 RESULT=PASS" || echo "D4 RESULT=FAIL"

echo "=== D5: resign 优雅让位 + **双进程必须都活着**（上轮疑云断言）"
LPID=$(pickpid $([ "$CL" = "8060" ] && echo 1 || echo 2))
T0=$(date +%s)
curl -s --noproxy '*' -u admin:demo123 -X POST "localhost:$CL/api/v1/leader/resign?confirm=true" >/dev/null
NL2=""
for i in $(seq 1 20); do
  NL2=$(leader_port)
  [ -n "$NL2" ] && [ "$NL2" != "$CL" ] && { T1=$(date +%s); break; }
  sleep 0.5
done
sleep 3
ALIVE1=$(pickpid 1); ALIVE2=$(pickpid 2)
DC=$(dumpconns)
echo "D5: 切换=$((T1-T0))s 新leader=$NL2 dump=$DC node1存活=$ALIVE1 node2存活=$ALIVE2"
if [ -n "$NL2" ] && [ "$((T1-T0))" -le 5 ] && [ "$DC" = "1" ] && [ -n "$ALIVE1" ] && [ -n "$ALIVE2" ]; then
  echo "D5 RESULT=PASS"
else
  echo "D5 RESULT=FAIL"
fi

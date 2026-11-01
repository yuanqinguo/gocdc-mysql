#!/bin/bash
# D2+D3 补跑：follower 不启管线 / kill -9 leader 接管计时
set -u
cd /mnt/d/workers/worker/qingfly/gocdc-mysql/.verify
SRC="docker exec vf-src mysql -uroot -p123456 -h127.0.0.1 --default-character-set=utf8mb4"
DST="docker exec vf-dst mysql -uroot -p123456 -h127.0.0.1 --default-character-set=utf8mb4"
dumpconns() { $SRC -N -e "SELECT COUNT(*) FROM information_schema.processlist WHERE command LIKE 'Binlog Dump%'" 2>/dev/null; }
roleof() { curl -s --noproxy '*' -u admin:demo123 "localhost:$1/api/v1/status" | python3 -c 'import sys,json
try: print(json.load(sys.stdin)["data"]["role"])
except: print("?")' 2>/dev/null; }
leader_port() { for p in 8060 8061; do [ "$(roleof $p)" = "leader" ] && { echo $p; return; }; done; echo ""; }
pickpid() { ps -eo pid,args | grep "[.]gocdc -config-file config-node$1" | grep -v grep | awk '{print $1}' | head -1; }

L=$(leader_port)
echo "当前 leader=$L（node$([ "$L" = "8060" ] && echo 1 || echo 2)）"
FP=$([ "$L" = "8060" ] && echo 8061 || echo 8060)
FR=$(roleof $FP)
FPORT=$([ "$FP" = "8060" ] && echo 9595 || echo 9596)
FREADY=$(curl -s --noproxy '*' localhost:$FPORT/readyz)
FPAUSED=$(curl -s --noproxy '*' localhost:$FPORT/metrics | grep -c '^gocdc_leader_state 1')
echo "D2: follower=$FP role=$FR readyz=[$FREADY] leader指标=$FPAUSED（期待 follower/not ready/0）"
[ "$FR" = "follower" ] && echo "D2 RESULT=PASS" || echo "D2 RESULT=FAIL"

echo "=== D3: kill -9 leader → 接管 ≤20s + 窗口数据收敛"
$SRC -e "INSERT INTO shop.new_table (val) VALUES ('d3-window-1')" 2>/dev/null
LPID=$(pickpid $([ "$L" = "8060" ] && echo 1 || echo 2))
T0=$(date +%s)
kill -9 "$LPID" && echo "killed leader pid=$LPID"
$SRC -e "INSERT INTO shop.new_table (val) VALUES ('d3-window-2')" 2>/dev/null
NL=""
for i in $(seq 1 25); do
  NL=$(leader_port)
  if [ -n "$NL" ] && [ "$NL" != "$L" ]; then T1=$(date +%s); break; fi
  sleep 1
done
DC=$(dumpconns)
sleep 6
C1=$($DST -N -e "SELECT COUNT(*) FROM shop.new_table WHERE val='d3-window-1'" 2>/dev/null)
C2=$($DST -N -e "SELECT COUNT(*) FROM shop.new_table WHERE val='d3-window-2'" 2>/dev/null)
echo "D3: 接管=$((T1-T0))s 新leader=$NL dump=$DC 窗口数据 w1=$C1 w2=$C2（期待1/1）"
if [ -n "$NL" ] && [ "$((T1-T0))" -le 20 ] && [ "$DC" = "1" ] && [ "$C1" = "1" ] && [ "$C2" = "1" ]; then
  echo "D3 RESULT=PASS"
else
  echo "D3 RESULT=FAIL"
fi

echo "=== 被杀 leader 重启回归（D4 复验）"
if [ "$L" = "8060" ]; then
  WEB_PASSWORD=demo123 nohup ./gocdc -config-file config-node1.yml > node1.log 2>&1 &
else
  WEB_PASSWORD=demo123 nohup ./gocdc -config-file config-node2.yml > node2.log 2>&1 &
fi
sleep 7
sleep 5
CL=$(leader_port)
DC=$(dumpconns)
R1=$(roleof 8060); R2=$(roleof 8061)
echo "D4复验: node1=$R1 node2=$R2 leader=$CL dump=$DC"
[ "$CL" = "$NL" ] && [ "$DC" = "1" ] && echo "D4 RESULT=PASS" || echo "D4 RESULT=FAIL"
pickpid 1 > node1.pid; pickpid 2 > node2.pid

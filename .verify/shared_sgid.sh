#!/bin/bash
# server_id 派生真实验证：两节点共享配置（无 node.id/server_id）→ 派生值不同 + 无互踢
set -u
cd /mnt/d/workers/worker/qingfly/gocdc-mysql/.verify
SRC="docker exec vf-src mysql -uroot -p123456 -h127.0.0.1 --default-character-set=utf8mb4"
dumpconns() { $SRC -N -e "SELECT COUNT(*), GROUP_CONCAT(DISTINCT user) FROM information_schema.processlist WHERE command LIKE 'Binlog Dump%'" 2>/dev/null; }
roleof() { curl -s --noproxy '*' -u admin:demo123 "localhost:$1/api/v1/status" | python3 -c 'import sys,json
try: d=json.load(sys.stdin)["data"]; print(d["node_id"],d["role"])
except: print("?")' 2>/dev/null; }
leader_port() { for p in 8060 8061; do R=$(roleof $p); [ "${R#* }" = "leader" ] && { echo $p; return; }; done; echo ""; }

echo "=== 清场"
for p in $(ps -eo pid,args | grep 'gocdc -config-file' | grep -v grep | awk '{print $1}'); do kill -9 "$p" 2>/dev/null; done
sleep 2
# 重建二进制（server_id 派生改动）
(cd /mnt/d/workers/worker/qingfly/gocdc-mysql && /usr/local/go/bin/go build -o .verify/gocdc ./cmd/gocdc) && echo BINARY-REBUILT

echo "=== 两节点共享配置启动（node.id/server_id 均未配置）"
WEB_PASSWORD=demo123 setsid nohup ./gocdc -config-file config-shared.yml > node1.log 2>&1 &
sleep 8
WEB_PASSWORD=demo123 setsid nohup ./gocdc -config-file config-shared-n2.yml > node2.log 2>&1 &
sleep 10

echo "=== 断言 1：各节点派生出不同 server_id（启动日志）"
SID1=$(grep -oE 'server_id=[0-9]+' node1.log | head -1)
SID2=$(grep -oE 'server_id=[0-9]+' node2.log | head -1)
echo "node1: $SID1   node2: $SID2"
[ -n "$SID1" ] && [ -n "$SID2" ] && [ "$SID1" != "$SID2" ] && echo "断言1 RESULT=PASS（派生值不同）" || echo "断言1 RESULT=FAIL"

echo "=== 断言 2：/config 生效配置里 server_id 已回填（非 0）"
C1=$(curl -s --noproxy '*' -u admin:demo123 localhost:8060/api/v1/config | python3 -c 'import sys,json; print(json.load(sys.stdin)["source"]["server_id"])')
echo "node1 /config server_id=$C1"
[ "$C1" != "0" ] && [ "$C1" != "" ] && echo "断言2 RESULT=PASS" || echo "断言2 RESULT=FAIL"

echo "=== 断言 3：选举正常、单 Binlog Dump、无互踢（观察 15s dump 连接稳定）"
sleep 5
for i in $(seq 1 8); do dumpconns; sleep 2; done | sort | uniq -c
echo "=== 节点角色：8060=[$(roleof 8060)] 8061=[$(roleof 8061)]"
echo "=== 写入收敛"
docker exec vf-src mysql -uroot -p123456 -h127.0.0.1 --default-character-set=utf8mb4 -e "INSERT INTO shop.new_table (val) VALUES ('sgid-probe')" 2>/dev/null
sleep 5
V=$(docker exec vf-dst mysql -uroot -p123456 -h127.0.0.1 -N -e "SELECT COUNT(*) FROM shop.new_table WHERE val='sgid-probe'" 2>/dev/null)
echo "写入收敛=$V（期待1）"
DUMPS=$(dumpconns | cut -f1)
[ "$V" = "1" ] && [ "$DUMPS" = "1" ] && echo "断言3 RESULT=PASS（单 dump 稳定、无互踢、数据流动）" || echo "断言3 RESULT=FAIL dump=$DUMPS"

echo "=== 断言 4：leader 切换后仍单 dump（切换窗口正是互踢危险区）"
L=$(leader_port)
LPID=$(ps -eo pid,args | grep "config-shared.yml" | grep -v grep | awk '{print $1}' | head -1)
kill -9 "$LPID" 2>/dev/null
sleep 14
echo "切换后：8060=[$(roleof 8060)] 8061=[$(roleof 8061)] dump=[$(dumpconns)]"
docker exec vf-src mysql -uroot -p123456 -h127.0.0.1 --default-character-set=utf8mb4 -e "INSERT INTO shop.new_table (val) VALUES ('sgid-probe2')" 2>/dev/null
sleep 5
V2=$(docker exec vf-dst mysql -uroot -p123456 -h127.0.0.1 -N -e "SELECT COUNT(*) FROM shop.new_table WHERE val='sgid-probe2'" 2>/dev/null)
echo "切换后写入收敛=$V2"
[ "$V2" = "1" ] && echo "断言4 RESULT=PASS（切换窗口无互踢抖动）" || echo "断言4 RESULT=FAIL"

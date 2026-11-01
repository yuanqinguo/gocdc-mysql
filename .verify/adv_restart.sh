#!/bin/bash
# ADV-09 TRUNCATE → pause 审批；ADV-01 无主键表双层拦截（启动核对 fail-fast）
set -u
cd /mnt/d/workers/worker/qingfly/gocdc-mysql/.verify
SRC="docker exec vf-src mysql -uroot -p123456 -h127.0.0.1 --default-character-set=utf8mb4"
DST="docker exec vf-dst mysql -uroot -p123456 -h127.0.0.1 --default-character-set=utf8mb4"
api() { curl -s --noproxy '*' -u admin:demo123 "localhost:8060/api/v1$1" ${2:+-X $2}; }
pending_id() { api /ddl-events | python3 -c 'import sys,json; evs=json.load(sys.stdin)["data"] or []; p=[e["id"] for e in evs if e["status"]=="pending"]; print(p[0] if p else "")'; }
start_node() { WEB_PASSWORD=demo123 nohup ./gocdc -config-file config-node1.yml > node1.log 2>&1 & sleep 4; ps aux | grep '[.]/gocdc' | awk '{print $2}' | head -1 > node1.pid; }

echo "=== ADV-09: TRUNCATE shop.new_table → 白名单外 pause"
$SRC -e "TRUNCATE TABLE shop.new_table" 2>/dev/null
sleep 3
EVID=$(pending_id)
echo "ADV-09 挂起: $EVID"
[ -z "$EVID" ] && { echo "ADV-09 RESULT=FAIL（未挂起）"; exit 1; }
api "/ddl/$EVID/skip?confirm=true" POST >/dev/null
sleep 4
$SRC -e "INSERT INTO shop.new_table (val) VALUES ('adv09-after')" 2>/dev/null
sleep 4
V=$($DST -N -e "SELECT COUNT(*) FROM shop.new_table WHERE val='adv09-after'" 2>/dev/null)
ALIVE=$(ps aux | grep '[.]/gocdc' | grep -v grep | wc -l)
echo "ADV-09: skip 后恢复流动=$V 进程存活=$ALIVE"
[ "$V" = "1" ] && [ "$ALIVE" = "1" ] && echo "ADV-09 RESULT=PASS" || echo "ADV-09 RESULT=FAIL"

echo "=== ADV-01: 无主键表双层拦截"
$SRC -e "CREATE TABLE shop.noltbl (v INT NULL)" 2>/dev/null
sleep 3
EVID=$(pending_id)
[ -n "$EVID" ] && api "/ddl/$EVID/skip?confirm=true" POST >/dev/null && echo "ADV-01 CREATE 挂起已 skip（目标不建表）"
sleep 2
# 停机 → 重启 → 启动核对应拒（双层：目标不存在 + 无主键无法寻址）
PID=$(cat node1.pid); kill -TERM "$PID"; sleep 3
start_node
OUT=$(grep -E 'noltbl|无主键|主键' node1.log | head -3)
EXITCHECK=$(ps aux | grep '[.]/gocdc' | grep -v grep | wc -l)
echo "ADV-01 重启后核对输出:"; echo "$OUT"
echo "ADV-01 重启后进程存活=$EXITCHECK（期待0=fatal 拒启）"
if [ "$EXITCHECK" = "0" ]; then
  echo "ADV-01 第一层 RESULT=PASS（拒启）"
else
  echo "ADV-01 第一层 RESULT=FAIL（竟然启动了）"
fi
# 清理：删掉源端无主键表 → 恢复启动
$SRC -e "DROP TABLE shop.noltbl" 2>/dev/null
start_node
sleep 3
ALIVE=$(ps aux | grep '[.]/gocdc' | grep -v grep | wc -l)
echo "ADV-01 清理后恢复启动存活=$ALIVE"
$SRC -e "INSERT INTO shop.new_table (val) VALUES ('adv01-recovered')" 2>/dev/null
sleep 4
V=$($DST -N -e "SELECT COUNT(*) FROM shop.new_table WHERE val='adv01-recovered'" 2>/dev/null)
echo "ADV-01 恢复后流动=$V"
[ "$V" = "1" ] && echo "ADV-01 RESULT=PASS（双层拦截+恢复）" || echo "ADV-01 RESULT=FAIL"

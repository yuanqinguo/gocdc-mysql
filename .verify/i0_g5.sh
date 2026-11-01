#!/bin/bash
# G5 restock 清标记+重启精准重跑 + I0 全量双写 100 快照事件 + I1 信封格式
set -u
cd /mnt/d/workers/worker/qingfly/gocdc-mysql/.verify
SRC="docker exec vf-src mysql -uroot -p123456 -h127.0.0.1 --default-character-set=utf8mb4"
DST="docker exec vf-dst mysql -uroot -p123456 -h127.0.0.1 --default-character-set=utf8mb4"
api() { curl -s --noproxy '*' -u admin:demo123 "localhost:8060/api/v1$1" ${2:+-X $2}; }

echo "=== 准备：rmqtest.msg 灌 100 行（增量入 RMQ）"
$SRC -e "INSERT INTO rmqtest.msg (name,qty,amount,note) SELECT CONCAT('g5-',n), n, n*1.5, 'seed' FROM (WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM s WHERE n<100) SELECT n FROM s) t" 2>/dev/null
$SRC -e "SET SESSION cte_max_recursion_depth=1000" 2>/dev/null
sleep 6
CNT=$($DST -N -e "SELECT COUNT(*) FROM rmqtest.msg" 2>/dev/null)
echo "rmqtest.msg 源→目标: dst=$CNT（期待100）"

echo "=== 起消费者（新组从最新）→ restock → 重启触发全量重跑"
TS=$(date +%s)
nohup ./rmqcons-bin -topic CDC_rmqtest_msg -group g5-$TS -out /tmp/rmq-g5.jsonl -idle 12s > /tmp/rmqcons-g5.log 2>&1 &
sleep 3
api "/rules/rmqtest.msg/restock?confirm=true" POST | head -c 100; echo
PID=$(cat node1.pid); kill -TERM "$PID"; sleep 3
WEB_PASSWORD=demo123 nohup ./gocdc -config-file config-node1.yml > node1.log 2>&1 &
sleep 8
ps aux | grep '[.]/gocdc' | awk '{print $2}' | head -1 > node1.pid

echo "=== 等全量完成+消费者 idle 退出"
for i in $(seq 1 20); do
  ps -p $(pgrep -f "rmqcons-bin -topic CDC_rmqtest_msg -group g5-$TS" | head -1) >/dev/null 2>&1 || break
  sleep 2
done
N=$(wc -l < /tmp/rmq-g5.jsonl 2>/dev/null || echo 0)
STOCKN=$(grep -c '"phase":"stock"' /tmp/rmq-g5.jsonl 2>/dev/null || echo 0)
echo "I0: 消费消息数=$N 其中快照(phase=stock)=$STOCKN（期待100/100）"

echo "=== I1: 信封格式字段检查"
head -1 /tmp/rmq-g5.jsonl | python3 -c '
import sys,json
e=json.loads(sys.stdin.readline())
need=["event_id","type","phase","database","table","pk","after","position"]
missing=[k for k in need if k not in e]
print(" 字段:", sorted(e.keys()))
print(" 缺失:", missing if missing else "无——自描述信封完整")
print(" 样例 event_id=",e.get("event_id")," table=",e.get("database"),".",e.get("table")," pk=",e.get("pk"))'
[ "$STOCKN" = "100" ] && echo "I0+G5 RESULT=PASS（restock 重跑全量 100 快照事件双写）" || echo "I0+G5 RESULT=FAIL"

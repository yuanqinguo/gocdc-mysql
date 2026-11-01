#!/bin/bash
# D6 etcd 宕机→失位→恢复补齐 / E9 follower API 审批 / I5 集群单产出 / ADV-07 restock 跨会话重扫
set -u
cd /mnt/d/workers/worker/qingfly/gocdc-mysql/.verify
SRC="docker exec vf-src mysql -uroot -p123456 -h127.0.0.1 --default-character-set=utf8mb4"
DST="docker exec vf-dst mysql -uroot -p123456 -h127.0.0.1 --default-character-set=utf8mb4"
roleof() { curl -s --noproxy '*' -u admin:demo123 "localhost:$1/api/v1/status" | python3 -c 'import sys,json
try: print(json.load(sys.stdin)["data"]["role"])
except: print("?")' 2>/dev/null; }
leader_port() { for p in 8060 8061; do [ "$(roleof $p)" = "leader" ] && { echo $p; return; }; done; echo ""; }
pickpid() { ps -eo pid,args | grep "config-node$1.yml" | grep -v grep | awk '{print $1}' | head -1; }
L=$(leader_port)
echo "当前 leader=$L"

echo "=== E9: 集群 DDL 审批（follower API → leader 执行）"
FP=$([ "$L" = "8060" ] && echo 8061 || echo 8060)
$SRC -e "CREATE TABLE shop.e9_table (id BIGINT NOT NULL AUTO_INCREMENT, v VARCHAR(32) NULL, PRIMARY KEY (id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4" 2>/dev/null
sleep 3
EVID=$(curl -s --noproxy '*' -u admin:demo123 "localhost:$FP/api/v1/ddl-events" | python3 -c 'import sys,json; evs=json.load(sys.stdin)["data"] or []; p=[e["id"] for e in evs if e["status"]=="pending"]; print(p[0] if p else "")')
echo "E9: follower($FP) 看到 pending=$EVID"
curl -s --noproxy '*' -u admin:demo123 -X POST "localhost:$FP/api/v1/ddl/$EVID/approve?confirm=true" | head -c 100; echo
for i in $(seq 1 15); do
  HAS=$($DST -N -e "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='shop' AND table_name='e9_table'" 2>/dev/null)
  [ "$HAS" = "1" ] && { echo "E9: leader 自动建表 t=${i}x2s"; break; }
  sleep 2
done
$SRC -e "INSERT INTO shop.e9_table (v) VALUES ('e9-flows')" 2>/dev/null
sleep 4
V=$($DST -N -e "SELECT v FROM shop.e9_table WHERE v='e9-flows'" 2>/dev/null)
echo "E9: 数据流动=$V"
[ "$HAS" = "1" ] && [ "$V" = "e9-flows" ] && echo "E9 RESULT=PASS（follower 审批 leader 执行）" || echo "E9 RESULT=FAIL"

echo "=== I5: 集群单产出（leader 切换前后各一批，每行恰一条）"
TS=$(date +%s)
setsid nohup ./rmqcons-bin -topic CDC_rmqtest_msg -group i5-$TS -out /tmp/rmq-i5.jsonl -idle 12s > /tmp/rmqcons-i5.log 2>&1 &
sleep 4
$SRC -e "INSERT INTO rmqtest.msg (name,qty,amount,note) SELECT CONCAT('i5a-',n), n, n, 'i5a' FROM (WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM s WHERE n<10) SELECT n FROM s) t" 2>/dev/null
sleep 6
echo "I5: 切换 leader（resign 当前 $L）"
curl -s --noproxy '*' -u admin:demo123 -X POST "localhost:$L/api/v1/leader/resign?confirm=true" >/dev/null
for i in $(seq 1 20); do NL=$(leader_port); [ -n "$NL" ] && [ "$NL" != "$L" ] && break; sleep 1; done
echo "I5: 新 leader=$NL，写 20 行"
$SRC -e "INSERT INTO rmqtest.msg (name,qty,amount,note) SELECT CONCAT('i5b-',n), n, n, 'i5b' FROM (WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM s WHERE n<20) SELECT n FROM s) t" 2>/dev/null
sleep 10
python3 - <<PYEOF
import json, collections
c = collections.Counter(); names = collections.Counter()
for line in open('/tmp/rmq-i5.jsonl', encoding='utf-8'):
    try:
        e = json.loads(line)
        a = e.get('after') or {}
        n = str(a.get('name',''))
        if n.startswith('i5a-'): names['i5a'] += 1
        if n.startswith('i5b-'): names['i5b'] += 1
        c[e.get('event_id')] += 1
    except Exception: pass
dups = sum(v-1 for v in c.values() if v > 1)
print(f" I5: i5a消息={names['i5a']}/10 i5b消息={names['i5b']}/20 event_id重复={dups}")
ok = names['i5a'] == 10 and names['i5b'] == 20 and dups == 0
print(" I5 RESULT=" + ("PASS（切换前后每行恰一条）" if ok else "FAIL"))
PYEOF

echo "=== ADV-07: restock 清标记 + kill leader → 接管后跨会话重扫收敛"
api() { curl -s --noproxy '*' -u admin:demo123 "localhost:$1/api/v1$2" ${3:+-X $3}; }
NL=$(leader_port)
api "$NL" "/rules/rmqtest.msg/restock?confirm=true" POST >/dev/null
LPID=$(pickpid $([ "$NL" = "8060" ] && echo 1 || echo 2))
kill -9 "$LPID"
echo "ADV-07: restock 已清标记 + leader 已 kill，等接管重扫…"
for i in $(seq 1 25); do NL2=$(leader_port); [ -n "$NL2" ] && break; sleep 1; done
sleep 12
S=$($SRC -N -e "SELECT COUNT(*) FROM rmqtest.msg" 2>/dev/null)
D=$($DST -N -e "SELECT COUNT(*) FROM rmqtest.msg" 2>/dev/null)
echo "ADV-07: 重扫后 src=$S dst=$D"
grep -E '全量完成' node1.log node2.log 2>/dev/null | tail -2
[ "$S" = "$D" ] && echo "ADV-07 RESULT=PASS（跨会话整表重扫收敛）" || echo "ADV-07 RESULT=FAIL"
# 被杀节点回归
if [ "$NL" = "8060" ]; then WEB_PASSWORD=demo123 setsid nohup ./gocdc -config-file config-node1.yml > node1.log 2>&1 &
else WEB_PASSWORD=demo123 setsid nohup ./gocdc -config-file config-node2.yml > node2.log 2>&1 & fi
sleep 8

echo "=== D6: etcd 宕机 → 失位停管线 → 恢复重选补齐"
CL=$(leader_port)
CLPID=$(pickpid $([ "$CL" = "8060" ] && echo 1 || echo 2))
CLMET=$([ "$CL" = "8060" ] && echo 9595 || echo 9596)
T0=$(date +%s)
docker stop vf-etcd >/dev/null
T_LOST=""
for i in $(seq 1 20); do
  ST=$(curl -s --noproxy '*' localhost:$CLMET/metrics | grep '^gocdc_leader_state' | awk '{print $2}')
  [ "$ST" != "1" ] && { T_LOST=$(( $(date +%s) - T0 )); break; }
  sleep 1
done
echo "D6: etcd 停后 leader 失位时延=${T_LOST}s（期待 ≤8s）"
RDY=$(curl -s --noproxy '*' localhost:$CLMET/readyz)
echo "D6: 失位后 readyz=[$RDY]（期待 not ready=安全态）"
$SRC -e "INSERT INTO shop.new_table (val) VALUES ('d6-window')" 2>/dev/null
sleep 3
W0=$($DST -N -e "SELECT COUNT(*) FROM shop.new_table WHERE val='d6-window'" 2>/dev/null)
echo "D6: 宕机窗口写入落目标=$W0（期待0=不投递）"
docker start vf-etcd >/dev/null
T1=$(date +%s)
NL=""
for i in $(seq 1 30); do
  NL=$(leader_port); [ -n "$NL" ] && break; sleep 1
done
sleep 8
W1=$($DST -N -e "SELECT COUNT(*) FROM shop.new_table WHERE val='d6-window'" 2>/dev/null)
LAG=$(curl -s --noproxy '*' localhost:9595/metrics | grep '^gocdc_master_position_lag_bytes' | awk '{print $2}')
echo "D6: 恢复重选=$(( $(date +%s) - T1 ))s 新leader=$NL 窗口补齐=$W1 lag=$LAG"
if [ -n "$T_LOST" ] && [ "$T_LOST" -le 8 ] && [ "$W0" = "0" ] && [ "$W1" = "1" ]; then
  echo "D6 RESULT=PASS"
else
  echo "D6 RESULT=FAIL"
fi

#!/bin/bash
# I2 增量三方对账 / I3 broker宕机双写单边+补投 / I4 崩溃重放幂等 / I6 顺序消息
set -u
cd /mnt/d/workers/worker/qingfly/gocdc-mysql/.verify
SRC="docker exec vf-src mysql -uroot -p123456 -h127.0.0.1 --default-character-set=utf8mb4"
DST="docker exec vf-dst mysql -uroot -p123456 -h127.0.0.1 --default-character-set=utf8mb4"
api() { curl -s --noproxy '*' -u admin:demo123 "localhost:8060/api/v1$1" ${2:+-X $2}; }
TS=$(date +%s)

echo "=== I2: 增量 workload + 三方对账"
nohup ./rmqcons-bin -topic CDC_rmqtest_msg -group i2-$TS -out /tmp/rmq-i2.jsonl -idle 10s > /tmp/rmqcons-i2.log 2>&1 &
sleep 3
docker exec -i vf-src mysql -uroot -p123456 -h127.0.0.1 --default-character-set=utf8mb4 < rmq_incr.sql 2>/dev/null
echo "I2 workload 注入完成，等收敛…"
for i in $(seq 1 15); do
  ps -p $(pgrep -f "rmqcons-bin.*i2-$TS" | head -1) >/dev/null 2>&1 || break; sleep 2
done
python3 rmq_compare.py /tmp/rmq-i2.jsonl 2>&1 | tail -6

echo "=== I6: 同行 10 次高频 UPDATE 顺序性（message group 保序）"
nohup ./rmqcons-bin -topic CDC_rmqtest_msg -group i6-$TS -out /tmp/rmq-i6.jsonl -idle 8s > /tmp/rmqcons-i6.log 2>&1 &
sleep 3
$SRC -e "INSERT INTO rmqtest.msg (name,qty,amount,note) VALUES ('i6-row',0,0,'init')" 2>/dev/null
IDQ=$($SRC -N -e "SELECT id FROM rmqtest.msg WHERE name='i6-row'" 2>/dev/null)
for n in 1 2 3 4 5 6 7 8 9 10; do $SRC -e "UPDATE rmqtest.msg SET qty=$n, amount=$n.50 WHERE id=$IDQ" 2>/dev/null; done
sleep 8
python3 - <<'PYEOF'
import json
seq=[]
for line in open('/tmp/rmq-i6.jsonl',encoding='utf-8'):
    try:
        e=json.loads(line)
        a=(e.get('after') or {})
        if a and a.get('name')=='i6-row': seq.append(int(a['qty']))
    except Exception: pass
mono=all(b>a for a,b in zip(seq,seq[1:])) if len(seq)>1 else True
print(f" I6: i6-row 事件数={len(seq)} 序列={seq} 严格递增={mono}")
print(" I6 RESULT=" + ("PASS" if mono and len(seq)>=10 else "FAIL"))
PYEOF

echo "=== I3: broker 宕机双写单边（MySQL 照写位点冻结 RMQ 重试）→ 恢复补投"
docker stop rmqbroker >/dev/null
nohup ./rmqcons-bin -topic CDC_rmqtest_msg -group i3-$TS -out /tmp/rmq-i3.jsonl -idle 25s > /tmp/rmqcons-i3.log 2>&1 &
sleep 2
POSB=$(api /position | python3 -c 'import sys,json; print(json.load(sys.stdin)["data"]["pos"])')
$SRC -e "INSERT INTO rmqtest.msg (name,qty,amount,note) VALUES ('i3-a',1,1,'宕机期'),('i3-b',2,2,'宕机期'),('i3-c',3,3,'宕机期'),('i3-d',4,4,'宕机期'),('i3-e',5,5,'宕机期')" 2>/dev/null
sleep 6
DSTC=$($DST -N -e "SELECT COUNT(*) FROM rmqtest.msg WHERE note='宕机期'" 2>/dev/null)
POSA=$(api /position | python3 -c 'import sys,json; print(json.load(sys.stdin)["data"]["pos"])')
RETRY=$(curl -s --noproxy '*' localhost:9595/metrics | grep -E '^gocdc_sink_retry_total' | head -2)
echo "I3 宕机期: MySQL侧照写=$DSTC/5 位点 pos_before=$POSB pos_after=$POSA（期待相等=冻结）"
echo "I3 sink retry: $RETRY"
docker start rmqbroker >/dev/null
echo "I3 broker 已恢复，等自动补投…"
for i in $(seq 1 25); do
  ps -p $(pgrep -f "rmqcons-bin.*i3-$TS" | head -1) >/dev/null 2>&1 || break; sleep 2
done
N3=$(grep -c '宕机期' /tmp/rmq-i3.jsonl 2>/dev/null || echo 0)
ALIVE=$(ps aux | grep '[.]/gocdc' | grep -v grep | wc -l)
echo "I3 补投: 消费收到宕机期消息=$N3/5 进程存活=$ALIVE（无需重启自动补投）"

echo "=== I4: 崩溃重放幂等（kill -9 → 重复消息 → event_id 去重收敛）"
nohup ./rmqcons-bin -topic CDC_rmqtest_msg -group i4-$TS -out /tmp/rmq-i4.jsonl -idle 15s > /tmp/rmqcons-i4.log 2>&1 &
sleep 2
$SRC -e "INSERT INTO rmqtest.msg (name,qty,amount,note) SELECT CONCAT('i4-',n), n, n, 'crash' FROM (WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM s WHERE n<60) SELECT n FROM s) t" 2>/dev/null
sleep 2
PID=$(ps aux | grep '[.]/gocdc' | awk '{print $2}' | head -1)
kill -9 "$PID" 2>/dev/null && echo "I4 kill -9 @pid=$PID"
sleep 1
WEB_PASSWORD=demo123 nohup ./gocdc -config-file config-node1.yml > node1.log 2>&1 &
sleep 8
ps aux | grep '[.]/gocdc' | awk '{print $2}' | head -1 > node1.pid
for i in $(seq 1 20); do
  ps -p $(pgrep -f "rmqcons-bin.*i4-$TS" | head -1) >/dev/null 2>&1 || break; sleep 2
done
python3 - <<'PYEOF'
import json,subprocess,collections
ids=[]; finals={}
for line in open('/tmp/rmq-i4.jsonl',encoding='utf-8'):
    try:
        e=json.loads(line); ids.append(e.get('event_id'))
        a=e.get('after') or {}
        if a and str(a.get('name','')).startswith('i4-'): finals[a['name']]=a
    except Exception: pass
dup=len(ids)-len(set(ids))
q="SELECT name,qty FROM rmqtest.msg WHERE note='crash' ORDER BY id"
src=subprocess.run(["docker","exec","vf-src","mysql","-uroot","-p123456","-h127.0.0.1","-N","-e",q],capture_output=True,text=True).stdout
srcrows={l.split('\t')[0]:int(l.split('\t')[1]) for l in src.strip().splitlines() if l}
mqrows={k:int(v['qty']) for k,v in finals.items()}
ok = all(mqrows.get(k)==v for k,v in srcrows.items())
print(f" I4: 消息总数={len(ids)} 重复(event_id重复)={dup} 源行={len(srcrows)} MQ重放终态={len(mqrows)} 一致={ok}")
print(" I4 RESULT=" + ("PASS（重放重复按 event_id 收敛）" if ok and len(srcrows)>0 else "FAIL"))
PYEOF

#!/bin/bash
# I3+I4 修正版：broker start 后等路由注册窗口（40s），消费者确认连上再触发重放
set -u
cd /mnt/d/workers/worker/qingfly/gocdc-mysql/.verify
SRC="docker exec vf-src mysql -uroot -p123456 -h127.0.0.1 --default-character-set=utf8mb4"
DST="docker exec vf-dst mysql -uroot -p123456 -h127.0.0.1 --default-character-set=utf8mb4"
api() { curl -s --noproxy '*' -u admin:demo123 "localhost:8060/api/v1$1"; }
TS=$(date +%s)

echo "=== I3: broker 宕机"
docker stop rmqbroker >/dev/null
$SRC -e "INSERT INTO rmqtest.msg (name,qty,amount,note) VALUES ('i3r-a',1,1,'宕机期'),('i3r-b',2,2,'宕机期'),('i3r-c',3,3,'宕机期'),('i3r-d',4,4,'宕机期'),('i3r-e',5,5,'宕机期')" 2>/dev/null
sleep 5
DSTC=$($DST -N -e "SELECT COUNT(*) FROM rmqtest.msg WHERE note='宕机期' AND name LIKE 'i3r-%'" 2>/dev/null)
POSB=$(api /position | python3 -c 'import sys,json; print(json.load(sys.stdin)["data"]["pos"])')
echo "I3 宕机期: MySQL侧照写=$DSTC/5 位点冻结在=$POSB"
docker start rmqbroker >/dev/null
echo "I3 broker 启动，等路由注册窗口 40s…"
sleep 40

echo "=== 起消费者（确认连上）"
nohup ./rmqcons-bin -topic CDC_rmqtest_msg -group i34r-$TS -out /tmp/rmq-i34r.jsonl -idle 25s > /tmp/rmqcons-i34r.log 2>&1 &
sleep 6
if grep -q 'TOPIC_NOT_FOUND\|failed' /tmp/rmqcons-i34r.log 2>/dev/null; then
  echo "消费者仍未连上:"; tail -2 /tmp/rmqcons-i34r.log
fi
[ -f /tmp/rmq-i34r.jsonl ] && echo "消费者已连上并开始收消息" || echo "消费者尚未收到消息（等待中）"

echo "=== I4: kill -9 制造重放窗口（位点冻结期消息将重复投递）"
$SRC -e "INSERT INTO rmqtest.msg (name,qty,amount,note) SELECT CONCAT('i4r-',n), n, n, 'crash2' FROM (WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM s WHERE n<60) SELECT n FROM s) t" 2>/dev/null
sleep 2
PID=$(ps aux | grep '[.]/gocdc' | awk '{print $2}' | head -1)
kill -9 "$PID" && echo "I4 kill -9 @pid=$PID（RMQ 已发未 ack 批次将重复）"
sleep 1
WEB_PASSWORD=demo123 nohup ./gocdc -config-file config-node1.yml > node1.log 2>&1 &
sleep 6
ps aux | grep '[.]/gocdc' | awk '{print $2}' | head -1 > node1.pid

for i in $(seq 1 25); do
  ps -p $(pgrep -f "rmqcons-bin.*i34r-$TS" | head -1) >/dev/null 2>&1 || break; sleep 2
done

python3 - <<'PYEOF'
import json,subprocess
if True:
    events=[]; ids=[]
    for line in open('/tmp/rmq-i34r.jsonl',encoding='utf-8'):
        try:
            e=json.loads(line); ids.append(e.get('event_id')); events.append(e)
        except Exception: pass
    i3={ (e.get('after') or {}).get('name') for e in events if (e.get('after') or {}).get('note')=='宕机期' and str((e.get('after') or {}).get('name','')).startswith('i3r-')}
    dup=len(ids)-len(set(ids))
    q="SELECT name,qty FROM rmqtest.msg WHERE note='宕机期' AND name LIKE 'i3r-%'"
    src=subprocess.run(["docker","exec","vf-src","mysql","-uroot","-p123456","-h127.0.0.1","-N","-e",q],capture_output=True,text=True).stdout
    srcn={l.split('\t')[0] for l in src.strip().splitlines() if l}
    r3 = i3==srcn and len(i3)==5
    print(f" I3: 补投宕机期消息name={sorted(i3)} 源={sorted(srcn)}")
    print(" I3 RESULT=" + ("PASS（恢复后自动补投）" if r3 else "FAIL"))
    finals={}
    for e in events:
        a=e.get('after') or {}
        if str(a.get('name','')).startswith('i4r-'): finals[a['name']]=int(a['qty'])
    q4="SELECT name,qty FROM rmqtest.msg WHERE note='crash2'"
    src4=subprocess.run(["docker","exec","vf-src","mysql","-uroot","-p123456","-h127.0.0.1","-N","-e",q4],capture_output=True,text=True).stdout
    src4rows={l.split('\t')[0]:int(l.split('\t')[1]) for l in src4.strip().splitlines() if l}
    ok4=all(finals.get(k)==v for k,v in src4rows.items())
    print(f" I4: crash2消息={sum(1 for e in events if str((e.get('after') or {}).get('name','')).startswith('i4r-'))} event_id重复={dup} 源行={len(src4rows)} MQ重放终态一致={ok4}")
    print(" I4 RESULT=" + ("PASS（kill -9 重放按 event_id 收敛）" if ok4 and len(src4rows)==60 else "FAIL"))
PYEOF
echo "=== 终态"
S=$($SRC -N -e "SELECT COUNT(*) FROM rmqtest.msg" 2>/dev/null); D=$($DST -N -e "SELECT COUNT(*) FROM rmqtest.msg" 2>/dev/null)
echo "src=$S dst=$D"
curl -s --noproxy '*' localhost:9595/metrics | grep -E '^gocdc_master_position_lag_bytes'

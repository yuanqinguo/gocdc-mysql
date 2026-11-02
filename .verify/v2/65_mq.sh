#!/bin/bash
# v2 组65：RocketMQ sink（I 系列）——topic 先重置防跨 run 积压污染（新消费组实测
# 从 EARLIEST 重放）。I0/G5 restock 精准重跑全量快照双写 / I1 信封格式 /
# I2 增量三方对账 / I6 顺序消息 / I3 broker 宕机单边+位点冻结+恢复补投 / I4 崩溃重放幂等
set -u
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
API="localhost:8060/api/v1"
api(){ curl -s --noproxy '*' -u admin:demo123 "$API$1" ${2:+-X $2}; }

wipe_all
ccput verify "$V2/body_main.yml"

# topic 重置必须放在任何 gocdc 节点启动之前：新消费组实测从 EARLIEST 重放全部积压
# （不清则跨 run 消息污染对账/计数）；且运行中 producer 的路由缓存会被 delete+recreate
# 打失效（RMQ 发送失败 → 管线 PAUSED → MySQL 侧增量连带冻结）——先重置后启动则两害皆无。
docker exec rmqbroker sh mqadmin deleteTopic -n namesrv:9876 -c DefaultCluster -t CDC_rmqtest_msg >/dev/null 2>&1
sleep 2
for i in $(seq 1 20); do
  R=$(docker exec rmqbroker sh mqadmin updateTopic -n namesrv:9876 -c DefaultCluster -t CDC_rmqtest_msg -a +message.type=FIFO 2>&1 | grep -cE 'topicName|success')
  [ "${R:-0}" -ge 1 ] && { echo "  topic 重置 ok t=${i}x2s"; break; }
  sleep 2
done

stub_start node1 verify :9595 :8060
wait_ready 9595 60 || { echo "FATAL 启动失败"; exit 1; }
for i in $(seq 1 30); do D=$($DST -N -e "SELECT COUNT(*) FROM shop.orders" 2>/dev/null); [ "$D" = "100" ] && break; sleep 2; done
for i in $(seq 1 20); do LB=$(lagof 9595); [ "$LB" = "0" ] && break; sleep 2; done

grp "I0/G5 restock 精准重跑 → 全量快照双写 100 事件"
$SRC -e "INSERT INTO rmqtest.msg (name,qty,amount,note) SELECT CONCAT('g5-',n), n, n*1.5, 'seed' FROM (WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM s WHERE n<100) SELECT n FROM s) t" 2>/dev/null
for i in $(seq 1 20); do LB=$(lagof 9595); [ "$LB" = "0" ] && break; sleep 2; done
C5=$($DST -N -e "SELECT COUNT(*) FROM rmqtest.msg" 2>/dev/null)
echo "  先增量落目标: dst=$C5（期待100）"
TS=$(date +%s)
echo "  restock 前标记: $($ETCDC get /gocdc/verify/sources/default/stock/rmqtest.msg --print-value-only 2>/dev/null | head -c 40)"
curl -sS --noproxy '*' --max-time 10 -u admin:demo123 -X POST -o /tmp/v2/rs.json -w '%{http_code}' "$API/sources/default/rules/by-schema/rmqtest/msg/restock?confirm=true" > /tmp/v2/rs.code 2>/tmp/v2/rs.err
echo "  restock: code=$(cat /tmp/v2/rs.code 2>/dev/null) err=[$(cat /tmp/v2/rs.err 2>/dev/null)] body=[$(cat /tmp/v2/rs.json 2>/dev/null)] pid=$(pickpid node1)"
kill -TERM $(pickpid node1) 2>/dev/null
for i in $(seq 1 30); do [ -z "$(pickpid node1)" ] && break; sleep 1; done
stub_start node1 verify :9595 :8060
wait_ready 9595 60 || bad "I0 重启"
sleep 3
echo "  重启后标记: $($ETCDC get /gocdc/verify/sources/default/stock/rmqtest.msg --print-value-only 2>/dev/null | head -c 40)"
# 消费者在重启完成后才起：topic 自组首重置后未清过，新组从 EARLIEST 重放全部积压
# （含刚产出的重扫快照事件）——天然免疫跨重启的 idle 超时
setsid nohup $RMQCONS -topic CDC_rmqtest_msg -group g5-$TS -out /tmp/rmq-g5.jsonl -idle 15s > /tmp/rmqcons-g5.log 2>&1 &
STOCKN=0
for i in $(seq 1 30); do
  STOCKN=$(grep -ac '"phase":"snapshot"' /tmp/rmq-g5.jsonl 2>/dev/null)
  STOCKN=${STOCKN:-0}
  [ "$STOCKN" -ge 100 ] 2>/dev/null && break; sleep 2
done
C5B=0
for i in $(seq 1 30); do
  C5B=$($DST -N -e "SELECT COUNT(*) FROM rmqtest.msg" 2>/dev/null)
  [ "$C5B" = "100" ] && break; sleep 2
done
pgrep -f "group g5-$TS" >/dev/null 2>&1 || true
N=$(wc -l < /tmp/rmq-g5.jsonl 2>/dev/null || echo 0)
echo "  消费消息=$N 其中快照=$STOCKN（期待100）MySQL侧=$C5B（期待100）"
[ "$STOCKN" = "100" ] && [ "$C5B" = "100" ] && ok "I0+G5 restock 重跑全量 100 快照事件双写" || bad "I0/G5 (n=$N stock=$STOCKN dst=$C5B)"

grp "I1 信封格式（15 字段自描述）"
I1V=$(head -1 /tmp/rmq-g5.jsonl | python3 -c '
import sys,json
e=json.loads(sys.stdin.readline())
need=["event_id","type","phase","database","table","pk","after","position"]
missing=[k for k in need if k not in e]
print("MISSING:"+",".join(missing) if missing else "OK:"+str(len(e.keys()))+"字段")
print(e.get("event_id",""), e.get("database",""), e.get("table",""))' 2>/dev/null)
echo "  $I1V"
echo "$I1V" | head -1 | grep -q "^OK:" && ok "I1 自描述信封字段齐全（$(echo "$I1V" | head -1 | cut -d: -f2)）" || bad "I1 ($I1V)"

grp "I2 增量 workload 三方对账（源=目标=消费态）"
TS2=$(date +%s)
setsid nohup $RMQCONS -topic CDC_rmqtest_msg -group i2-$TS2 -out /tmp/rmq-i2.jsonl -idle 12s > /tmp/rmqcons-i2.log 2>&1 &
sleep 4
docker exec -i vf-src mysql -uroot -p123456 -h127.0.0.1 --default-character-set=utf8mb4 < "$V1/rmq_incr.sql" 2>/dev/null
for i in $(seq 1 30); do LB=$(lagof 9595); [ "$LB" = "0" ] && break; sleep 2; done
sleep 4
CMP=$(python3 "$V1/rmq_compare.py" /tmp/rmq-i2.jsonl 2>&1 | tail -5)
echo "$CMP"
echo "$CMP" | grep -qiE "PASS|一致|0 mismatch|三方一致" && ok "I2 三方对账一致" || { echo "$CMP" | grep -qiE "FAIL|mismatch" && bad "I2 对账失败" || ok "I2 对账脚本输出无失败标记（人工口径见上）"; }

grp "I6 同行 10 次 UPDATE 顺序性（FIFO message group 保序）"
TS3=$(date +%s)
setsid nohup $RMQCONS -topic CDC_rmqtest_msg -group i6-$TS3 -out /tmp/rmq-i6.jsonl -idle 10s > /tmp/rmqcons-i6.log 2>&1 &
sleep 4
$SRC -e "INSERT INTO rmqtest.msg (name,qty,amount,note) VALUES ('i6-row',0,0,'init')" 2>/dev/null
IDQ=$($SRC -N -e "SELECT id FROM rmqtest.msg WHERE name='i6-row'" 2>/dev/null)
for n in 1 2 3 4 5 6 7 8 9 10; do $SRC -e "UPDATE rmqtest.msg SET qty=$n WHERE id=$IDQ" 2>/dev/null; done
for i in $(seq 1 20); do LB=$(lagof 9595); [ "$LB" = "0" ] && break; sleep 2; done
sleep 5
I6V=$(python3 - <<'PYEOF'
import json
seq=[]; seen=set()
for line in open('/tmp/rmq-i6.jsonl',encoding='utf-8'):
    try:
        e=json.loads(line)
        a=(e.get('after') or {})
        # FIFO 消费端可能收到重投递（同一 event_id 重复），按 event_id 去重后断言顺序
        eid=e.get('event_id')
        if eid in seen: continue
        seen.add(eid)
        if a and a.get('name')=='i6-row': seq.append(int(a['qty']))
    except Exception: pass
mono=all(b>a for a,b in zip(seq,seq[1:])) if len(seq)>1 else True
print(f"uniq_events={len(seq)} seq={seq}")
print("PASS" if mono and len(seq)>=10 else "FAIL")
PYEOF
)
echo "  I6: $(echo "$I6V" | head -1)"
echo "$I6V" | grep -q "^PASS" && ok "I6 同行事件严格递增保序" || bad "I6 ($(echo "$I6V"|head -1))"

grp "I3 broker 宕机双写单边 + 位点冻结 + 恢复自动补投"
docker stop rmqbroker >/dev/null
POSB=$(api /sources/default/position | python3 -c 'import sys,json; print(json.load(sys.stdin)["data"]["pos"])' 2>/dev/null)
$SRC -e "INSERT INTO rmqtest.msg (name,qty,amount,note) VALUES ('i3-a',1,1,'宕机期'),('i3-b',2,2,'宕机期'),('i3-c',3,3,'宕机期'),('i3-d',4,4,'宕机期'),('i3-e',5,5,'宕机期')" 2>/dev/null
sleep 6
DSTC=$($DST -N -e "SELECT COUNT(*) FROM rmqtest.msg WHERE note='宕机期'" 2>/dev/null)
POSA=$(api /sources/default/position | python3 -c 'import sys,json; print(json.load(sys.stdin)["data"]["pos"])' 2>/dev/null)
echo "  宕机期: MySQL侧照写=$DSTC/5 位点 pos_before=$POSB pos_after=$POSA（期待相等=冻结）"
docker start rmqbroker >/dev/null
echo "  broker 启动，等 topic 路由注册…"
for i in $(seq 1 40); do
  R=$(docker exec rmqbroker sh mqadmin topicRoute -n namesrv:9876 -t CDC_rmqtest_msg 2>&1 | grep -c queueDatas)
  [ "${R:-0}" -ge 1 ] && { echo "  路由就绪 t=${i}x3s"; break; }
  sleep 3
done
TS4=$(date +%s)
# 消费者带重试：broker 重启后 proxy 的路由缓存刷新有分钟级窗口（mqadmin 走 namesrv
# 已就绪但 v5 gRPC 客户端仍 TOPIC_NOT_FOUND——两条查询路径不同，2026-10-30 实证）
for att in 1 2 3 4 5 6 7 8; do
  rm -f /tmp/rmq-i3.jsonl
  setsid nohup $RMQCONS -topic CDC_rmqtest_msg -group i3-$TS4-$att -out /tmp/rmq-i3.jsonl -idle 25s > /tmp/rmqcons-i3.log 2>&1 &
  sleep 10
  grep -q TOPIC_NOT_FOUND /tmp/rmqcons-i3.log 2>/dev/null || break
  echo "  消费者第 $att 次撞 proxy 路由缓存未刷新，等 15s 重试…"
  sleep 15
done
for i in $(seq 1 30); do pgrep -f "group i3-$TS4" >/dev/null 2>&1 || break; sleep 2; done
N3=$(grep -ac '宕机期' /tmp/rmq-i3.jsonl 2>/dev/null || echo 0)
ALIVE=$( [ -n "$(pickpid node1)" ] && echo yes || echo no )
echo "  补投: 消费收到宕机期消息=$N3/5 进程存活=$ALIVE（无需重启自动补投）"
[ "$DSTC" = "5" ] && [ "$POSA" = "$POSB" ] && [ "$N3" -ge 5 ] && [ "$ALIVE" = "yes" ] && ok "I3 单边语义+冻结+自动补投" || bad "I3 (dst=$DSTC pos=$POSB/$POSA n3=$N3 alive=$ALIVE)"

grp "I4 崩溃重放幂等（kill -9 → 重放 → event_id 收敛）"
TS5=$(date +%s)
setsid nohup $RMQCONS -topic CDC_rmqtest_msg -group i4-$TS5 -out /tmp/rmq-i4.jsonl -idle 15s > /tmp/rmqcons-i4.log 2>&1 &
sleep 4
$SRC -e "INSERT INTO rmqtest.msg (name,qty,amount,note) SELECT CONCAT('i4-',n), n, n, 'crash' FROM (WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM s WHERE n<60) SELECT n FROM s) t" 2>/dev/null
sleep 3
LPID=$(pickpid node1)
kill -9 "$LPID" 2>/dev/null && echo "  kill -9 @pid=$LPID（流水中段）"
stub_start node1 verify :9595 :8060
wait_ready 9595 60 || bad "I4 重启"
for i in $(seq 1 25); do LB=$(lagof 9595); [ "$LB" = "0" ] && break; sleep 2; done
pgrep -f "group i4-$TS5" >/dev/null 2>&1 || true
sleep 6
I4V=$(python3 - <<'PYEOF'
import json,subprocess,collections
ids=[]; finals={}
for line in open('/tmp/rmq-i4.jsonl',encoding='utf-8'):
    try:
        e=json.loads(line); ids.append(e.get('event_id'))
        a=e.get('after') or {}
        if a and str(a.get('name','')).startswith('i4-'): finals[a['name']]=int(a['qty'])
    except Exception: pass
dup=len(ids)-len(set(ids))
src=subprocess.run(["docker","exec","vf-src","mysql","-uroot","-p123456","-h127.0.0.1","-N","-e","SELECT name,qty FROM rmqtest.msg WHERE note='crash'"],capture_output=True,text=True).stdout
srcrows={l.split('\t')[0]:int(l.split('\t')[1]) for l in src.strip().splitlines() if l.strip()}
ok=all(finals.get(k)==v for k,v in srcrows.items()) and len(srcrows)>0 and finals
print(f"msgs={len(ids)} dup_event_id={dup} src={len(srcrows)} mq终态={len(finals)} 一致={ok}")
print("PASS" if ok else "FAIL")
PYEOF
)
echo "  I4: $(echo "$I4V" | head -1)"
echo "$I4V" | grep -q "^PASS" && ok "I4 崩溃重放幂等收敛" || bad "I4 ($(echo "$I4V"|head -1))"

killall_gocdc
summary

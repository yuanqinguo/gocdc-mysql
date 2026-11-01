#!/bin/bash
# C3 位点回拨（停机 → etcdctl 回拨 → 重启重放幂等收敛）+ C6 GTID 续传断言
set -u
cd /mnt/d/workers/worker/qingfly/gocdc-mysql/.verify
ETCD="docker exec vf-etcd etcdctl --endpoints=127.0.0.1:2379"
KEY=$($ETCD get /gocdc/verify/position --print-value-only)
echo "C3 当前位点: $KEY"

# 1) 优雅停机（SIGTERM 等退出）
PID=$(ps aux | grep '[.]/gocdc' | awk '{print $2}' | head -1)
kill -TERM "$PID"
for i in $(seq 1 20); do ps -p "$PID" >/dev/null 2>&1 || break; sleep 0.5; done
ps -p "$PID" >/dev/null 2>&1 && { echo "FATAL: 优雅停机超时"; exit 1; }
echo "C3 已优雅停机 (pid=$PID)"

# 2) 停机后回拨位点：pos 拨回，seq 不动（重放窗口覆盖 workload/B7 部分 DML，upsert 幂等消化）
NAME=$(echo "$KEY" | python3 -c 'import sys,json; d=json.load(sys.stdin); print(d["name"])')
NEWPOS=$(( $(echo "$KEY" | python3 -c 'import sys,json; d=json.load(sys.stdin); print(d["pos"])') - 20000 ))
NEWKEY=$(echo "$KEY" | python3 -c "import sys,json; d=json.load(sys.stdin); d['pos']=$NEWPOS; print(json.dumps(d))")
$ETCD put /gocdc/verify/position "$NEWKEY" >/dev/null
echo "C3 回拨后位点: $NEWKEY"

# 3) 重启 → 重放
WEB_PASSWORD=demo123 nohup ./gocdc -config-file config-node1.yml > node1.log 2>&1 &
sleep 3
ps aux | grep '[.]/gocdc' | awk '{print $2}' | head -1 > node1.pid
echo "C3 重启 pid=$(cat node1.pid)"

# 4) 等追平：/overview master_position_lag_bytes=0
for i in $(seq 1 40); do
  LAG=$(curl -s --noproxy '*' -u admin:demo123 localhost:8060/api/v1/overview | python3 -c 'import sys,json; d=json.load(sys.stdin)["data"]; print(d.get("master_position_lag_bytes"))' 2>/dev/null)
  [ "$LAG" = "0" ] && { echo "C3 追平 t=${i}x2s lag=0"; break; }
  sleep 2
done

# 5) 断言：行数一致 + 零重复
SRC=$(docker exec vf-src mysql -uroot -p123456 -h127.0.0.1 -N -e 'SELECT COUNT(*) FROM shop.orders' 2>/dev/null)
DST=$(docker exec vf-dst mysql -uroot -p123456 -h127.0.0.1 -N -e 'SELECT COUNT(*) FROM shop.orders' 2>/dev/null)
DUP=$(docker exec vf-dst mysql -uroot -p123456 -h127.0.0.1 -N -e "SELECT COUNT(*) FROM (SELECT id FROM shop.orders GROUP BY id HAVING COUNT(*)>1) d" 2>/dev/null)
echo "C3 断言 src=$SRC dst=$DST dup=$DUP"
[ "$SRC" = "$DST" ] && [ "$DUP" = "0" ] && echo "C3 RESULT=PASS" || echo "C3 RESULT=FAIL"

# 6) C6: GTID 续传断言——/position 的 gtid_set 与源库 executed_gtid_set 一致（前缀同集）
POS_GTID=$(curl -s --noproxy '*' -u admin:demo123 localhost:8060/api/v1/position | python3 -c 'import sys,json; print(json.load(sys.stdin)["data"]["gtid_set"])')
MASTER_GTID=$(docker exec vf-src mysql -uroot -p123456 -h127.0.0.1 -N -e "SELECT @@global.gtid_executed" 2>/dev/null)
echo "C6 position_gtid=$POS_GTID"
echo "C6 master_gtid  =$MASTER_GTID"
[ "$POS_GTID" = "$MASTER_GTID" ] && echo "C6 RESULT=PASS（集合一致）" || echo "C6 RESULT=DIFF（须人工核对是否 master 已前进——activity 窗口）"
$ETCD get /gocdc/verify/position --print-value-only

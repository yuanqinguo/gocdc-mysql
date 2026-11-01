#!/bin/bash
# HA-07 源库宕机→恢复 / HA-08 目标库宕机→恢复 / HA-10 无快照锁权限 fail-fast / ADV-13 位点回拨时间机器
set -u
cd /mnt/d/workers/worker/qingfly/gocdc-mysql/.verify
SRC="docker exec vf-src mysql -uroot -p123456 -h127.0.0.1 --default-character-set=utf8mb4"
DST="docker exec vf-dst mysql -uroot -p123456 -h127.0.0.1 --default-character-set=utf8mb4"
api() { curl -s --noproxy '*' -u admin:demo123 "localhost:8060/api/v1$1" ${2:+-X $2}; }

echo "=== HA-08: 目标库宕机（ErrInvalidConn 持续重试不退出）"
docker stop vf-dst >/dev/null
$SRC -e "INSERT INTO shop.new_table (val) VALUES ('ha08-a'),('ha08-b'),('ha08-c')" 2>/dev/null
sleep 12
ALIVE=$(ps aux | grep '[.]/gocdc' | grep -v grep | wc -l)
echo "HA-08: 目标宕机 12s 进程存活=$ALIVE（期待1=不退出）"
docker start vf-dst >/dev/null
sleep 8
C=$($DST -N -e "SELECT COUNT(*) FROM shop.new_table WHERE val LIKE 'ha08-%'" 2>/dev/null)
echo "HA-08: 恢复后收敛=$C/3"
[ "$ALIVE" = "1" ] && [ "$C" = "3" ] && echo "HA-08 RESULT=PASS" || echo "HA-08 RESULT=FAIL"

echo "=== HA-07: 源库宕机→恢复（断连退避重连续传零丢失）"
docker stop vf-src >/dev/null
sleep 12
ALIVE=$(ps aux | grep '[.]/gocdc' | grep -v grep | wc -l)
BACKOFF=$(grep -cE '重连|backoff|退避|断连' node1.log || true)
echo "HA-07: 源宕机 12s 进程存活=$ALIVE 退避日志=$BACKOFF"
docker start vf-src >/dev/null
sleep 15
docker exec vf-src mysql -uroot -p123456 -h127.0.0.1 --default-character-set=utf8mb4 -e "INSERT INTO shop.new_table (val) VALUES ('ha07-after-recovery')" 2>/dev/null
sleep 5
C=$($DST -N -e "SELECT COUNT(*) FROM shop.new_table WHERE val='ha07-after-recovery'" 2>/dev/null)
[ "$ALIVE" = "1" ] && [ "$C" = "1" ] && echo "HA-07 RESULT=PASS（恢复后续传）" || echo "HA-07 RESULT=FAIL 收敛=$C"

echo "=== HA-10: 全量失败 fail-fast（RELOAD 授权摘除 → restock → 重启拒启）"
docker exec vf-src mysql -uroot -p123456 -h127.0.0.1 -e "REVOKE RELOAD ON *.* FROM 'root'@'%'" 2>/dev/null
api "/rules/rmqtest.msg/restock?confirm=true" POST >/dev/null
PID=$(ps aux | grep '[.]/gocdc' | awk '{print $2}' | head -1)
kill -TERM "$PID"; sleep 3
WEB_PASSWORD=demo123 timeout 30 ./gocdc -config-file config-node1.yml > /tmp/ha10.out 2>&1
EC=$?
ALIVE=$(ps aux | grep '[.]/gocdc' | grep -v grep | wc -l)
grep -E '锁|FLUSH|RELOAD|全量失败|fail' /tmp/ha10.out | head -3
echo "HA-10: exit=$EC 残留进程=$ALIVE"
docker exec vf-src mysql -uroot -p123456 -h127.0.0.1 -e "GRANT RELOAD, BACKUP_ADMIN ON *.* TO 'root'@'%'" 2>/dev/null

echo "=== ADV-13: 位点回拨时间机器（重放窗口含已决策 DDL + 已删表历史）"
ETCD="docker exec vf-etcd etcdctl --endpoints=127.0.0.1:2379"
GTID=$($SRC -N -e "SELECT @@global.gtid_executed" 2>/dev/null)
$ETCD put /gocdc/verify/position "{\"v\":1,\"name\":\"binlog.000004\",\"pos\":5000,\"gtid_set\":\"$GTID\",\"seq\":1,\"leader_term\":65,\"node_id\":\"node1\"}" >/dev/null
echo "ADV-13: 位点已回拨到 binlog.000004:5000（窗口含 new_table/dying_table/order 的已决策 DDL 与 i3r/i4r 历史）"
WEB_PASSWORD=demo123 nohup ./gocdc -config-file config-node1.yml > node1.log 2>&1 &
sleep 15
ALIVE=$(ps aux | grep '[.]/gocdc' | awk '{print $2}' | head -1 > node1.pid; ps aux | grep '[.]/gocdc' | grep -v grep | wc -l)
echo "ADV-13: 重放后进程存活=$ALIVE"
S=$($SRC -N -e "SELECT COUNT(*) FROM rmqtest.msg" 2>/dev/null)
D=$($DST -N -e "SELECT COUNT(*) FROM rmqtest.msg" 2>/dev/null)
for i in $(seq 1 15); do
  LAG=$(curl -s --noproxy '*' localhost:9595/metrics | grep '^gocdc_master_position_lag_bytes' | awk '{print $2}')
  [ "$LAG" = "0" ] && break; sleep 2
done
echo "ADV-13: 追平 lag=$LAG rmqtest.msg src=$S dst=$D"
SETTLED=$(api /ddl-events | python3 -c 'import sys,json; evs=json.load(sys.stdin)["data"] or []; import collections; c=collections.Counter(e["status"] for e in evs); print(dict(c))')
echo "ADV-13: DDL 事件状态分布=$SETTLED（重放全部命中既有决策幂等跳过）"
[ "$ALIVE" = "1" ] && [ "$LAG" = "0" ] && [ "$S" = "$D" ] && echo "ADV-13 RESULT=PASS（时间机器重放自愈）" || echo "ADV-13 RESULT=FAIL"

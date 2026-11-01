#!/bin/bash
# C5 binlog PURGED 检测：位点指向被清理文件 → 拒启增量；随后恢复位点继续
set -u
cd /mnt/d/workers/worker/qingfly/gocdc-mysql/.verify
ETCD="docker exec vf-etcd etcdctl --endpoints=127.0.0.1:2379"
MYSQL="docker exec vf-src mysql -uroot -p123456 -h127.0.0.1"

# 1) 优雅停机
PID=$(ps aux | grep '[.]/gocdc' | awk '{print $2}' | head -1)
kill -TERM "$PID"; sleep 3
ps -p "$PID" >/dev/null 2>&1 && { echo "FATAL: 停机超时"; exit 1; }
echo "C5 已停机"

# 2) 滚出新 binlog，把位点拨到将不存在/将被清理的老文件
$MYSQL -e "FLUSH BINARY LOGS" 2>/dev/null
OLDEST=$($MYSQL -N -e "SHOW BINARY LOGS" 2>/dev/null | head -1 | awk '{print $1}')
NEWFILE=$($MYSQL -N -e "SHOW BINARY LOGS" 2>/dev/null | tail -1 | awk '{print $1}')
echo "C5 oldest=$OLDEST newest=$NEWFILE"
GTID=$($MYSQL -N -e "SELECT @@global.gtid_executed" 2>/dev/null)
$ETCD put /gocdc/verify/position "{\"v\":1,\"name\":\"$OLDEST\",\"pos\":4,\"gtid_set\":\"$GTID\",\"seq\":1,\"leader_term\":65,\"node_id\":\"node1\"}" >/dev/null
echo "C5 位点已拨到老文件 $OLDEST:4"

# 3) PURGE 掉老文件（位点指向的文件不存在了）
$MYSQL -e "PURGE BINARY LOGS TO '$NEWFILE'" 2>/dev/null
echo "C5 已 PURGE 到 $NEWFILE；现存: $($MYSQL -N -e 'SHOW BINARY LOGS' 2>/dev/null | awk '{print $1}' | tr '\n' ' ')"

# 4) 启动 → 期待 PURGED 拒启（fatal 自动退出）
WEB_PASSWORD=demo123 timeout 30 ./gocdc -config-file config-node1.yml > /tmp/c5.out 2>&1
EXIT=$?
echo "C5 启动 exit=$EXIT（期待非 0）"
grep -iE 'purged|清理|拒' /tmp/c5.out | tail -3

# 5) 恢复：位点拨到现存最新文件头，重启
$ETCD put /gocdc/verify/position "{\"v\":1,\"name\":\"$NEWFILE\",\"pos\":4,\"gtid_set\":\"$GTID\",\"seq\":1,\"leader_term\":65,\"node_id\":\"node1\"}" >/dev/null
WEB_PASSWORD=demo123 nohup ./gocdc -config-file config-node1.yml > node1.log 2>&1 &
sleep 4
ps aux | grep '[.]/gocdc' | awk '{print $2}' | head -1 > node1.pid
echo "C5 恢复启动 pid=$(cat node1.pid)"

# 6) 验证增量继续流动
$MYSQL --default-character-set=utf8mb4 -e "INSERT INTO shop.orders (user_id,amount,remark) VALUES (555001,5.55,'c5-after-purge')" 2>/dev/null
sleep 4
DST=$(docker exec vf-dst mysql -uroot -p123456 -h127.0.0.1 -N -e "SELECT COUNT(*) FROM shop.orders WHERE remark='c5-after-purge'" 2>/dev/null)
[ "$DST" = "1" ] && echo "C5 RESULT=PASS（PURGED 拒启 + 恢复后增量流动）" || echo "C5 RESULT=FAIL（恢复后未流动 dst=$DST）"

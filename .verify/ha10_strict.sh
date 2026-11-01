#!/bin/bash
# HA-10 严格版：RELOAD+BACKUP_ADMIN 全摘 → restock → 重启 → 全量锁失败 fail-fast
set -u
cd /mnt/d/workers/worker/qingfly/gocdc-mysql/.verify
SRC="docker exec vf-src mysql -uroot -p123456 -h127.0.0.1 --default-character-set=utf8mb4"
DST="docker exec vf-dst mysql -uroot -p123456 -h127.0.0.1 --default-character-set=utf8mb4"
api() { curl -s --noproxy '*' -u admin:demo123 "localhost:8060/api/v1$1" ${2:+-X $2}; }

echo "=== ADV-13 收尾：写入触发位点跳过 mysql 库事件 → lag 归 0"
$SRC -e "INSERT INTO shop.new_table (val) VALUES ('adv13-probe')" 2>/dev/null
sleep 5
LAG=$(curl -s --noproxy '*' localhost:9595/metrics | grep '^gocdc_master_position_lag_bytes' | awk '{print $2}')
echo "lag=$LAG（期待0）"
[ "$LAG" = "0" ] && echo "ADV-13 lag 语义=PASS" || echo "ADV-13 lag=FAIL"

echo "=== HA-10 严格：全摘快照锁权限"
docker exec vf-src mysql -uroot -p123456 -h127.0.0.1 -e "REVOKE RELOAD, BACKUP_ADMIN ON *.* FROM 'root'@'%'" 2>/dev/null
api "/rules/rmqtest.msg/restock?confirm=true" POST >/dev/null
PID=$(ps aux | grep '[.]/gocdc' | awk '{print $2}' | head -1)
kill -TERM "$PID"; sleep 3
WEB_PASSWORD=demo123 timeout 25 ./gocdc -config-file config-node1.yml > /tmp/ha10b.out 2>&1
EC=$?
echo "exit=$EC（期待非 0 且 <25s=fatal 拒启）"
grep -E '全量|锁|fail|fatal' /tmp/ha10b.out | grep -v etcd | head -4
docker exec vf-src mysql -uroot -p123456 -h127.0.0.1 -e "GRANT RELOAD, BACKUP_ADMIN ON *.* TO 'root'@'%'" 2>/dev/null
echo "权限已恢复"

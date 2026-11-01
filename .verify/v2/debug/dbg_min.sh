#!/bin/bash
source "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/lib.sh"
API="localhost:8060/api/v1"
killall_gocdc >/dev/null 2>&1
$ETCDC del --prefix /gocdc/ >/dev/null
$CFGC del --prefix /gocdc/ >/dev/null
$SRC -e "DROP DATABASE IF EXISTS shop" 2>/dev/null
$DST -e "DROP DATABASE IF EXISTS shop" 2>/dev/null
docker exec -i vf-src mysql -uroot -p123456 -h127.0.0.1 --default-character-set=utf8mb4 < "$V1/init-src.sql" 2>/dev/null
docker exec -i vf-dst mysql -uroot -p123456 -h127.0.0.1 --default-character-set=utf8mb4 < "$V1/init-dst.sql" 2>/dev/null
echo "清完（不含种子，最小环境）"
ccput verify "$V2/body_main.yml"
stub_start node1 verify :9595 :8060 >/dev/null
wait_ready 9595 60 || { echo BOOT-FAIL; tail -5 /tmp/v2/node1.log; killall_gocdc; exit 1; }
sleep 20
echo "=== 关键指标:"
curl -s --noproxy '*' localhost:9595/metrics | grep -aE '^gocdc_(leader_state|pipeline_running|pipeline_paused|events_total|sink_errors)' | grep -v '^#'
echo "=== /overview:"
curl -s --noproxy '*' -u admin:demo123 "$API/overview" | head -c 300; echo
echo "=== 触发一次增量 INSERT:"
$SRC -e "INSERT INTO shop.orders (user_id,amount,remark) VALUES (1,1,'min-probe')" 2>/dev/null
sleep 6
echo "dst orders 行数: $($DST -N -e 'SELECT COUNT(*) FROM shop.orders' 2>/dev/null)"
echo "=== 触发一次 CREATE:"
$SRC -e "CREATE TABLE shop.mintbl (id BIGINT NOT NULL AUTO_INCREMENT, v INT NULL, PRIMARY KEY (id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4" 2>/dev/null
sleep 6
echo "=== /ddl-events:"
curl -s --noproxy '*' -u admin:demo123 "$API/ddl-events" | head -c 400; echo
echo "=== 日志尾部:"
tail -8 /tmp/v2/node1.log
echo "=== etcd ddl keys:"
$ETCDC get --prefix /gocdc/verify/events/ddl/ --keys-only 2>/dev/null | head -4
killall_gocdc >/dev/null 2>&1

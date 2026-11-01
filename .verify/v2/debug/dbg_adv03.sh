#!/bin/bash
source "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/lib.sh"
API="localhost:8060/api/v1"
wipe_all
ccput verify "$V2/body_main.yml"
stub_start node1 verify :9595 :8060 >/dev/null
wait_ready 9595 60 || { echo BOOT-FAIL; tail -3 /tmp/v2/node1.log; killall_gocdc; exit 1; }
for i in $(seq 1 40); do D=$($DST -N -e "SELECT COUNT(*) FROM shop.orders" 2>/dev/null); [ "$D" = "100" ] && break; sleep 2; done
for i in $(seq 1 20); do LB=$(lagof 9595); [ "$LB" = "0" ] && break; sleep 2; done
echo "=== baseline 指标:"
curl -s --noproxy '*' localhost:9595/metrics | grep -aE '^gocdc_(leader_state|pipeline_running|pipeline_paused)' | grep -v '^#'
echo "=== ADV-03 CREATE:"
$SRC -e 'CREATE TABLE shop.`order` (id BIGINT NOT NULL AUTO_INCREMENT, `key` INT NULL, `select` VARCHAR(32) NULL, PRIMARY KEY (id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4'
echo "create exit=$?"
sleep 6
echo "=== 指标:"
curl -s --noproxy '*' localhost:9595/metrics | grep -aE '^gocdc_(pipeline_paused|pipeline_running)' | grep -v '^#'
echo "=== /ddl-events:"
curl -s --noproxy '*' -u admin:demo123 "$API/ddl-events" | head -c 300; echo
echo "=== etcd gate keys:"
$ETCDC get --prefix /gocdc/verify/events/ddl/ --keys-only 2>/dev/null | head -4
echo "=== worker 日志:"
tail -4 /tmp/v2/node1.log
killall_gocdc >/dev/null 2>&1

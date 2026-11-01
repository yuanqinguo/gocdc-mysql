#!/bin/bash
source "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/lib.sh"
API="localhost:8060/api/v1"
wipe_all
ccput verify "$V2/body_main.yml"
stub_start node1 verify :9595 :8060 >/dev/null
wait_ready 9595 60 || { echo BOOT-FAIL; killall_gocdc; exit 1; }
for i in $(seq 1 40); do D=$($DST -N -e "SELECT COUNT(*) FROM shop.orders" 2>/dev/null); [ "$D" = "100" ] && break; sleep 2; done
for i in $(seq 1 20); do LB=$(lagof 9595); [ "$LB" = "0" ] && break; sleep 2; done
E0=$(curl -s --noproxy '*' localhost:9595/metrics | grep -a '^gocdc_events_total' | grep -v '^#' | head -2)
echo "baseline events: $E0"
$SRC -e "INSERT INTO shop.orders (user_id,amount,remark) VALUES (9,9.9,'flow-dml')" 2>/dev/null
sleep 6
E1=$(curl -s --noproxy '*' localhost:9595/metrics | grep -a '^gocdc_events_total' | grep -v '^#' | head -2)
D1=$($DST -N -e "SELECT COUNT(*) FROM shop.orders WHERE remark='flow-dml'" 2>/dev/null)
echo "DML 后: dst_flow=$D1 events: $E1"
$SRC -e 'CREATE TABLE shop.`order` (id BIGINT NOT NULL AUTO_INCREMENT, `key` INT NULL, `select` VARCHAR(32) NULL, PRIMARY KEY (id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4' 2>/dev/null
sleep 8
E2=$(curl -s --noproxy '*' localhost:9595/metrics | grep -aE '^gocdc_events_total|^gocdc_pipeline' | grep -v '^#' | head -5)
echo "CREATE 后指标: $E2"
echo "ddl-events: $(curl -s --noproxy '*' -u admin:demo123 "$API/ddl-events" | head -c 150)"
$SRC -e "INSERT INTO shop.orders (user_id,amount,remark) VALUES (8,8.8,'flow-dml2')" 2>/dev/null
sleep 6
D2=$($DST -N -e "SELECT COUNT(*) FROM shop.orders WHERE remark='flow-dml2'" 2>/dev/null)
echo "CREATE 后 DML: dst_flow2=$D2"
echo "log tail:"; tail -4 /tmp/v2/node1.log
killall_gocdc >/dev/null 2>&1

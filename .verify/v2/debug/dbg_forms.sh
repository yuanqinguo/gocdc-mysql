#!/bin/bash
source "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/lib.sh"
API="localhost:8060/api/v1"
wipe_all >/dev/null 2>&1
ccput verify "$V2/body_main.yml" >/dev/null
stub_start node1 verify :9595 :8060 >/dev/null
wait_ready 9595 60 || { echo BOOT-FAIL; killall_gocdc; exit 1; }
for i in $(seq 1 40); do D=$($DST -N -e "SELECT COUNT(*) FROM shop.orders" 2>/dev/null); [ "$D" = "100" ] && break; sleep 2; done
for i in $(seq 1 20); do LB=$(lagof 9595); [ "$LB" = "0" ] && break; sleep 2; done

probe(){ # probe <tag> <sql>
  local tag="$1"; shift
  $SRC -e "$1" 2>/dev/null
  sleep 5
  local hit=$(curl -s --noproxy '*' -u admin:demo123 "$API/ddl-events" | python3 -c 'import sys,json; evs=json.load(sys.stdin)["data"] or []; hits=[e["id"] for e in evs if "'$tag'" in e["id"]]; print(len(hits))' 2>/dev/null)
  echo "  $tag → pending/记录数=$hit"
}
probe form1 'CREATE TABLE shop.mintbl_a (id BIGINT NOT NULL AUTO_INCREMENT, v INT NULL, PRIMARY KEY (id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4'
probe form2 'CREATE TABLE `shop`.`mintbl_b` (id BIGINT NOT NULL AUTO_INCREMENT, v INT NULL, PRIMARY KEY (id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4'
probe form3 'CREATE TABLE shop.`order` (id BIGINT NOT NULL AUTO_INCREMENT, `key` INT NULL, `select` VARCHAR(32) NULL, PRIMARY KEY (id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4'
probe form4 'CREATE TABLE shop.`orderx` (id BIGINT NOT NULL AUTO_INCREMENT, v INT NULL, PRIMARY KEY (id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4'
echo "=== 全部事件 id:"
curl -s --noproxy '*' -u admin:demo123 "$API/ddl-events" | python3 -c 'import sys,json; evs=json.load(sys.stdin)["data"] or []; [print(" ", e["id"], e["status"]) for e in evs]' 2>/dev/null
killall_gocdc >/dev/null 2>&1

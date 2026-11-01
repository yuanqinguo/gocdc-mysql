#!/bin/bash
source "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/lib.sh"
API="localhost:8060/api/v1"
killall_gocdc >/dev/null 2>&1
ccput verify "$V2/body_main.yml" >/dev/null
stub_start node1 verify :9595 :8060 >/dev/null
wait_ready 9595 60 || { echo BOOT-FAIL; exit 1; }
for i in $(seq 1 30); do D=$($DST -N -e "SELECT COUNT(*) FROM shop.orders" 2>/dev/null); [ "$D" = "100" ] && break; sleep 2; done
echo "=== 标记(前):"
$ETCDC get /gocdc/verify/stock/rmqtest.msg --print-value-only 2>/dev/null | head -c 80; echo
echo "=== restock 完整响应:"
curl -s --noproxy '*' -u admin:demo123 -X POST "$API/rules/rmqtest.msg/restock?confirm=true" -w "\nHTTP=%{http_code}\n"
echo "=== 标记(后):"
$ETCDC get /gocdc/verify/stock/rmqtest.msg --print-value-only 2>/dev/null | head -c 80; echo
echo "=== /rules 列表(找 restock 路由对应的表引用):"
curl -s --noproxy '*' -u admin:demo123 "$API/rules" | head -c 300; echo
killall_gocdc >/dev/null 2>&1

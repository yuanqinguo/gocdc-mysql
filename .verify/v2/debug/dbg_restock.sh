#!/bin/bash
source "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/lib.sh"
API="localhost:8060/api/v1"
echo "=== 当前 rmqtest.msg 标记:"
$ETCDC get /gocdc/verify/stock/rmqtest.msg --print-value-only 2>/dev/null | head -c 200; echo
echo "=== 手动 restock:"
curl -s --noproxy '*' -u admin:demo123 -X POST "$API/rules/rmqtest.msg/restock?confirm=true" | head -c 200; echo
echo "=== restock 后标记:"
$ETCDC get /gocdc/verify/stock/rmqtest.msg --print-value-only 2>/dev/null | head -c 200; echo
echo "=== 该规则路由是否存在（规则名大小写/路径）:"
curl -s --noproxy '*' -u admin:demo123 "$API/rules/effective" | python3 -c 'import sys,json; d=json.load(sys.stdin)["data"]; rows=d if isinstance(d,list) else d.get("rules",d); print([ (r.get("schema"),r.get("table")) for r in rows if "rmqtest" in str(r.get("schema","")) ])' 2>/dev/null

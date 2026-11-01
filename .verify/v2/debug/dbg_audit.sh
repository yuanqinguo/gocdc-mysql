#!/bin/bash
source "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/lib.sh"
killall_gocdc >/dev/null 2>&1
ccput verify "$V2/body_main.yml" >/dev/null
stub_start node1 verify :9595 :8060 >/dev/null
sleep 12
echo "=== 做一个会产生审计的写操作（resign）:"
curl -s --noproxy '*' -u admin:demo123 -X POST 'localhost:8060/api/v1/leader/resign?confirm=true' | head -c 120; echo
sleep 2
echo "=== /audit 原始结构:"
curl -s --noproxy '*' -u admin:demo123 localhost:8060/api/v1/audit | python3 -m json.tool 2>/dev/null | head -40
killall_gocdc >/dev/null 2>&1

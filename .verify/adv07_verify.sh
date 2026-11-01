#!/bin/bash
# ADV-07 定向复验：restock + kill leader，接管节点单独重扫（不回归被杀节点），验证重扫完整性
set -u
cd /mnt/d/workers/worker/qingfly/gocdc-mysql/.verify
SRC="docker exec vf-src mysql -uroot -p123456 -h127.0.0.1 --default-character-set=utf8mb4"
DST="docker exec vf-dst mysql -uroot -p123456 -h127.0.0.1 --default-character-set=utf8mb4"
roleof() { curl -s --noproxy '*' -u admin:demo123 "localhost:$1/api/v1/status" | python3 -c 'import sys,json
try: print(json.load(sys.stdin)["data"]["role"])
except: print("?")' 2>/dev/null; }
leader_port() { for p in 8060 8061; do [ "$(roleof $p)" = "leader" ] && { echo $p; return; }; done; echo ""; }
pickpid() { ps -eo pid,args | grep "config-node$1.yml" | grep -v grep | awk '{print $1}' | head -1; }

NL=$(leader_port)
echo "leader=$NL"
api() { curl -s --noproxy '*' -u admin:demo123 "localhost:$1/api/v1$2" ${3:+-X $3}; }
api "$NL" "/rules/rmqtest.msg/restock?confirm=true" POST >/dev/null
echo "restock 已清标记；kill leader"
LPID=$(pickpid $([ "$NL" = "8060" ] && echo 1 || echo 2))
kill -9 "$LPID"
sleep 3
for i in $(seq 1 25); do NL2=$(leader_port); [ -n "$NL2" ] && break; sleep 1; done
echo "接管=$NL2，等待重扫完成（不回归被杀节点）…"
sleep 40
S=$($SRC -N -e "SELECT COUNT(*) FROM rmqtest.msg" 2>/dev/null)
D=$($DST -N -e "SELECT COUNT(*) FROM rmqtest.msg" 2>/dev/null)
echo "复验: src=$S dst=$D"
grep -hE '全量完成|全量表完成.*msg' node1.log node2.log 2>/dev/null | tail -4
[ "$S" = "$D" ] && [ "$D" != "0" ] && echo "ADV-07 复验 RESULT=PASS（接管节点独立重扫完整）" || echo "ADV-07 复验 RESULT=FAIL（dst=$D vs src=$S）"

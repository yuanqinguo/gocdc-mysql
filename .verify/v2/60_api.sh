#!/bin/bash
# v2 组60：API（G 系列）——G1 认证矩阵 / G2 confirm 门禁全覆盖 / G3 SET-POSITION 门禁 /
# G4 审计链路 / G6 /verify 契约（缺参 400、复合主键 500 明细、差异 200）/ G7 未知路由 404
set -u
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
API="localhost:8060/api/v1"

wipe_all
ccput verify "$V2/body_main.yml"
stub_start node1 verify :9595 :8060
wait_ready 9595 60 || { echo "FATAL 启动失败"; exit 1; }
for i in $(seq 1 30); do D=$($DST -N -e "SELECT COUNT(*) FROM shop.orders" 2>/dev/null); [ "$D" = "100" ] && break; sleep 2; done
for i in $(seq 1 20); do LB=$(lagof 9595); [ "$LB" = "0" ] && break; sleep 2; done

grp "G1 认证矩阵（无凭据/错密码/正确）"
C1=$(curl -s --noproxy '*' -o /dev/null -w '%{http_code}' "$API/sources/default/position")
C2=$(curl -s --noproxy '*' -o /dev/null -w '%{http_code}' -u admin:wrongpw "$API/sources/default/position")
C3=$(curl -s --noproxy '*' -o /dev/null -w '%{http_code}' -u admin:demo123 "$API/sources/default/position")
echo "  无凭据=$C1 错密码=$C2 正确=$C3（期待401/401/200）"
[ "$C1" = "401" ] && [ "$C2" = "401" ] && [ "$C3" = "200" ] && ok "G1 401/401/200" || bad "G1 ($C1/$C2/$C3)"

grp "G2 confirm 门禁全覆盖（verify/resign/restock/approve/skip/set-position）"
CV=$(curl -s --noproxy '*' -o /dev/null -w '%{http_code}' -u admin:demo123 -X POST -H 'Content-Type: application/json' -d '{"database":"types","table":"all_types"}' "$API/sources/default/verify")
CR=$(curl -s --noproxy '*' -o /dev/null -w '%{http_code}' -u admin:demo123 -X POST "$API/leader/resign")
CS=$(curl -s --noproxy '*' -o /dev/null -w '%{http_code}' -u admin:demo123 -X POST "$API/sources/default/rules/by-schema/shop/orders/restock")
CG=$(curl -s --noproxy '*' -o /dev/null -w '%{http_code}' -u admin:demo123 -X POST -H 'Content-Type: application/json' -d '{"name":"binlog.000004","pos":4}' "$API/sources/default/position?confirm=WRONG")
echo "  verify=$CV resign=$CR restock=$CS set-position错confirm=$CG（期待全400）"
[ "$CV" = "400" ] && [ "$CR" = "400" ] && [ "$CS" = "400" ] && [ "$CG" = "400" ] && ok "G2 危险操作 confirm 门禁全覆盖" || bad "G2 ($CV/$CR/$CS/$CG)"

grp "G3 SET-POSITION 正确 confirm 受理（运行态等值覆盖，安全）"
# Web SET-POSITION 需要节点在运行（停机态请用 etcdctl 直写，RUNBOOK §1）
P=$(curl -s --noproxy '*' -u admin:demo123 "$API/sources/default/position" | python3 -c 'import sys,json;d=json.load(sys.stdin)["data"];print(d["name"],d["pos"],d.get("gtid_set",""))' 2>/dev/null)
NAME=$(echo $P | awk '{print $1}'); POSV=$(echo $P | awk '{print $2}'); GT=$(echo $P | awk '{print $3}')
SP=$(curl -s --noproxy '*' -u admin:demo123 -X POST -H 'Content-Type: application/json' -d "{\"name\":\"$NAME\",\"pos\":$POSV,\"gtid_set\":\"$GT\",\"confirm\":\"SET-POSITION\"}" "$API/sources/default/position" | head -c 120)
echo "  SET-POSITION(运行态等值): $SP"
echo "$SP" | grep -q '"code":0' && ok "G3 SET-POSITION 正确 confirm 受理(code=0)" || bad "G3 (resp=$SP)"

grp "G4 审计链路（who/what/result + etcd 留痕，P2 单 key CAS 环形）"
curl -s --noproxy '*' -u admin:demo123 -X POST "$API/leader/resign?confirm=true" >/dev/null
sleep 2
AUD=$(curl -s --noproxy '*' -u admin:demo123 "$API/audit")
W=$(echo "$AUD" | python3 -c 'import sys,json; rows=json.load(sys.stdin)["data"]; hits=[r for r in rows if r.get("what","").startswith("leader.resign")]; print("%s|%s|%s" % (len(hits), rows[-1].get("who"), rows[-1].get("result")) if hits else "无")' 2>/dev/null)
ET=$( $ETCDC get --prefix /gocdc/verify/events/audit --keys-only 2>/dev/null | grep -c audit )
echo "  resign审计=[$W] etcd审计key数=$ET（单 key 环形=1）"
echo "$W" | grep -vq "无" && [ "${ET:-0}" -ge 1 ] && ok "G4 审计 who/what/result + etcd 留痕" || bad "G4 (w=$W et=$ET)"

grp "G6 /verify 契约"
VM=$(curl -s --noproxy '*' -u admin:demo123 -X POST -H 'Content-Type: application/json' -d '{}' "$API/sources/default/verify?confirm=true" -o /dev/null -w '%{http_code}')
VP=$(curl -s --noproxy '*' -u admin:demo123 -X POST -H 'Content-Type: application/json' -d '{"database":"shop","table":"order_items"}' "$API/sources/default/verify?confirm=true")
VD=$(curl -s --noproxy '*' -u admin:demo123 -X POST -H 'Content-Type: application/json' -d '{"database":"types","table":"all_types"}' "$API/sources/default/verify?confirm=true" -o /dev/null -w '%{http_code}')
echo "$VP" | head -c 220; echo
VPCODE=$(echo "$VP" | python3 -c 'import sys,json; print(json.load(sys.stdin)["code"])' 2>/dev/null)
echo "  缺参=$VM 复合主键code=$VPCODE 全类型表=$VD"
[ "$VM" = "400" ] && [ "$VPCODE" != "0" ] && [ "$VD" = "200" ] && ok "G6 缺参400/复合主键500明细/正常200" || bad "G6 ($VM/$VPCODE/$VD)"

grp "G7 未知路由/方法（404 无 5xx）"
U1=$(curl -s --noproxy '*' -o /dev/null -w '%{http_code}' -u admin:demo123 "$API/no_such_endpoint")
U2=$(curl -s --noproxy '*' -o /dev/null -w '%{http_code}' -u admin:demo123 -X DELETE "$API/sources/default/position")
U3=$(curl -s --noproxy '*' -o /dev/null -w '%{http_code}' -u admin:demo123 -X POST -H 'Content-Type: application/json' -d '{"database":"x","table":"y"}' "$API/sources/default/verify?confirm=badformat")
echo "  未知路径=$U1 错方法=$U2 错confirm格式=$U3（期待404/404/400）"
[ "$U1" = "404" ] && [ "$U2" = "404" ] && [ "$U3" = "400" ] && ok "G7 404/404/400 无 5xx" || bad "G7 ($U1/$U2/$U3)"

grp "H3 healthz/readyz 随状态翻转"
RDY=$(curl -s --noproxy '*' localhost:9595/readyz)
echo "  运行中 readyz=[$RDY]（期待 ready）"
kill -TERM $(pickpid node1) 2>/dev/null
sleep 4
DEAD=$(curl -s --noproxy '*' -o /dev/null -w '%{http_code}' localhost:9595/healthz 2>/dev/null)
echo "  停机后 healthz http=$DEAD（期待000=连接拒绝）"
echo "$RDY" | grep -qi "ready" && [ "$DEAD" = "000" ] && ok "H3 状态翻转正常" || bad "H3 (rdy=$RDY dead=$DEAD)"

summary

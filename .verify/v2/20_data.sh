#!/bin/bash
# v2 组20：数据一致性（B 系列）+ F4/F5 + H1/H2/H4
# B1 全量 stock / B2+B3 全类型增量 DML / B4 主键变更 / B5 联合主键 / B7 大事务 /
# B8 verify 注入差异 / B9 mask / B10 高频 UPDATE / F4 do_stock=false / F5 effective / H1/H2/H4
set -u
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

wipe_all
ccput verify "$V2/body_main.yml"
stub_start node1 verify :9595 :8060
grp "B1 全量 stock（并行快照）"
wait_ready 9595 60 || { bad "B1 启动"; exit 1; }
# 注意不能拿 lag=0 判 stock 完成（stock 期间位点不动、master 也未动 → lag 恒 0），必须轮询行数收敛
B1OK=0
for i in $(seq 1 30); do
  MATCH=1
  for t in shop.orders shop.users shop.order_items types.all_types rmqtest.msg cluster_e2e.tab; do
    S=$($SRC -N -e "SELECT COUNT(*) FROM $t" 2>/dev/null); D=$($DST -N -e "SELECT COUNT(*) FROM $t" 2>/dev/null)
    [ "$S" != "$D" ] && { MATCH=0; break; }
  done
  [ "$MATCH" = "1" ] && { B1OK=1; echo "  行数收敛 t=${i}x2s"; break; }
  sleep 2
done
SC=(); DC=()
for t in shop.orders shop.users shop.order_items types.all_types rmqtest.msg cluster_e2e.tab; do
  S=$($SRC -N -e "SELECT COUNT(*) FROM $t" 2>/dev/null); D=$($DST -N -e "SELECT COUNT(*) FROM $t" 2>/dev/null)
  SC+=("$S"); DC+=("$D"); echo "  $t src=$S dst=$D"
done
TOTAL=$(IFS=+; echo $(( ${SC[0]}+${SC[1]}+${SC[2]}+${SC[3]}+${SC[4]}+${SC[5]} )))
[ "$B1OK" = "1" ] && ok "B1 全量 stock 六表行数收敛（合计 $TOTAL 行）" || bad "B1 stock 行数不一致"
V=$(curl -s --noproxy '*' -u admin:demo123 -X POST 'localhost:8060/api/v1/verify?confirm=true' -H 'Content-Type: application/json' -d '{"database":"types","table":"all_types"}')
echo "  verify all_types: $(echo "$V" | head -c 150)"
echo "$V" | grep -q '"mismatch":0' && ok "B1b 干净表 verify 0 mismatch" || bad "B1b verify"

grp "B2/B3+B4+B5 全类型增量 DML / 主键变更 / 联合主键"
docker exec -i vf-src mysql -uroot -p123456 -h127.0.0.1 --default-character-set=utf8mb4 < "$V1/workload1.sql" 2>/dev/null
for i in $(seq 1 30); do L=$(lagof 9595); [ "$L" = "0" ] && { echo "  追平 t=${i}x2s"; break; }; sleep 2; done
# 行数收敛轮询（§5.1 第二层）：lag gauge 按 SaveInterval=3s 采样，workload 落库后首探
# 可能读到静默期的陈旧 0（2026-10-31 实证假「追平」→ types=5 假 FAIL，秒后自愈）。
# MySQL 落地断言必须轮询到期望值再判（期望值不变，非放松）。
B235OK=0
for i in $(seq 1 30); do
  T4=$($DST -N -e "SELECT COUNT(*) FROM types.all_types" 2>/dev/null)
  B4NEW=$($DST -N -e "SELECT COUNT(*) FROM shop.orders WHERE id=99999" 2>/dev/null)
  B4OLD=$($DST -N -e "SELECT COUNT(*) FROM shop.orders WHERE id=5" 2>/dev/null)
  B5A=$($DST -N -e "SELECT COUNT(*) FROM shop.order_items WHERE order_id=777" 2>/dev/null)
  B5T=$($DST -N -e "SELECT COUNT(*) FROM shop.order_items" 2>/dev/null)
  [ "$T4" = "7" ] && [ "$B4NEW" = "1" ] && [ "$B4OLD" = "0" ] && [ "$B5A" = "1" ] && [ "$B5T" = "101" ] && { B235OK=1; echo "  行数收敛 t=${i}x2s"; break; }
  sleep 2
done
[ "$B235OK" = "1" ] || {
  T4=$($DST -N -e "SELECT COUNT(*) FROM types.all_types" 2>/dev/null)
  B4NEW=$($DST -N -e "SELECT COUNT(*) FROM shop.orders WHERE id=99999" 2>/dev/null)
  B4OLD=$($DST -N -e "SELECT COUNT(*) FROM shop.orders WHERE id=5" 2>/dev/null)
  B5A=$($DST -N -e "SELECT COUNT(*) FROM shop.order_items WHERE order_id=777" 2>/dev/null)
  B5T=$($DST -N -e "SELECT COUNT(*) FROM shop.order_items" 2>/dev/null)
}
echo "  all_types=$T4（期待7=seed5+workload3-delete1）pk变更 new=$B4NEW old=$B4OLD（期待1/0）联合主键 777剩余=$B5A 总数=$B5T（期待1/101）"
[ "$T4" = "7" ] && [ "$B4NEW" = "1" ] && [ "$B4OLD" = "0" ] && [ "$B5A" = "1" ] && [ "$B5T" = "101" ] \
  && ok "B2-B5 全类型/主键变更/联合主键收敛" || bad "B2-B5 (types=$T4 new=$B4NEW old=$B4OLD items=$B5T)"
V=$(curl -s --noproxy '*' -u admin:demo123 -X POST 'localhost:8060/api/v1/verify?confirm=true' -H 'Content-Type: application/json' -d '{"database":"types","table":"all_types"}')
echo "$V" | grep -q '"mismatch":0' && ok "B2b 全类型极值/NULL/emoji verify=0" || bad "B2b verify: $(echo "$V"|head -c 200)"
N4=$($DST -N -e "SELECT c_ubig FROM types.all_types WHERE id=6" 2>/dev/null)
[ "$N4" = "18446744073709551615" ] && ok "B2c uint64 极值回绕重解释正确（id=6 行）" || bad "B2c c_ubig=$N4"

grp "F4 do_stock=false 只增量"
B4S=$($SRC -N -e "SELECT COUNT(*) FROM biz2.events" 2>/dev/null)
# 行数收敛轮询（§5.1 第二层，同 B2-B5 理由）
for i in $(seq 1 15); do
  B4D=$($DST -N -e "SELECT COUNT(*) FROM biz2.events" 2>/dev/null)
  B4I=$($DST -N -e "SELECT COUNT(*) FROM biz2.events WHERE kind='incr'" 2>/dev/null)
  [ "$B4D" = "2" ] && [ "$B4I" = "2" ] && { echo "  收敛 t=${i}x2s"; break; }
  sleep 2
done
echo "  biz2 src=$B4S dst=$B4D 其中增量=$B4I（期待22/2/2——seed20不同步,增量2同步）"
[ "$B4S" = "22" ] && [ "$B4D" = "2" ] && [ "$B4I" = "2" ] && ok "F4 预插不同步、增量同步" || bad "F4 (src=$B4S dst=$B4D incr=$B4I)"

grp "B8 verify 注入差异 → CDC 覆盖复 0"
$DST -e "UPDATE types.all_types SET c_int=999999 WHERE id=1" 2>/dev/null
V=$(curl -s --noproxy '*' -u admin:demo123 -X POST 'localhost:8060/api/v1/verify?confirm=true' -H 'Content-Type: application/json' -d '{"database":"types","table":"all_types"}')
echo "  注入后: $(echo "$V" | head -c 260)"
echo "$V" | grep -q '"mismatch":1' && echo "$V" | grep -q 'c_int' && ok "B8 注入差异逐列检出（明细含 c_int）" || bad "B8 检出"
$SRC -e "UPDATE types.all_types SET c_int=-2147483648 WHERE id=1" 2>/dev/null
B8OK=0
for i in $(seq 1 15); do
  sleep 2
  V=$(curl -s --noproxy '*' -u admin:demo123 -X POST 'localhost:8060/api/v1/verify?confirm=true' -H 'Content-Type: application/json' -d '{"database":"types","table":"all_types"}')
  echo "$V" | grep -q '"mismatch":0' && { B8OK=1; echo "  CDC 覆盖复 0 t=${i}x2s"; break; }
done
[ "$B8OK" = "1" ] && ok "B8b CDC 覆盖后复 0" || bad "B8b: $(echo "$V"|head -c 200)"

grp "B9 mask 脱敏"
MP=$($DST -N -e "SELECT phone FROM shop.users WHERE id=12" 2>/dev/null)
SP=$($SRC -N -e "SELECT phone FROM shop.users WHERE id=12" 2>/dev/null)
echo "  src=$SP dst=$MP（期待 139****0012）"
[ "$MP" = "139****0012" ] && ok "B9 mask 13900000012→139****0012" || bad "B9 dst=$MP"

grp "B10 同行 25 次 UPDATE + B7 5000 行大事务"
docker exec -i vf-src mysql -uroot -p123456 -h127.0.0.1 --default-character-set=utf8mb4 < "$V1/b7b10.sql" >/dev/null 2>&1
for i in $(seq 1 60); do L=$(lagof 9595); [ "$L" = "0" ] && { echo "  追平 t=${i}x2s"; break; }; sleep 2; done
# 行数收敛轮询（§5.1 第二层）：b7 5000 行大事务落地需要时间，单点 lag 采样可能陈旧
B7OK=0
for i in $(seq 1 30); do
  SA=$($SRC -N -e "SELECT amount FROM shop.orders WHERE id=99999" 2>/dev/null)
  DA=$($DST -N -e "SELECT amount FROM shop.orders WHERE id=99999" 2>/dev/null)
  DR=$($DST -N -e "SELECT remark FROM shop.orders WHERE id=99999" 2>/dev/null)
  SO=$($SRC -N -e "SELECT COUNT(*) FROM shop.orders" 2>/dev/null); DO=$($DST -N -e "SELECT COUNT(*) FROM shop.orders" 2>/dev/null)
  [ "$SA" = "$DA" ] && [ "$DR" = "b10-final" ] && [ "$SO" = "5100" ] && [ "$SO" = "$DO" ] && { B7OK=1; echo "  行数收敛 t=${i}x2s"; break; }
  sleep 2
done
[ "$B7OK" = "1" ] || {
  SA=$($SRC -N -e "SELECT amount FROM shop.orders WHERE id=99999" 2>/dev/null)
  DA=$($DST -N -e "SELECT amount FROM shop.orders WHERE id=99999" 2>/dev/null)
  DR=$($DST -N -e "SELECT remark FROM shop.orders WHERE id=99999" 2>/dev/null)
  SO=$($SRC -N -e "SELECT COUNT(*) FROM shop.orders" 2>/dev/null); DO=$($DST -N -e "SELECT COUNT(*) FROM shop.orders" 2>/dev/null)
}
echo "  b10 终值 src=$SA dst=$DA remark=$DR（期待25.50/b10-final）; b7 行数 src=$SO dst=$DO（期待5100）"
[ "$SA" = "$DA" ] && [ "$DR" = "b10-final" ] && [ "$SO" = "$DO" ] && [ "$SO" = "5100" ] \
  && ok "B10+B7 终值一致、大事务收敛" || bad "B10/B7 (src=$SA dst=$DA rows=$SO/$DO)"

grp "F5 /rules/effective 合并视图 + H1/H2/H4"
EFF=$(curl -s --noproxy '*' -u admin:demo123 localhost:8060/api/v1/rules/effective)
N=$(echo "$EFF" | python3 -c 'import sys,json;d=json.load(sys.stdin)["data"];rows=d if isinstance(d,list) else d.get("rules",d);print(len(rows))' 2>/dev/null)
AT=$(echo "$EFF" | python3 -c 'import sys,json;d=json.load(sys.stdin)["data"];rows=d if isinstance(d,list) else d.get("rules",d);print([r for r in rows if "all_types" in str(r.get("table",""))+str(r.get("table_ref",""))][0].get("ddl_policy"))' 2>/dev/null)
MK=$(echo "$EFF" | python3 -c 'import sys,json;d=json.load(sys.stdin)["data"];rows=d if isinstance(d,list) else d.get("rules",d);print([r for r in rows if str(r.get("table",""))=="users"][0].get("mask_columns"))' 2>/dev/null)
EX=$(echo "$EFF" | python3 -c 'import sys,json;d=json.load(sys.stdin)["data"];rows=d if isinstance(d,list) else d.get("rules",d);print(len([r for r in rows if "tmp_log" in str(r)]))' 2>/dev/null)
echo "  表数=$N all_types.ddl=$AT users.mask=$MK tmp_log命中=$EX"
[ "${N:-0}" -ge 8 ] && [ "$AT" = "auto" ] && [ "$MK" != "None" ] && [ "$EX" = "0" ] \
  && ok "F5 effective 合并正确（表级 auto 覆盖/mask/不含 exclude 表）" || bad "F5"
MF=$(curl -s --noproxy '*' localhost:9595/metrics | grep -oE '^gocdc_[a-z_]+' | sort -u | wc -l)
echo "  H1 指标族数=$MF"
[ "$MF" -ge 15 ] && ok "H1 指标族齐全" || bad "H1 指标族=$MF"
L2=$(lagof 9595)
[ "$L2" = "0" ] && ok "H2 追平口径 lag=0" || bad "H2 lag=$L2"
HITS=$(grep -cE '(password|pwd|root)["=: ]*123456|demo123' /tmp/v2/node1.log 2>/dev/null || true)
[ "${HITS:-0}" = "0" ] && ok "H4 日志无密码泄漏" || bad "H4 泄漏=$HITS"

killall_gocdc
summary

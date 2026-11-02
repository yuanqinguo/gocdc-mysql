#!/bin/bash
# examples/ 三形态示例配置验收（不入组清单——run_all_groups/tally/PLAYBOOK 不含
# 本脚本；复跑直接 bash 本文件，前台执行）。三个独立集群前缀互不污染：
#   ex-single ← examples/single.yml      （ha.enabled=false，role=single）
#   ex-cluster ← examples/cluster.yml    （ha.enabled=true，双节点选举+resign 切换）
#   ex-ms ← examples/multi-source.yml    （sources[] 双源聚合 ms_merged）
# 数据面依赖 wipe_all（主库 shop.*）+ ms_wipe（msa/msb/ms_merged）一次性重建。
set -u
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

EX(){ curl -s --max-time 5 --noproxy '*' -u admin:demo123 "localhost:$1$2" ${3:+-X $3}; }
# ex_leader <port1> <port2>：返回 role=leader 的端口（集群形态选主查询）
ex_leader(){ local p r; for p in "$@"; do r=$(roleof $p); [ "$r" = "leader" ] && { echo $p; return; }; done; echo ""; }

wipe_all
ms_wipe

# ════════ ① 单节点形态（examples/single.yml）════════
grp "EX-SINGLE 单节点（ha.enabled=false：role=single + 全量快照 + 增量）"
ccput gocdc-single "$REPO/examples/single.yml"
stub_start exs1 gocdc-single :19531 :18031
wait_ready 19531 40 && ok "EX-S1 boot healthz（配置加载+校验通过）" || { bad "EX-S1 boot"; tail -30 /tmp/v2/exs1.log; exit 1; }
[ "$(roleof 18031)" = "single" ] && ok "EX-S2 role=single（ha.enabled=false 生效）" || bad "EX-S2 role=$(roleof 18031)"
wait_src_state 18031 default running 60 && ok "EX-S3 管线 running（全量快照完成）" || bad "EX-S3 state=$(src_state 18031 default)"
SU=$($SRC -N -e "SELECT COUNT(*) FROM shop.users" 2>/dev/null)
DU=$($DST -N -e "SELECT COUNT(*) FROM shop.users" 2>/dev/null)
echo "  全量对账: src=$SU dst=$DU"
[ -n "$SU" ] && [ "$SU" = "$DU" ] && [ "$SU" -ge 1 ] && ok "EX-S4 全量快照 src=dst（$SU 行）" || bad "EX-S4 src=$SU dst=$DU"
$SRC -e "INSERT INTO shop.users (name,phone,created) VALUES ('ex-single',13900000099,NOW())" 2>/dev/null
wait_row "$DST" "SELECT COUNT(*) FROM shop.users WHERE name='ex-single'" 1 30 && ok "EX-S5 增量落目标" || bad "EX-S5 增量缺失"
killall_gocdc

# ════════ ② HA 集群形态（examples/cluster.yml，双节点）════════
grp "EX-CLUSTER HA 集群（ha.enabled=true：选举 leader/follower + 切换续传）"
ccput gocdc-cluster "$REPO/examples/cluster.yml"
stub_start exc1 gocdc-cluster :19532 :18032
stub_start exc2 gocdc-cluster :19533 :18033
wait_ready 19532 40 && wait_ready 19533 40 && ok "EX-C1 双节点 boot healthz" || bad "EX-C1 boot"
L1=""
for i in $(seq 1 25); do L1=$(ex_leader 18032 18033); [ -n "$L1" ] && break; sleep 2; done
[ -n "$L1" ] && ok "EX-C2 选举产生 leader=:$L1" || bad "EX-C2 选举超时"
FP1=$([ "$L1" = "18032" ] && echo 18033 || echo 18032)
[ "$(roleof $FP1)" = "follower" ] && ok "EX-C3 standby=:$FP1 role=follower" || bad "EX-C3 role=$(roleof $FP1)"
SO=$($SRC -N -e "SELECT COUNT(*) FROM shop.orders" 2>/dev/null)
DO=0
for i in $(seq 1 30); do DO=$($DST -N -e "SELECT COUNT(*) FROM shop.orders" 2>/dev/null); [ "$DO" = "$SO" ] && break; sleep 2; done
echo "  全量对账: src=$SO dst=$DO"
[ "$SO" = "$DO" ] && [ "$SO" = "100" ] && ok "EX-C4 全量快照 100 行收敛" || bad "EX-C4 src=$SO dst=$DO"
$SRC -e "INSERT INTO shop.orders (user_id,amount,remark) VALUES (99,9.9,'ex-cluster-a')" 2>/dev/null
wait_row "$DST" "SELECT COUNT(*) FROM shop.orders WHERE remark='ex-cluster-a'" 1 30 && ok "EX-C5 leader 期增量落目标" || bad "EX-C5 增量缺失"
EXC=$(EX $L1 /api/v1/leader/resign?confirm=true -X POST | head -c 60)
L2=""
for i in $(seq 1 25); do L2=$(ex_leader 18032 18033); [ -n "$L2" ] && [ "$L2" != "$L1" ] && break; sleep 2; done
[ -n "$L2" ] && ok "EX-C6 resign 切换 → 新 leader=:$L2" || bad "EX-C6 切换失败（resign=$EXC）"
$SRC -e "INSERT INTO shop.orders (user_id,amount,remark) VALUES (99,8.8,'ex-cluster-b')" 2>/dev/null
wait_row "$DST" "SELECT COUNT(*) FROM shop.orders WHERE remark='ex-cluster-b'" 1 30 && ok "EX-C7 新 leader 增量续传零丢失" || bad "EX-C7 切换后增量缺失"
killall_gocdc

# ════════ ③ 多源形态（examples/multi-source.yml）════════
grp "EX-MS 多源（sources[] 双源聚合：两源 running + 各自增量落 ms_merged）"
ccput gocdc-ms "$REPO/examples/multi-source.yml"
stub_start exm1 gocdc-ms :19534 :18034
wait_ready 19534 40 && ok "EX-M1 boot healthz（sources[] 校验+V4 通过）" || { bad "EX-M1 boot"; tail -30 /tmp/v2/exm1.log; exit 1; }
NS=$(EX 18034 /api/v1/sources | jget 'len(d)' 2>/dev/null)
[ "$NS" = "2" ] && ok "EX-M2 /sources 返回 2 源" || bad "EX-M2 n=$NS"
wait_src_state 18034 src-a running 90 && ok "EX-M3 src-a running（workers=8 源级覆盖生效）" || bad "EX-M3 state=$(src_state 18034 src-a)"
wait_src_state 18034 src-b running 90 && ok "EX-M4 src-b running（继承集群默认）" || bad "EX-M4 state=$(src_state 18034 src-b)"
A1=$($SRC -N -e "SELECT COUNT(*) FROM msa.orders" 2>/dev/null)
B1=$($SRC2 -N -e "SELECT COUNT(*) FROM msb.users" 2>/dev/null)
MA=0; MB=0
for i in $(seq 1 30); do
  MA=$($DST -N -e "SELECT COUNT(*) FROM ms_merged.orders" 2>/dev/null)
  MB=$($DST -N -e "SELECT COUNT(*) FROM ms_merged.users" 2>/dev/null)
  [ "$MA" = "$A1" ] && [ "$MB" = "$B1" ] && break; sleep 2
done
echo "  全量对账: src-a=$A1→ms_merged.orders=$MA / src-b=$B1→ms_merged.users=$MB"
[ "$MA" = "$A1" ] && [ "$MB" = "$B1" ] && ok "EX-M5 双源全量快照各自收敛" || bad "EX-M5 a=$A1/$MA b=$B1/$MB"
$SRC -e "INSERT INTO msa.orders (val,amt) VALUES ('ex-ms-a',1.1)" 2>/dev/null
$SRC2 -e "INSERT INTO msb.users (name,phone) VALUES ('ex-ms-b','13900000123')" 2>/dev/null
wait_row "$DST" "SELECT COUNT(*) FROM ms_merged.orders WHERE val='ex-ms-a'" 1 30 && ok "EX-M6 src-a 增量落 ms_merged.orders" || bad "EX-M6 src-a 增量缺失"
wait_row "$DST" "SELECT COUNT(*) FROM ms_merged.users WHERE name='ex-ms-b'" 1 30 && ok "EX-M7 src-b 增量落 ms_merged.users" || bad "EX-M7 src-b 增量缺失"
killall_gocdc

summary

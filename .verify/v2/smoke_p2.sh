#!/bin/bash
# P2 冒烟（临时脚本，不入套件）：单源 v1 配置（自动包装 default）经新运行时
# 完整跑一轮 boot → 增量同步 → /status running → 键空间 v2 落位 → 位点续传。
set -u
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

wipe_all
grp "P2-SMOKE ① 单源 v1 配置 boot（body_single，ha=false）"
flagput verify-single "$V2/body_single.yml"
flag_start s1 verify-single :19595 :18060
wait_ready 19595 30 && ok "单源 boot healthz" || { bad "boot"; tail -30 /tmp/v2/s1.log; exit 1; }

# 键空间 v2 落位：position 在 sources/default/ 下
sleep 3
P=$($ETCDC get "/gocdc/verify-single/sources/default/position" --print-value-only 2>/dev/null | head -c 40)
[ -n "$P" ] && ok "键空间 v2：sources/default/position 已落位" || bad "v2 position key 缺失"
OLD=$($ETCDC get "/gocdc/verify-single/position" --print-value-only 2>/dev/null)
[ -z "$OLD" ] && ok "旧键 /position 未被使用" || bad "旧键 /position 存在值"

grp "P2-SMOKE ② 增量同步（rmqtest.msg 白名单表）"
$SRC -e "INSERT INTO rmqtest.msg (name,qty,amount) VALUES ('p2-smoke',1,9.90)" 2>/dev/null
sleep 3
N=$($DST -N -e "SELECT COUNT(*) FROM rmqtest.msg WHERE name='p2-smoke'" 2>/dev/null)
[ "$N" = "1" ] && ok "增量写入同步到目标" || { bad "增量未同步"; tail -20 /tmp/v2/s1.log; }

grp "P2-SMOKE ③ /status running + API 兼容"
ST=$(curl -s --noproxy '*' -u admin:demo123 "localhost:18060/api/v1/status")
echo "$ST" | jget 'd["sources"][0]["pipeline_state"]' | grep -qx "running" && ok "/status pipeline_state=running" || bad "state=$(echo "$ST" | head -c 300)"
echo "$ST" | jget 'd["role"]' | grep -qx "single" && ok "role=single" || bad "role"

grp "P2-SMOKE ④ 重启续传（位点在 v2 键）"
killall_gocdc
flag_start s2 verify-single :19595 :18060
wait_ready 19595 30 && ok "重启 boot" || bad "重启 boot"
$SRC -e "INSERT INTO rmqtest.msg (name,qty,amount) VALUES ('p2-smoke2',2,19.90)" 2>/dev/null
sleep 3
N=$($DST -N -e "SELECT COUNT(*) FROM rmqtest.msg WHERE name IN ('p2-smoke','p2-smoke2')" 2>/dev/null)
[ "$N" = "2" ] && ok "重启后增量续传（v2 位点生效）" || bad "续传缺失 N=$N"

killall_gocdc
summary

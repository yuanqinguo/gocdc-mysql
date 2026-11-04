#!/bin/bash
# v2 组55：管线人工暂停/恢复（PP 系列，13 §3）——
# PP-01 pause confirm 门禁 / PP-02 暂停受理（paused + paused_by_op + 保持 Leader）/
# PP-03 重复暂停拒绝 / PP-04 暂停期增量冻结（积压 binlog + 位点停摆）/
# PP-05 resume 门禁/受理/重复拒绝 / PP-06 恢复续跑收敛 / PP-07 暂停期改位点
# （SET-POSITION 沿用 gtid_set——整值覆盖抹空集合=恢复即熔断的回归）+ 恢复采用 /
# PP-08 暂停标志跨重启（本任期不启动）+ 恢复续跑 / PP-09 审计留痕
set -u
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
API="localhost:8060/api/v1"
ST(){ curl -s --max-time 3 --connect-timeout 2 --noproxy '*' -u admin:demo123 "$API/status" | jget 'd["sources"][0]["pipeline_state"]' 2>/dev/null; echo; }
SOP(){ curl -s --max-time 3 --connect-timeout 2 --noproxy '*' -u admin:demo123 "$API/status" | jget 'd["sources"][0].get("pipeline_paused_by_op")' 2>/dev/null; echo; }

wipe_all
ccput verify "$V2/body_main.yml"
stub_start node1 verify :9595 :8060
wait_ready 9595 60 || { echo "FATAL 启动失败"; exit 1; }
for i in $(seq 1 30); do D=$($DST -N -e "SELECT COUNT(*) FROM shop.orders" 2>/dev/null); [ "$D" = "100" ] && break; sleep 2; done
for i in $(seq 1 20); do LB=$(lagof 9595); [ "$LB" = "0" ] && break; sleep 2; done

grp "PP-01 pause confirm 门禁（无 confirm=400）"
CP=$(curl -s --noproxy '*' -o /dev/null -w '%{http_code}' -u admin:demo123 -X POST "$API/sources/default/pipeline/pause")
echo "  pause 无 confirm=$CP（期待400）"
[ "$CP" = "400" ] && ok "PP-01 pause confirm 门禁" || bad "PP-01 ($CP)"

grp "PP-02 人工暂停受理（paused + paused_by_op + 保持 Leader）"
R=$(curl -s --noproxy '*' -u admin:demo123 -X POST "$API/sources/default/pipeline/pause?confirm=true" | head -c 120)
echo "  pause(resp)=$R"
PSTOP=0
for i in $(seq 1 20); do [ "$(ST)" = "paused" ] && PSTOP=1 && break; sleep 1; done
OP=$(SOP); ROLE=$(roleof 8060)
echo "  轮询到 paused=$PSTOP paused_by_op=$OP role=$ROLE（期待1/True/leader）"
echo "$R" | grep -q '"code":0' && [ "$PSTOP" = "1" ] && [ "$OP" = "True" ] && [ "$ROLE" = "leader" ] \
  && ok "PP-02 暂停受理且保持 Leader/进程在线" || bad "PP-02 (state=$PSTOP op=$OP role=$ROLE)"
sleep 4   # 等暂停前最后一个位点落盘周期（SaveInterval 3s）结束，PP-04 基准才干净

grp "PP-03 重复暂停拒绝（状态机 CAS 防重）"
R2=$(curl -s --noproxy '*' -u admin:demo123 -X POST "$API/sources/default/pipeline/pause?confirm=true" | head -c 200)
echo "  二次pause(resp)=$R2"
echo "$R2" | grep -q '"code":0' && bad "PP-03 二次暂停不应成功" || ok "PP-03 重复暂停被拒（已处于人工暂停状态）"

grp "PP-04 暂停期增量冻结（零投递 + 位点停摆）"
UPD0=$(curl -s --noproxy '*' -u admin:demo123 "$API/sources/default/position" | jget 'd["updated_at"]' 2>/dev/null)
$SRC -e "INSERT INTO shop.orders (user_id,amount,remark) VALUES (901,9.01,'pp-backlog-1'),(902,9.02,'pp-backlog-2'),(903,9.03,'pp-backlog-3'),(904,9.04,'pp-backlog-4'),(905,9.05,'pp-backlog-5')" 2>/dev/null
sleep 8
BK=$($DST -N -e "SELECT COUNT(*) FROM shop.orders WHERE remark LIKE 'pp-backlog%'" 2>/dev/null)
UPD1=$(curl -s --noproxy '*' -u admin:demo123 "$API/sources/default/position" | jget 'd["updated_at"]' 2>/dev/null)
echo "  暂停期落地=$BK（期待0）位点 updated_at 前=$UPD0 后=$UPD1"
[ "$BK" = "0" ] && [ -n "$UPD0" ] && [ "$UPD0" = "$UPD1" ] && ok "PP-04 暂停期零投递位点冻结" || bad "PP-04 (bk=$BK upd:$UPD0→$UPD1)"

grp "PP-05 resume 门禁/受理/重复拒绝"
CR=$(curl -s --noproxy '*' -o /dev/null -w '%{http_code}' -u admin:demo123 -X POST "$API/sources/default/pipeline/resume")
echo "  resume 无 confirm=$CR（期待400）"
RR=$(curl -s --noproxy '*' -u admin:demo123 -X POST "$API/sources/default/pipeline/resume?confirm=true" | head -c 120)
echo "  resume(resp)=$RR"
RUNOK=0
for i in $(seq 1 30); do [ "$(ST)" = "running" ] && RUNOK=1 && break; sleep 2; done
RR2=$(curl -s --noproxy '*' -u admin:demo123 -X POST "$API/sources/default/pipeline/resume?confirm=true" | head -c 200)
echo "  running=$RUNOK 二次resume=$RR2"
[ "$CR" = "400" ] && echo "$RR" | grep -q '"code":0' && [ "$RUNOK" = "1" ] \
  && echo "$RR2" | grep -vq '"code":0' && ok "PP-05 resume 门禁/受理/重复拒绝" || bad "PP-05 (cr=$CR run=$RUNOK r2=$RR2)"

grp "PP-06 恢复续跑：暂停期积压收敛"
BK6=0
for i in $(seq 1 20); do
  BK6=$($DST -N -e "SELECT COUNT(*) FROM shop.orders WHERE remark LIKE 'pp-backlog%'" 2>/dev/null)
  [ "$BK6" = "5" ] && break; sleep 2
done
for i in $(seq 1 15); do LB=$(lagof 9595); [ "$LB" = "0" ] && break; sleep 2; done
VM6=$(curl -s --noproxy '*' -u admin:demo123 -X POST -H 'Content-Type: application/json' \
  -d '{"database":"shop","table":"orders","pk":"id"}' "$API/sources/default/verify?confirm=true" | jget 'd["mismatch"]' 2>/dev/null)
echo "  积压落地=$BK6/5 lag=$LB verify_mismatch=$VM6"
[ "$BK6" = "5" ] && [ "$LB" = "0" ] && [ "$VM6" = "0" ] && ok "PP-06 恢复后积压收敛且对账一致" || bad "PP-06 (bk=$BK6 lag=$LB vm=$VM6)"

grp "PP-07 暂停期改位点：SET-POSITION 沿用 gtid_set + 恢复采用"
# 回归背景：Store.Save 整值覆盖，若 SET-POSITION 只写 name/pos 会把 GTID 集抹空
# （GTID 模式恢复即熔断「位点缺 gtid_set」）——修复后以 etcd 现值为底沿用。
POS7=$(curl -s --noproxy '*' -u admin:demo123 "$API/sources/default/position" | python3 -c 'import sys,json;d=json.load(sys.stdin)["data"];print(d["name"],d["pos"],d.get("gtid_set",""))' 2>/dev/null)
N7=$(echo $POS7 | awk '{print $1}'); P7=$(echo $POS7 | awk '{print $2}'); G7=$(echo $POS7 | awk '{print $3}')
BACK=$(python3 -c "print(max(4, $P7 - 20000))")
curl -s --noproxy '*' -u admin:demo123 -X POST "$API/sources/default/pipeline/pause?confirm=true" >/dev/null
for i in $(seq 1 20); do [ "$(ST)" = "paused" ] && break; sleep 1; done
SP=$(curl -s --noproxy '*' -u admin:demo123 -X POST -H 'Content-Type: application/json' \
  -d "{\"name\":\"$N7\",\"pos\":$BACK,\"confirm\":\"SET-POSITION\"}" "$API/sources/default/position" | head -c 120)
sleep 1
GT7=$(curl -s --noproxy '*' -u admin:demo123 "$API/sources/default/position" | jget 'd.get("gtid_set","")' 2>/dev/null)
echo "  回拨 $N7:$P7→$BACK (resp)=$SP"
echo "  改后位点 gtid_set 保留=${GT7:0:40}（期待非空=uuid:1）"
curl -s --noproxy '*' -u admin:demo123 -X POST "$API/sources/default/pipeline/resume?confirm=true" >/dev/null
RUN7=0
for i in $(seq 1 30); do [ "$(ST)" = "running" ] && RUN7=1 && break; sleep 2; done
ERR7=$(curl -s --noproxy '*' -u admin:demo123 "$API/status" | jget 'd.get("pipeline_error","")' 2>/dev/null)
for i in $(seq 1 15); do LB7=$(lagof 9595); [ "$LB7" = "0" ] && break; sleep 2; done
DST7=$($DST -N -e "SELECT COUNT(*) FROM shop.orders WHERE remark LIKE 'pp-backlog%'" 2>/dev/null)
echo "  恢复后 running=$RUN7 error=[$ERR7] lag=$LB7 积压仍=$DST7/5"
echo "$SP" | grep -q '"code":0' && [ -n "$GT7" ] && [ "$RUN7" = "1" ] && [ "$LB7" = "0" ] && [ "$DST7" = "5" ] \
  && ok "PP-07 SET-POSITION 保留 GTID 集，恢复采用新位点续跑" || bad "PP-07 (gt=[${GT7:0:12}] run=$RUN7 lag=$LB7 dst=$DST7)"

grp "PP-08 暂停标志跨重启（本任期不启动，显式恢复后续跑）"
curl -s --noproxy '*' -u admin:demo123 -X POST "$API/sources/default/pipeline/pause?confirm=true" >/dev/null
for i in $(seq 1 20); do [ "$(ST)" = "paused" ] && break; sleep 1; done
kill -9 $(pickpid node1) 2>/dev/null
stub_start node1 verify :9595 :8060
wait_ready 9595 60 || bad "PP-08 重启失败"
R8=0
for i in $(seq 1 40); do
  [ "$(ST)" = "paused" ] && [ "$(SOP)" = "True" ] && R8=1 && break; sleep 2
done
D8=$($DST -N -e "SELECT COUNT(*) FROM shop.orders" 2>/dev/null)
echo "  重启后(当选+读标志) paused+op=$R8 行数=$D8（暂停态未被覆盖/未重扫）"
curl -s --noproxy '*' -u admin:demo123 -X POST "$API/sources/default/pipeline/resume?confirm=true" >/dev/null
RUN8=0
for i in $(seq 1 30); do [ "$(ST)" = "running" ] && RUN8=1 && break; sleep 2; done
$SRC -e "INSERT INTO shop.orders (user_id,amount,remark) VALUES (911,9.11,'pp-after-restart-1'),(912,9.12,'pp-after-restart-2'),(913,9.13,'pp-after-restart-3')" 2>/dev/null
D8B=0
for i in $(seq 1 20); do
  D8B=$($DST -N -e "SELECT COUNT(*) FROM shop.orders" 2>/dev/null)
  [ "$D8B" = "108" ] && break; sleep 2
done
echo "  恢复后 running=$RUN8 恢复续跑收敛=$D8B/108"
[ "$R8" = "1" ] && [ "$D8" = "105" ] && [ "$RUN8" = "1" ] && [ "$D8B" = "108" ] \
  && ok "PP-08 标志跨重启生效 + 恢复后续跑收敛" || bad "PP-08 (flag=$R8 d8=$D8 run=$RUN8 d8b=$D8B)"

grp "PP-09 pause/resume 审计留痕（Web /audit + etcd events/audit）"
# what 存储形态 = "what path detail" 拼接串 → Web 侧按前缀匹配（G4 同法）；
# etcd 环形真身 key = {prefix}/events/audit（装配双重拼接已修复，ADV-17）
AUD=$(curl -s --noproxy '*' -u admin:demo123 "$API/audit")
HIT=$(echo "$AUD" | python3 -c 'import sys,json; rows=json.load(sys.stdin)["data"] or []; ps=[r for r in rows if r.get("what","").startswith("pipeline.pause")]; rs=[r for r in rows if r.get("what","").startswith("pipeline.resume")]; print(f"{len(ps)}|{len(rs)}")' 2>/dev/null)
ETP=$($ETCDC get /gocdc/verify/events/audit --print-value-only 2>/dev/null | grep -c 'pipeline\.pause' || true)
ETR=$($ETCDC get /gocdc/verify/events/audit --print-value-only 2>/dev/null | grep -c 'pipeline\.resume' || true)
echo "  Web审计 pause|resume=$HIT  etcd含pause=$ETP resume=$ETR"
echo "$HIT" | grep -qE '^[1-9]+\|[1-9]+$' && [ "${ETP:-0}" -ge 1 ] && [ "${ETR:-0}" -ge 1 ] \
  && ok "PP-09 审计 who/what 双通道留痕" || bad "PP-09 (web=$HIT etcd=$ETP/$ETR)"

killall_gocdc
summary

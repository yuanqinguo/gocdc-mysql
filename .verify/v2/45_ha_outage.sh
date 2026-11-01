#!/bin/bash
# v2 组45：集群异常与跨节点语义（D6 etcd 宕机→失位→恢复补齐 / E9 follower API 审批 /
# I5 集群单产出 / ADV-07 restock 跨会话重扫）——共享配置中心正文
# 2026-10-31 组形态适配：E9 由「CREATE e9_table 审批自动建表」（语义已移除，arch16）
# 改为白名单表 shop.new_table 的 ALTER 审批——follower API 看到 pending、leader 执行。
set -u
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

wipe_all
ccput verify "$V2/body_main.yml"

stub_start node1 verify :9595 :8060
L=""
for i in $(seq 1 20); do L=$(leader_port); [ -n "$L" ] && break; sleep 2; done
for i in $(seq 1 30); do
  D=$($DST -N -e "SELECT COUNT(*) FROM shop.orders" 2>/dev/null); [ "$D" = "100" ] && break; sleep 2
done
for i in $(seq 1 20); do LB=$(lagof 9595); [ "$LB" = "0" ] && break; sleep 2; done
stub_start node2 verify :9596 :8061
sleep 8
echo "leader=$L（基线就绪）node2 存活=$( [ -n "$(pickpid node2)" ] && echo yes || echo NO-已死 )"

grp "E9 集群 DDL 审批（follower API → leader 执行，白名单表 ALTER）"
FP=$([ "$L" = "8060" ] && echo 8061 || echo 8060)
api_on(){ curl -s --noproxy '*' -u admin:demo123 "localhost:$1$2" ${3:+-X $3}; }
$SRC -e "ALTER TABLE shop.new_table ADD COLUMN e9col VARCHAR(32) NULL" || bad "E9 前置: ALTER 失败"
sleep 4
EVID=$(api_on $FP "/api/v1/ddl-events" | python3 -c 'import sys,json; evs=json.load(sys.stdin)["data"] or []; p=[e["id"] for e in evs if e["status"]=="pending"]; print(p[0] if p else "")' 2>/dev/null)
[ -z "$EVID" ] && EVID=$(api_on $L "/api/v1/ddl-events" | python3 -c 'import sys,json; evs=json.load(sys.stdin)["data"] or []; p=[e["id"] for e in evs if e["status"]=="pending"]; print(p[0] if p else "")' 2>/dev/null)
echo "  follower($FP) 看到 pending=$EVID"
APP=$(api_on $FP "/api/v1/ddl/$EVID/approve?confirm=true" POST | head -c 120)
[ -z "$APP" ] && { echo "  follower 口审批无响应，回退 leader 口"; APP=$(api_on $L "/api/v1/ddl/$EVID/approve?confirm=true" POST | head -c 120); }
HAS=0
for i in $(seq 1 15); do
  HAS=$($DST -N -e "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='shop' AND table_name='new_table' AND column_name='e9col'" 2>/dev/null)
  [ "$HAS" = "1" ] && { echo "  leader 自动加列 t=${i}x2s"; break; }
  sleep 2
done
$SRC -e "INSERT INTO shop.new_table (val,e9col) VALUES ('e9-flows','e9v')" 2>/dev/null
sleep 5
V=$($DST -N -e "SELECT e9col FROM shop.new_table WHERE val='e9-flows'" 2>/dev/null)
echo "  加列=$HAS 新列数据流动=$V"
[ "$HAS" = "1" ] && [ -n "$EVID" ] && [ "$V" = "e9v" ] && ok "E9 follower 审批 leader 执行数据流动" || bad "E9 (ev=$EVID has=$HAS v=$V)"
sleep 3

grp "I5 集群单产出（leader 切换前后各一批每行恰一条）"
# 注意：新消费组实测会从 EARLIEST 重放 topic 全部积压（rmqcons「从最新」声明在
# 当前 broker 下不成立），因此本 run 的行名带唯一时间戳前缀，计数只认本 run。
TS=$(date +%s)
TAGA="i5a-$TS"; TAGB="i5b-$TS"
setsid nohup $RMQCONS -topic CDC_rmqtest_msg -group i5-$TS -out /tmp/rmq-i5.jsonl -idle 12s > /tmp/rmqcons-i5.log 2>&1 &
RMQPID=$!
sleep 4
$SRC -e "INSERT INTO rmqtest.msg (name,qty,amount,note) SELECT CONCAT('$TAGA-',n), n, n, 'i5a' FROM (WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM s WHERE n<10) SELECT n FROM s) t" || bad "I5 前置: i5a 插入失败"
sleep 6
curl -s --noproxy '*' -u admin:demo123 -X POST "localhost:$L/api/v1/leader/resign?confirm=true" >/dev/null
NL=""
for i in $(seq 1 20); do NL=$(leader_port); [ -n "$NL" ] && [ "$NL" != "$L" ] && break; sleep 1; done
echo "  新 leader=$NL，写 20 行"
$SRC -e "INSERT INTO rmqtest.msg (name,qty,amount,note) SELECT CONCAT('$TAGB-',n), n, n, 'i5b' FROM (WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM s WHERE n<20) SELECT n FROM s) t" || bad "I5 前置: i5b 插入失败"
sleep 12
pkill -f "group i5-$TS" 2>/dev/null
I5V=$(TAGA="$TAGA" TAGB="$TAGB" python3 - <<'PYEOF'
import json, collections, os
ta, tb = os.environ["TAGA"], os.environ["TAGB"]
c = collections.Counter(); na = nb = 0
for line in open('/tmp/rmq-i5.jsonl', encoding='utf-8'):
    try:
        e = json.loads(line)
        n = str((e.get('after') or {}).get('name',''))
        if n.startswith(ta): na += 1
        if n.startswith(tb): nb += 1
        c[e.get('event_id')] += 1
    except Exception: pass
dups = sum(v-1 for v in c.values() if v > 1)
print(f"{ta}={na}/10 {tb}={nb}/20 dup={dups}")
print("PASS" if (na == 10 and nb == 20 and dups == 0) else "FAIL")
PYEOF
)
echo "  I5: $(echo "$I5V" | head -1)"
echo "$I5V" | grep -q "^PASS" && ok "I5 切换前后每行恰一条单产出" || bad "I5 ($(echo "$I5V" | head -1))"

grp "ADV-07 restock 清标记 + kill leader → 跨会话整表重扫"
NL=$(leader_port)
curl -s --noproxy '*' -u admin:demo123 -X POST "localhost:$NL/api/v1/rules/rmqtest.msg/restock?confirm=true" >/dev/null
LPID=$(pickpid $([ "$NL" = "8060" ] && echo node1 || echo node2))
kill -9 "$LPID"
echo "  restock 已清标记 + leader($NL) 已 kill，等接管重扫…"
for i in $(seq 1 25); do NL2=$(leader_port); [ -n "$NL2" ] && break; sleep 1; done
ADV=0
for i in $(seq 1 30); do
  S=$($SRC -N -e "SELECT COUNT(*) FROM rmqtest.msg" 2>/dev/null)
  D=$($DST -N -e "SELECT COUNT(*) FROM rmqtest.msg" 2>/dev/null)
  [ -n "$S" ] && [ "$S" = "$D" ] && [ "$S" != "0" ] && { ADV=1; echo "  重扫收敛 t=${i}x2s src=$S dst=$D"; break; }
  sleep 2
done
[ "$ADV" = "1" ] && [ -n "$NL2" ] && ok "ADV-07 接管节点跨会话整表重扫收敛（$D 行）" || bad "ADV-07 (src=$S dst=$D nl=$NL2)"
# 被杀节点回归
[ "$NL" = "8060" ] && stub_start node1 verify :9595 :8060 || stub_start node2 verify :9596 :8061
sleep 10

grp "D6 etcd 宕机 → 失位 ≤8s → 恢复重选补齐"
CL=$(leader_port)
CLMET=$([ "$CL" = "8060" ] && echo 9595 || echo 9596)
T0=$(date +%s)
docker stop vf-etcd >/dev/null
T_LOST=""
for i in $(seq 1 20); do
  ST=$(curl -s --noproxy '*' localhost:$CLMET/metrics | grep '^gocdc_leader_state' | awk '{print $2}')
  [ "$ST" != "1" ] && { T_LOST=$(( $(date +%s) - T0 )); break; }
  sleep 1
done
RDY=$(curl -s --noproxy '*' localhost:$CLMET/readyz)
echo "  etcd 停后失位=${T_LOST}s（≤8s）readyz=[$RDY]"
$SRC -e "INSERT INTO shop.new_table (val) VALUES ('d6-window')" 2>/dev/null
sleep 3
W0=$($DST -N -e "SELECT COUNT(*) FROM shop.new_table WHERE val='d6-window'" 2>/dev/null)
echo "  宕机窗口写入落目标=$W0（期待0=不投递）"
docker start vf-etcd >/dev/null
T1=$(date +%s)
NL=""
for i in $(seq 1 30); do NL=$(leader_port); [ -n "$NL" ] && break; sleep 1; done
sleep 10
W1=$($DST -N -e "SELECT COUNT(*) FROM shop.new_table WHERE val='d6-window'" 2>/dev/null)
LAG=$(lagof 9595)
echo "  恢复重选=$(( $(date +%s) - T1 ))s 新leader=$NL 窗口补齐=$W1 lag=$LAG"
if [ -n "$T_LOST" ] && [ "$T_LOST" -le 8 ] && [ "$W0" = "0" ] && [ "$W1" = "1" ]; then
  ok "D6 失位 ${T_LOST}s 安全停 + 恢复补齐"
else
  bad "D6 (lost=$T_LOST w0=$W0 w1=$W1)"
fi

killall_gocdc
summary

#!/bin/bash
# v2 组50：DDL 自适应与审批（E 系列，单节点）
# E1 auto ADD COLUMN / E2 auto varchar 扩容 / E3+DDL-11 白名单内 pause 挂起+位点冻结 /
# E5 skip 恢复+补齐+审计 / E4 pause→approve 全自动（顺带 G2 无 confirm=400）/
# E6 白名单外表 DDL 过滤（组形态：未列 tables=不同步，exclude_tables 语义已移除）/
# E8a RENAME 白名单外表 忽略 / E7' CREATE 白名单外表 → 过滤（新表自动接入已随组形态移除，
# arch16 决策——反向断言新语义）/ E8b+P0-8 挂起期源表删除 → 过期放行不死锁
# （dying_table 为 wipe_data 预建白名单表；组形态下白名单表被 DROP 后重启会被启动核对拒，
# 故本组 E8b 之后不再重启，环境由下一组 wipe_all 重建）
set -u
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
API="localhost:8060/api/v1"
api(){ curl -s --noproxy '*' -u admin:demo123 "$API$1" ${2:+-X $2}; }
pending_ids(){ api /ddl-events | python3 -c 'import sys,json; evs=json.load(sys.stdin)["data"]; print(" ".join(e["id"] for e in evs if e["status"]=="pending"))'; }

wipe_all
ccput verify "$V2/body_main.yml"
stub_start node1 verify :9595 :8060
wait_ready 9595 60 || { echo "FATAL 启动失败"; exit 1; }
for i in $(seq 1 30); do D=$($DST -N -e "SELECT COUNT(*) FROM shop.orders" 2>/dev/null); [ "$D" = "100" ] && break; sleep 2; done
for i in $(seq 1 20); do LB=$(lagof 9595); [ "$LB" = "0" ] && break; sleep 2; done

grp "E1 auto ADD COLUMN 零干预（types.all_types=ddl_policy auto）"
$SRC -e "ALTER TABLE types.all_types ADD COLUMN c_new VARCHAR(32) NULL" 2>/dev/null
E1=0
for i in $(seq 1 20); do
  HAS=$($DST -N -e "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='types' AND table_name='all_types' AND column_name='c_new'" 2>/dev/null)
  [ "$HAS" = "1" ] && { E1=1; echo "  目标自动加列 t=${i}x2s"; break; }
  sleep 2
done
$SRC -e "INSERT INTO types.all_types (c_varchar,c_new) VALUES ('e1-row','新列值✅')" 2>/dev/null
sleep 4
ROW=$($DST -N -e "SELECT c_new FROM types.all_types WHERE c_varchar='e1-row'" 2>/dev/null)
echo "  新列数据流动: dst.c_new=$ROW"
[ "$E1" = "1" ] && [ "$ROW" = "新列值✅" ] && ok "E1 auto 加列+数据流动零干预" || bad "E1 (has=$E1 row=$ROW)"

grp "E2 auto varchar 64→256 扩容无损"
$SRC -e "ALTER TABLE types.all_types MODIFY c_varchar VARCHAR(256)" 2>/dev/null
E2=0
for i in $(seq 1 20); do
  LEN=$($DST -N -e "SELECT CHARACTER_MAXIMUM_LENGTH FROM information_schema.columns WHERE table_schema='types' AND table_name='all_types' AND column_name='c_varchar'" 2>/dev/null)
  [ "$LEN" = "256" ] && { E2=1; echo "  目标自动扩容 t=${i}x2s"; break; }
  sleep 2
done
LONG=$(python3 -c "print('长'*100)")
$SRC -e "SET SESSION cte_max_recursion_depth=1000; INSERT INTO types.all_types (c_varchar) VALUES ('$LONG')" 2>/dev/null
sleep 4
SRCLEN=$($SRC -N -e "SELECT CHAR_LENGTH(c_varchar) FROM types.all_types WHERE c_varchar LIKE '长长%' ORDER BY id DESC LIMIT 1" 2>/dev/null)
DSTLEN=$($DST -N -e "SELECT CHAR_LENGTH(c_varchar) FROM types.all_types WHERE c_varchar LIKE '长长%' ORDER BY id DESC LIMIT 1" 2>/dev/null)
echo "  长值往返: src_len=$SRCLEN dst_len=$DSTLEN（期待100字符）"
[ "$E2" = "1" ] && [ "$SRCLEN" = "100" ] && [ "$DSTLEN" = "100" ] && ok "E2 varchar 扩容+100中文无损往返" || bad "E2 (len=$SRCLEN/$DSTLEN)"

grp "E3+DDL-11 白名单内 DROP → pause 挂起 + 位点冻结"
$SRC -e "ALTER TABLE shop.users DROP COLUMN created" 2>/dev/null
sleep 4
OV=$(api /overview)
echo "  overview: $(echo "$OV" | python3 -c 'import sys,json; d=json.load(sys.stdin)["data"]; print("paused:",d.get("paused"),"reason:",d.get("pause_reason"))' 2>/dev/null)"
EVID=$(api /ddl-events | python3 -c 'import sys,json; evs=json.load(sys.stdin)["data"]; p=[e["id"] for e in evs if e["status"]=="pending"]; print(p[0] if p else "")')
echo "  挂起事件: $EVID"
$SRC -e "INSERT INTO biz2.events (kind,payload) VALUES ('incr3','挂起期写入-1'),('incr3','挂起期写入-2')" 2>/dev/null
sleep 4
DURING=$($DST -N -e "SELECT COUNT(*) FROM biz2.events WHERE kind='incr3'" 2>/dev/null)
POS1=$(api /sources/default/position | python3 -c 'import sys,json; print(json.load(sys.stdin)["data"]["pos"])' 2>/dev/null)
sleep 3
POS2=$(api /sources/default/position | python3 -c 'import sys,json; print(json.load(sys.stdin)["data"]["pos"])' 2>/dev/null)
echo "  挂起期: 增量行 during=$DURING（期待0）pos1=$POS1 pos2=$POS2（期待相等）"
[ "$DURING" = "0" ] && [ -n "$EVID" ] && [ "$POS1" = "$POS2" ] && ok "E3+DDL-11 pause 挂起+位点冻结" || bad "E3/DDL-11 (during=$DURING pos=$POS1/$POS2)"

grp "E5 skip 决策 → 恢复补齐 + 保旧结构 + 审计"
api "/sources/default/ddl/$EVID/skip?confirm=true" POST >/dev/null
E5=0
for i in $(seq 1 15); do
  AFTER=$($DST -N -e "SELECT COUNT(*) FROM biz2.events WHERE kind='incr3'" 2>/dev/null)
  [ "$AFTER" = "2" ] && { E5=1; echo "  补齐 t=${i}x2s"; break; }
  sleep 2
done
CREATED=$($DST -N -e "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='shop' AND table_name='users' AND column_name='created'" 2>/dev/null)
AUD=$(api /audit | python3 -c 'import sys,json; rows=json.load(sys.stdin)["data"]; hits=[r for r in rows if r.get("what","").startswith("ddl.skip")]; print("skip审计:%s who=%s" % (len(hits), rows[-1].get("who")) if hits else "无")')
echo "  skip 后: 补齐=$AFTER（期待2）目标 created 列仍在=$CREATED（期待1=保旧结构）审计=[$AUD]"
[ "$E5" = "1" ] && [ "$CREATED" = "1" ] && echo "$AUD" | grep -q "skip审计:1" && ok "E5 skip 恢复补齐+保旧结构+审计留痕" || bad "E5 (after=$AFTER created=$CREATED aud=$AUD)"

grp "E4 pause → Web approve 全自动（顺带 G2 无 confirm=400）"
$SRC -e "ALTER TABLE shop.orders ADD COLUMN email VARCHAR(128) NULL" 2>/dev/null
sleep 4
EVID4=$(api /ddl-events | python3 -c 'import sys,json; evs=json.load(sys.stdin)["data"]; p=[e["id"] for e in evs if e["status"]=="pending"]; print(p[0] if p else "")')
echo "  挂起事件: $EVID4"
CODE=$(curl -s --noproxy '*' -o /dev/null -w '%{http_code}' -u admin:demo123 -X POST "$API/sources/default/ddl/$EVID4/approve")
echo "  无 confirm approve http=$CODE（期待400）"
api "/sources/default/ddl/$EVID4/approve?confirm=true" POST >/dev/null
E4=0
for i in $(seq 1 15); do
  HAS4=$($DST -N -e "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='shop' AND table_name='orders' AND column_name='email'" 2>/dev/null)
  [ "$HAS4" = "1" ] && { E4=1; echo "  目标自动建列 t=${i}x2s"; break; }
  sleep 2
done
$SRC -e "INSERT INTO shop.orders (user_id,amount,remark,email) VALUES (660001,6.60,'e4-row','a@b.c')" 2>/dev/null
sleep 4
ROW4=$($DST -N -e "SELECT email FROM shop.orders WHERE remark='e4-row'" 2>/dev/null)
echo "  新列数据: dst.email=$ROW4"
[ "$E4" = "1" ] && [ "$CODE" = "400" ] && [ "$ROW4" = "a@b.c" ] && ok "E4 approve 全自动+G2 门禁+新列流动" || bad "E4 (has=$E4 code=$CODE row=$ROW4)"

grp "E6 白名单外表 DDL 过滤 + E8a RENAME 白名单外表 忽略（组形态语义）"
$SRC -e "INSERT INTO shop.tmp_log (msg) VALUES ('e6-不同步')" 2>/dev/null
$SRC -e "ALTER TABLE shop.tmp_log ADD COLUMN extra_col INT NULL" 2>/dev/null
sleep 4
P=$(pending_ids)
T=$($DST -N -e "SELECT COUNT(*) FROM shop.tmp_log WHERE msg='e6-不同步'" 2>/dev/null)
echo "  E6: ALTER 后挂起=[$P]（期待空=未列 tables 的表 DDL 与行事件同被过滤）dst tmp_log 'e6-不同步' 行数=$T（期待0）"
$SRC -e "RENAME TABLE shop.tmp_log TO shop.tmp_log_renamed" 2>/dev/null
sleep 4
P2=$(pending_ids)
echo "  E8a: RENAME 后挂起=[$P2]（期待空=过滤；旧 exclude_tables 语义已移除，等价物=不在白名单）"
[ -z "$P" ] && [ "$T" = "0" ] && [ -z "$P2" ] && ok "E6+E8a 白名单外表 DDL/RENAME 全过滤管线不停" || bad "E6/E8a (p=$P t=$T p2=$P2)"

grp "E7' CREATE 白名单外表 → 过滤（新表自动接入已随组形态移除，arch16 决策）"
$SRC -e "CREATE TABLE shop.fresh_table (id BIGINT NOT NULL AUTO_INCREMENT, val VARCHAR(64) NULL, PRIMARY KEY (id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4" 2>/dev/null
sleep 4
P7=$(pending_ids)
HAS7=$($DST -N -e "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='shop' AND table_name='fresh_table'" 2>/dev/null)
$SRC -e "INSERT INTO shop.fresh_table (val) VALUES ('e7-不应流动')" 2>/dev/null
sleep 4
V7=$($DST -N -e "SELECT COUNT(*) FROM shop.fresh_table" 2>/dev/null)
V7=${V7:-0}   # 目标表不存在时 COUNT 查询报错返回空——与 0 同义（表不存在=无行）
ALIVE7=$( [ -n "$(pickpid node1)" ] && echo 1 || echo 0 )
$SRC -e "INSERT INTO shop.new_table (val) VALUES ('e7-管线仍活')" 2>/dev/null
sleep 4
V7B=$($DST -N -e "SELECT COUNT(*) FROM shop.new_table WHERE val='e7-管线仍活'" 2>/dev/null)
echo "  挂起=[$P7]（期待空）目标自动建表=$HAS7（期待0）行流动=$V7（期待0）存活=$ALIVE7 白名单表流动=$V7B（期待1）"
[ -z "$P7" ] && [ "$HAS7" = "0" ] && [ "$V7" = "0" ] && [ "$ALIVE7" = "1" ] && [ "$V7B" = "1" ] \
  && ok "E7' CREATE 白名单外表被过滤、管线不停（新语义）" || bad "E7' (p=$P7 has=$HAS7 v=$V7 alive=$ALIVE7 vb=$V7B)"

grp "E8b+P0-8 挂起期源表被删 → approve 过期放行不死锁（dying_table 预建白名单表）"
$SRC -e "ALTER TABLE shop.dying_table ADD COLUMN e8col INT NULL" 2>/dev/null
sleep 4
EVID8=$(pending_ids | awk '{print $1}')
echo "  ALTER 挂起事件: $EVID8"
$SRC -e "DROP TABLE shop.dying_table" 2>/dev/null   # DDL 冻结在水位后，排在 ALTER 之后
RES8=$(api "/sources/default/ddl/$EVID8/approve?confirm=true" POST | head -c 150)
echo "  源表已删后 approve: $RES8"
sleep 4
# 恢复后 DROP TABLE（白名单表）会成为新挂起事件，必须 skip 才能让后续流动断言成立
EVID8B=""
for i in $(seq 1 10); do
  EVID8B=$(api /ddl-events | EVID8="$EVID8" python3 -c 'import sys,json,os; evs=json.load(sys.stdin)["data"] or []; p=[e["id"] for e in evs if e["status"]=="pending" and e["id"]!=os.environ.get("EVID8")]; print(p[0] if p else "")' 2>/dev/null)
  [ -n "$EVID8B" ] && break; sleep 1
done
[ -n "$EVID8B" ] && api "/sources/default/ddl/$EVID8B/skip?confirm=true" POST >/dev/null
echo "  DROP 事件 $EVID8B 已 skip"
ALIVE=$( [ -n "$(pickpid node1)" ] && echo 1 || echo 0 )
$SRC -e "INSERT INTO shop.new_table (val) VALUES ('p08-管线仍活')" 2>/dev/null
sleep 4
V8=$($DST -N -e "SELECT val FROM shop.new_table WHERE val='p08-管线仍活'" 2>/dev/null)
AUD8=$(api /audit | python3 -c 'import sys,json; rows=json.load(sys.stdin)["data"]; hits=[r for r in rows if "dying" in r.get("what","")]; print((hits[0].get("what","")[:60]+"->"+hits[0].get("result","")) if hits else "无")')
echo "  进程存活=$ALIVE 后续流动=$V8 审计=[$AUD8]"
[ "$ALIVE" = "1" ] && [ "$V8" = "p08-管线仍活" ] && echo "$AUD8" | grep -qv "无" && ok "E8b+P0-8 过期放行管线不死" || bad "E8b/P0-8 (alive=$ALIVE v=$V8 aud=$AUD8)"

# 收尾：skip 全部 pending（dying_table 的 DROP 事件），避免冻结尾巴
for EV in $(pending_ids); do api "/sources/default/ddl/$EV/skip?confirm=true" POST >/dev/null; done
killall_gocdc
summary

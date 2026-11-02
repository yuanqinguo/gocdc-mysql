#!/bin/bash
# v2 组70：反方对抗（ADV 系列）——ADV-01 无主键双层拦截（临时白名单正文）/
# 02 目标额外UK / 03 保留字（预建白名单表全链）/ 04 零日期 / 05 60KB TEXT /
# 06 自增漂移 / 08 重复skip幂等 / 09 TRUNCATE 审批 / 10 auth_disabled 裸奔 /
# 13 位点回拨时间机器（已决策 DDL+旧行像重放） / 14 restock 不重启不重扫
# 组形态适配：CREATE 挂起自动接入已移除（arch16）——order/adv13t 为预建白名单表，
# ADV-01 经临时正文（+noltbl）构造双层拦截。
set -u
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
API="localhost:8060/api/v1"
api(){ curl -s --noproxy '*' -u admin:demo123 "$API$1" ${2:+-X $2}; }
pending_id(){
  local p="" i
  for i in $(seq 1 8); do
    p=$(api /ddl-events | python3 -c 'import sys,json; evs=json.load(sys.stdin)["data"] or []; p=[e["id"] for e in evs if e["status"]=="pending"]; print(p[0] if p else "")' 2>/dev/null)
    [ -n "$p" ] && { echo "$p"; return; }
    sleep 1
  done
}

wipe_all
ccput verify "$V2/body_main.yml"
stub_start node1 verify :9595 :8060
wait_ready 9595 60 || { echo "FATAL 启动失败"; exit 1; }
BOK=0
for i in $(seq 1 30); do
  D=$($DST -N -e "SELECT COUNT(*) FROM shop.orders" 2>/dev/null)
  [ "$D" = "100" ] && { BOK=1; echo "  [baseline] stock 收敛 t=${i}x2s"; break; }
  sleep 2
done
[ "$BOK" = "1" ] || { echo "FATAL baseline stock 未收敛（dst orders=$D）"; tail -5 /tmp/v2/node1.log; exit 1; }
for i in $(seq 1 20); do LB=$(lagof 9595); [ "$LB" = "0" ] && break; sleep 2; done
echo "  [baseline] 就绪 lag=$LB"

grp "ADV-03 保留字表名/列名（预建白名单表：DML 全链 + 保留字列 DDL 审批）"
# 组形态下 CREATE 挂起自动接入已移除：order 表由 wipe_data 双端预建并列入白名单，
# 本 case 验证保留字标识符在 DML 与 DDL 审批全链可用
$SRC -e 'INSERT INTO shop.`order` (`key`,`select`) VALUES (42, "reserved")' 2>/dev/null
$SRC -e 'UPDATE shop.`order` SET `select`="reserved-upd" WHERE `key`=42' 2>/dev/null
sleep 4
V=$($DST -N -e 'SELECT `select` FROM shop.`order` WHERE `key`=42' 2>/dev/null)
echo "  保留字 DML: dst.[select]=$V（期待 reserved-upd）"
$SRC -e 'ALTER TABLE shop.`order` ADD COLUMN `desc` VARCHAR(32) NULL' 2>/dev/null
sleep 4
EVID=$(pending_id)
echo "  保留字 DDL 挂起: $EVID"
A3=0
V3D=""
if [ -n "$EVID" ]; then
  api "/ddl/$EVID/approve?confirm=true" POST >/dev/null
  for i in $(seq 1 15); do
    T=$($DST -N -e "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='shop' AND table_name='order' AND column_name='desc'" 2>/dev/null)
    [ "$T" = "1" ] && { A3=1; echo "  目标自动加列 t=${i}x2s"; break; }; sleep 2
  done
  $SRC -e 'INSERT INTO shop.`order` (`key`,`desc`) VALUES (43, "d43")' 2>/dev/null
  sleep 4
  # 注意：不可用 $V2 做局部变量——lib.sh 的 V2 是本套件目录路径（2026-10-31 实证：
  # 覆盖后 sed/ccput 读 d43/body_main.yml 产空文件，配置中心被写空，全组连锁崩）
  V3D=$($DST -N -e 'SELECT `desc` FROM shop.`order` WHERE `key`=43' 2>/dev/null)
  echo "  新列数据流动: dst.[desc]=$V3D"
fi
[ "$V" = "reserved-upd" ] && [ "$A3" = "1" ] && [ "$V3D" = "d43" ] && ok "ADV-03 保留字全链收敛" || bad "ADV-03 (v=$V t=$A3 v2=$V3D)"

grp "ADV-04 零日期（on_convert_error=null 不炸）"
$SRC -e "SET SESSION sql_mode='NO_ENGINE_SUBSTITUTION'; INSERT INTO types.all_types (c_date,c_dt,c_varchar) VALUES ('0000-00-00','0000-00-00 00:00:00','adv04-zero')" 2>/dev/null
sleep 5
D=$($DST -N -e "SELECT COALESCE(CAST(c_date AS CHAR),'NULL-VAL') FROM types.all_types WHERE c_varchar='adv04-zero'" 2>/dev/null)
ALIVE=$( [ -n "$(pickpid node1)" ] && echo 1 || echo 0 )
echo "  目标零日期=$D 存活=$ALIVE"
[ "$D" = "NULL-VAL" ] && [ "$ALIVE" = "1" ] && ok "ADV-04 零日期降级 NULL 管线不受影响" || bad "ADV-04 (d=$D alive=$ALIVE)"

grp "ADV-05 60KB 超长 TEXT"
$SRC -e "INSERT INTO types.all_types (c_text,c_varchar) VALUES (REPEAT('A',60000),'adv05-60k')" 2>/dev/null
sleep 5
SL=$($SRC -N -e "SELECT LENGTH(c_text) FROM types.all_types WHERE c_varchar='adv05-60k'" 2>/dev/null)
DL=$($DST -N -e "SELECT LENGTH(c_text) FROM types.all_types WHERE c_varchar='adv05-60k'" 2>/dev/null)
echo "  src_len=$SL dst_len=$DL"
[ "$SL" = "60000" ] && [ "$DL" = "60000" ] && ok "ADV-05 60KB TEXT 双端无损" || bad "ADV-05 ($SL/$DL)"

grp "ADV-06 目标 AUTO_INCREMENT 漂移"
$DST -e "INSERT INTO shop.new_table (id,val) VALUES (999999,'adv06-target-explicit')" 2>/dev/null
$SRC -e "INSERT INTO shop.new_table (val) VALUES ('adv06-src-next')" 2>/dev/null
sleep 5
DSTV=$($DST -N -e "SELECT COUNT(*) FROM shop.new_table WHERE val='adv06-src-next'" 2>/dev/null)
[ "$DSTV" = "1" ] && ok "ADV-06 漂移不阻塞后续同步" || bad "ADV-06 (v=$DSTV)"

grp "ADV-02 目标额外 UNIQUE KEY"
$DST -e "ALTER TABLE shop.new_table ADD UNIQUE KEY uk_val (val)" 2>/dev/null
$SRC -e "INSERT INTO shop.new_table (val) VALUES ('adv02-uk')" 2>/dev/null
sleep 4
$SRC -e "UPDATE shop.new_table SET val='adv02-uk-upd' WHERE val='adv02-uk'" 2>/dev/null
sleep 5
U=$($DST -N -e "SELECT COUNT(*) FROM shop.new_table WHERE val='adv02-uk-upd'" 2>/dev/null)
ALIVE=$( [ -n "$(pickpid node1)" ] && echo 1 || echo 0 )
echo "  UK路径UPDATE收敛=$U 存活=$ALIVE"
[ "$U" = "1" ] && [ "$ALIVE" = "1" ] && ok "ADV-02 UK 路径不炸" || bad "ADV-02 (u=$U)"
$DST -e "ALTER TABLE shop.new_table DROP INDEX uk_val" 2>/dev/null

grp "ADV-08 同一 DDL 重复 skip 幂等（Settled 短路）"
$SRC -e "ALTER TABLE shop.new_table ADD COLUMN adv8zz INT NULL" 2>/dev/null
sleep 4
EVID8=$(pending_id)
R1=$(api "/ddl/$EVID8/skip?confirm=true" POST | head -c 60)
sleep 1
R2=$(api "/ddl/$EVID8/skip?confirm=true" POST | head -c 60)
echo "  两次 skip: [$R1] [$R2]"
echo "$R1" | grep -q '"code":0' && echo "$R2" | grep -q '"code":0' && ok "ADV-08 二次 skip 仍 200" || bad "ADV-08 ($R1/$R2)"
$SRC -e "ALTER TABLE shop.new_table DROP COLUMN adv8zz" 2>/dev/null
sleep 3
EVID8B=$(pending_id)
[ -n "$EVID8B" ] && api "/ddl/$EVID8B/skip?confirm=true" POST >/dev/null

grp "ADV-09 TRUNCATE → pause 审批"
$SRC -e "TRUNCATE TABLE shop.new_table" 2>/dev/null
sleep 4
EVID9=$(pending_id)
[ -z "$EVID9" ] && { bad "ADV-09 未挂起"; }
api "/ddl/$EVID9/skip?confirm=true" POST >/dev/null
sleep 5
$SRC -e "INSERT INTO shop.new_table (val) VALUES ('adv09-after')" 2>/dev/null
sleep 5
V9=$($DST -N -e "SELECT COUNT(*) FROM shop.new_table WHERE val='adv09-after'" 2>/dev/null)
ALIVE=$( [ -n "$(pickpid node1)" ] && echo 1 || echo 0 )
echo "  skip后流动=$V9 存活=$ALIVE"
[ "$V9" = "1" ] && [ "$ALIVE" = "1" ] && ok "ADV-09 TRUNCATE 审批+恢复" || bad "ADV-09 (v=$V9)"

grp "ADV-01 无主键表双层拦截（启动核对 fail-fast，临时白名单正文）"
# 组形态语义：只有白名单表参与核对/同步——本 case 用「正文 + noltbl 表级条目」
# 临时形态构造，两层验证后恢复正文。
kill -TERM $(pickpid node1) 2>/dev/null
for i in $(seq 1 30); do [ -z "$(pickpid node1)" ] && break; sleep 1; done
$SRC -e "CREATE TABLE shop.noltbl (id INT NULL, v INT NULL)" 2>/dev/null
sed '/table: dying_table/a\      - {table: noltbl, do_stock: true, order_by_column: v}' \
  "$V2/body_main.yml" > /tmp/v2/body-adv01.yml
ccput verify /tmp/v2/body-adv01.yml
d=/tmp/v2/adv01; rm -rf $d; mkdir -p $d
cat > $d/config.local.yml <<'EOF'
cluster: verify
bootstrap:
  config_source: etcd
  config_center:
    etcd_addrs: [127.0.0.1:12380]
  etcd_addrs:
    - 127.0.0.1:12379
EOF
cp "$BIN" $d/gocdcbin
# 第一层：目标不存在 → 拒启
(cd $d && WEB_PASSWORD=demo123 METRICS_ADDR=:9595 WEB_ADDR=:8060 timeout 30 ./gocdcbin > out1.log 2>&1; echo $? > e1)
L1E=$(cat $d/e1); L1=$(grep -aE '核对错误|fatal' $d/out1.log | head -2)
echo "  第一层（目标无 noltbl）exit=$L1E: $L1"
# 第二层：两端补建无主键表 → 再启 → 寻址键核对拒启
$DST -e "CREATE TABLE shop.noltbl (id INT NULL, v INT NULL)" 2>/dev/null
(cd $d && WEB_PASSWORD=demo123 METRICS_ADDR=:9595 WEB_ADDR=:8060 timeout 30 ./gocdcbin > out2.log 2>&1; echo $? > e2)
L2E=$(cat $d/e2); L2=$(grep -aE '核对错误|fatal|主键|寻址' $d/out2.log | head -3)
echo "  第二层（无主键）exit=$L2E: $L2"
[ "$L1E" != "0" ] && [ "$L2E" != "0" ] && ok "ADV-01 双层拦截拒启" || bad "ADV-01 ($L1E/$L2E)"
$SRC -e "DROP TABLE shop.noltbl" 2>/dev/null; $DST -e "DROP TABLE shop.noltbl" 2>/dev/null
ccput verify "$V2/body_main.yml"
stub_start node1 verify :9595 :8060
wait_ready 9595 60 || bad "ADV-01 恢复启动"
V1=0
for i in $(seq 1 30); do
  if [ $((i % 5)) = "1" ]; then
    $SRC -e "INSERT INTO shop.new_table (val) VALUES ('adv01-recovered')" 2>/dev/null
  fi
  V1=$($DST -N -e "SELECT COUNT(*) FROM shop.new_table WHERE val='adv01-recovered'" 2>/dev/null)
  [ "$V1" = "1" ] && break; sleep 2
done
[ "$V1" = "1" ] && ok "ADV-01b 清理后恢复流动" || bad "ADV-01b (v=$V1)"

grp "ADV-10 auth_disabled=true 显式裸奔（配置中心正文切换）"
sed 's/auth_disabled: false/auth_disabled: true/' "$V2/body_main.yml" > /tmp/v2/body-auth.yml
kill -TERM $(pickpid node1) 2>/dev/null
for i in $(seq 1 30); do [ -z "$(pickpid node1)" ] && break; sleep 1; done
killall_gocdc   # 优雅停机偶发超窗：强制清残留，防新节点端口占用秒死
sleep 2
ccput verify /tmp/v2/body-auth.yml
stub_start node1 verify :9595 :8060
wait_ready 9595 60 || bad "ADV-10 启动"
sleep 3
NOAUTH=$(curl -s --noproxy '*' -o /dev/null -w '%{http_code}' "$API/position")
GATE=$(curl -s --noproxy '*' -o /dev/null -w '%{http_code}' -X POST "$API/leader/resign")
echo "  无凭据GET=$NOAUTH（期待200）无confirm写=$GATE（期待400=门禁独立）"
[ "$NOAUTH" = "200" ] && [ "$GATE" = "400" ] && ok "ADV-10 裸奔放行+confirm 门禁独立" || bad "ADV-10 ($NOAUTH/$GATE)"
kill -TERM $(pickpid node1) 2>/dev/null
for i in $(seq 1 30); do [ -z "$(pickpid node1)" ] && break; sleep 1; done
killall_gocdc
sleep 2
ccput verify "$V2/body_main.yml"
stub_start node1 verify :9595 :8060
wait_ready 9595 60 || bad "ADV-10 复原启动"
sleep 3
R1=$(curl -s --noproxy '*' -o /dev/null -w '%{http_code}' "$API/position")
R2=$(curl -s --noproxy '*' -u admin:demo123 -o /dev/null -w '%{http_code}' "$API/position")
[ "$R1" = "401" ] && [ "$R2" = "200" ] && ok "ADV-10b 复原 401/200" || bad "ADV-10b ($R1/$R2)"

grp "ADV-13 位点回拨时间机器（重放窗口含已决策 DDL/结构变更前旧行像）"
# 组形态适配：adv13t 为预建白名单表（白名单表被 DROP 后启动核对必拒，旧「已删表
# 重放」路径收敛到 L2 集成覆盖）。本 case 回放窗口 = 已 settle 的 DDL（加列 approve）
# + 加列前的旧行像（列数少于目标当前结构 → on_convert_error 降级路径）。
$SRC -e "ALTER TABLE shop.adv13t ADD COLUMN v13 INT NULL" 2>/dev/null
sleep 4
E13=$(pending_id); api "/ddl/$E13/approve?confirm=true" POST >/dev/null
sleep 5
$SRC -e "INSERT INTO shop.adv13t (v) VALUES (1),(2),(3)" 2>/dev/null
sleep 5
KEY=$($ETCDC get /gocdc/verify/sources/default/position --print-value-only)
kill -TERM $(pickpid node1) 2>/dev/null
for i in $(seq 1 30); do [ -z "$(pickpid node1)" ] && break; sleep 1; done
NEWPOS=$(echo "$KEY" | python3 -c "import sys,json;d=json.load(sys.stdin);d['pos']=max(4,d['pos']-40000);print(json.dumps(d))")
$ETCDC put /gocdc/verify/sources/default/position "$NEWPOS" >/dev/null
echo "  已回拨 40000 字节（窗口含 adv13t 的已决策 DDL+旧行像），重启…"
stub_start node1 verify :9595 :8060
wait_ready 9595 60 || bad "ADV-13 重启"
A13=0
for i in $(seq 1 40); do
  L=$(lagof 9595); [ "$L" = "0" ] && { A13=1; echo "  追平 t=${i}x2s"; break; }
  ALIVE=$( [ -n "$(pickpid node1)" ] && echo 1 || echo 0 )
  [ "$ALIVE" = "0" ] && break
  sleep 2
done
DUP13=$($DST -N -e "SELECT COUNT(*)-COUNT(DISTINCT id) FROM shop.new_table" 2>/dev/null)
A13ROWS=$($DST -N -e "SELECT COUNT(*) FROM shop.adv13t" 2>/dev/null)
ALIVE=$( [ -n "$(pickpid node1)" ] && echo 1 || echo 0 )
echo "  管线存活=$ALIVE lag追平=$A13 new_table重复=$DUP13 adv13t行=$A13ROWS（期待3）"
[ "$ALIVE" = "1" ] && [ "$A13" = "1" ] && [ "$DUP13" = "0" ] && [ "$A13ROWS" = "3" ] && ok "ADV-13 时间机器自愈不死锁" || bad "ADV-13 (alive=$ALIVE lag=$A13 dup=$DUP13 rows=$A13ROWS)"

grp "ADV-14 restock 不重启 → 不自发重扫（文档语义）"
PRE=$($DST -N -e "SELECT COUNT(*) FROM rmqtest.msg" 2>/dev/null)
api "/rules/rmqtest.msg/restock?confirm=true" POST >/dev/null
sleep 20
POSTN=$($DST -N -e "SELECT COUNT(*) FROM rmqtest.msg" 2>/dev/null)
RESN=$(grep -ac '全量表完成.*rmqtest' /tmp/v2/node1.log 2>/dev/null)
echo "  restock 后 20s: dst=$PRE→$POSTN 全量日志=$RESN（期待不变/0）"
[ "$PRE" = "$POSTN" ] && [ "${RESN:-0}" = "0" ] && ok "ADV-14 运行中不自发重扫" || bad "ADV-14 ($PRE→$POSTN log=$RESN)"

for EV in $(pending_id); do api "/ddl/$EV/skip?confirm=true" POST >/dev/null; done
killall_gocdc
summary

#!/bin/bash
# v2 组35：多源核心（MS 系列数据面，单节点 ha=false，PRD 08 §2）
# MS-01 双源端到端一致性 / MS-02 双源并发首启全量 / MS-05 跨源 DDL 冻结半径 /
# MS-06 逐源 pause/resume+暂停期改位点 / MS-07 逐源位点回拨 / MS-08 逐源 restock /
# MS-09 MQ 双源 topic 不相交+保序 / MS-11(脚本分支) 配置校验 /
# MS-14 源级路由/旧路由移除 / MS-15 server_id 派生互异 / MS-16 指标 source label
set -u
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

W=18070   # 本组 web 端口
M=19597   # 本组 metrics 端口
A="localhost:$W/api/v1"

wipe_all
ms_wipe || exit 1

grp "MS-BOOT 双源启动（body_ms：src-a@13306 + src-b@13406 → ms_merged 聚合）"
flagput ms-verify "$V2/body_ms.yml"
flag_start ms1 ms-verify ":$M" ":$W"
wait_ready $M 40 && ok "MS-B1 双源进程 healthz" || { bad "MS-B1"; tail -30 /tmp/v2/ms1.log; exit 1; }
wait_src_state $W src-a running 40 && ok "MS-B2 src-a → running" || bad "MS-B2 src-a state=$(src_state $W src-a)"
wait_src_state $W src-b running 40 && ok "MS-B3 src-b → running" || bad "MS-B3 src-b state=$(src_state $W src-b)"

grp "MS-02 双源并发首启全量（50+60 行种子 → 交接零丢失）"
NA=$($DST -N -e "SELECT COUNT(*) FROM ms_merged.orders" 2>/dev/null)
NB=$($DST -N -e "SELECT COUNT(*) FROM ms_merged.users" 2>/dev/null)
[ "$NA" = "50" ] && ok "MS-02a src-a 全量 50 行到位（增量起点交接）" || bad "MS-02a orders=$NA 期待50"
[ "$NB" = "60" ] && ok "MS-02b src-b 全量 60 行到位" || bad "MS-02b users=$NB 期待60"

grp "MS-01 双源端到端一致性（双写 UPDATE/DELETE/PK 变更 → 聚合目标逐行一致）"
$SRC -e "INSERT INTO msa.orders (val,amt) VALUES ('a-incr-1',9.99),('a-incr-2',19.99);
UPDATE msa.orders SET amt=amt+1 WHERE id<=5;
DELETE FROM msa.orders WHERE id=7;
UPDATE msa.orders SET id=999 WHERE id=8;" 2>/dev/null
$SRC2 -e "INSERT INTO msb.users (name,phone) VALUES ('b-incr-1','13800000001'),('b-incr-2',NULL);
UPDATE msb.users SET name=CONCAT(name,'-u') WHERE id<=3;
DELETE FROM msb.users WHERE id=9;" 2>/dev/null
sleep 1
wait_row "$DST" "SELECT COUNT(*) FROM ms_merged.orders WHERE val IN ('a-incr-1','a-incr-2')" 2 15 || true
wait_row "$DST" "SELECT COUNT(*) FROM ms_merged.users WHERE id=9" 0 15 || true
R1=$($DST -N -e "SELECT COUNT(*) FROM ms_merged.orders WHERE val IN ('a-incr-1','a-incr-2')" 2>/dev/null)
R2=$($DST -N -e "SELECT amt FROM ms_merged.orders WHERE id=1" 2>/dev/null)
R3=$($DST -N -e "SELECT COUNT(*) FROM ms_merged.orders WHERE id IN (7,8)" 2>/dev/null)
R4=$($DST -N -e "SELECT val FROM ms_merged.orders WHERE id=999" 2>/dev/null)
R5=$($DST -N -e "SELECT COUNT(*) FROM ms_merged.users WHERE name IN ('b-incr-1','b-incr-2')" 2>/dev/null)
R6=$($DST -N -e "SELECT COUNT(*) FROM ms_merged.users WHERE id=9" 2>/dev/null)
[ "$R1" = "2" ] && ok "MS-01a src-a 增量 INSERT 聚合到位" || bad "MS-01a R1=$R1"
[ "$R3" = "0" ] && ok "MS-01b src-a DELETE+PK 变更收敛（旧 id 消失）" || bad "MS-01b R3=$R3"
[ "$R4" = "a-seed-8" ] && ok "MS-01c PK 变更后新 id 数据完整" || bad "MS-01c R4=$R4"
[ "$R5" = "2" ] && ok "MS-01d src-b 增量聚合到位" || bad "MS-01d R5=$R5"
[ "$R6" = "0" ] && ok "MS-01e src-b DELETE 到位" || bad "MS-01e R6=$R6"
# 抽样对账（逐源 verify：mismatch=0）
V=$(curl -s --noproxy '*' -u admin:demo123 -X POST "$A/sources/src-a/verify?confirm=true" \
  -H 'Content-Type: application/json' -d '{"database":"msa","table":"orders","pk":"id","sample":100}')
echo "$V" | jget 'd["mismatch"]' | grep -qx "0" && ok "MS-01f src-a verify mismatch=0" || bad "MS-01f verify=$V"
V=$(curl -s --noproxy '*' -u admin:demo123 -X POST "$A/sources/src-b/verify?confirm=true" \
  -H 'Content-Type: application/json' -d '{"database":"msb","table":"users","pk":"id","sample":100}')
echo "$V" | jget 'd["mismatch"]' | grep -qx "0" && ok "MS-01g src-b verify mismatch=0" || bad "MS-01g verify=$V"

grp "MS-15 server_id 派生互异（同进程两源同时拉 binlog 不互踢）"
CA=$(dumpconns)  # vf-src 侧 dump 连接数
CB=$(docker exec vf-src2 mysql -uroot -p123456 -h127.0.0.1 -N -e "SELECT COUNT(*) FROM information_schema.processlist WHERE command LIKE 'Binlog Dump%'" 2>/dev/null)
[ "${CA:-0}" -ge 1 ] && [ "${CB:-0}" -ge 1 ] && ok "MS-15a 两源库各有 dump 连接（互踢未发生）" || bad "MS-15a src=$CA src2=$CB"
# 两源持续同步即派生互异的运行期证据（互踢会断流）
$SRC -e "INSERT INTO msa.evlog (v) VALUES (101)" 2>/dev/null
$SRC2 -e "INSERT INTO msb.tlog (v) VALUES (202)" 2>/dev/null
sleep 3
EA=$($DST -N -e "SELECT COUNT(*) FROM ms_merged.evlog WHERE v=101" 2>/dev/null)
EB=$($DST -N -e "SELECT COUNT(*) FROM ms_merged.tlog WHERE v=202" 2>/dev/null)
[ "$EA" = "1" ] && [ "$EB" = "1" ] && ok "MS-15b 双源持续同步（server_id 冲突会断流）" || bad "MS-15b a=$EA b=$EB"

grp "MS-05 跨源 DDL 冻结半径（src-a DDL pending 只冻结 src-a）"
CUR=$($SRC -N -e "SELECT MAX(id) FROM msa.orders" 2>/dev/null)
$SRC -e "ALTER TABLE msa.orders ADD COLUMN ms05 INT DEFAULT NULL" 2>/dev/null
sleep 3
[ "$(src_state $W src-a)" = "paused" ] && ok "MS-05a src-a 因 DDL 进入 paused" || bad "MS-05a state=$(src_state $W src-a)"
# src-a 冻结期间 src-b 持续推进
$SRC2 -e "INSERT INTO msb.tlog (v) VALUES (203)" 2>/dev/null
sleep 3
EB2=$($DST -N -e "SELECT COUNT(*) FROM ms_merged.tlog WHERE v=203" 2>/dev/null)
[ "$EB2" = "1" ] && ok "MS-05b src-b 不受 src-a DDL 冻结影响" || bad "MS-05b v=203 缺失"
# approve（新路由带 sid）→ src-a 自动恢复 + DDL 记录带 source 字段
DE=$(curl -s --noproxy '*' -u admin:demo123 "$A/sources/src-a/ddl-events")
echo "$DE" | jget 'd[0]["source"]' | grep -qx "src-a" && ok "MS-05c DDL 记录含 source=src-a" || bad "MS-05c $DE"
EID=$(echo "$DE" | jget 'd[0]["id"]')
curl -s --noproxy '*' -u admin:demo123 -X POST "$A/sources/src-a/ddl/$EID/approve?confirm=true" >/dev/null
wait_src_state $W src-a running 20 && ok "MS-05d approve 后 src-a 恢复 running" || bad "MS-05d state=$(src_state $W src-a)"
NEWC=$($DST -N -e "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='ms_merged' AND table_name='orders' AND column_name='ms05'" 2>/dev/null)
[ "$NEWC" = "1" ] && ok "MS-05e DDL 已执行到目标（新列存在）" || bad "MS-05e ms05 列缺失"

grp "MS-06 逐源人工暂停/恢复（暂停期改位点）"
curl -s --noproxy '*' -u admin:demo123 -X POST "$A/sources/src-a/pipeline/pause?confirm=true" >/dev/null
sleep 2
[ "$(src_state $W src-a)" = "paused" ] && ok "MS-06a src-a 人工暂停" || bad "MS-06a"
$SRC2 -e "INSERT INTO msb.tlog (v) VALUES (204)" 2>/dev/null
sleep 2
EB3=$($DST -N -e "SELECT COUNT(*) FROM ms_merged.tlog WHERE v=204" 2>/dev/null)
[ "$EB3" = "1" ] && ok "MS-06b src-b 不受 src-a 暂停影响" || bad "MS-06b"
$SRC -e "INSERT INTO msa.evlog (v) VALUES (999)" 2>/dev/null   # 暂停期 src-a 积压
sleep 2
E99=$($DST -N -e "SELECT COUNT(*) FROM ms_merged.evlog WHERE v=999" 2>/dev/null)
[ "$E99" = "0" ] && ok "MS-06c 暂停期 src-a 增量冻结（积压未投递）" || bad "MS-06c v=999 已投递（未冻结）"
curl -s --noproxy '*' -u admin:demo123 -X POST "$A/sources/src-a/pipeline/resume?confirm=true" >/dev/null
sleep 4
[ "$(src_state $W src-a)" = "running" ] && ok "MS-06d src-a 恢复 running" || bad "MS-06d state=$(src_state $W src-a)"
E99=$($DST -N -e "SELECT COUNT(*) FROM ms_merged.evlog WHERE v=999" 2>/dev/null)
[ "$E99" = "1" ] && ok "MS-06e 恢复后积压数据补投" || bad "MS-06e v=999 缺失"

grp "MS-07 逐源位点回拨（src-a 回拨重放，src-b 位点不动）"
PB=$(curl -s --noproxy '*' -u admin:demo123 "$A/sources/src-a/position")
PNAME=$(echo "$PB" | jget 'd["name"]'); PPOS=$(echo "$PB" | jget 'd["pos"]')
ROLL=$((PPOS-4000)); [ $ROLL -lt 4 ] && ROLL=4
curl -s --noproxy '*' -u admin:demo123 -X POST "$A/sources/src-a/position?confirm=true" \
  -H 'Content-Type: application/json' -d "{\"name\":\"$PNAME\",\"pos\":$ROLL,\"confirm\":\"SET-POSITION\"}" >/dev/null
killall_gocdc   # 位点改动重启生效
flag_start ms2 ms-verify ":$M" ":$W"
wait_ready $M 40 && wait_src_state $W src-a running 40 && wait_src_state $W src-b running 40 && ok "MS-07a 回拨后双源重启" || bad "MS-07a"
sleep 4
PB2=$(curl -s --noproxy '*' -u admin:demo123 "$A/sources/src-a/position")
SEQ2=$(echo "$PB2" | jget 'd["seq"]')
[ "${SEQ2:-0}" -gt 0 ] && ok "MS-07b src-a 回拨后位点推进（重放收敛）" || bad "MS-07b seq=$SEQ2"
V=$(curl -s --noproxy '*' -u admin:demo123 -X POST "$A/sources/src-a/verify?confirm=true" \
  -H 'Content-Type: application/json' -d '{"database":"msa","table":"orders","pk":"id","sample":100}')
echo "$V" | jget 'd["mismatch"]' | grep -qx "0" && ok "MS-07c 回拨重放后幂等收敛 verify=0" || bad "MS-07c $V"

grp "MS-08 逐源 restock（只重扫该源该表）"
curl -s --noproxy '*' -u admin:demo123 -X POST "$A/sources/src-a/rules/by-schema/msa/orders/restock?confirm=true" >/dev/null
killall_gocdc
flag_start ms3 ms-verify ":$M" ":$W"
wait_ready $M 40 && wait_src_state $W src-a running 60 && ok "MS-08a restock 重启后 src-a running" || bad "MS-08a"
sleep 4
RC=$($DST -N -e "SELECT COUNT(*) FROM ms_merged.orders" 2>/dev/null)
[ "$RC" -ge 50 ] && ok "MS-08b msa.orders 整表重扫完成（幂等收敛）" || bad "MS-08b orders=$RC"
RU=$($DST -N -e "SELECT COUNT(*) FROM ms_merged.users" 2>/dev/null)
[ "$RU" -ge 60 ] && ok "MS-08c msb.users 未重扫（数据仍在）" || bad "MS-08c users=$RU"

grp "MS-09 MQ 双源：topic 不相交 + envelope source 字段 + src-a 保序"
# 全新消费组从最新位点开始——先起消费者再写入
TS9=$(date +%s)
setsid nohup $RMQCONS -topic CDC_msa_msa_orders -group ms09-$TS9 -out /tmp/rmq-msa.jsonl -idle 8s > /tmp/rmqcons-ms09.log 2>&1 &
sleep 2
$SRC -e "INSERT INTO msa.orders (val,amt) VALUES ('ms09-1',1),('ms09-2',2),('ms09-3',3)" 2>/dev/null
sleep 10
N=$(wc -l < /tmp/rmq-msa.jsonl 2>/dev/null || echo 0)
[ "${N:-0}" -ge 3 ] && ok "MS-09a src-a FIFO topic 收到消息（N=$N）" || bad "MS-09a N=$N $(tail -3 /tmp/rmqcons-ms09.log 2>/dev/null)"
grep -q '"source":"src-a"' /tmp/rmq-msa.jsonl && ok "MS-09b envelope 带 source=src-a" || bad "MS-09b"
grep -q '\"val\":\"ms09-1' /tmp/rmq-msa.jsonl && ok "MS-09c 消息内容完整" || bad "MS-09c"

grp "MS-11(脚本) 配置校验：v1 顶层形态拒启 / 跨源目标冲突 / 非法 id"
ccrun_bad(){ # <nm> <body-string> <expect-substr>：坏配置写配置中心 + 前台桩启动预期拒启
  local nm=$1 body=$2 want=$3 d=/tmp/v2/$1
  rm -rf "$d"; mkdir -p "$d"
  printf '%s' "$body" > "$d/body.yml"
  ccput ms-verify "$d/body.yml" >/dev/null
  cat > "$d/config.local.yml" <<'EOF'
cluster: ms-verify
bootstrap:
  config_source: etcd
  config_center: { etcd_addrs: [127.0.0.1:12380], key: /gocdc/ms-verify/config }
  etcd_addrs: [127.0.0.1:12379]
EOF
  # key 必须显式：2026-11 键默认策略改为公司约定固定键 /config/edu.mall.gocdc/system，
  # 缺省桩会去读不存在的固定键 → 泛化 fatal 抢在正文校验前（MS-11 三连 FAIL 实证）
  cp "$BIN" "$d/gocdcbin"
  (cd "$d" && WEB_PASSWORD=demo123 METRICS_ADDR=:19598 timeout 25 ./gocdcbin > out.log 2>&1; echo $? > exit.log)
  echo "  [$nm] exit=$(cat "$d/exit.log") $(grep -m1 配置错误 "$d/out.log" | head -c 130)"
  if grep -q "$want" "$d/out.log" && [ "$(cat "$d/exit.log")" = "1" ]; then
    ok "MS-11 $nm 拒启（含「$want」）"
  else
    bad "MS-11 $nm exit=$(cat "$d/exit.log") 缺「$want」"
  fi
}
V1BASE='cluster: ms-verify
ha: {enabled: false, lease_ttl_seconds: 10}
observability: {metrics_addr: ":19598", web: {enabled: false}, log: {level: info}}'
# 旧「不允许双写」专项检查已并入 v1 形态移除：顶层 source 键（哪怕与 sources[]
# 并存的双写形态）一律在 load 链拒启——want 断言用现行报错语（2026-11-04 实证）。
ccrun_bad dualwrite "$V1BASE
source: {addr: 127.0.0.1:13306, user: root, password: \"123456\", charset: utf8mb4, flavor: mysql, timezone: UTC, exclude_schemas: [mysql, sys, information_schema, performance_schema]}
sources:
  - id: x" "v1 单源形态已移除"
# 跨源目标冲突：两源同目标库同表
MSCLASH='cluster: ms-verify
ha: {enabled: false, lease_ttl_seconds: 10}
observability: {metrics_addr: ":19598", web: {enabled: false}, log: {level: info}}
sources:
  - id: sa
    source: {addr: 127.0.0.1:13306, user: root, password: "123456", charset: utf8mb4, flavor: mysql, timezone: UTC, exclude_schemas: [mysql, sys, information_schema, performance_schema]}
    targets: {mysql: {enabled: true, addr: 127.0.0.1:13307, user: root, password: "123456", charset: utf8mb4, max_open_conns: 5, tx_chunk_rows: 100, target_database: ms_clash}}
    rules: [{schema: msa, sinks: {mysql: true}, tables: [{table: orders, order_by_column: id, do_stock: false}]}]
  - id: sb
    source: {addr: 127.0.0.1:13406, user: root, password: "123456", charset: utf8mb4, flavor: mysql, timezone: UTC, exclude_schemas: [mysql, sys, information_schema, performance_schema]}
    targets: {mysql: {enabled: true, addr: 127.0.0.1:13307, user: root, password: "123456", charset: utf8mb4, max_open_conns: 5, tx_chunk_rows: 100, target_database: ms_clash}}
    rules: [{schema: msa, sinks: {mysql: true}, tables: [{table: orders, order_by_column: id, do_stock: false}]}]'
ccrun_bad clash "$MSCLASH" "跨源目标冲突"
# 非法 id
MSBADID=$(echo "$MSCLASH" | sed 's/- id: sb/- id: Bad_ID/; s/13406/13406/')
ccrun_bad badid "$MSBADID" "非法"

grp "MS-14 源级路由（旧顶层路由已移除 404 / /sources 清单 / /ddl-events 合并带 source）"
RC1=$(curl -s -o /dev/null -w '%{http_code}' --noproxy '*' -u admin:demo123 "$A/position")
[ "$RC1" = "404" ] && ok "MS-14a 旧路由 /position 已移除 → 404" || bad "MS-14a code=$RC1"
RC2=$(curl -s -o /dev/null -w '%{http_code}' --noproxy '*' -u admin:demo123 -X POST "$A/pipeline/pause?confirm=true")
[ "$RC2" = "404" ] && ok "MS-14b 旧写路由 pipeline/pause 已移除 → 404" || bad "MS-14b code=$RC2"
SL=$(curl -s --noproxy '*' -u admin:demo123 "$A/sources")
echo "$SL" | jget 'len(d)' | grep -qx "2" && ok "MS-14c GET /sources 返回 2 源" || bad "MS-14c $SL"
echo "$SL" | jget 'd[0]["source_addr"]' | grep -q "13306" && ok "MS-14d 源清单含源地址" || bad "MS-14d"
RC3=$(curl -s -o /dev/null -w '%{http_code}' --noproxy '*' -u admin:demo123 "$A/sources/nope/position")
[ "$RC3" = "404" ] && ok "MS-14e 未知 sid → 404" || bad "MS-14e code=$RC3"
# 审计带 source（源级 pause 留痕）
AU=$(curl -s --noproxy '*' -u admin:demo123 "$A/audit")
echo "$AU" | jget '[a["source"] for a in d if a.get("source")]' | grep -q "src-a" && ok "MS-14f 审计条目带 source=src-a" || bad "MS-14f"

grp "MS-16 指标 source label（源私有带 label，cluster 级不带）"
MET=$(curl -s --noproxy '*' "localhost:$M/metrics")
echo "$MET" | grep -q '^gocdc_events_in_total{.*source="src-[ab]"' && ok "MS-16a events_in 带 source label" || bad "MS-16a $(echo "$MET" | grep -m1 events_in_total)"
echo "$MET" | grep -q 'gocdc_master_position_lag_bytes{.*source="src-b"' && ok "MS-16b master_lag 带 source=src-b" || bad "MS-16b"
echo "$MET" | grep -q '^gocdc_leader_state ' && ok "MS-16c leader_state 不带 source（cluster 级）" || bad "MS-16c"
echo "$MET" | grep -q 'gocdc_source_up{source="src-a"}' && ok "MS-16d gocdc_source_up 有 src-a 序列" || bad "MS-16d $(echo "$MET" | grep -m1 source_up)"

killall_gocdc
summary

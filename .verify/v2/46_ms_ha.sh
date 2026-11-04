#!/bin/bash
# v2 组46：多源 HA 与故障隔离（MS 系列故障面，PRD 08 §2）
# MS-03 全量中杀 leader 接管 / MS-04 源库宕机隔离与恢复 / MS-10 背压隔离（分目标库）/
# MS-12 v1 配置升级 + etcd 键迁移幂等 / MS-13 旧 leader 位点写拒绝
set -u
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

W=18072; W2=18073; M=19599
A="localhost:$W/api/v1"

wipe_all
ms_wipe || exit 1

grp "MS-03 双源全量中杀进程 → 重启续扫（chunk_rows=20 分块断点）"
# 用双源全量中途 kill：首启即全量（50+60 行 × chunk 20），boot 后立即杀
flagput ms-verify "$V2/body_ms.yml"
flag_start ha1 ms-verify ":$M" ":$W"
sleep 5   # 全量进行中（或刚完成一部分）
killall_gocdc
flag_start ha2 ms-verify ":$M" ":$W"
wait_ready $M 40 && wait_src_state $W src-a running 60 && wait_src_state $W src-b running 60 \
  && ok "MS-03a 中断重启后双源 running" || { bad "MS-03a a=$(src_state $W src-a) b=$(src_state $W src-b)"; tail -20 /tmp/v2/ha2.log; }
sleep 4
NA=$($DST -N -e "SELECT COUNT(*) FROM ms_merged.orders" 2>/dev/null)
NB=$($DST -N -e "SELECT COUNT(*) FROM ms_merged.users" 2>/dev/null)
[ "$NA" = "50" ] && ok "MS-03b src-a 全量收敛（会话失配整表重扫或断点续扫，终态 50）" || bad "MS-03b orders=$NA"
[ "$NB" = "60" ] && ok "MS-03c src-b 全量收敛" || bad "MS-03c users=$NB"

grp "MS-12 v1 配置升级 + etcd 键迁移幂等 + 位点连续"
# 双源跑到稳态后停机 → 手工把 v2 键搬回旧键形态（模拟 v1 存量）→ 换 v1 配置启动
#（v1 自动包装 default 单源 + 键迁移把旧键搬到 sources/default/）→ 位点续传
killall_gocdc
V1POS=$($ETCDC get "/gocdc/ms-verify/sources/src-a/position" --print-value-only 2>/dev/null)
[ -n "$V1POS" ] && ok "MS-12a v2 位点已持久化（迁移源数据就绪）" || bad "MS-12a v2 position 空"
# 造 v1 存量形态：清 sources 子树，把 src-a 位点写到旧键 /position
$ETCDC del --prefix "/gocdc/ms-verify/sources/" >/dev/null
$ETCDC del --prefix "/gocdc/ms-verify/migration/" >/dev/null
$ETCDC put "/gocdc/ms-verify/position" "$V1POS" >/dev/null
V1BODY='config_version: 2
cluster: ms-verify
ha: {enabled: false, lease_ttl_seconds: 10}
source:
  addr: 127.0.0.1:13306
  user: root
  password: "123456"
  charset: utf8mb4
  flavor: mysql
  timezone: UTC
  heartbeat_interval_ms: 3000
  read_timeout_ms: 60000
  exclude_schemas: [mysql, sys, information_schema, performance_schema]
  use_gtid: true
pipeline: {workers: 4, worker_queue_size: 4096, batch_size: 200, flush_interval_ms: 200, position_save_interval_ms: 2000}
stock: {lock_mode: auto, chunk_rows: 5000, workers: 2}
targets:
  mysql:
    enabled: true
    addr: 127.0.0.1:13307
    user: root
    password: "123456"
    charset: utf8mb4
    max_open_conns: 10
    tx_chunk_rows: 100
    target_database: ms_merged_a
    ddl_sync: false
rules:
  - schema: msa
    sinks: {mysql: true}
    ddl_policy: ignore
    tables:
      - {table: orders, order_by_column: id, do_stock: false}
      - {table: evlog, order_by_column: id, do_stock: false}
observability:
  metrics_addr: ${METRICS_ADDR}
  web:
    enabled: true
    addr: ${WEB_ADDR}
    username: admin
    password: ${WEB_PASSWORD}
  log:
    level: info'
D=/tmp/v2/v1up; rm -rf "$D"; mkdir -p "$D"; printf '%s' "$V1BODY" > "$D/body-v1.yml"
flagput ms-verify "$D/body-v1.yml"   # v1 正文走配置中心（形态约束：不用全量配置文件模式）
flag_start v1up ms-verify ":$M" ":$W"
wait_ready $M 40 && ok "MS-12b v1 配置启动（自动包装 default）" || { bad "MS-12b"; tail -20 /tmp/v2/v1up.log; }
wait_src_state $W default running 40 && ok "MS-12c default 源 running" || bad "MS-12c state=$(src_state $W default)"
NEWPOS=$($ETCDC get "/gocdc/ms-verify/sources/default/position" --print-value-only 2>/dev/null)
OLDLEFT=$($ETCDC get "/gocdc/ms-verify/position" --print-value-only 2>/dev/null)
MARK=$($ETCDC get "/gocdc/ms-verify/migration/sources/v1tov2" --print-value-only 2>/dev/null)
[ -n "$NEWPOS" ] && [ -z "$OLDLEFT" ] && ok "MS-12d 旧键已搬到 sources/default/position（旧键清空）" || bad "MS-12d new=${NEWPOS:0:30} old=${OLDLEFT:0:30}"
echo "$MARK" | grep -q "moved_keys" && ok "MS-12e 迁移完成标记落盘" || bad "MS-12e mark=$MARK"
# 迁移幂等：重启再跑一次，不重复搬、位点连续
killall_gocdc
flag_start v1up2 ms-verify ":$M" ":$W"
wait_ready $M 40 && wait_src_state $W default running 40 && ok "MS-12f 二次启动（迁移幂等跳过）" || bad "MS-12f"
$SRC -e "INSERT INTO msa.evlog (v) VALUES (1200)" 2>/dev/null
sleep 3
E12=$($DST -N -e "SELECT COUNT(*) FROM ms_merged_a.evlog WHERE v=1200" 2>/dev/null)
[ "$E12" = "1" ] && ok "MS-12g 升级路径增量续传（位点连续）" || bad "MS-12g v=1200 缺失"

grp "MS-04 源库宕机隔离与恢复（停 vf-src2 → 仅 src-b failed，src-a 不受影响）"
killall_gocdc
flagput ms-verify "$V2/body_ms.yml"
docker stop vf-src2 >/dev/null 2>&1
flag_start iso1 ms-verify ":$M" ":$W"
wait_ready $M 40 && ok "MS-04a 进程存活（源级失败不进程退出，D5）" || bad "MS-04a"
wait_src_phase $W src-b failed 40 && ok "MS-04b src-b 装配 failed（源库不可达）" || bad "MS-04b phase=$(src_phase $W src-b)"
wait_src_state $W src-a running 40 && ok "MS-04c src-a 正常 running（失败隔离）" || bad "MS-04c state=$(src_state $W src-a)"
$SRC -e "INSERT INTO msa.evlog (v) VALUES (401)" 2>/dev/null
sleep 3
E401=$($DST -N -e "SELECT COUNT(*) FROM ms_merged.evlog WHERE v=401" 2>/dev/null)
[ "$E401" = "1" ] && ok "MS-04d src-a 数据持续同步" || bad "MS-04d"
# 源库恢复 → src-b 装配成功自动加入（退避重试自愈）
docker start vf-src2 >/dev/null 2>&1
for i in $(seq 1 60); do
  [ "$(src_phase $W src-b)" = "ready" ] && [ "$(src_state $W src-b)" = "running" ] && break
  sleep 2
done
[ "$(src_state $W src-b)" = "running" ] && ok "MS-04e src-b 恢复后自动加入并 running" || bad "MS-04e phase=$(src_phase $W src-b) state=$(src_state $W src-b)"
$SRC2 -e "INSERT INTO msb.tlog (v) VALUES (402)" 2>/dev/null
sleep 3
E402=$($DST -N -e "SELECT COUNT(*) FROM ms_merged.tlog WHERE v=402" 2>/dev/null)
[ "$E402" = "1" ] && ok "MS-04f src-b 恢复后数据续传" || bad "MS-04f"

grp "MS-10 背压隔离（分目标库：停 vf-dst → 仅 src-a 反压，src-b lag 不受影响）"
killall_gocdc
flagput ms-verify "$V2/body_ms_split.yml"
docker stop vf-dst >/dev/null 2>&1
flag_start bp1 ms-verify ":$M" ":$W"
sleep 8
# dst 宕机：src-a 装配失败（router.Ping fail）或熔断；src-b 指向 dst2 正常
PH_A=$(src_phase $W src-a); ST_B=$(src_state $W src-b)
[ "$ST_B" = "running" ] && ok "MS-10a src-b（dst2）正常 running" || bad "MS-10a state=$ST_B"
$SRC2 -e "INSERT INTO msb.tlog (v) VALUES (500)" 2>/dev/null
wait_row "$DST2" "SELECT COUNT(*) FROM ms_merged_b.tlog WHERE v=500" 1 20 || true
E500=$($DST2 -N -e "SELECT COUNT(*) FROM ms_merged_b.tlog WHERE v=500" 2>/dev/null)
[ "$E500" = "1" ] && ok "MS-10b src-b 数据正常投递（无背压传导）" || bad "MS-10b v=500 缺失（E500=$E500）"
{ [ "$PH_A" = "failed" ] || [ "$(src_state $W src-a)" != "running" ]; } \
  && ok "MS-10c src-a（dst 宕机）未运行（failed/装配重试）" || bad "MS-10c phase=$PH_A state=$(src_state $W src-a)"
# dst 恢复 → src-a 自愈
docker start vf-dst >/dev/null 2>&1
for i in $(seq 1 60); do
  [ "$(src_state $W src-a)" = "running" ] && break
  sleep 2
done
[ "$(src_state $W src-a)" = "running" ] && ok "MS-10d dst 恢复后 src-a 自愈 running" || bad "MS-10d phase=$(src_phase $W src-a) state=$(src_state $W src-a)"

grp "MS-13 旧 leader 位点写拒绝（协调面 etcd 失联 → gate 消失 → 熔断）"
killall_gocdc
flagput ms-verify "$V2/body_ms.yml"
flag_start lg1 ms-verify ":$M" ":$W"
wait_ready $M 40 && wait_src_state $W src-a running 40 && wait_src_state $W src-b running 40 && ok "MS-13a 双源基线 running" || bad "MS-13a"
# 协调面失联：pause vf-etcd → lease 到期 → gate 消失 → 位点写被拒 → 熔断停管线
docker pause vf-etcd >/dev/null 2>&1
sleep 14   # lease_ttl=10s + 观察窗口
docker unpause vf-etcd >/dev/null 2>&1
sleep 3
STA=$(src_state $W src-a); STB=$(src_state $W src-b)
{ [ "$STA" != "running" ] || [ "$STB" != "running" ]; } \
  && ok "MS-13b etcd 失联后源管线熔断（$STA/$STB，gate 拒写）" || bad "MS-13b a=$STA b=$STB（仍 running=未熔断）"
grep -q "门槛\|熔断\|gate" /tmp/v2/lg1.log && ok "MS-13c 日志含门槛拒绝/熔断证据" || bad "MS-13c"
# 位点未被推进损坏（无主写入为零）：重启后正常续传
killall_gocdc
flag_start lg2 ms-verify ":$M" ":$W"
wait_ready $M 40 && wait_src_state $W src-a running 40 && wait_src_state $W src-b running 40 && ok "MS-13d 恢复后双源续传" || bad "MS-13d"
$SRC -e "INSERT INTO msa.evlog (v) VALUES (1300)" 2>/dev/null
$SRC2 -e "INSERT INTO msb.tlog (v) VALUES (1301)" 2>/dev/null
sleep 3
E13A=$($DST -N -e "SELECT COUNT(*) FROM ms_merged.evlog WHERE v=1300" 2>/dev/null)
E13B=$($DST -N -e "SELECT COUNT(*) FROM ms_merged.tlog WHERE v=1301" 2>/dev/null)
[ "$E13A" = "1" ] && [ "$E13B" = "1" ] && ok "MS-13e 双源数据零丢失续传" || bad "MS-13e a=$E13A b=$E13B"

killall_gocdc
summary

#!/bin/bash
# v2 组46：多源 HA 与故障隔离（MS 系列故障面，PRD 08 §2）
# MS-03 全量中杀 leader 接管 / MS-04 源库宕机隔离与恢复 / MS-10 背压隔离（分目标库）/
# MS-12 v1 正文拒启 + etcd 键迁移幂等 + 位点连续 / MS-13 门键失主位点写拒绝熔断
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

grp "MS-12 v1 正文拒启 + etcd 键迁移幂等 + 位点连续"
# v1 顶层正文已移除（load 加载即拒）——升级路径 = 人工改写 sources[]。键迁移器
# 仍在（main 装配期：legacy 键 → sources/default/，幂等 CAS + 完成标记，迁移
# 落点源 id 恒为 default，正文源 id 须填 default 对齐，RUNBOOK §9.4）。
# 全程四步：①v1 正文拒启（位点子树不被触碰）②造 v1 键布局 ③v2 正文（id=default）
# 启动触发迁移 ④幂等重启 + 位点连续。
killall_gocdc
V1POS=$($ETCDC get "/gocdc/ms-verify/sources/src-a/position" --print-value-only 2>/dev/null)
[ -n "$V1POS" ] && ok "MS-12a v2 位点已持久化（迁移源数据就绪）" || bad "MS-12a v2 position 空"
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
# ① v1 顶层正文 → 加载即拒（配置校验 fatal 退出，发生在装配前）
D=/tmp/v2/v1up; rm -rf "$D"; mkdir -p "$D"; printf '%s' "$V1BODY" > "$D/body-v1.yml"
flagput ms-verify "$D/body-v1.yml"
flag_start v1up ms-verify ":$M" ":$W"
V1REJ=0
for i in $(seq 1 15); do
  grep -q "顶层 source 键" /tmp/v2/v1up.log 2>/dev/null && { V1REJ=1; break; }; sleep 1
done
PGONE=$( [ -z "$(pickpid v1up)" ] && echo 1 || echo 0 )
STILL=$($ETCDC get "/gocdc/ms-verify/sources/src-a/position" --print-value-only 2>/dev/null)
[ "$V1REJ" = "1" ] && [ "$PGONE" = "1" ] && [ "$STILL" = "$V1POS" ] \
  && ok "MS-12b v1 顶层正文拒启（fatal 退出，位点子树未被触碰）" \
  || bad "MS-12b rej=$V1REJ pidgone=$PGONE pos_untouched=$([ "$STILL" = "$V1POS" ] && echo yes || echo no)"
# ② 造 v1 存量键布局：sources 子树清空、位点回到旧键 /position、清迁移标记
$ETCDC del --prefix "/gocdc/ms-verify/sources/" >/dev/null
$ETCDC del --prefix "/gocdc/ms-verify/migration/" >/dev/null
$ETCDC put "/gocdc/ms-verify/position" "$V1POS" >/dev/null
# ③ v2 正文（源 id=default 对齐迁移落点）→ 装配期键迁移 + 旧位点续传
D2=/tmp/v2/mig1; rm -rf "$D2"; mkdir -p "$D2"
cat > "$D2/body-default.yml" <<'EOF'
config_version: 2
cluster: ms-verify
ha: {enabled: false, lease_ttl_seconds: 10}
sources:
  - id: default
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
    level: info
EOF
flagput ms-verify "$D2/body-default.yml"
flag_start mig1 ms-verify ":$M" ":$W"
wait_ready $M 40 && wait_src_state $W default running 40 && ok "MS-12c default 源 running（迁移后旧位点续传）" || bad "MS-12c state=$(src_state $W default)"
NEWPOS=$($ETCDC get "/gocdc/ms-verify/sources/default/position" --print-value-only 2>/dev/null)
OLDLEFT=$($ETCDC get "/gocdc/ms-verify/position" --print-value-only 2>/dev/null)
MARK=$($ETCDC get "/gocdc/ms-verify/migration/sources/v1tov2" --print-value-only 2>/dev/null)
[ -n "$NEWPOS" ] && [ -z "$OLDLEFT" ] && ok "MS-12d 旧键已搬到 sources/default/position（旧键清空）" || bad "MS-12d new=${NEWPOS:0:30} old=${OLDLEFT:0:30}"
echo "$MARK" | grep -q "moved_keys" && ok "MS-12e 迁移完成标记落盘" || bad "MS-12e mark=$MARK"
# ④ 迁移幂等：重启再跑一次（标记在 → 全跳过不重写），位点连续
killall_gocdc
flag_start mig2 ms-verify ":$M" ":$W"
wait_ready $M 40 && wait_src_state $W default running 40 && ok "MS-12f 二次启动 running" || bad "MS-12f"
MARK2=$($ETCDC get "/gocdc/ms-verify/migration/sources/v1tov2" --print-value-only 2>/dev/null)
[ "$MARK2" = "$MARK" ] && echo "  迁移标记未重写（幂等跳过）" || echo "  ⚠️ 标记被重写：$MARK2"
$SRC -e "INSERT INTO msa.evlog (v) VALUES (1200)" 2>/dev/null
wait_row "$DST" "SELECT COUNT(*) FROM ms_merged_a.evlog WHERE v=1200" 1 15 || true
E12=$($DST -N -e "SELECT COUNT(*) FROM ms_merged_a.evlog WHERE v=1200" 2>/dev/null)
{ [ "$E12" = "1" ] && [ "$MARK2" = "$MARK" ]; } && ok "MS-12g 升级路径增量续传（位点连续+迁移幂等）" || bad "MS-12g v=1200 缺失/标记漂移"

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

grp "MS-13 门键失主 → 位点写拒绝熔断（split-brain 保护）"
killall_gocdc
flagput ms-verify "$V2/body_ms.yml"
flag_start lg1 ms-verify ":$M" ":$W"
wait_ready $M 40 && wait_src_state $W src-a running 40 && wait_src_state $W src-b running 40 && ok "MS-13a 双源基线 running" || bad "MS-13a"
# 门槛拒绝的确定性注入是删门键 {prefix}/leader（etcd 可达 + Compare(LeaseValue)
# ==本 lease 恒不成立 → ErrGateRejected → 立即熔断，persister.go：失主/分区拒写）。
# 不可用 pause vf-etcd 造"失联"：那是网络错误路径，位点保存走退避重试（03 §5.2
# 失败不关管线），达到熔断上限要凑 maxFailures 次超时——测不到门槛拒绝语义，
# 且旧版 grep 'gate' 命中的是启动行 gate_key 字段（假阳性，2026-11-04 实证）。
# 另注意：persister 周期保存只在水位有推进时发起（Advance 判据）——空闲源即使
# 失主也不会尝试保存、测不到拒绝。删门键后给两源各插一行强制水位推进，
# 下一次位点保存（interval 2s）必吃 ErrGateRejected → 双源全熔断。
$ETCDC del "/gocdc/ms-verify/leader" >/dev/null
$SRC -e "INSERT INTO msa.evlog (v) VALUES (1390)" 2>/dev/null
$SRC2 -e "INSERT INTO msb.tlog (v) VALUES (1391)" 2>/dev/null
BRK=0
for i in $(seq 1 30); do
  SA=$(src_state $W src-a); SB=$(src_state $W src-b)
  [ "$SA" = "paused" ] && [ "$SB" = "paused" ] && { BRK=1; echo "  双源熔断 paused t=${i}s"; break; }
  sleep 1
done
[ "$BRK" = "1" ] && ok "MS-13b 门键失主后位点写拒绝，双源熔断 paused" || bad "MS-13b a=$(src_state $W src-a) b=$(src_state $W src-b)"
grep -q "门槛拒绝\|立即熔断" /tmp/v2/lg1.log && ok "MS-13c 日志含门槛拒绝/立即熔断证据" || bad "MS-13c"
# 位点未被推进损坏（无主写入为零）：重启后门键随 session 重建，正常续传
killall_gocdc
flag_start lg2 ms-verify ":$M" ":$W"
wait_ready $M 40 && wait_src_state $W src-a running 40 && wait_src_state $W src-b running 40 && ok "MS-13d 恢复后双源续传" || bad "MS-13d"
$SRC -e "INSERT INTO msa.evlog (v) VALUES (1300)" 2>/dev/null
$SRC2 -e "INSERT INTO msb.tlog (v) VALUES (1301)" 2>/dev/null
wait_row "$DST" "SELECT COUNT(*) FROM ms_merged.evlog WHERE v=1300" 1 15 || true
wait_row "$DST" "SELECT COUNT(*) FROM ms_merged.tlog WHERE v=1301" 1 15 || true
E13A=$($DST -N -e "SELECT COUNT(*) FROM ms_merged.evlog WHERE v=1300" 2>/dev/null)
E13B=$($DST -N -e "SELECT COUNT(*) FROM ms_merged.tlog WHERE v=1301" 2>/dev/null)
[ "$E13A" = "1" ] && [ "$E13B" = "1" ] && ok "MS-13e 双源数据零丢失续传" || bad "MS-13e a=$E13A b=$E13B"

killall_gocdc
summary

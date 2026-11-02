#!/bin/bash
# v2 组10：启动/配置（A 系列）——A1 flag 契约 / A2 坏配置完整清单 / A3 ${ENV} 缺失 /
# A4 binlog_format 拒启 / A5a 目标缺列 fail-fast / A5b 目标多余可空列放行 /
# A6 三端点+配置中心来源 / A7 下游不可达拒启 / A8 /config 打码
# 全部启动走「零参数 + 工作目录引导桩（config_source=etcd）」，配置正文来自 cfg-etcd:12380。
set -u
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

# cc_run <name> <cluster> <bodyfile|->: 前台桩启动（预期拒启），打印 exit 与输出
cc_run(){
  local name=$1 cluster=$2 body=$3
  local d=/tmp/v2/$name; rm -rf "$d"; mkdir -p "$d"
  cat > "$d/config.local.yml" <<EOF
cluster: $cluster
bootstrap:
  config_source: etcd
  config_center:
    etcd_addrs: [127.0.0.1:12380]
  etcd_addrs:
    - 127.0.0.1:12379
EOF
  cp "$BIN" "$d/gocdcbin"
  [ "$body" != "-" ] && ccput "$cluster" "$body"
  (cd "$d" && WEB_PASSWORD=demo123 METRICS_ADDR=:9595 WEB_ADDR=:8060 timeout 30 ./gocdcbin > out.log 2>&1; echo $? > exit.log)
  echo "  [$name] exit=$(cat "$d/exit.log")"
  sed 's/^/    /' "$d/out.log" | head -30
}

wipe_all
grp "A1 flag 契约（-version / 无引导+无flag / -h）"
VOUT=$("$BIN" -version 2>&1); VE=$?
echo "  -version exit=$VE output=[$VOUT]"
[ $VE -eq 0 ] && [ -n "$VOUT" ] && ok "A1a -version 输出版本 exit=0" || bad "A1a -version"

mkdir -p /tmp/v2/noboot && rm -f /tmp/v2/noboot/config.local.yml
cp "$BIN" /tmp/v2/noboot/gocdcbin
(cd /tmp/v2/noboot && WEB_PASSWORD=demo123 timeout 15 ./gocdcbin > out.log 2>&1; echo $? > exit.log)
NE=$(cat /tmp/v2/noboot/exit.log); NOUT=$(cat /tmp/v2/noboot/out.log)
echo "  无引导 exit=$NE output头部: $(echo "$NOUT" | head -1)"
[ "$NE" = "1" ] && echo "$NOUT" | grep -q "引导文件" && ok "A1b 无引导文件+无flag → 引导说明 exit=1" || bad "A1b 无引导拒启"

HOUT=$("$BIN" -h 2>&1); HC=$(echo "$HOUT" | grep -cE '^\s+-(cluster|etcd|config-key|config-file|version)\b')
echo "  -h flag 数=$HC（期待5）: $(echo "$HOUT" | grep -E '^\s+-' | tr '\n' ' ' | head -c 200)"
[ "$HC" = "5" ] && ok "A1c -h 恰 5 flag" || bad "A1c -h flag 数=$HC"

grp "A3 \${ENV} 缺失指名报错（配置中心正文含未设环境变量）"
sed 's/password: "123456"/password: ${NO_SUCH_ENV_XYZ}/' "$V2/body_main.yml" > /tmp/v2/body-a3.yml
cc_run a3 verify /tmp/v2/body-a3.yml
grep -q "NO_SUCH_ENV_XYZ" /tmp/v2/a3/out.log && [ "$(cat /tmp/v2/a3/exit.log)" = "1" ] \
  && ok "A3 精确报 NO_SUCH_ENV_XYZ 未设置 exit=1" || bad "A3"

grp "A2 坏配置完整清单不短路（配置中心正文 16+ 处错误）"
cat > /tmp/v2/body-a2.yml <<'EOF'
config_version: 3
cluster: othercl
source:
  addr: ""
  user: ""
  password: "123456"
  charset: ""
  flavor: oracle
  server_id: 0
  timezone: ""
  exclude_schemas: [mysql, sys, information_schema]
ha:
  lease_ttl_seconds: 1
  leader_check_interval_ms: 0
  resign_on_shutdown: true
pipeline:
  workers: 0
  worker_queue_size: 8192
  batch_size: 0
  flush_interval_ms: 200
  position_save_interval_ms: 3000
  on_convert_error: bogus
  decimal_as: int
targets:
  mysql:
    enabled: true
    addr: 127.0.0.1:13307
    user: root
    password: "123456"
    charset: utf8mb4
    ddl_sync: true
  rocketmq:
    enabled: true
    name_servers: [127.0.0.1:8081]
    group: gocdc-verify
    topic_prefix: "CDC."
    order_by: row
rules:
  - schema: shop
    sinks: {mysql: true}
    tables: []
  - schema: shop
    sinks: {mysql: true}
    tables:
      - {table: orders, do_stock: true}
      - {table: orders, do_stock: true, order_by_column: id}
  - schema: bad schema
    sinks: {mysql: true}
    tables:
      - {table: t1, do_stock: false, mask_columns: {"bad col": phone}}
observability:
  metrics_addr: ":9595"
  web:
    enabled: true
    addr: ":8060"
    username: admin
    password: demo123
    auth_disabled: false
  log:
    level: info
EOF
cc_run a2 verify /tmp/v2/body-a2.yml
ERRN=$(grep -c '错\|必须\|须\|不能\|必填\|重复\|非法' /tmp/v2/a2/out.log)
echo "  错误行数=$ERRN（期待 ≥15，不短路）"
[ "$(cat /tmp/v2/a2/exit.log)" = "1" ] && [ "$ERRN" -ge 15 ] \
  && grep -q "属于别的集群\|cluster" /tmp/v2/a2/out.log \
  && ok "A2 完整错误清单 exit=1（含 cluster 裁决）" || bad "A2 错误行数=$ERRN"

grp "A7 下游不可达：源级 failed + 退避重试（多源 D5：不再进程退出，严格改进）"
sed 's|addr: 127.0.0.1:13307|addr: 127.0.0.1:13399|' "$V2/body_main.yml" > /tmp/v2/body-a7.yml
ccput verify /tmp/v2/body-a7.yml
stub_start a7 verify :9595 :8060
if wait_ready 9595 40; then
  # 源装配失败被隔离：轮询等失败落地（ping 超时 5s+ 在装配期），进程保持存活
  wait_log /tmp/v2/a7.log "源装配失败" 30
  wait_phase 8060 default failed 10
  PH=$(curl -s --noproxy '*' -u admin:demo123 localhost:8060/api/v1/status | jget 'd["sources"][0]["phase"]' | tr -d '[:space:]')
  if grep -q "下游目标不可达" /tmp/v2/a7.log && grep -qiE "connection refused|i/o timeout" /tmp/v2/a7.log \
    && [ "$PH" = "failed" ]; then
    ok "A7 源 failed(startup)+进程存活（失败隔离，退避重试）"
  else
    bad "A7 phase=$PH $(grep -m1 源装配失败 /tmp/v2/a7.log | head -c 120)"
  fi
else
  bad "A7 进程未存活（healthz 超时）"
fi
killall_gocdc

grp "A4 binlog_format=MIXED：源级 failed + 退避重试（多源 D5 语义）"
$SRC -e "SET GLOBAL binlog_format='MIXED'" 2>/dev/null
ccput verify "$V2/body_main.yml"
stub_start a4 verify :9595 :8060
if wait_ready 9595 40; then
  wait_log /tmp/v2/a4.log "源装配失败" 30
  if grep -qE 'binlog_format|ROW' /tmp/v2/a4.log && grep -q "源装配失败" /tmp/v2/a4.log; then
    ok "A4 非 ROW 源 failed + 退避重试（进程存活）"
  else
    bad "A4 $(grep -m1 源装配失败 /tmp/v2/a4.log | head -c 150)"
  fi
else
  bad "A4 进程未存活"
fi
killall_gocdc
$SRC -e "SET GLOBAL binlog_format='ROW'" 2>/dev/null
echo "  已恢复 ROW: $($SRC -N -e 'SELECT @@global.binlog_format' 2>/dev/null)"

grp "A5a 目标缺列：源级 failed + 精准报列（多源 D5 语义）"
$DST -e "ALTER TABLE shop.orders DROP COLUMN remark" 2>/dev/null
ccput verify "$V2/body_main.yml"
stub_start a5a verify :9595 :8060
if wait_ready 9595 40; then
  wait_log /tmp/v2/a5a.log "源装配失败" 30
  if grep -q "remark" /tmp/v2/a5a.log && grep -q "目标表结构核对未通过\|源装配失败" /tmp/v2/a5a.log; then
    ok "A5a 精准报缺少源列 remark（源 failed，进程存活）"
  else
    bad "A5a $(grep -m1 -E 'remark|装配失败' /tmp/v2/a5a.log | head -c 150)"
  fi
else
  bad "A5a 进程未存活"
fi
killall_gocdc
$DST -e "ALTER TABLE shop.orders ADD COLUMN remark VARCHAR(255) NULL AFTER amount" 2>/dev/null

grp "A5b 目标多余可空列静默放行"
$DST -e "ALTER TABLE shop.orders ADD COLUMN extra_col VARCHAR(32) NULL" 2>/dev/null
ccput verify "$V2/body_main.yml"
stub_start node1 verify :9595 :8060
sleep 8
if wait_ready 9595 60; then
  FATALN=$(grep -ci 'fatal\|缺少' /tmp/v2/node1.log || true)
  [ "$FATALN" = "0" ] && ok "A5b 多余可空列静默放行（启动+全量正常）" || bad "A5b 日志有 fatal=$FATALN"
else
  bad "A5b 未就绪"
fi

grp "A6/A8 正常启动三端点 + /config 打码 + 配置中心来源"
ST=$(curl -s --noproxy '*' -u admin:demo123 localhost:8060/api/v1/status)
echo "  /status: $(echo "$ST" | head -c 220)"
echo "$ST" | grep -q '"config_source":"etcd:/gocdc/verify/config"' && ok "A6 /status config_source=etcd（配置中心形态生效）" || bad "A6 config_source"
echo "$ST" | grep -q '"role":"leader"' && ok "A6b role=leader" || bad "A6b role"
H=$(curl -s --noproxy '*' localhost:9595/healthz); R=$(curl -s --noproxy '*' localhost:9595/readyz)
MN=$(curl -s --noproxy '*' localhost:9595/metrics | grep -c '^gocdc_')
echo "  healthz=[$H] readyz=[$R] gocdc_指标=$MN"
[ "$H" = "ok" ] && echo "$R" | grep -qi "ready" && [ "$MN" -gt 50 ] && ok "A6c 三端点正常（/healthz /readyz /metrics）" || bad "A6c h=$H r=$R"
CFG=$(curl -s --noproxy '*' -u admin:demo123 localhost:8060/api/v1/config)
MASKN=$(echo "$CFG" | grep -o '\*\*\*' | wc -l)
LEAK=$(echo "$CFG" | grep -c 'password[":= ]*123456\|demo123' || true)
echo "  /config 打码 ***数=$MASKN 泄漏=$LEAK"
[ "$MASKN" -ge 2 ] && [ "$LEAK" = "0" ] && ok "A8 /config 密码全打码无泄漏" || bad "A8 mask=$MASKN leak=$LEAK"

killall_gocdc
summary

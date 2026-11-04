#!/bin/bash
# v2 组90：高优先级补验集（etcd 配置中心形态专项）
# ① bootstrap etcd 桩 + 双 etcd 分离（12380 配置中心 ≠ 12379 协调面）
# ② 规则级 target_database 非同名覆盖 ③ 带点库名全链（stock/增量/verify/DDL/RMQ）
# ④ ha.enabled=false 单节点（纯 flag 形态）⑤ 多 cluster 前缀隔离共存 ⑥ 桩错误路径
set -u
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
API="localhost:8060/api/v1"

wipe_all
ccput verify "$V2/body_hiprio.yml"
stub_start node1 verify :9595 :8060
wait_ready 9595 60 || { echo "FATAL 启动失败"; exit 1; }
# 等选举+加载完成（/status 出 role 为准）
for i in $(seq 1 30); do
  R=$(roleof 8060)
  [ -n "$R" ] && break; sleep 2
done
# 等 stock 收敛（edu.mall.merge.users=3）
for i in $(seq 1 30); do
  M0=$($DST -N -e "SELECT COUNT(*) FROM \`edu.mall.merge\`.users" 2>/dev/null)
  [ "$M0" = "3" ] && break; sleep 2
done

grp "① 桩形态 + 双 etcd 分离断言"
ST=$(curl -s --noproxy '*' -u admin:demo123 "$API/status")
echo "  /status: $(echo "$ST" | head -c 200)"
echo "$ST" | grep -q '"config_source":"etcd:/gocdc/verify/config"' && ok "①a config_source=etcd（配置中心拉取）" || bad "①a"
echo "$ST" | grep -q '"role":"leader"' && ok "①b 协调面(12379)选举正常（与配置中心分离）" || bad "①b"

grp "②③ 带点库名 + target_database 非同名覆盖（stock/增量/verify/DDL/RMQ）"
M=$($DST -N -e "SELECT COUNT(*) FROM \`edu.mall.merge\`.users" 2>/dev/null)
echo "  stock: merge.users=$M（期待3）"
[ "$M" = "3" ] && ok "③a stock 落非同名目标库" || bad "③a (m=$M)"
$SRC -e "INSERT INTO \`edu.mall.admin\`.users (name,phone) VALUES ('dotdb-incr','13900000003')" 2>/dev/null
$SRC -e "UPDATE \`edu.mall.admin\`.users SET name='dotdb-a-upd' WHERE name='dotdb-a'" 2>/dev/null
V2Q=0
for i in $(seq 1 15); do
  LB=$(lagof 9595); [ "$LB" = "0" ] && { V2Q=1; break; }; sleep 2
done
# lag=0 是读取位点口径；sink 批次应用可再晚一拍（实测 UPDATE 晚于 INSERT 落库），
# 断言必须轮询收敛，瞬时单查必假阴
MI=0; MU=0
for i in $(seq 1 15); do
  MI=$($DST -N -e "SELECT COUNT(*) FROM \`edu.mall.merge\`.users WHERE name='dotdb-incr'" 2>/dev/null)
  MU=$($DST -N -e "SELECT COUNT(*) FROM \`edu.mall.merge\`.users WHERE name='dotdb-a-upd'" 2>/dev/null)
  [ "$MI" = "1" ] && [ "$MU" = "1" ] && { echo "  收敛 t=${i}x2s"; break; }
  sleep 2
done
echo "  增量: INSERT=$MI UPDATE=$MU（期待1/1）"
[ "$MI" = "1" ] && [ "$MU" = "1" ] && ok "③b 带点库名增量落 merge" || bad "③b ($MI/$MU)"
VR=$(curl -s --noproxy '*' -u admin:demo123 -X POST "$API/sources/default/verify?confirm=true" -H 'Content-Type: application/json' -d '{"database":"edu.mall.admin","table":"users"}')
echo "  verify: $(echo "$VR" | head -c 150)"
echo "$VR" | grep -q '"mismatch":0' && ok "③c verify 走 target_database 对账 0 mismatch" || bad "③c"
$SRC -e "ALTER TABLE \`edu.mall.admin\`.users ADD COLUMN email VARCHAR(64) NULL" 2>/dev/null
DC=0
for i in $(seq 1 15); do
  C=$($DST -N -e "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='edu.mall.merge' AND table_name='users' AND column_name='email'" 2>/dev/null)
  [ "$C" = "1" ] && { DC=1; break; }; sleep 2
done
[ "$DC" = "1" ] && ok "③d 带点库名 DDL auto 同步" || bad "③d (c=$DC)"

grp "③e RMQ 带点库名双写（topic 点换下划线）"
TS=$(date +%s)
setsid nohup $RMQCONS -topic CDC_edu_mall_admin_users -group hiprio-$TS -out /tmp/rmq-hiprio.jsonl -idle 12s > /tmp/rmqcons-hiprio.log 2>&1 &
sleep 4
$SRC -e "INSERT INTO \`edu.mall.admin\`.users (name,phone,email) VALUES ('dotdb-rmq','13900000004','rmq@x.c')" 2>/dev/null
RMQN=0
for i in $(seq 1 20); do
  RMQN=$(grep -ac 'dotdb-rmq' /tmp/rmq-hiprio.jsonl 2>/dev/null); RMQN=${RMQN:-0}
  [ "$RMQN" -ge 1 ] && break; sleep 2
done
CERR=$(grep -ac ILLEGAL_TOPIC /tmp/rmqcons-hiprio.log 2>/dev/null); CERR=${CERR:-0}
echo "  dotdb-rmq 消息=$RMQN ILLEGAL_TOPIC=$CERR"
[ "$RMQN" -ge 1 ] && [ "$CERR" = "0" ] && ok "③e 带点库名 RMQ 双写（CDC_edu_mall_admin_users）" || bad "③e (n=$RMQN err=$CERR)"

grp "④ ha.enabled=false 单节点 + 纯 flag 形态 + ⑤ 多 cluster 共存"
flagput verify-single "$V2/body_single.yml"
flag_start node2s verify-single :9596 :8061
# 等纯 flag 节点 web 就绪 + role=single + 门键落地
for i in $(seq 1 40); do
  SR=$(roleof 8061)
  [ "$SR" = "single" ] && break; sleep 2
done
wait_ready 9596 30 >/dev/null || bad "node2s 未就绪"
SS=$(curl -s --noproxy '*' -u admin:demo123 "localhost:8061/api/v1/status")
echo "  verify-single /status: $(echo "$SS" | head -c 200)"
echo "$SS" | grep -q '"role":"single"' && ok "④a ha.enabled=false role=single（纯 flag 启动）" || bad "④a"
echo "$SS" | grep -q '"config_source":"etcd:/gocdc/verify-single/config"' && ok "④b 纯 flag 单 etcd 双角色（配置中心=协调面 12379）" || bad "④b"
S1=0; GATE=""
for i in $(seq 1 20); do
  S1=$(curl -s --noproxy '*' localhost:9596/metrics | grep -a '^gocdc_leader_state' | awk '{print $2}')
  GATE=$($ETCDC get /gocdc/verify-single/leader --print-value-only 2>/dev/null)
  [ "$S1" = "1" ] && [ -n "$GATE" ] && break; sleep 2
done
echo "  leader_state=$S1 门键=[$(echo "$GATE" | head -c 40)]"
[ "$S1" = "1" ] && [ -n "$GATE" ] && ok "④c 跳过选举直接 leader + 租约门键" || bad "④c"
NPROC=$(ps -eo args | awk '$1 ~ /gocdcbin$/' | wc -l)
KV1=$($ETCDC get /gocdc/verify/leader --print-value-only 2>/dev/null)
KV2=$($ETCDC get /gocdc/verify-single/leader --print-value-only 2>/dev/null)
echo "  存活进程=$NPROC verify门键=$([ -n "$KV1" ] && echo 有) verify-single门键=$([ -n "$KV2" ] && echo 有)"
[ "$NPROC" -ge 2 ] && [ -n "$KV1" ] && [ -n "$KV2" ] && ok "⑤ 多 cluster 前缀隔离共存" || bad "⑤ (n=$NPROC)"
RR=$(curl -s --noproxy '*' -u admin:demo123 -o /dev/null -w '%{http_code}' -X POST "localhost:8061/api/v1/leader/resign?confirm=true")
S2=$(curl -s --noproxy '*' localhost:9596/metrics | grep -a '^gocdc_leader_state' | awk '{print $2}')
echo "  单节点 resign http=$RR leader_state=$S2（期待200/1=无操作不停）"
[ "$RR" = "200" ] && [ "$S2" = "1" ] && ok "④d 单节点 resign 无操作" || bad "④d"
# 停机 → 门键随租约消失（租约 TTL ≤10s + 优雅撤销，轮询等待）
kill -TERM $(pickpid node2s) 2>/dev/null
for i in $(seq 1 30); do [ -z "$(pickpid node2s)" ] && break; sleep 1; done
GATE2="kept"
for i in $(seq 1 15); do
  GATE2=$($ETCDC get /gocdc/verify-single/leader --print-value-only 2>/dev/null)
  [ -z "$GATE2" ] && break; sleep 1
done
[ -z "$GATE2" ] && ok "④e 停机后门键随租约消失" || bad "④e (gate=$GATE2)"

grp "⑥ 桩错误路径（key 不存在 / 配置中心不可达）"
d=/tmp/v2/stub-ghost; rm -rf $d; mkdir -p $d
cat > $d/config.local.yml <<'EOF'
cluster: ghost
bootstrap:
  config_source: etcd
  config_center:
    etcd_addrs: [127.0.0.1:12380]
    key: /gocdc/ghost/config
  etcd_addrs:
    - 127.0.0.1:12379
EOF
cp "$BIN" $d/gocdcbin
(cd $d && WEB_PASSWORD=demo123 timeout 20 ./gocdcbin > out.log 2>&1; echo $? > e.log)
GE=$(cat $d/e.log); GOUT=$(grep -aE '不存在|fatal' $d/out.log | head -1)
echo "  ghost: exit=$GE $GOUT"
d2=/tmp/v2/stub-down; rm -rf $d2; mkdir -p $d2
cat > $d2/config.local.yml <<'EOF'
cluster: verify
bootstrap:
  config_source: etcd
  config_center:
    etcd_addrs: [127.0.0.1:12399]
  etcd_addrs:
    - 127.0.0.1:12379
EOF
cp "$BIN" $d2/gocdcbin
(cd $d2 && WEB_PASSWORD=demo123 timeout 30 ./gocdcbin > out.log 2>&1; echo $? > e.log)
DE=$(cat $d2/e.log); DOUT=$(grep -aE '配置中心|连接失败|fatal|重试' $d2/out.log | head -1)
echo "  down: exit=$DE $DOUT"
[ "$GE" != "0" ] && echo "$GOUT" | grep -q "不存在" && [ "$DE" != "0" ] \
  && ok "⑥ 桩错误路径双拒启" || bad "⑥ (ge=$GE de=$DE)"

killall_gocdc
summary

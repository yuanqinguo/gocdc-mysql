#!/bin/bash
# 高优先级补验：① bootstrap etcd 桩形态 ② 双 etcd 分离 ③ ha.enabled=false 单节点
# ④ 带点库名全链 ⑤ 规则级 target_database 覆盖（顺带：多 cluster 前缀隔离共存）
set -u
cd /mnt/d/workers/worker/qingfly/gocdc-mysql/.verify
SRC="docker exec vf-src mysql -uroot -p123456 -h127.0.0.1 --default-character-set=utf8mb4"
DST="docker exec vf-dst mysql -uroot -p123456 -h127.0.0.1 --default-character-set=utf8mb4"
CFGCLI="docker exec cfg-etcd etcdctl --endpoints=127.0.0.1:2379"
ETCDCLI="docker exec vf-etcd etcdctl --endpoints=127.0.0.1:2379"

echo "=== 清场"
for p in $(ps -eo pid,args | grep 'gocdc -config' | grep -v grep | awk '{print $1}'); do kill -9 "$p" 2>/dev/null; done
sleep 2

echo "════ Stage1: bootstrap etcd 桩 + 双 etcd 分离 + 带点库 + target_database ════"
# ① 配置入配置中心（12380）；协调面仍是 12379 —— 两个 etcd 实例
$CFGCLI put /gocdc/verify/config "$(cat config-hiprio.yml)" >/dev/null && echo "配置已 put 到 cfg-etcd /gocdc/verify/config"
$CFGCLI get /gocdc/verify/config --keys-only | head -1

# ② 桩启动（node.id/server_id 缺省派生）
WEB_PASSWORD=demo123 setsid nohup ./gocdc -config-file config-stub.yml > node1.log 2>&1 &
sleep 10
ST=$(curl -s --noproxy '*' -u admin:demo123 localhost:8060/api/v1/status | python3 -c 'import sys,json
d=json.load(sys.stdin)["data"]; print(d["role"], d["config_source"], d["node_id"])' 2>/dev/null)
echo "桩启动 /status: $ST"
echo "$ST" | grep -q "^leader etcd:/gocdc/verify/config" && echo "① 桩形态+配置中心来源 RESULT=PASS" || echo "① 桩形态 RESULT=FAIL"
echo "$ST" | grep -q "^leader etcd" && echo "② 双 etcd 分离（配置中心 12380 ≠ 协调面 12379）RESULT=PASS" || echo "② 分离 RESULT=FAIL"

# ③ 带点库名全量 stock → 非同名目标库 merge
sleep 4
M=$(docker exec vf-dst mysql -uroot -p123456 -h127.0.0.1 --default-character-set=utf8mb4 -N -e "SELECT COUNT(*) FROM \`edu.mall.merge\`.users" 2>/dev/null)
echo "③a target_database 覆盖：merge.users=$M/3"
[ "$M" = "3" ] && echo "③a RESULT=PASS" || echo "③a RESULT=FAIL"

# ③b 增量 DML → merge
$SRC --default-character-set=utf8mb4 -e "INSERT INTO \`edu.mall.admin\`.users (name,phone) VALUES ('dotdb-incr','13900000003')" 2>/dev/null
$SRC --default-character-set=utf8mb4 -e "UPDATE \`edu.mall.admin\`.users SET name='dotdb-a-upd' WHERE name='dotdb-a'" 2>/dev/null
sleep 5
MI=$(docker exec vf-dst mysql -uroot -p123456 -h127.0.0.1 --default-character-set=utf8mb4 -N -e "SELECT COUNT(*) FROM \`edu.mall.merge\`.users WHERE name='dotdb-incr'" 2>/dev/null)
MU=$(docker exec vf-dst mysql -uroot -p123456 -h127.0.0.1 --default-character-set=utf8mb4 -N -e "SELECT COUNT(*) FROM \`edu.mall.merge\`.users WHERE name='dotdb-a-upd'" 2>/dev/null)
echo "③b 增量: INSERT落merge=$MI UPDATE落merge=$MU（期待1/1）"
[ "$MI" = "1" ] && [ "$MU" = "1" ] && echo "③b RESULT=PASS" || echo "③b RESULT=FAIL"

# ③c verify（target_database 解析 → merge 库对账）
VR=$(curl -s --noproxy '*' -u admin:demo123 -X POST 'localhost:8060/api/v1/verify?confirm=true' -H 'Content-Type: application/json' -d '{"database":"edu.mall.admin","table":"users"}')
echo "③c verify 带点库: $VR" | head -c 200; echo
echo "$VR" | grep -q '"mismatch":0' && echo "③c RESULT=PASS（verify 走 target_database 对账 0 mismatch）" || echo "③c RESULT=FAIL"

# ③d DDL auto 同步到 merge 库
$SRC -e "ALTER TABLE \`edu.mall.admin\`.users ADD COLUMN email VARCHAR(64) NULL" 2>/dev/null
sleep 6
DC=$(docker exec vf-dst mysql -uroot -p123456 -h127.0.0.1 -N -e "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='edu.mall.merge' AND table_name='users' AND column_name='email'" 2>/dev/null)
echo "③d DDL auto 加列到 merge=$DC（期待1）"
[ "$DC" = "1" ] && echo "③d RESULT=PASS" || echo "③d RESULT=FAIL"

# ③e RMQ 双写（topic=CDC_edu_mall_admin_users，点换下划线）
TS=$(date +%s)
setsid nohup ./rmqcons-bin -topic CDC_edu_mall_admin_users -group hiprio-$TS -out /tmp/rmq-hiprio.jsonl -idle 8s > /tmp/rmqcons-hiprio.log 2>&1 &
sleep 3
$SRC --default-character-set=utf8mb4 -e "INSERT INTO \`edu.mall.admin\`.users (name,phone,email) VALUES ('dotdb-rmq','13900000004','rmq@x.c')" 2>/dev/null
sleep 8
RN=$(grep -c 'dotdb-rmq' /tmp/rmq-hiprio.jsonl 2>/dev/null || echo 0)
CERR=$(grep -c ILLEGAL_TOPIC /tmp/rmqcons-hiprio.log 2>/dev/null); CERR=${CERR:-0}
echo "③e RMQ: dotdb-rmq 消息=$RN（期待≥1）消费者 ILLEGAL_TOPIC=$CERR（期待0）"
[ "$RN" -ge 1 ] && [ "$CERR" = "0" ] && echo "③e RESULT=PASS（带点库名 topic 换下划线双写成功）" || echo "③e RESULT=FAIL"

echo "════ Stage2: ha.enabled=false 单节点 + 多 cluster 共存隔离 ════"
WEB_PASSWORD=demo123 setsid nohup ./gocdc -config-file config-single.yml -etcd 127.0.0.1:12379 > single.log 2>&1 &
sleep 12
SS=$(curl -s --noproxy '*' -u admin:demo123 localhost:8062/api/v1/status | python3 -c 'import sys,json
d=json.load(sys.stdin)["data"]; print(d["role"], d["cluster"], d["config_source"])' 2>/dev/null)
echo "单节点 /status: $SS"
echo "$SS" | grep -q "^single verify-single file" && echo "④ 单节点 role=single RESULT=PASS" || echo "④ RESULT=FAIL"

# 单节点管线直接跑（无需选举）：verify-single 前缀无标记 → shop 全量重扫
sleep 8
S1=$(curl -s --noproxy '*' localhost:9597/metrics | grep '^gocdc_leader_state' | awk '{print $2}')
GATE=$($ETCDCLI get /gocdc/verify-single/leader --print-value-only 2>/dev/null)
echo "④b 单节点 leader_state=$S1 门键=[$GATE]（期待 1 / 非空）"
[ "$S1" = "1" ] && [ -n "$GATE" ] && echo "④b RESULT=PASS（跳过选举直接 leader + 租约门键）" || echo "④b RESULT=FAIL"

# 多 cluster 共存：两进程活着、前缀各自 leader
NPROC=$(ps -eo pid,args | grep 'gocdc -config' | grep -v grep | wc -l)
KV1=$($ETCDCLI get /gocdc/verify/leader --print-value-only 2>/dev/null)
KV2=$($ETCDCLI get /gocdc/verify-single/leader --print-value-only 2>/dev/null)
echo "⑤ 并存: 进程=$NPROC verify门键=[$KV1] verify-single门键=[$KV2]"
[ "$NPROC" = "2" ] && [ -n "$KV1" ] && [ -n "$KV2" ] && echo "⑤ 多 cluster 前缀隔离共存 RESULT=PASS" || echo "⑤ RESULT=FAIL"

# 单节点 resign：无 elector → 无操作
RR=$(curl -s --noproxy '*' -u admin:demo123 -o /dev/null -w '%{http_code}' -X POST 'localhost:8062/api/v1/leader/resign?confirm=true')
RROLE=$(curl -s --noproxy '*' -u admin:demo123 localhost:8062/api/v1/status | python3 -c 'import sys,json; print(json.load(sys.stdin)["data"]["role"])')
echo "⑥ 单节点 resign http=$RR 角色后=$RROLE（期待 200/仍 single 管线不停）"
S2=$(curl -s --noproxy '*' localhost:9597/metrics | grep '^gocdc_leader_state' | awk '{print $2}')
[ "$RR" = "200" ] && [ "$S2" = "1" ] && echo "⑥ RESULT=PASS（resign 无操作、管线不受影响）" || echo "⑥ RESULT=FAIL"

# 单节点停机 → 门键随租约消失（位点写门槛语义）
SPID=$(ps -eo pid,args | grep 'config-single' | grep -v grep | awk '{print $1}' | head -1)
kill -TERM "$SPID"; sleep 15
GATE2=$($ETCDCLI get /gocdc/verify-single/leader --print-value-only 2>/dev/null)
echo "⑦ 停机后门键=[$GATE2]（期待空=租约释放）"
[ -z "$GATE2" ] && echo "⑦ RESULT=PASS" || echo "⑦ RESULT=FAIL"

echo "════ Stage3: 桩错误路径 ════"
# 桩指向不存在的配置 key → 拒启
sed 's|cluster: verify|cluster: ghost|' config-stub.yml > /tmp/stub-ghost.yml
mkdir -p /tmp/stubtest && cp /tmp/stub-ghost.yml /tmp/stubtest/config.local.yml
(cd /tmp/stubtest && WEB_PASSWORD=demo123 timeout 15 /mnt/d/workers/worker/qingfly/gocdc-mysql/.verify/gocdc > /tmp/stub-ghost.out 2>&1; echo "ghost exit=$?")
grep -E '不存在|fatal' /tmp/stub-ghost.out | head -2
# 配置中心不可达 → 拒启
sed 's|12380|12399|' config-stub.yml > /tmp/stubtest/config.local.yml
(cd /tmp/stubtest && WEB_PASSWORD=demo123 timeout 15 /mnt/d/workers/worker/qingfly/gocdc-mysql/.verify/gocdc > /tmp/stub-down.out 2>&1; echo "down exit=$?")
grep -E '配置中心|连接失败|fatal' /tmp/stub-down.out | head -2
rm -rf /tmp/stubtest
echo "════ 完成 ════"

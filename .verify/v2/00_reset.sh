#!/bin/bash
# v2 环境整体重建：容器/卷全清 → 重建 6 容器 → 编译二进制 → 建库播种 → FIFO topic → 健康自检
# 之后每个 case 组用 lib.sh wipe_all 做组级恢复。
set -u
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

echo "════ 0) 清遗留进程与 /tmp 产物"
killall_gocdc
rm -rf /tmp/v2 /tmp/rmq-*.jsonl /tmp/rmqcons-*.log /tmp/c5.out /tmp/stub* 2>/dev/null
echo done

echo "════ 1) 容器全清（含卷）"
docker rm -f -v vf-src vf-dst vf-etcd cfg-etcd rmqnamesrv rmqbroker 2>/dev/null
docker network create gocdc-verify 2>/dev/null || true

echo "════ 2) 重建容器"
docker run -d --name vf-src --network gocdc-verify -p 13306:3306 \
  -e MYSQL_ROOT_PASSWORD=123456 mysql:8.0 \
  --server-id=101 --log-bin=binlog --binlog-format=ROW --binlog-row-image=FULL \
  --gtid-mode=ON --enforce-gtid-consistency=ON --log-replica-updates=ON \
  --character-set-server=utf8mb4 --collation-server=utf8mb4_0900_ai_ci \
  --cte-max-recursion-depth=500000 >/dev/null

docker run -d --name vf-dst --network gocdc-verify -p 13307:3306 \
  -e MYSQL_ROOT_PASSWORD=123456 mysql:8.0 \
  --character-set-server=utf8mb4 --collation-server=utf8mb4_0900_ai_ci >/dev/null

docker run -d --name vf-etcd --network gocdc-verify -p 12379:2379 \
  docker.cnb.cool/qingflyfei/edu.image.base:etcd-3.5.16 \
  etcd --name=vf-etcd --advertise-client-urls=http://vf-etcd:2379 --listen-client-urls=http://0.0.0.0:2379 >/dev/null

docker run -d --name cfg-etcd --network gocdc-verify -p 12380:2379 \
  docker.cnb.cool/qingflyfei/edu.image.base:etcd-3.5.16 \
  etcd --name=cfg-etcd --advertise-client-urls=http://cfg-etcd:2379 --listen-client-urls=http://0.0.0.0:2379 >/dev/null

docker run -d --name rmqnamesrv --network gocdc-verify --hostname namesrv \
  -e "JAVA_OPT_EXT=-Xms256m -Xmx256m" apache/rocketmq:5.3.1 sh mqnamesrv >/dev/null

docker run -d --name rmqbroker --network gocdc-verify --hostname broker \
  -e "NAMESRV_ADDR=namesrv:9876" -e "JAVA_OPT_EXT=-Xms512m -Xmx1g" -p 8081:8081 \
  apache/rocketmq:5.3.1 sh mqbroker -n namesrv:9876 --enable-proxy >/dev/null

docker ps --format '{{.Names}}\t{{.Status}}' | grep -E 'vf-|cfg-etcd|rmq'

echo "════ 3) 等 MySQL 就绪"
for i in $(seq 1 60); do
  $SRC -e "SELECT 1" >/dev/null 2>&1 && $DST -e "SELECT 1" >/dev/null 2>&1 && { echo "mysql ready t=${i}s"; break; }
  sleep 1
done

echo "════ 4) 编译 Linux 二进制（当前 HEAD）"
(cd "$REPO" && /usr/local/go/bin/go build -o .verify/gocdc ./cmd/gocdc) || exit 1
# RocketMQ 消费侧测试桩（I5/I 系单产出断言依赖；源码在 .verify/rmqcons/）
(cd "$REPO" && /usr/local/go/bin/go build -o .verify/rmqcons-bin ./.verify/rmqcons) || exit 1
"$BIN" -version

echo "════ 5) etcd 健康"
$ETCDC endpoint health 2>&1 | head -1
$CFGC endpoint health 2>&1 | head -1

echo "════ 6) 建库播种（wipe_data）"
wipe_data

echo "════ 7) FIFO topic 预建（等 broker 注册路由；以 topicRoute 实存为准——updateTopic 回显含 topicName 字样属假阳性）"
for t in CDC_rmqtest_msg CDC_edu_mall_admin_users; do
  for i in $(seq 1 30); do
    docker exec rmqbroker sh mqadmin updateTopic -n namesrv:9876 -c DefaultCluster \
      -t "$t" -a +message.type=FIFO >/dev/null 2>&1
    R=$(docker exec rmqbroker sh mqadmin topicRoute -n namesrv:9876 -t "$t" 2>&1 | grep -c brokerName)
    [ "$R" -ge 1 ] && { echo "$t ok t=${i}x2s"; break; }
    sleep 2
  done
done

echo "════ 8) 健康自检"
echo "binlog: $($SRC -N -e 'SELECT @@binlog_format, @@binlog_row_image, @@gtid_mode' 2>/dev/null)"
echo "src tables: $($SRC -N -e "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema IN ('shop','biz2','types','rmqtest','cluster_e2e')" 2>/dev/null)"
echo "dst tables: $($DST -N -e "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema IN ('shop','biz2','types','rmqtest','cluster_e2e')" 2>/dev/null)"
echo "seed orders=$($SRC -N -e 'SELECT COUNT(*) FROM shop.orders' 2>/dev/null) users=$($SRC -N -e 'SELECT COUNT(*) FROM shop.users' 2>/dev/null) dotdb=$($SRC -N -e 'SELECT COUNT(*) FROM \`edu.mall.admin\`.users' 2>/dev/null)"
docker exec rmqbroker sh mqadmin topicRoute -n namesrv:9876 -t CDC_rmqtest_msg 2>&1 | grep -c messageType | head -1
echo "════ RESET DONE ════"

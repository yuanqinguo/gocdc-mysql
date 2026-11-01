#!/bin/bash
# 高优先级 case 环境准备：配置中心 etcd(12380) + 带点库名表 + FIFO topic
set -u
cd /mnt/d/workers/worker/qingfly/gocdc-mysql/.verify

echo "=== 1) 配置中心 etcd（与协调面 12379 分离）"
docker rm -f cfg-etcd >/dev/null 2>&1
docker run -d --name cfg-etcd --network gocdc-verify -p 12380:2379 \
  docker.cnb.cool/qingflyfei/edu.image.base:etcd-3.5.16 \
  etcd --name=cfg-etcd --advertise-client-urls=http://cfg-etcd:2379 --listen-client-urls=http://0.0.0.0:2379 >/dev/null
sleep 3
docker exec cfg-etcd etcdctl --endpoints=127.0.0.1:2379 endpoint health 2>&1 | head -1

echo "=== 2) 带点库名：src edu.mall.admin.users / dst edu.mall.merge.users"
docker exec vf-src mysql -uroot -p123456 -h127.0.0.1 -e "
CREATE DATABASE IF NOT EXISTS \`edu.mall.admin\`;
CREATE TABLE \`edu.mall.admin\`.users (
  id BIGINT NOT NULL AUTO_INCREMENT, name VARCHAR(64) NOT NULL, phone VARCHAR(20) NULL,
  PRIMARY KEY (id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4" 2>/dev/null
docker exec vf-dst mysql -uroot -p123456 -h127.0.0.1 -e "
CREATE DATABASE IF NOT EXISTS \`edu.mall.merge\`;
CREATE TABLE \`edu.mall.merge\`.users (
  id BIGINT NOT NULL AUTO_INCREMENT, name VARCHAR(64) NOT NULL, phone VARCHAR(20) NULL,
  PRIMARY KEY (id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4" 2>/dev/null
SRCN=$(docker exec vf-src mysql -uroot -p123456 -h127.0.0.1 -N -e "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='edu.mall.admin'" 2>/dev/null)
DSTN=$(docker exec vf-dst mysql -uroot -p123456 -h127.0.0.1 -N -e "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='edu.mall.merge'" 2>/dev/null)
echo "src edu.mall.admin 表数=$SRCN dst edu.mall.merge 表数=$DSTN（期待1/1）"

echo "=== 3) RMQ topic（带点库名 → 点换下划线）"
docker exec rmqbroker sh mqadmin updateTopic -n namesrv:9876 -c DefaultCluster \
  -t CDC_edu_mall_admin_users -a +message.type=FIFO 2>&1 | grep -oE 'topicName=[^,]+' | head -1

echo "=== 4) 预置种子（stock 验证用）"
docker exec vf-src mysql -uroot -p123456 -h127.0.0.1 --default-character-set=utf8mb4 -e "
INSERT INTO \`edu.mall.admin\`.users (name,phone) VALUES ('dotdb-a','13900000001'),('dotdb-b','13900000002'),('dotdb-c',NULL)" 2>/dev/null
echo "种子 3 行已插"

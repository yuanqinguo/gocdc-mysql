#!/bin/bash
# 给既有环境补建多源组容器（不整体 reset）：vf-src2 / vf-dst2
docker inspect vf-src2 >/dev/null 2>&1 || docker run -d --name vf-src2 --network gocdc-verify -p 13406:3306 \
  -e MYSQL_ROOT_PASSWORD=123456 mysql:8.0 \
  --server-id=201 --log-bin=binlog --binlog-format=ROW --binlog-row-image=FULL \
  --gtid-mode=ON --enforce-gtid-consistency=ON --log-replica-updates=ON \
  --character-set-server=utf8mb4 --collation-server=utf8mb4_0900_ai_ci \
  --cte-max-recursion-depth=500000 >/dev/null
docker inspect vf-dst2 >/dev/null 2>&1 || docker run -d --name vf-dst2 --network gocdc-verify -p 13407:3306 \
  -e MYSQL_ROOT_PASSWORD=123456 mysql:8.0 \
  --character-set-server=utf8mb4 --collation-server=utf8mb4_0900_ai_ci >/dev/null
for i in $(seq 1 60); do
  docker exec vf-src2 mysql -uroot -p123456 -h127.0.0.1 -e "SELECT 1" >/dev/null 2>&1 && \
    docker exec vf-dst2 mysql -uroot -p123456 -h127.0.0.1 -e "SELECT 1" >/dev/null 2>&1 && { echo NEW_CONTAINERS_READY; break; }
  sleep 1
done
# MS 组 FIFO topic（幂等预建）
for t in CDC_msa_msa_orders CDC_msa_msa_evlog CDC_msb_msb_users; do
  for i in $(seq 1 30); do
    docker exec rmqbroker sh mqadmin updateTopic -n namesrv:9876 -c DefaultCluster \
      -t "$t" -a +message.type=FIFO >/dev/null 2>&1
    R=$(docker exec rmqbroker sh mqadmin topicRoute -n namesrv:9876 -t "$t" 2>&1 | grep -c brokerName)
    [ "$R" -ge 1 ] && { echo "topic $t ok"; break; }
    sleep 2
  done
done
docker ps --format '{{.Names}}' | grep -E 'vf-|cfg-|rmq'

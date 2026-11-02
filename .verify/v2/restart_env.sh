#!/bin/bash
docker start vf-src vf-src2 vf-dst vf-dst2 vf-etcd cfg-etcd rmqnamesrv rmqbroker 2>&1 | head -6
sleep 10
for i in $(seq 1 40); do
  S=$(docker exec vf-src mysql -uroot -p123456 -h127.0.0.1 -N -e "SELECT 1" 2>/dev/null)
  D=$(docker exec vf-dst mysql -uroot -p123456 -h127.0.0.1 -N -e "SELECT 1" 2>/dev/null)
  [ "$S" = "1" ] && [ "$D" = "1" ] && { echo "mysql ready t=${i}x2s"; break; }
  sleep 2
done
docker ps --format "{{.Names}}\t{{.Status}}" | grep -E "vf-|cfg-etcd|rmq"
# RMQ broker 重启后路由注册有分钟级窗口，等 CDC_rmqtest_msg 路由就绪
for i in $(seq 1 40); do
  R=$(docker exec rmqbroker sh mqadmin topicRoute -n namesrv:9876 -t CDC_rmqtest_msg 2>&1 | grep -c queueDatas)
  [ "${R:-0}" -ge 1 ] && { echo "topic route ready t=${i}x3s"; break; }
  sleep 3
done

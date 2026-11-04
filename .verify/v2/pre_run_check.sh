#!/bin/bash
# run 前置检查：topic 路由就绪等待 + 从 HEAD 重编二进制 + stab 残留写入器查杀
for i in $(seq 1 40); do
  R=$(docker exec rmqbroker sh mqadmin topicRoute -n namesrv:9876 -t CDC_rmqtest_msg 2>&1 | grep -c queueDatas)
  [ "${R:-0}" -ge 1 ] && { echo "topic route ready t=${i}x3s"; break; }
  sleep 3
done
cd /mnt/d/course/gocdc-mysql || exit 1
/usr/local/go/bin/go build -o .verify/gocdc ./cmd/gocdc || exit 1
echo BUILD-OK
ps -eo pid,args | grep -E "stab|soak" | grep -v grep && echo "STAB-WRITER-FOUND(先杀!)" || echo NO-STAB-WRITER

#!/bin/bash
ps -eo pid,etime,args | grep -E "00_reset|10_boot|gocdcbin|timeout" | grep -v grep
echo "--- 端口监听"
ss -ltn | grep -E "8060|9595" || echo "无"
echo "--- /tmp/v2"
ls /tmp/v2/ 2>/dev/null
echo "--- a5b log tail"
tail -5 /tmp/v2/node1.log 2>/dev/null
echo "--- etcd keys"
docker exec vf-etcd etcdctl --endpoints=127.0.0.1:2379 get --prefix /gocdc/ --keys-only 2>/dev/null | head -10

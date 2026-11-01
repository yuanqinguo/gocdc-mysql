#!/bin/bash
echo "=== phase 分布:"
grep -ao '"phase":"[a-z]*"' /tmp/rmq-i5.jsonl | sort | uniq -c
echo "=== i5a 消息的 ts 字段（看是否跨时段）:"
grep -a '"i5a-1"' /tmp/rmq-i5.jsonl | python3 -c '
import sys, json
for line in sys.stdin:
    e = json.loads(line)
    print(e.get("phase"), e.get("ts"), e.get("exec_ts"), str((e.get("after") or {}).get("id")))' 2>/dev/null | head -8
echo "=== topic 消费组状态:"
docker exec rmqbroker sh mqadmin consumerStatus -g i5-$(date +%s) -n namesrv:9876 2>/dev/null | head -5
docker exec rmqbroker sh -c "ls /home/rocketmq/store/consumergroup 2>/dev/null | grep i5 | head; ls /home/rocketmq/store/consumequeue/CDC_rmqtest_msg 2>/dev/null | head -3"

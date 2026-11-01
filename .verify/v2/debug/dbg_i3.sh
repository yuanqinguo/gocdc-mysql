#!/bin/bash
source "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/lib.sh"
echo "=== node1 RMQ 重试/恢复轨迹:"
grep -aiE 'rocketmq|rmq|retry|重试|恢复|补' /tmp/v2/node1.log | tail -12
echo "=== i3 消费文件:"
ls -la /tmp/rmq-i3.jsonl 2>/dev/null && grep -ac 'i3-' /tmp/rmq-i3.jsonl 2>/dev/null
echo "=== 消费者日志:"
tail -3 /tmp/rmqcons-i3.log 2>/dev/null
echo "=== 现在起全新组收 10s 看积压:"
TS=$(date +%s)
timeout 14 $RMQCONS -topic CDC_rmqtest_msg -group probe3-$TS -out /tmp/rmq-probe3.jsonl -idle 9s >/dev/null 2>&1
grep -ac 'i3-' /tmp/rmq-probe3.jsonl 2>/dev/null
echo "=== dst 里宕机期5行还在吗:"
$DST -N -e "SELECT COUNT(*) FROM rmqtest.msg WHERE note='宕机期'" 2>/dev/null

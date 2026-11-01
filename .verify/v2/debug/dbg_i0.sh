#!/bin/bash
source "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/lib.sh"
echo "=== 上一 run 的全量轨迹（node1.log）:"
grep -aE '全量|快照|restock|重跑' /tmp/v2/node1.log | tail -6
echo "=== 全新组现在收（看 topic 是否保有 stock 相位消息）:"
TS=$(date +%s)
timeout 14 $RMQCONS -topic CDC_rmqtest_msg -group probe4-$TS -out /tmp/rmq-probe4.jsonl -idle 9s >/dev/null 2>&1
echo "probe 行数=$(wc -l < /tmp/rmq-probe4.jsonl 2>/dev/null || echo 0)"
grep -ao '"phase":"[a-z]*"' /tmp/rmq-probe4.jsonl 2>/dev/null | sort | uniq -c
echo "=== probe 里 g5- 前缀:"
grep -ac 'g5-' /tmp/rmq-probe4.jsonl 2>/dev/null

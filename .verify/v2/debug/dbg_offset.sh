#!/bin/bash
cd "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/.verify"
rm -f /tmp/rmq-offset-probe.jsonl
timeout 15 ./rmqcons-bin -topic CDC_rmqtest_msg -group offsetprobe-$(date +%s) -out /tmp/rmq-offset-probe.jsonl -idle 8s > /tmp/rmqcons-offsetprobe.log 2>&1
echo "exit=$?"
echo "新组收到消息数: $(wc -l < /tmp/rmq-offset-probe.jsonl 2>/dev/null || echo 0)"
tail -3 /tmp/rmqcons-offsetprobe.log

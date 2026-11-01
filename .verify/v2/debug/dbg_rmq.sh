#!/bin/bash
cd "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/.verify"
timeout 20 ./rmqcons-bin -topic CDC_rmqtest_msg -group dbgprobe-$(date +%s) -out /tmp/rmq-dbg.jsonl -idle 6s > /tmp/rmqcons-dbg.log 2>&1
echo "exit=$?"
echo "--- consumer log:"
tail -10 /tmp/rmqcons-dbg.log
echo "--- jsonl lines: $(wc -l < /tmp/rmq-dbg.jsonl 2>/dev/null || echo 0)"

#!/bin/bash
echo "--- 脚本方式 env:"
env | grep -i proxy || echo "（无）"
echo "--- unset 后直连 gRPC 端口语义测试："
cd "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/.verify"
unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY all_proxy ALL_PROXY
timeout 20 ./rmqcons-bin -topic CDC_rmqtest_msg -group dbgprobe2-$(date +%s) -out /tmp/rmq-dbg.jsonl -idle 6s > /tmp/rmqcons-dbg.log 2>&1
echo "exit=$?"
tail -5 /tmp/rmqcons-dbg.log

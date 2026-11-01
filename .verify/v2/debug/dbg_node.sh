#!/bin/bash
echo "=== node1.log 尾部 40 行"
tail -40 /tmp/v2/node1.log 2>/dev/null
echo "=== 关键指标"
curl -s --noproxy '*' localhost:9595/metrics 2>/dev/null | grep -E '^gocdc_(pipeline_paused|sink_errors|events_total|events_failed|stock_|leader_state|watermark|queue)' | grep -v '^#' | head -20
echo "=== pause 原因"
curl -s --noproxy '*' localhost:9595/metrics 2>/dev/null | grep -iE 'paused|pause_reason' | grep -v '^#' | head

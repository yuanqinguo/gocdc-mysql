#!/bin/bash
echo "=== i5 jsonl phase 分布:"
grep -ao '"phase":"[a-z]*"' /tmp/rmq-i5.jsonl | sort | uniq -c
echo "=== jsonl 行数: $(wc -l < /tmp/rmq-i5.jsonl)"
echo "=== node1 rmqtest 相关:"
grep -ai 'rmqtest' /tmp/v2/node1.log | tail -5
echo "=== node1/node2 sink 错误:"
grep -aiE 'sink|写入失败|投递' /tmp/v2/node1.log /tmp/v2/node2.log 2>/dev/null | grep -aiv 'INFO' | tail -8
echo "=== dst rmqtest.msg:"
docker exec vf-dst mysql -uroot -p123456 -h127.0.0.1 -N -e 'SELECT COUNT(*) FROM rmqtest.msg' 2>/dev/null
echo "=== src rmqtest.msg:"
docker exec vf-src mysql -uroot -p123456 -h127.0.0.1 -N -e 'SELECT COUNT(*) FROM rmqtest.msg' 2>/dev/null

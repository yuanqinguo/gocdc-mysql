#!/bin/bash
M="docker exec vf-src mysql -uroot -p123456 -h127.0.0.1 -N -e"
echo "=== 全部 e9_table DDL 事件:"
$M "SHOW BINLOG EVENTS IN 'binlog.000004'" 2>/dev/null | grep -i "e9_table" | awk -F'\t' '{print $2, $3, $6}'
echo "=== src shop 现状:"
$M "SELECT table_name FROM information_schema.tables WHERE table_schema='shop'"

#!/bin/bash
M="docker exec vf-src mysql -uroot -p123456 -h127.0.0.1 -N -e"
echo "=== 全部 e9_table binlog 事件（pos>83000）:"
$M "SHOW BINLOG EVENTS IN 'binlog.000004'" 2>/dev/null | awk -F'\t' '$2>83400' | grep -iE "e9_table|Query" | head -20
echo "=== src shop 现状:"
$M "SELECT table_name FROM information_schema.tables WHERE table_schema='shop'"
echo "=== γ 的 wipe GRANT 位置（每次 wipe 有两个 GRANT）:"
$M "SHOW BINLOG EVENTS IN 'binlog.000004'" 2>/dev/null | grep "GRANT SELECT" | awk -F'\t' '{print $2, $6}' | tail -6

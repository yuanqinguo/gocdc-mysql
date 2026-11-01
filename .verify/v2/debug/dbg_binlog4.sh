#!/bin/bash
M="docker exec vf-src mysql -uroot -p123456 -h127.0.0.1 -N -e"
echo "=== src 表现状（order/orderx/mintbl 是否存在）:"
$M "SELECT table_name FROM information_schema.tables WHERE table_schema='shop'"
echo "=== binlog 尾部 CREATE 事件（含引号形态）:"
$M "SHOW BINLOG EVENTS IN 'binlog.000004'" 2>/dev/null | grep -aE "CREATE TABLE shop" | tail -6

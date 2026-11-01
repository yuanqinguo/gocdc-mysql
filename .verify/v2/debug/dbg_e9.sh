#!/bin/bash
M="docker exec vf-src mysql -uroot -p123456 -h127.0.0.1 -N -e"
D="docker exec vf-dst mysql -uroot -p123456 -h127.0.0.1 -N -e"
echo "=== src shop 表清单:"
$M "SELECT table_name FROM information_schema.tables WHERE table_schema='shop'"
echo "=== dst shop 表清单:"
$D "SELECT table_name FROM information_schema.tables WHERE table_schema='shop'"
echo "=== binlog 中 e9_table 相关事件:"
$M "SHOW BINLOG EVENTS IN 'binlog.000004'" 2>/dev/null | grep -i "e9_table" | head -10
echo "=== 当前 rmqtest.msg src/dst:"
$M "SELECT COUNT(*) FROM rmqtest.msg" 2>/dev/null
$D "SELECT COUNT(*) FROM rmqtest.msg" 2>/dev/null
echo "=== etcd /gocdc/verify keys:"
docker exec vf-etcd etcdctl --endpoints=127.0.0.1:2379 get --prefix /gocdc/verify/ --keys-only 2>/dev/null | head -20

#!/bin/bash
docker exec vf-src mysqlbinlog --start-position=60900 --stop-position=61800 /var/lib/mysql/binlog.000004 2>/dev/null | grep -E "^#260|CREATE TABLE|DROP TABLE" | head -12
echo "--- β 的 E9 CREATE (pos 82703) 时间:"
docker exec vf-src mysqlbinlog --start-position=82650 --stop-position=83000 /var/lib/mysql/binlog.000004 2>/dev/null | grep -E "^#260|CREATE TABLE" | head -6
echo "--- 当前 binlog 尾部时间:"
docker exec vf-src mysql -uroot -p123456 -h127.0.0.1 -N -e "SHOW BINARY LOG STATUS" 2>/dev/null

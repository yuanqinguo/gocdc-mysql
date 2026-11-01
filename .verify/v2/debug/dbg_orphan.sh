#!/bin/bash
echo "=== 孤儿脚本/进程扫描:"
ps -eo pid,etime,args | grep -E "v2/|45_ha|40_ha|30_pos|20_data|10_boot" | grep -v grep
echo "=== e9 CREATE (pos 61123) 精确时间:"
docker exec vf-src sh -c "mysqlbinlog --start-position=61050 --stop-position=61400 /var/lib/mysql/binlog.000004 2>/dev/null | grep -E '^#|CREATE TABLE' | head -8"
echo "=== 全部 MySQL 当前连接:"
docker exec vf-src mysql -uroot -p123456 -h127.0.0.1 -N -e "SELECT id,user,time,state,LEFT(COALESCE(info,''),60) FROM information_schema.processlist WHERE user!='root' OR command NOT LIKE 'Binlog%'" 2>/dev/null | head -15

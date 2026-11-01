#!/bin/bash
echo "=== mysqlbinlog 直测:"
docker exec vf-src mysqlbinlog --short-form --start-position=60900 --stop-position=61400 /var/lib/mysql/binlog.000004 2>&1 | head -20
echo "=== SHOW MASTER STATUS:"
docker exec vf-src mysql -uroot -p123456 -h127.0.0.1 -N -e "SHOW MASTER STATUS" 2>&1 | grep -v Warning

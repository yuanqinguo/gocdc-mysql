#!/bin/bash
# HA-10 复盘：REVOKE 是否真咬到 gocdc 的账号路径（root@'%' over TCP）
D(){ docker exec vf-src mysql -uroot -p123456 "$@" 2>/dev/null; }
T(){ docker exec vf-dst mysql -hvf-src -uroot -p123456 -N -e "$1" 2>&1 | grep -v 'Using a password'; }

echo "=== current_user over TCP（等价 gocdc 连接路径，验证账号解析）"
T "SELECT CURRENT_USER(), @@hostname"

echo "=== grants BEFORE revoke"
D -N -e "SHOW GRANTS FOR 'root'@'%'"

echo "=== revoke（不吞错）"
docker exec vf-src mysql -uroot -p123456 -e "REVOKE RELOAD, BACKUP_ADMIN ON *.* FROM 'root'@'%'" 2>&1 | grep -v 'Using a password'
echo "rc=$?"

echo "=== grants AFTER revoke"
D -N -e "SHOW GRANTS FOR 'root'@'%'"

echo "=== TCP FLUSH TABLES（需 RELOAD；成功=REVOKE 未咬到）"
T "FLUSH TABLES"
echo "flush_rc=$?"

echo "=== TCP LOCK INSTANCE FOR BACKUP（需 BACKUP_ADMIN）"
T "LOCK INSTANCE FOR BACKUP; UNLOCK INSTANCE"
echo "lock_rc=$?"

echo "=== restore"
docker exec vf-src mysql -uroot -p123456 -e "GRANT RELOAD, BACKUP_ADMIN ON *.* TO 'root'@'%'" 2>&1 | grep -v 'Using a password'
D -N -e "SHOW GRANTS FOR 'root'@'%'"

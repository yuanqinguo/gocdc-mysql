#!/bin/bash
# HA-10 复盘③：GRANT 恢复为何失败 + 当前授权状态检查
D(){ docker exec vf-src mysql -uroot -p123456 "$@" 2>/dev/null; }

echo "=== root@'%' 当前关键权限"
D -N -e "SELECT privilege_name FROM information_schema.user_privileges WHERE grantee=\"'root'@'%'\" AND privilege_name IN ('RELOAD','BACKUP_ADMIN','FLUSH_TABLES')" 2>/dev/null
echo "=== root@'localhost' 关键权限（授权方是否持有被授权限）"
docker exec vf-src mysql -uroot -p123456 -N -e "SELECT privilege_name FROM information_schema.user_privileges WHERE grantee=\"'root'@'localhost'\" AND privilege_name IN ('RELOAD','BACKUP_ADMIN','FLUSH_TABLES','SYSTEM_USER')" 2>&1 | grep -v 'Using a password'
echo "=== 直接执行 GRANT 看真实报错（不吞错）"
docker exec vf-src mysql -uroot -p123456 -e "GRANT RELOAD, FLUSH_TABLES, BACKUP_ADMIN ON *.* TO 'root'@'%'" 2>&1 | grep -v 'Using a password'
echo "GRANT_EXIT=${PIPESTATUS[0]}"
echo "=== 恢复后核验"
D -N -e "SELECT privilege_name FROM information_schema.user_privileges WHERE grantee=\"'root'@'%'\" AND privilege_name IN ('RELOAD','BACKUP_ADMIN','FLUSH_TABLES')" 2>/dev/null

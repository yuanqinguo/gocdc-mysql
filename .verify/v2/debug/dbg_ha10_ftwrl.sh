#!/bin/bash
# HA-10 复盘②：REVOKE RELOAD+BACKUP_ADMIN 后，FTWRL 是否真被拒？
# （dbg_ha10_priv 只测了普通 FLUSH TABLES——它由 FLUSH_TABLES 动态权限放行；
#   FTWRL 是否也被 FLUSH_TABLES 覆盖是本案关键）
D(){ docker exec vf-src mysql -uroot -p123456 "$@" 2>/dev/null; }

echo "=== revoke（真实 rc）"
docker exec vf-src mysql -uroot -p123456 -e "REVOKE RELOAD, BACKUP_ADMIN ON *.* FROM 'root'@'%'" 2>&1 | grep -v 'Using a password'
D -N -e "SELECT COUNT(*) FROM information_schema.user_privileges WHERE grantee=\"'root'@'%'\" AND privilege_name IN ('BACKUP_ADMIN')" 2>/dev/null | xargs echo "BACKUP_ADMIN 残留(0=已摘):"
D -N -e "SELECT COUNT(*) FROM information_schema.user_privileges WHERE grantee=\"'root'@'%'\" AND privilege_name='RELOAD'" 2>/dev/null | xargs echo "RELOAD 残留(0=已摘):"
D -N -e "SELECT COUNT(*) FROM information_schema.user_privileges WHERE grantee=\"'root'@'%'\" AND privilege_name='FLUSH_TABLES'" 2>/dev/null | xargs echo "FLUSH_TABLES 残留(1=仍在,REVOKE 未动它):"

echo "=== TCP FLUSH TABLES WITH READ LOCK（gocdc boot 同款，全新连接）"
docker exec vf-dst mysql -hvf-src -uroot -p123456 -N -e "FLUSH TABLES WITH READ LOCK; SELECT 'FTWRL-OK'; UNLOCK TABLES" 2>&1 | grep -v 'Using a password'

echo "=== TCP LOCK INSTANCE FOR BACKUP"
docker exec vf-dst mysql -hvf-src -uroot -p123456 -N -e "LOCK INSTANCE FOR BACKUP; UNLOCK INSTANCE" 2>&1 | grep -v 'Using a password'

echo "=== restore"
docker exec vf-src mysql -uroot -p123456 -e "GRANT RELOAD, BACKUP_ADMIN ON *.* TO 'root'@'%'" 2>&1 | grep -v 'Using a password'
D -N -e "SELECT COUNT(*) FROM information_schema.user_privileges WHERE grantee=\"'root'@'%'\" AND privilege_name='BACKUP_ADMIN'" 2>/dev/null | xargs echo "BACKUP_ADMIN 恢复(1=ok):"

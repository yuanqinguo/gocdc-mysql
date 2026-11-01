#!/bin/bash
# v2 组80：异常注入矩阵——HA-08 目标库宕机→恢复（ErrInvalidConn 重试）/
# HA-07 源库宕机→恢复（退避重连续传）/ HA-10 严格版（RELOAD+BACKUP_ADMIN 全摘 fail-fast）
set -u
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
API="localhost:8060/api/v1"
api(){ curl -s --noproxy '*' -u admin:demo123 "$API$1" ${2:+-X $2}; }

wipe_all
ccput verify "$V2/body_main.yml"
stub_start node1 verify :9595 :8060
wait_ready 9595 60 || { echo "FATAL 启动失败"; exit 1; }
for i in $(seq 1 30); do D=$($DST -N -e "SELECT COUNT(*) FROM shop.orders" 2>/dev/null); [ "$D" = "100" ] && break; sleep 2; done
for i in $(seq 1 20); do LB=$(lagof 9595); [ "$LB" = "0" ] && break; sleep 2; done

grp "HA-08 目标库宕机 → 恢复（ErrInvalidConn 持续重试不退出）"
docker stop vf-dst >/dev/null
$SRC -e "INSERT INTO shop.new_table (val) VALUES ('ha08-a'),('ha08-b'),('ha08-c')" 2>/dev/null
sleep 12
ALIVE=$( [ -n "$(pickpid node1)" ] && echo 1 || echo 0 )
echo "  目标宕机 12s 进程存活=$ALIVE（期待1=不退出）"
docker start vf-dst >/dev/null
sleep 10
H8=0
for i in $(seq 1 20); do
  C=$($DST -N -e "SELECT COUNT(*) FROM shop.new_table WHERE val LIKE 'ha08-%'" 2>/dev/null)
  [ "$C" = "3" ] && { H8=1; echo "  恢复收敛 t=${i}x2s"; break; }; sleep 2
done
[ "$ALIVE" = "1" ] && [ "$H8" = "1" ] && ok "HA-08 宕机不退出+恢复收敛" || bad "HA-08 (alive=$ALIVE c=$C)"

grp "HA-07 源库宕机 → 恢复（退避重连续传零丢失）"
docker stop vf-src >/dev/null
sleep 12
ALIVE=$( [ -n "$(pickpid node1)" ] && echo 1 || echo 0 )
echo "  源宕机 12s 进程存活=$ALIVE"
docker start vf-src >/dev/null
sleep 15
$SRC -e "INSERT INTO shop.new_table (val) VALUES ('ha07-after-recovery')" 2>/dev/null
H7=0
for i in $(seq 1 20); do
  C=$($DST -N -e "SELECT COUNT(*) FROM shop.new_table WHERE val='ha07-after-recovery'" 2>/dev/null)
  [ "$C" = "1" ] && { H7=1; break; }; sleep 2
done
[ "$ALIVE" = "1" ] && [ "$H7" = "1" ] && ok "HA-07 恢复后续传零丢失" || bad "HA-07 (alive=$ALIVE c=$C)"

grp "HA-10 严格版：RELOAD+BACKUP_ADMIN 全摘 → restock → 重启全量锁失败 fail-fast"
# 摘/还权限必须由 socket 会话（root@localhost）执行：MySQL 要求授权者持有被授
# 权限，root@'%' 自摘自还则恢复 GRANT 必失败（2>/dev/null 吞错 → 环境带伤传染后续组）
docker exec vf-src mysql -uroot -p123456 -e "REVOKE RELOAD, BACKUP_ADMIN ON *.* FROM 'root'@'%'" 2>/dev/null
api "/rules/rmqtest.msg/restock?confirm=true" POST >/dev/null
kill -TERM $(pickpid node1) 2>/dev/null
for i in $(seq 1 30); do [ -z "$(pickpid node1)" ] && break; sleep 1; done
d=/tmp/v2/ha10; rm -rf $d; mkdir -p $d
cat > $d/config.local.yml <<'EOF'
cluster: verify
bootstrap:
  config_source: etcd
  config_center:
    etcd_addrs: [127.0.0.1:12380]
  etcd_addrs:
    - 127.0.0.1:12379
EOF
cp "$BIN" $d/gocdcbin
(cd $d && WEB_PASSWORD=demo123 METRICS_ADDR=:9595 WEB_ADDR=:8060 timeout 40 ./gocdcbin > out.log 2>&1; echo $? > e.log)
HE=$(cat $d/e.log)
HMSG=$(grep -aE '全量|锁|fail|fatal|RELOAD|BACKUP' $d/out.log | grep -av etcd | head -3)
echo "  exit=$HE（非0=本轮未起管线）: $HMSG"
docker exec vf-src mysql -uroot -p123456 -e "GRANT RELOAD, BACKUP_ADMIN ON *.* TO 'root'@'%'" 2>/dev/null
G=$(docker exec vf-src mysql -uroot -p123456 -N -e "SHOW GRANTS FOR 'root'@'%'" 2>/dev/null | grep -c BACKUP_ADMIN)
[ "$G" -ge 1 ] && echo "  权限已恢复（BACKUP_ADMIN 在授权）" || echo "  ❌ 权限恢复失败（后续组 wipe_data 将 1045）"
[ "$HE" != "0" ] && [ -n "$HMSG" ] && ok "HA-10 全量锁权限缺失 fail-fast（进程态拒绝，不静默降级）" || bad "HA-10 (exit=$HE)"

# 恢复常态：重启节点确认管线可起
stub_start node1 verify :9595 :8060
wait_ready 9595 60 || bad "HA-10 恢复启动"
H10=0
for i in $(seq 1 30); do
  C=$($DST -N -e "SELECT COUNT(*) FROM rmqtest.msg" 2>/dev/null)
  S=$($SRC -N -e "SELECT COUNT(*) FROM rmqtest.msg" 2>/dev/null)
  [ -n "$S" ] && [ "$S" = "$C" ] && { H10=1; echo "  restock 重扫收敛 src=$S dst=$C t=${i}x2s"; break; }; sleep 2
done
[ "$H10" = "1" ] && ok "HA-10b 权限恢复后 restock 重跑收敛" || bad "HA-10b (src=$S dst=$C)"

killall_gocdc
summary

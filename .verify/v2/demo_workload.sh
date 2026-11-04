#!/bin/bash
# 演示负载：持续向源库写入（INSERT/UPDATE/DELETE），供 web-admin / 目标库自测观察。
# 覆盖的表：
#   shop.users / shop.orders / shop.order_items   —— 全量+增量（orders/users 有 mask 规则）
#   biz2.events                                   —— do_stock=false，仅增量
#   rmqtest.msg                                   —— MySQL + RocketMQ 双写
#   shop.tmp_log                                  —— 白名单外，用于验证「不同步」
# 用法：demo_workload.sh [duration_sec] [interval_sec]   默认 7 天 / 3 秒
set -u
unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY all_proxy ALL_PROXY 2>/dev/null
export NO_PROXY="*"; export no_proxy="*"

DURATION=${1:-604800}
INTERVAL=${2:-3}
LOG=/tmp/demo-workload.log
M="docker exec vf-src mysql -uroot -p123456 -h127.0.0.1 --default-character-set=utf8mb4 -N"

echo "$(date '+%F %T') WORKLOAD_START duration=${DURATION}s interval=${INTERVAL}s" >> "$LOG"
START=$SECONDS
N=0
while [ $((SECONDS - START)) -lt "$DURATION" ]; do
  N=$((N + 1))
  UID_R=$((RANDOM % 100000 + 1))
  PHONE=$(printf '139%08d' $((RANDOM % 100000000)))

  # 用户（mask 规则：目标端 phone 应为 139****XXXX）
  $M -e "INSERT INTO shop.users (name,phone,created) VALUES ('demo-u-$N','$PHONE',NOW() - INTERVAL $((RANDOM % 30)) DAY)" >/dev/null 2>&1

  # 订单 + 订单项（之后更新一次、按周期删除少量，覆盖三种事件）
  A="$((RANDOM % 100000 / 100)).$(printf '%02d' $((RANDOM % 100)))"
  OID=$($M -e "INSERT INTO shop.orders (user_id,amount,remark,phone) VALUES ($UID_R,$A,'demo-order-$N','$PHONE'); SELECT LAST_INSERT_ID()" 2>/dev/null | tail -1)
  if [ -n "$OID" ]; then
    $M -e "INSERT INTO shop.order_items (order_id,item_no,product,qty) VALUES ($OID,$((RANDOM % 3 + 1)),'demo-prod-$N',$((RANDOM % 5 + 1)))" >/dev/null 2>&1
    $M -e "UPDATE shop.orders SET amount=amount+1, remark=CONCAT(remark,'+upd'), updated_at=NOW() WHERE id=$OID" >/dev/null 2>&1
  fi

  # 仅增量表（do_stock=false）
  $M -e "INSERT INTO biz2.events (kind,payload) VALUES ('demo','event-$N')" >/dev/null 2>&1

  # 双写表（MySQL + RocketMQ）
  $M -e "INSERT INTO rmqtest.msg (name,qty,amount,note) VALUES ('demo-$N',$((RANDOM % 100)),$((RANDOM % 10000)) / 100.0,'demo-note')" >/dev/null 2>&1

  # 白名单外表：永远不应出现在目标库
  $M -e "INSERT INTO shop.tmp_log (msg) VALUES ('not-synced-$N')" >/dev/null 2>&1

  # 周期性 DELETE（覆盖删除事件，同时避免表无限膨胀）
  if [ $((N % 6)) -eq 0 ] && [ -n "$OID" ] && [ "$OID" -gt 20 ] 2>/dev/null; then
    $M -e "DELETE FROM shop.order_items WHERE order_id=$OID; DELETE FROM shop.orders WHERE id=$OID" >/dev/null 2>&1
  fi

  # 周期性 UPDATE 用户手机号（覆盖更新事件 + mask）
  if [ $((N % 3)) -eq 0 ]; then
    $M -e "UPDATE shop.users SET phone=CONCAT('138',LPAD($((RANDOM % 100000000)),8,'0')) WHERE id=$((RANDOM % 100000 + 1))" >/dev/null 2>&1
  fi

  # 每 60 秒记录一次源库水位
  if [ $((N % 20)) -eq 0 ]; then
    C=$($M -e "SELECT COUNT(*) FROM shop.orders" 2>/dev/null)
    echo "$(date '+%F %T') cycles=$N src_orders=$C" >> "$LOG"
  fi

  sleep "$INTERVAL"
done
echo "$(date '+%F %T') WORKLOAD_END cycles=$N" >> "$LOG"

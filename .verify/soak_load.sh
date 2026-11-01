#!/bin/bash
# soak_load.sh — gocdc 长跑验收负载（CRUD 持续写入 rmqtest.msg，MySQL+RocketMQ 双写规则）
# 用法: soak_load.sh [duration_sec]   默认 30600（8.5h）
# 注意：docker exec 全部内联（bash 函数包装 mysql 客户端会丢 stdout——实测诡异，勿改）
set -u
DURATION=${1:-30600}
LOG=/tmp/soak/soak.log
MON=/tmp/soak/monitor.log
mkdir -p /tmp/soak

MAXID=$(docker exec vf-src mysql -uroot -p123456 -h127.0.0.1 --default-character-set=utf8mb4 -N -e "SELECT COALESCE(MAX(id),0) FROM rmqtest.msg" 2>/dev/null)
echo "$(date '+%F %T') SOAK_START duration=$DURATION base_maxid=$MAXID" >> "$LOG"

DDL_DONE=0
LAST_MON=0
while true; do
  NOW=$SECONDS
  [ "$NOW" -ge "$DURATION" ] && break

  # ── 第 4 小时：auto DDL（一次，考验 DDL 自动同步链路）──
  if [ "$DDL_DONE" = "0" ] && [ "$NOW" -ge 14400 ]; then
    docker exec vf-src mysql -uroot -p123456 -h127.0.0.1 -e "ALTER TABLE rmqtest.msg ADD COLUMN soak_col VARCHAR(32) NULL" >> "$LOG" 2>&1
    echo "$(date '+%F %T') DDL_ADD_SOAK_COL executed" >> "$LOG"
    DDL_DONE=1
  fi

  # ── INSERT 5~20 行 ──
  N=$((RANDOM % 16 + 5))
  VS=""
  for i in $(seq 1 $N); do
    [ "$i" -gt 1 ] && VS="$VS,"
    VS="$VS('soak-$MAXID-$i', $((RANDOM % 100)), $((RANDOM % 10000)) / 100.0, 'soak-note')"
  done
  OUT=$(docker exec vf-src mysql -uroot -p123456 -h127.0.0.1 -N -e "INSERT INTO rmqtest.msg (name, qty, amount, note) VALUES $VS; SELECT MAX(id) FROM rmqtest.msg" 2>/dev/null)
  NEWMAX=$(echo "$OUT" | tail -1)
  [ -n "$NEWMAX" ] && MAXID=$NEWMAX

  # ── UPDATE 3~8 行（随机已有 id）──
  if [ "$MAXID" -gt 10 ] 2>/dev/null; then
    U=$((RANDOM % 6 + 3))
    for i in $(seq 1 $U); do
      RID=$((RANDOM % MAXID + 1))
      docker exec vf-src mysql -uroot -p123456 -h127.0.0.1 -e "UPDATE rmqtest.msg SET qty=qty+1, note=CONCAT('upd-', $RID, '-', $RANDOM) WHERE id=$RID" >/dev/null 2>&1
    done
  fi

  # ── DELETE 0~3 行（保持水位）──
  if [ "$MAXID" -gt 1000 ] 2>/dev/null; then
    D=$((RANDOM % 4))
    for i in $(seq 1 $D); do
      RID=$((RANDOM % MAXID + 1))
      docker exec vf-src mysql -uroot -p123456 -h127.0.0.1 -e "DELETE FROM rmqtest.msg WHERE id=$RID" >/dev/null 2>&1
    done
  fi

  # ── 巡检（每 600s）──
  if [ $((NOW - LAST_MON)) -ge 600 ]; then
    SRC=$(docker exec vf-src mysql -uroot -p123456 -h127.0.0.1 -N -e "SELECT COUNT(*) FROM rmqtest.msg" 2>/dev/null)
    DST=$(docker exec vf-dst mysql -uroot -p123456 -h127.0.0.1 -N -e "SELECT COUNT(*) FROM rmqtest.msg" 2>/dev/null)
    echo "$(date '+%F %T') src=$SRC dst=$DST maxid=$MAXID" >> "$MON"
    LAST_MON=$NOW
  fi

  sleep 3
done
FINAL_SRC=$(docker exec vf-src mysql -uroot -p123456 -h127.0.0.1 -N -e "SELECT COUNT(*) FROM rmqtest.msg" 2>/dev/null)
FINAL_MAX=$(docker exec vf-src mysql -uroot -p123456 -h127.0.0.1 -N -e "SELECT MAX(id) FROM rmqtest.msg" 2>/dev/null)
echo "$(date '+%F %T') SOAK_END src_rows=$FINAL_SRC maxid=$FINAL_MAX" >> "$LOG"

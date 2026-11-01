#!/bin/bash
# E4: 默认 pause 策略 ADD COLUMN → Web approve → 目标自动执行 DDL + resume + 新列数据流动
set -u
cd /mnt/d/workers/worker/qingfly/gocdc-mysql/.verify
SRC="docker exec vf-src mysql -uroot -p123456 -h127.0.0.1 --default-character-set=utf8mb4"
DST="docker exec vf-dst mysql -uroot -p123456 -h127.0.0.1 --default-character-set=utf8mb4"
api() { curl -s --noproxy '*' -u admin:demo123 "localhost:8060/api/v1$1" ${2:+-X $2}; }

echo "=== E4: 源端 ADD COLUMN email（白名单内 ADD，pause 策略 → 挂起）"
$SRC -e "ALTER TABLE shop.orders ADD COLUMN email VARCHAR(128) NULL" 2>/dev/null
sleep 3
EVID=$(api /ddl-events | python3 -c 'import sys,json; evs=json.load(sys.stdin)["data"]; p=[e["id"] for e in evs if e["status"]=="pending"]; print(p[0] if p else "")')
echo "E4 挂起事件: $EVID"
[ -z "$EVID" ] && { echo "E4 RESULT=FAIL（无挂起）"; exit 1; }

echo "=== G2 顺带：无 confirm 的 approve 应 400"
CODE=$(curl -s --noproxy '*' -o /dev/null -w '%{http_code}' -u admin:demo123 -X POST "localhost:8060/api/v1/ddl/$EVID/approve")
echo "approve 无 confirm http=$CODE（期待400）"

echo "=== E4: Web approve（一次点击全自动）"
api "/ddl/$EVID/approve?confirm=true" POST | head -c 150; echo
for i in $(seq 1 15); do
  HAS=$($DST -N -e "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='shop' AND table_name='orders' AND column_name='email'" 2>/dev/null)
  [ "$HAS" = "1" ] && { echo "E4 目标自动建列 t=${i}x2s"; break; }
  sleep 2
done

echo "=== E4: resume 后新列数据流动"
$SRC -e "INSERT INTO shop.orders (user_id,amount,remark,email) VALUES (660001,6.60,'e4-row','a@b.c')" 2>/dev/null
sleep 4
ROW=$($DST -N -e "SELECT email FROM shop.orders WHERE remark='e4-row'" 2>/dev/null)
echo "E4 新列数据: dst.email=$ROW"
[ "$HAS" = "1" ] && [ "$ROW" = "a@b.c" ] && echo "E4 RESULT=PASS（approve 自动执行+resume+新列流动）" || echo "E4 RESULT=FAIL"

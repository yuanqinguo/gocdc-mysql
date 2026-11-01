#!/bin/bash
# E3+E5+DDL-11: 白名单外 DROP COLUMN → pause 挂起（位点冻结）→ skip → 恢复补齐 + 审计留痕
# 注意：curl 一律完整写出且 '*' 必须引号（上一版 API 变量里裸 * 被 glob 展开挂死——教训）
set -u
cd /mnt/d/workers/worker/qingfly/gocdc-mysql/.verify
SRC="docker exec vf-src mysql -uroot -p123456 -h127.0.0.1 --default-character-set=utf8mb4"
DST="docker exec vf-dst mysql -uroot -p123456 -h127.0.0.1 --default-character-set=utf8mb4"
api() { curl -s --noproxy '*' -u admin:demo123 "localhost:8060/api/v1$1" ${2:+-X $2}; }

echo "=== E3: DROP shop.users.created（白名单外 → pause）"
$SRC -e "ALTER TABLE shop.users DROP COLUMN created" 2>/dev/null
sleep 3
echo "挂起状态 overview:"; api /overview | python3 -c 'import sys,json; d=json.load(sys.stdin)["data"]; print(" paused:",d.get("paused"),"reason:",d.get("pause_reason"))'
echo "ddl-events:"; api /ddl-events | python3 -c '
import sys,json
evs=json.load(sys.stdin)["data"]
p=[e for e in evs if e["status"]=="pending"]
print(" pending:",len(p))
for e in p: print("  id=%s sql=%s" % (e["id"], e["sql"][:70]))'
EVID=$(api /ddl-events | python3 -c 'import sys,json; evs=json.load(sys.stdin)["data"]; p=[e["id"] for e in evs if e["status"]=="pending"]; print(p[0] if p else "")')
echo "EVID=$EVID"

echo "=== DDL-11: 挂起期间写入冻结（不投递）"
$SRC -e "INSERT INTO biz2.events (kind,payload) VALUES ('incr3','挂起期写入-1'),('incr3','挂起期写入-2')" 2>/dev/null
sleep 4
DURING=$($DST -N -e "SELECT COUNT(*) FROM biz2.events WHERE kind='incr3'" 2>/dev/null)
POS1=$(api /position | python3 -c 'import sys,json; print(json.load(sys.stdin)["data"]["pos"])')
sleep 3
POS2=$(api /position | python3 -c 'import sys,json; print(json.load(sys.stdin)["data"]["pos"])')
echo "DDL-11 挂起期: 目标增量行 during=$DURING（期待0）；pos1=$POS1 pos2=$POS2（期待相等）"

echo "=== E5: skip 决策"
api "/ddl/$EVID/skip?confirm=true" POST | head -c 150; echo
sleep 5
AFTER=$($DST -N -e "SELECT COUNT(*) FROM biz2.events WHERE kind='incr3'" 2>/dev/null)
CREATED=$($DST -N -e "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='shop' AND table_name='users' AND column_name='created'" 2>/dev/null)
echo "E5 skip 后: 补齐 after=$AFTER（期待2）；目标 created 列仍在（保旧结构）: $CREATED（期待1）"

echo "=== E5 审计留痕"
api /audit | python3 -c '
import sys,json
rows=json.load(sys.stdin)["data"]
print(" audit 总条数:",len(rows))
for r in rows[-4:]: print("  who=%s what=%s result=%s" % (r.get("who"),r.get("what"),r.get("result")))'

echo "=== RESULT"
[ "$DURING" = "0" ] && [ "$POS1" = "$POS2" ] && [ "$AFTER" = "2" ] && [ "$CREATED" = "1" ] && echo "E3+E5+DDL-11 RESULT=PASS" || echo "E3+E5+DDL-11 RESULT=FAIL"

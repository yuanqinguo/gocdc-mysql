#!/bin/bash
# E6: exclude 表 DDL ignore 管线不停；E7: 新表 CREATE TABLE pause→approve 自动建表接入；
# E8: RENAME exclude 表忽略；P0-8 回归：挂起期间源表被删 → approve 过期放行管线不死
set -u
cd /mnt/d/workers/worker/qingfly/gocdc-mysql/.verify
SRC="docker exec vf-src mysql -uroot -p123456 -h127.0.0.1 --default-character-set=utf8mb4"
DST="docker exec vf-dst mysql -uroot -p123456 -h127.0.0.1 --default-character-set=utf8mb4"
api() { curl -s --noproxy '*' -u admin:demo123 "localhost:8060/api/v1$1" ${2:+-X $2}; }
pending_ids() { api /ddl-events | python3 -c 'import sys,json; evs=json.load(sys.stdin)["data"]; print(" ".join(e["id"] for e in evs if e["status"]=="pending"))'; }

echo "=== E6: exclude 表（shop.tmp_log）DDL ignore"
$SRC -e "INSERT INTO shop.tmp_log (msg) VALUES ('e6-不同步')" 2>/dev/null
$SRC -e "ALTER TABLE shop.tmp_log ADD COLUMN extra_col INT NULL" 2>/dev/null
sleep 4
P=$(pending_ids)
T=$(docker exec vf-dst mysql -uroot -p123456 -h127.0.0.1 -N -e "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='shop' AND table_name='tmp_log'" 2>/dev/null)
echo "E6 挂起事件=[$P]（期待空）；目标无 tmp_log 表: $T（期待0）"
[ -z "$P" ] && [ "$T" = "0" ] && echo "E6 RESULT=PASS" || echo "E6 RESULT=FAIL"

echo "=== E8前半: RENAME exclude 表 → 忽略不挂起"
$SRC -e "RENAME TABLE shop.tmp_log TO shop.tmp_log_renamed" 2>/dev/null
sleep 4
P=$(pending_ids)
echo "E8 前半 挂起事件=[$P]（期待空=忽略）"
[ -z "$P" ] && echo "E8-前半 RESULT=PASS" || echo "E8-前半 RESULT=FAIL（运维注意：新表名 tmp_log_renamed 不再命中 exclude）"

echo "=== E7: CREATE TABLE shop.new_table → pause 挂起 → approve 自动建表"
$SRC -e "CREATE TABLE shop.new_table (id BIGINT NOT NULL AUTO_INCREMENT, val VARCHAR(64) NULL, PRIMARY KEY (id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4" 2>/dev/null
sleep 3
EVID=$(pending_ids | awk '{print $1}')
echo "E7 挂起事件: $EVID"
if [ -n "$EVID" ]; then
  api "/ddl/$EVID/approve?confirm=true" POST >/dev/null
  for i in $(seq 1 15); do
    HAS=$($DST -N -e "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='shop' AND table_name='new_table'" 2>/dev/null)
    [ "$HAS" = "1" ] && { echo "E7 目标自动建表 t=${i}x2s"; break; }
    sleep 2
  done
  $SRC -e "INSERT INTO shop.new_table (val) VALUES ('e7-数据流动')" 2>/dev/null
  sleep 4
  V=$($DST -N -e "SELECT val FROM shop.new_table WHERE val='e7-数据流动'" 2>/dev/null)
  echo "E7 新表数据: dst=$V"
  [ "$HAS" = "1" ] && [ "$V" = "e7-数据流动" ] && echo "E7 RESULT=PASS" || echo "E7 RESULT=FAIL"
else
  echo "E7 RESULT=FAIL（CREATE 未挂起）"
fi

echo "=== E8后半/P0-8: 挂起期间源表被删 → approve 过期放行"
$SRC -e "CREATE TABLE shop.dying_table (id BIGINT NOT NULL AUTO_INCREMENT, v INT NULL, PRIMARY KEY (id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4" 2>/dev/null
sleep 3
EVID=$(pending_ids | awk '{print $1}')
echo "P0-8 挂起事件: $EVID"
$SRC -e "DROP TABLE shop.dying_table" 2>/dev/null
echo "P0-8 源表已删，执行 approve:"
api "/ddl/$EVID/approve?confirm=true" POST | head -c 200; echo
sleep 4
ALIVE=$(ps aux | grep '[.]/gocdc' | grep -v grep | wc -l)
$SRC -e "INSERT INTO shop.new_table (val) VALUES ('p08-管线仍活')" 2>/dev/null
sleep 4
V=$($DST -N -e "SELECT val FROM shop.new_table WHERE val='p08-管线仍活'" 2>/dev/null)
AUD=$(api /audit | python3 -c 'import sys,json; rows=json.load(sys.stdin)["data"]; hits=[r for r in rows if "dying" in str(r)]; print(hits[0].get("what")+" -> "+hits[0].get("result") if hits else "无")')
echo "P0-8: 进程存活=$ALIVE 后续流动=$V 审计=$AUD"
[ "$ALIVE" = "1" ] && [ "$V" = "p08-管线仍活" ] && echo "E8后半/P0-8 RESULT=PASS（过期放行不死锁）" || echo "E8后半/P0-8 RESULT=FAIL"

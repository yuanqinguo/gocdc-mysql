#!/bin/bash
# ADV-02 目标额外UK / ADV-03 保留字 / ADV-04 零日期 / ADV-05 60KB TEXT / ADV-06 自增漂移
set -u
cd /mnt/d/workers/worker/qingfly/gocdc-mysql/.verify
SRC="docker exec vf-src mysql -uroot -p123456 -h127.0.0.1 --default-character-set=utf8mb4"
DST="docker exec vf-dst mysql -uroot -p123456 -h127.0.0.1 --default-character-set=utf8mb4"
api() { curl -s --noproxy '*' -u admin:demo123 "localhost:8060/api/v1$1" ${2:+-X $2}; }
pending_id() { api /ddl-events | python3 -c 'import sys,json; evs=json.load(sys.stdin)["data"] or []; p=[e["id"] for e in evs if e["status"]=="pending"]; print(p[0] if p else "")'; }

echo "=== ADV-03: 保留字表名/列名（CREATE 挂起 → approve 自动建表 → 全链）"
$SRC -e 'CREATE TABLE shop.`order` (id BIGINT NOT NULL AUTO_INCREMENT, `key` INT NULL, `select` VARCHAR(32) NULL, PRIMARY KEY (id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4' 2>/dev/null
sleep 3
EVID=$(pending_id)
echo "ADV-03 挂起: $EVID"
api "/ddl/$EVID/approve?confirm=true" POST >/dev/null
sleep 6
T=$($DST -N -e "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='shop' AND table_name='\`order\`'" 2>/dev/null || $DST -N -e "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='shop' AND table_name='order'" 2>/dev/null)
$SRC -e 'INSERT INTO shop.`order` (`key`,`select`) VALUES (42, '"'"'reserved'"'"')' 2>/dev/null
sleep 4
V=$($DST -N -e 'SELECT `select` FROM shop.`order` WHERE `key`=42' 2>/dev/null)
echo "ADV-03: 目标建表=$T 保留字数据流动=$V（期待 reserved）"
[ "$T" = "1" ] && [ "$V" = "reserved" ] && echo "ADV-03 RESULT=PASS" || echo "ADV-03 RESULT=FAIL"

echo "=== ADV-04: 零日期（on_convert_error 默认 null 策略不炸）"
$SRC -e "SET SESSION sql_mode='NO_ENGINE_SUBSTITUTION'" 2>/dev/null
$SRC -e "INSERT INTO types.all_types (c_date,c_dt,c_varchar) VALUES ('0000-00-00','0000-00-00 00:00:00','adv04-zero')" 2>/dev/null
sleep 4
D=$($DST -N -e "SELECT IFNULL(TO_CHAR(c_date),'NULL') FROM types.all_types WHERE c_varchar='adv04-zero'" 2>/dev/null)
D=$($DST -N -e "SELECT COALESCE(CAST(c_date AS CHAR),'NULL-VAL') FROM types.all_types WHERE c_varchar='adv04-zero'" 2>/dev/null)
ALIVE=$(ps aux | grep '[.]/gocdc' | grep -v grep | wc -l)
echo "ADV-04: 目标零日期=$D 进程存活=$ALIVE（期待 NULL-VAL/1）"
[ "$D" = "NULL-VAL" ] && [ "$ALIVE" = "1" ] && echo "ADV-04 RESULT=PASS" || echo "ADV-04 RESULT=FAIL"

echo "=== ADV-05: 60KB 超长 TEXT"
$SRC -e "INSERT INTO types.all_types (c_text,c_varchar) VALUES (REPEAT('A',60000),'adv05-60k')" 2>/dev/null
sleep 5
SL=$($SRC -N -e "SELECT LENGTH(c_text) FROM types.all_types WHERE c_varchar='adv05-60k'" 2>/dev/null)
DL=$($DST -N -e "SELECT LENGTH(c_text) FROM types.all_types WHERE c_varchar='adv05-60k'" 2>/dev/null)
echo "ADV-05: src_len=$SL dst_len=$DL（期待 60000/60000）"
[ "$SL" = "60000" ] && [ "$DL" = "60000" ] && echo "ADV-05 RESULT=PASS" || echo "ADV-05 RESULT=FAIL"

echo "=== ADV-06: 目标 AUTO_INCREMENT 漂移"
$DST -e "INSERT INTO shop.new_table (id,val) VALUES (999999,'adv06-target-explicit')" 2>/dev/null
$SRC -e "INSERT INTO shop.new_table (val) VALUES ('adv06-src-next')" 2>/dev/null
sleep 4
DSTV=$($DST -N -e "SELECT COUNT(*) FROM shop.new_table WHERE val='adv06-src-next'" 2>/dev/null)
echo "ADV-06: 目标漂移后下一行同步=$DSTV（期待1）"
[ "$DSTV" = "1" ] && echo "ADV-06 RESULT=PASS" || echo "ADV-06 RESULT=FAIL"

echo "=== ADV-02: 目标额外 UNIQUE KEY"
$DST -e "ALTER TABLE shop.new_table ADD UNIQUE KEY uk_val (val)" 2>/dev/null
$SRC -e "INSERT INTO shop.new_table (val) VALUES ('adv02-uk')" 2>/dev/null
$SRC -e "UPDATE shop.new_table SET val='adv02-uk' WHERE val='adv02-uk' AND id=(SELECT t.id FROM (SELECT id FROM shop.new_table WHERE val='adv02-uk') t)" 2>/dev/null
$SRC -e "UPDATE shop.new_table SET val='adv02-uk-upd' WHERE val='adv02-uk'" 2>/dev/null
sleep 5
U=$($DST -N -e "SELECT COUNT(*) FROM shop.new_table WHERE val='adv02-uk-upd'" 2>/dev/null)
ALIVE=$(ps aux | grep '[.]/gocdc' | grep -v grep | wc -l)
echo "ADV-02: UK 路径 UPDATE 收敛=$U 进程存活=$ALIVE（期待1/1）"
[ "$U" = "1" ] && [ "$ALIVE" = "1" ] && echo "ADV-02 RESULT=PASS" || echo "ADV-02 RESULT=FAIL"
$DST -e "ALTER TABLE shop.new_table DROP INDEX uk_val" 2>/dev/null
echo "（UK 已移除复原）"

#!/bin/bash
# ADV-01 第二层：目标预建无主键表 → 启动核对报「无主键且未配 order_by_column」
set -u
cd /mnt/d/workers/worker/qingfly/gocdc-mysql/.verify
SRC="docker exec vf-src mysql -uroot -p123456 -h127.0.0.1 --default-character-set=utf8mb4"
DST="docker exec vf-dst mysql -uroot -p123456 -h127.0.0.1 --default-character-set=utf8mb4"
start_node() { WEB_PASSWORD=demo123 nohup ./gocdc -config-file config-node1.yml > node1.log 2>&1 & sleep 4; ps aux | grep '[.]/gocdc' | awk '{print $2}' | head -1 > node1.pid; }

# 恢复 exclude 表原名（§9 注5 实证后的环境修复）
$SRC -e "RENAME TABLE shop.tmp_log_renamed TO shop.tmp_log" 2>/dev/null
echo "tmp_log 已恢复原名"

# 第二层：两端各建无主键 noltbl → 启动
$SRC -e "CREATE TABLE shop.noltbl (id INT NULL, v INT NULL)" 2>/dev/null
$DST -e "CREATE TABLE shop.noltbl (id INT NULL, v INT NULL)" 2>/dev/null
start_node
sleep 2
echo "=== 第二层核对输出:"
grep -E '核对错误|fatal' node1.log | head -3
ALIVE=$(ps aux | grep '[.]/gocdc' | grep -v grep | wc -l)
[ "$ALIVE" = "0" ] && echo "ADV-01 第二层 RESULT=PASS（无主键拒启）" || echo "ADV-01 第二层 RESULT=FAIL"

# 清理恢复
$SRC -e "DROP TABLE shop.noltbl" 2>/dev/null
$DST -e "DROP TABLE shop.noltbl" 2>/dev/null
start_node
sleep 3
ALIVE=$(ps aux | grep '[.]/gocdc' | grep -v grep | wc -l)
$SRC -e "INSERT INTO shop.new_table (val) VALUES ('adv01-recovered')" 2>/dev/null
sleep 4
V=$($DST -N -e "SELECT COUNT(*) FROM shop.new_table WHERE val='adv01-recovered'" 2>/dev/null)
echo "恢复: 存活=$ALIVE 流动=$V"
[ "$ALIVE" = "1" ] && [ "$V" = "1" ] && echo "ADV-01 RESULT=PASS（双层拦截+恢复流动）" || echo "ADV-01 RESULT=FAIL"

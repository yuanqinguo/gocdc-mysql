#!/bin/bash
# E1 auto ADD COLUMN 零干预 + E2 auto varchar 扩容（types.all_types 为 ddl_policy=auto）
set -u
cd /mnt/d/workers/worker/qingfly/gocdc-mysql/.verify
SRC="docker exec vf-src mysql -uroot -p123456 -h127.0.0.1 --default-character-set=utf8mb4"
DST="docker exec vf-dst mysql -uroot -p123456 -h127.0.0.1 --default-character-set=utf8mb4"

echo "=== E1: ADD COLUMN c_new（auto，零干预）"
$SRC -e "ALTER TABLE types.all_types ADD COLUMN c_new VARCHAR(32) NULL" 2>/dev/null
for i in $(seq 1 20); do
  HAS=$($DST -N -e "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='types' AND table_name='all_types' AND column_name='c_new'" 2>/dev/null)
  [ "$HAS" = "1" ] && { echo "E1 目标自动加列 t=${i}x2s"; break; }
  sleep 2
done
$SRC -e "INSERT INTO types.all_types (c_varchar,c_new) VALUES ('e1-row','新列值✅')" 2>/dev/null
sleep 4
ROW=$($DST -N -e "SELECT c_new FROM types.all_types WHERE c_varchar='e1-row'" 2>/dev/null)
echo "E1 新列数据流动: dst.c_new=$ROW"
[ "$ROW" = "新列值✅" ] && echo "E1 RESULT=PASS" || echo "E1 RESULT=FAIL"

echo "=== E2: varchar 64→256 扩容"
$SRC -e "ALTER TABLE types.all_types MODIFY c_varchar VARCHAR(256)" 2>/dev/null
for i in $(seq 1 20); do
  LEN=$($DST -N -e "SELECT CHARACTER_MAXIMUM_LENGTH FROM information_schema.columns WHERE table_schema='types' AND table_name='all_types' AND column_name='c_varchar'" 2>/dev/null)
  [ "$LEN" = "256" ] && { echo "E2 目标自动扩容 t=${i}x2s"; break; }
  sleep 2
done
LONG=$(python3 -c "print('长'*100)")
$SRC -e "INSERT INTO types.all_types (c_varchar) VALUES ('$LONG')" 2>/dev/null
sleep 4
SRCLEN=$($SRC -N -e "SELECT CHAR_LENGTH(c_varchar) FROM types.all_types WHERE c_varchar=LEFT('$LONG',2) ORDER BY id DESC LIMIT 1" 2>/dev/null)
DSTLEN=$($DST -N -e "SELECT CHAR_LENGTH(c_varchar) FROM types.all_types WHERE c_varchar=LEFT('$LONG',2) ORDER BY id DESC LIMIT 1" 2>/dev/null)
echo "E2 长值往返: src_len=$SRCLEN dst_len=$DSTLEN"
[ "$SRCLEN" = "100" ] && [ "$DSTLEN" = "100" ] && echo "E2 RESULT=PASS" || echo "E2 RESULT=FAIL"

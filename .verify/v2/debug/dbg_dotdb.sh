#!/bin/bash
SRC="docker exec vf-src mysql -uroot -p123456 -h127.0.0.1 --default-character-set=utf8mb4"
$SRC -e "SHOW DATABASES" 2>&1 | tail -8
echo "--- 显式插入+查询"
$SRC -e "INSERT INTO \`edu.mall.admin\`.users (name,phone) VALUES ('t1','139')" 2>&1 | head -3
$SRC -N -e "SELECT COUNT(*) FROM \`edu.mall.admin\`.users" 2>&1 | head -3

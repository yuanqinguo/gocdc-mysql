#!/bin/bash
# 重跑前重置：对齐 merge 结构（上轮 DDL 停摆期源端加的列）、清脏数据、清 verify 前缀
set -u
cd /mnt/d/workers/worker/qingfly/gocdc-mysql/.verify
docker exec vf-dst mysql -uroot -p123456 -h127.0.0.1 -e "
ALTER TABLE \`edu.mall.merge\`.users ADD COLUMN IF NOT EXISTS email VARCHAR(64) NULL" 2>/dev/null || \
docker exec vf-dst mysql -uroot -p123456 -h127.0.0.1 -e "
ALTER TABLE \`edu.mall.merge\`.users ADD COLUMN email VARCHAR(64) NULL" 2>/dev/null
docker exec vf-dst mysql -uroot -p123456 -h127.0.0.1 -e "TRUNCATE TABLE \`edu.mall.merge\`.users" 2>/dev/null
docker exec vf-src mysql -uroot -p123456 -h127.0.0.1 -e "TRUNCATE TABLE \`edu.mall.admin\`.users" 2>/dev/null
docker exec vf-src mysql -uroot -p123456 -h127.0.0.1 --default-character-set=utf8mb4 -e "
INSERT INTO \`edu.mall.admin\`.users (name,phone) VALUES ('dotdb-a','13900000001'),('dotdb-b','13900000002'),('dotdb-c',NULL)" 2>/dev/null
docker exec vf-etcd etcdctl --endpoints=127.0.0.1:2379 del --prefix /gocdc/verify/ >/dev/null
echo "结构对齐+数据重置+前缀清理完成"
docker exec vf-dst mysql -uroot -p123456 -h127.0.0.1 -N -e "SELECT COLUMN_NAME FROM information_schema.columns WHERE table_schema='edu.mall.merge' AND table_name='users' ORDER BY ordinal_position" 2>/dev/null | tr '\n' ' '
echo

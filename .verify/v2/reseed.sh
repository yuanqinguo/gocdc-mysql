#!/bin/bash
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
wipe_data
echo "src tables=$($SRC -N -e 'SELECT COUNT(*) FROM information_schema.tables WHERE table_schema IN ("shop","biz2","types","rmqtest","cluster_e2e")' 2>/dev/null)"
echo "dst tables=$($DST -N -e 'SELECT COUNT(*) FROM information_schema.tables WHERE table_schema IN ("shop","biz2","types","rmqtest","cluster_e2e")' 2>/dev/null)"
echo "orders=$($SRC -N -e 'SELECT COUNT(*) FROM shop.orders' 2>/dev/null) users=$($SRC -N -e 'SELECT COUNT(*) FROM shop.users' 2>/dev/null) dotdb=$($SRC -N -e 'SELECT COUNT(*) FROM \`edu.mall.admin\`.users' 2>/dev/null)"
echo "binlog=$($SRC -N -e 'SELECT @@binlog_format,@@binlog_row_image,@@gtid_mode' 2>/dev/null)"

#!/bin/bash
# 00_reset 后补充自检：带点库播种行数 + FIFO topic 属性（显示层引用核实用，非 case）
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
echo "dotdb rows = $($SRC -N -e 'SELECT COUNT(*) FROM `edu.mall.admin`.users' 2>/dev/null)"
echo "dotdb dst   = $($DST -N -e 'SELECT COUNT(*) FROM `edu.mall.merge`.users' 2>/dev/null)"
echo "--- topicRoute CDC_rmqtest_msg ---"
docker exec rmqbroker sh mqadmin topicRoute -n namesrv:9876 -t CDC_rmqtest_msg 2>/dev/null | grep -iE 'messageType|attributes' | head -3
echo "--- topicList CDC_* ---"
docker exec rmqbroker sh mqadmin topicList -n namesrv:9876 2>/dev/null | grep CDC_

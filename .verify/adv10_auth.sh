#!/bin/bash
# ADV-10: auth_disabled=true 显式裸奔——无凭据 200 放行；confirm 门禁仍独立强制（400）
set -u
cd /mnt/d/workers/worker/qingfly/gocdc-mysql/.verify
start_node() { WEB_PASSWORD=demo123 nohup ./gocdc -config-file config-node1.yml > node1.log 2>&1 & sleep 4; ps aux | grep '[.]/gocdc' | awk '{print $2}' | head -1 > node1.pid; }

sed -i 's/auth_disabled: false/auth_disabled: true/' config-node1.yml
grep 'auth_disabled' config-node1.yml

PID=$(cat node1.pid); kill -TERM "$PID"; sleep 3
start_node
sleep 2
ALIVE=$(ps aux | grep '[.]/gocdc' | grep -v grep | wc -l)
NOAUTH=$(curl -s --noproxy '*' -o /dev/null -w '%{http_code}' localhost:8060/api/v1/position)
GATE=$(curl -s --noproxy '*' -o /dev/null -w '%{http_code}' -X POST localhost:8060/api/v1/leader/resign)
echo "ADV-10: 启动存活=$ALIVE（校验通过）无凭据GET=$NOAUTH（期待200）无confirm写=$GATE（期待400）"
[ "$ALIVE" = "1" ] && [ "$NOAUTH" = "200" ] && [ "$GATE" = "400" ] && echo "ADV-10 RESULT=PASS" || echo "ADV-10 RESULT=FAIL"

# 改回
sed -i 's/auth_disabled: true/auth_disabled: false/' config-node1.yml
PID=$(cat node1.pid); kill -TERM "$PID"; sleep 3
start_node
sleep 2
R1=$(curl -s --noproxy '*' -o /dev/null -w '%{http_code}' localhost:8060/api/v1/position)
R2=$(curl -s --noproxy '*' -o /dev/null -w '%{http_code}' -u admin:demo123 localhost:8060/api/v1/position)
echo "复原: 无凭据=$R1（期待401） 正确凭据=$R2（期待200）"
[ "$R1" = "401" ] && [ "$R2" = "200" ] && echo "ADV-10 复原=PASS" || echo "ADV-10 复原=FAIL"

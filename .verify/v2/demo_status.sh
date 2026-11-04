#!/bin/bash
# 演示环境体检：节点状态 + 位点 + 源/目标行数对比 + 持续负载进度
set -u
unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY all_proxy ALL_PROXY 2>/dev/null
export NO_PROXY="*"; export no_proxy="*"
SRC="docker exec vf-src mysql -uroot -p123456 -h127.0.0.1 --default-character-set=utf8mb4 -N"
DST="docker exec vf-dst mysql -uroot -p123456 -h127.0.0.1 --default-character-set=utf8mb4 -N"

echo "── 集群状态 ──"
for wp in 8060 8061; do
  r=$(curl -s --max-time 3 --noproxy '*' -u admin:demo123 "localhost:$wp/api/v1/status" \
      | python3 -c "import sys,json;d=json.load(sys.stdin)['data'];print(f\"role={d['role']} state={d['sources'][0]['pipeline_state']} leader={d['leader']} term={d['term']}\")" 2>/dev/null)
  echo "  :$wp $r"
done
echo "── 位点 ──"
curl -s --max-time 3 --noproxy '*' -u admin:demo123 http://localhost:8060/api/v1/sources/default/position | head -c 400; echo
echo "── 行数对比（src / dst）──"
for t in shop.orders shop.users shop.order_items types.all_types biz2.events rmqtest.msg cluster_e2e.tab shop.tmp_log; do
  s=$($SRC -e "SELECT COUNT(*) FROM $t" 2>/dev/null)
  d=$($DST -e "SELECT COUNT(*) FROM $t" 2>/dev/null)
  printf '  %-22s src=%-8s dst=%-8s\n' "$t" "${s:-?}" "${d:-?}"
done
echo "── 负载日志尾部 ──"
tail -n 3 /tmp/demo-workload.log 2>/dev/null || echo "  （负载未运行）"

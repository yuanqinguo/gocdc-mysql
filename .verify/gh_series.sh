#!/bin/bash
# G1 认证矩阵 / G2 confirm 门禁（resign/verify）/ G3 SET-POSITION 门禁 / F5 rules/effective / H 可观测
set -u
cd /mnt/d/workers/worker/qingfly/gocdc-mysql/.verify
API="localhost:8060/api/v1"

echo "=== G1 认证矩阵（无凭据/错密码/正确）"
C1=$(curl -s --noproxy '*' -o /dev/null -w '%{http_code}' "$API/position")
C2=$(curl -s --noproxy '*' -o /dev/null -w '%{http_code}' -u admin:wrongpw "$API/position")
C3=$(curl -s --noproxy '*' -o /dev/null -w '%{http_code}' -u admin:demo123 "$API/position")
echo "G1: 无凭据=$C1 错密码=$C2 正确=$C3（期待401/401/200）"
[ "$C1" = "401" ] && [ "$C2" = "401" ] && [ "$C3" = "200" ] && echo "G1 RESULT=PASS" || echo "G1 RESULT=FAIL"

echo "=== G2 confirm 门禁（verify/resign 无 confirm 400）"
CV=$(curl -s --noproxy '*' -o /dev/null -w '%{http_code}' -u admin:demo123 -X POST -H 'Content-Type: application/json' -d '{"database":"types","table":"all_types"}' "$API/verify")
CR=$(curl -s --noproxy '*' -o /dev/null -w '%{http_code}' -u admin:demo123 -X POST "$API/leader/resign")
echo "G2: verify无confirm=$CV（期待400）resign无confirm=$CR（期待400）"
[ "$CV" = "400" ] && [ "$CR" = "400" ] && echo "G2 RESULT=PASS" || echo "G2 RESULT=FAIL"

echo "=== G3 SET-POSITION 门禁（错 confirm 400；不动真位点——只验门禁与正确 confirm 的受理语义）"
CG=$(curl -s --noproxy '*' -o /dev/null -w '%{http_code}' -u admin:demo123 -X POST -H 'Content-Type: application/json' -d '{"name":"binlog.000004","pos":4}' "$API/position?confirm=WRONG")
echo "G3: confirm=WRONG http=$CG（期待400）"
[ "$CG" = "400" ] && echo "G3 RESULT=PASS" || echo "G3 RESULT=FAIL"

echo "=== F5 /rules/effective 合并视图"
curl -s --noproxy '*' -u admin:demo123 "$API/rules/effective" | python3 -c '
import sys,json
d=json.load(sys.stdin)["data"]
rows = d if isinstance(d,list) else d.get("rules",d)
print(" 表数量:",len(rows))
for r in rows:
    if isinstance(r,dict):
        print("  %-22s stock=%s ddl=%s sinks=%s mask=%s excl=%s" % (
          r.get("table_ref") or (str(r.get("schema"))+"."+str(r.get("table") or "*")),
          r.get("do_stock"), r.get("ddl_policy"), r.get("sinks"), bool(r.get("mask_columns")), r.get("exclude_tables")))'

echo "=== H1 metrics 指标族"
curl -s --noproxy '*' localhost:9595/metrics | grep -oE '^gocdc_[a-z_]+' | sort -u | head -40

echo "=== H2 lag 语义（追平=0）"
curl -s --noproxy '*' localhost:9595/metrics | grep -E 'gocdc_master_position_lag_bytes|gocdc_position_watermark|gocdc_position_last_saved' | grep -v '^#'

echo "=== H4 日志敏感信息"
cd /mnt/d/workers/worker/qingfly/gocdc-mysql/.verify
HITS=$(grep -cE '123456|123456|demo123' node1.log 2>/dev/null || true)
echo "H4: 日志中密码出现次数=$HITS（期待0）"
[ "$HITS" = "0" ] && echo "H4 RESULT=PASS" || echo "H4 RESULT=FAIL"

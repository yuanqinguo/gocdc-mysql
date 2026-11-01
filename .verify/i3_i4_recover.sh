#!/bin/bash
# I3 补投 + I4 崩溃重放幂等（broker 就绪后重启 gocdc，位点从 86323 重放）
set -u
cd /mnt/d/workers/worker/qingfly/gocdc-mysql/.verify
SRC="docker exec vf-src mysql -uroot -p123456 -h127.0.0.1 --default-character-set=utf8mb4"
DST="docker exec vf-dst mysql -uroot -p123456 -h127.0.0.1 --default-character-set=utf8mb4"
api() { curl -s --noproxy '*' -u admin:demo123 "localhost:8060/api/v1$1"; }
TS=$(date +%s)

nohup ./rmqcons-bin -topic CDC_rmqtest_msg -group i34-$TS -out /tmp/rmq-i34.jsonl -idle 20s > /tmp/rmqcons-i34.log 2>&1 &
sleep 4
WEB_PASSWORD=demo123 nohup ./gocdc -config-file config-node1.yml > node1.log 2>&1 &
sleep 6
ps aux | grep '[.]/gocdc' | awk '{print $2}' | head -1 > node1.pid
echo "gocdc 重启 pid=$(cat node1.pid)"

for i in $(seq 1 20); do
  ps -p $(pgrep -f "rmqcons-bin.*i34-$TS" | head -1) >/dev/null 2>&1 || break; sleep 2
done

python3 - <<'PYEOF'
import json,subprocess,collections
events=[]; ids=[]
for line in open('/tmp/rmq-i34.jsonl',encoding='utf-8'):
    try:
        e=json.loads(line); ids.append(e.get('event_id')); events.append(e)
    except Exception: pass
i3=[e for e in events if (e.get('after') or {}).get('note')=='宕机期']
i3names={e['after']['name'] for e in i3}
dup=len(ids)-len(set(ids))
print(f" 补投: 宕机期消息={len(i3)} 去重后name={sorted(i3names)}（期待5个 i3-a~e）")
q="SELECT name,qty FROM rmqtest.msg WHERE note='宕机' OR note='宕机期' ORDER BY id"
q="SELECT name,qty FROM rmqtest.msg WHERE note='宕机期' ORDER BY id"
src=subprocess.run(["docker","exec","vf-src","mysql","-uroot","-p123456","-h127.0.0.1","-N","-e",q],capture_output=True,text=True).stdout
srcnames={l.split('\t')[0] for l in src.strip().splitlines() if l}
print(f" 源端宕机期行={len(srcnames)}")
i4=[e for e in events if str((e.get('after') or {}).get('name','')).startswith('i4-')]
i4finals={}
for e in i4:
    a=e['after']; i4finals[a['name']]=int(a['qty'])
q4="SELECT name,qty FROM rmqtest.msg WHERE note='crash' ORDER BY id"
src4=subprocess.run(["docker","exec","vf-src","mysql","-uroot","-p123456","-h127.0.0.1","-N","-e",q4],capture_output=True,text=True).stdout
src4rows={l.split('\t')[0]:int(l.split('\t')[1]) for l in src4.strip().splitlines() if l}
ok4=all(i4finals.get(k)==v for k,v in src4rows.items())
print(f" I4: crash消息={len(i4)} 全event重复={dup} 源crash行={len(src4rows)} MQ重放终态一致={ok4}")
r3 = i3names==srcnames and len(i3names)==5
print(" I3 RESULT=" + ("PASS（恢复后自动补投5条）" if r3 else f"FAIL {i3names} vs {srcnames}"))
print(" I4 RESULT=" + ("PASS（重放收敛）" if ok4 and len(src4rows)==60 else f"FAIL ok4={ok4} n={len(src4rows)}"))
PYEOF
echo "=== MySQL 侧收敛断言"
S=$($SRC -N -e "SELECT COUNT(*) FROM rmqtest.msg" 2>/dev/null); D=$($DST -N -e "SELECT COUNT(*) FROM rmqtest.msg" 2>/dev/null)
echo "rmqtest.msg src=$S dst=$D"
api /position | python3 -c 'import sys,json; d=json.load(sys.stdin)["data"]; print("watermark:",d["pos"],"seq",d["seq"])'
curl -s --noproxy '*' localhost:9595/metrics | grep -E '^gocdc_master_position_lag_bytes'

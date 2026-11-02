#!/bin/bash
# v2 组30：位点（C 系列）——C1 /position 实时 / C2 kill -9 大事务中段重放 /
# C3 位点回拨（停机后 etcdctl）/ C5 binlog PURGED 拒启+恢复 / C6 GTID 续传集合一致
set -u
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

wipe_all
ccput verify "$V2/body_main.yml"
stub_start node1 verify :9595 :8060
wait_ready 9595 60 || { echo "FATAL 启动失败"; exit 1; }
# 等 stock 完成（行数收敛口径）
for i in $(seq 1 30); do
  D=$($DST -N -e "SELECT COUNT(*) FROM shop.orders" 2>/dev/null)
  [ "$D" = "100" ] && break; sleep 2
done
for i in $(seq 1 20); do L=$(lagof 9595); [ "$L" = "0" ] && break; sleep 2; done

grp "C1 GET /position 实时反映 etcd"
P1=$(curl -s --noproxy '*' -u admin:demo123 localhost:8060/api/v1/position)
$SRC -e "INSERT INTO shop.orders (user_id,amount,remark) VALUES (1,1.00,'c1-probe')" 2>/dev/null
sleep 6
P2=$(curl -s --noproxy '*' -u admin:demo123 localhost:8060/api/v1/position)
S1=$(echo "$P1" | python3 -c 'import sys,json;d=json.load(sys.stdin)["data"];print(d.get("seq"),d.get("pos"),d.get("gtid_set","")[-40:])')
S2=$(echo "$P2" | python3 -c 'import sys,json;d=json.load(sys.stdin)["data"];print(d.get("seq"),d.get("pos"),d.get("gtid_set","")[-40:])')
echo "  前: $S1"; echo "  后: $S2"
[ "$S1" != "$S2" ] && ok "C1 /position 写入后前进（seq/pos/gtid）" || bad "C1 位点未前进"

grp "C2 kill -9 大事务中段（重放幂等零重复）"
docker exec -i vf-src mysql -uroot -p123456 -h127.0.0.1 --default-character-set=utf8mb4 < "$V1/c2_bigtx.sql" >/dev/null 2>&1
sleep 2   # 事务已提交、管线正在消化积压
kill -9 $(pickpid node1) 2>/dev/null
echo "  已 kill -9（积压消化中段），宕机期再插 2×50k"
docker exec -i vf-src mysql -uroot -p123456 -h127.0.0.1 --default-character-set=utf8mb4 < "$V1/c2b_bigtx.sql" >/dev/null 2>&1
docker exec -i vf-src mysql -uroot -p123456 -h127.0.0.1 --default-character-set=utf8mb4 < "$V1/c2c_bigtx.sql" >/dev/null 2>&1
stub_start node1 verify :9595 :8060
wait_ready 9595 60 || bad "C2 重启失败"
T0=$(date +%s)
for i in $(seq 1 90); do
  S=$($SRC -N -e "SELECT COUNT(*) FROM shop.orders" 2>/dev/null)
  D=$($DST -N -e "SELECT COUNT(*) FROM shop.orders" 2>/dev/null)
  [ "$S" = "$D" ] && { echo "  收敛 t=$(( $(date +%s) - T0 ))s src=$S dst=$D"; break; }
  sleep 2
done
DUP=$($DST -N -e "SELECT COUNT(*)-COUNT(DISTINCT id) FROM shop.orders" 2>/dev/null)
echo "  distinct 差值=$DUP（期待0）"
[ "$S" = "$D" ] && [ "$DUP" = "0" ] && ok "C2 重启重放幂等收敛零重复（$S 行）" || bad "C2 (src=$S dst=$D dup=$DUP)"

grp "C3 位点回拨（必须停机后做）"
KEY=$($ETCDC get /gocdc/verify/sources/default/position --print-value-only)
echo "  当前位点: $(echo "$KEY" | head -c 120)..."
kill -TERM $(pickpid node1) 2>/dev/null
for i in $(seq 1 30); do [ -z "$(pickpid node1)" ] && break; sleep 1; done
[ -n "$(pickpid node1)" ] && { echo "FATAL 优雅停机超时"; exit 1; }
NEWPOS=$(echo "$KEY" | python3 -c "import sys,json;d=json.load(sys.stdin);d['pos']=d['pos']-20000;print(json.dumps(d))")
$ETCDC put /gocdc/verify/sources/default/position "$NEWPOS" >/dev/null
echo "  已回拨 20000 字节，重启重放"
stub_start node1 verify :9595 :8060
wait_ready 9595 60 || bad "C3 重启失败"
for i in $(seq 1 60); do
  L=$(lagof 9595); [ "$L" = "0" ] && { echo "  追平 t=${i}x2s"; break; }; sleep 2
done
SO=$($SRC -N -e "SELECT COUNT(*) FROM shop.orders" 2>/dev/null); DO_=$($DST -N -e "SELECT COUNT(*) FROM shop.orders" 2>/dev/null)
DUPO=$($DST -N -e "SELECT COUNT(*)-COUNT(DISTINCT id) FROM shop.orders" 2>/dev/null)
echo "  src=$SO dst=$DO_ dup=$DUPO"
[ "$SO" = "$DO_" ] && [ "$DUPO" = "0" ] && ok "C3 回拨重放幂等收敛" || bad "C3 (src=$SO dst=$DO_ dup=$DUPO)"

grp "C6 GTID 续传集合一致"
# 保存位点按 position_save_interval 周期落盘：C3 的 lag=0 是内存处理进度，
# 与已 checkpoint 到 etcd 的位点天然差一拍——轮询等落盘追平，不能瞬时快照比对
C6OK=0
for i in $(seq 1 15); do
  PG=$(curl -s --noproxy '*' -u admin:demo123 localhost:8060/api/v1/position | python3 -c 'import sys,json;print(json.load(sys.stdin)["data"]["gtid_set"])')
  MG=$($SRC -N -e "SELECT @@global.gtid_executed" 2>/dev/null)
  [ "$PG" = "$MG" ] && { C6OK=1; echo "  追平 t=${i}s"; break; }
  sleep 1
done
[ "$C6OK" = "1" ] && echo "  position=$PG"; echo "  master  =$MG"
[ "$C6OK" = "1" ] && ok "C6 GTID 集与 master 完全一致" || bad "C6 (pos=${PG:-无} master=$MG)"

grp "C5 binlog PURGED 检测（拒启+恢复）"
kill -TERM $(pickpid node1) 2>/dev/null
for i in $(seq 1 30); do [ -z "$(pickpid node1)" ] && break; sleep 1; done
$SRC -e "FLUSH BINARY LOGS" 2>/dev/null
OLDEST=$($SRC -N -e "SHOW BINARY LOGS" 2>/dev/null | head -1 | awk '{print $1}')
NEWFILE=$($SRC -N -e "SHOW BINARY LOGS" 2>/dev/null | tail -1 | awk '{print $1}')
GTID=$($SRC -N -e "SELECT @@global.gtid_executed" 2>/dev/null)
$ETCDC put /gocdc/verify/sources/default/position "{\"v\":1,\"name\":\"$OLDEST\",\"pos\":4,\"gtid_set\":\"$GTID\",\"seq\":1,\"leader_term\":1,\"node_id\":\"node1\"}" >/dev/null
$SRC -e "PURGE BINARY LOGS TO '$NEWFILE'" 2>/dev/null
echo "  位点拨到已 PURGE 的 $OLDEST:4（现存: $($SRC -N -e 'SHOW BINARY LOGS' 2>/dev/null | awk '{print $1}' | tr '\n' ' ')）"
# 前台启动 → 期待 PURGED 拒启 exit≠0
d=/tmp/v2/c5run; rm -rf $d; mkdir -p $d
cat > $d/config.local.yml <<EOF
cluster: verify
bootstrap:
  config_source: etcd
  config_center:
    etcd_addrs: [127.0.0.1:12380]
  etcd_addrs:
    - 127.0.0.1:12379
EOF
cp "$BIN" $d/gocdcbin
(cd $d && WEB_PASSWORD=demo123 METRICS_ADDR=:9595 WEB_ADDR=:8060 timeout 30 ./gocdcbin > out.log 2>&1; echo $? > exit.log)
echo "  拒启 exit=$(cat $d/exit.log): $(grep -iE 'purged|缺口|拒|不启' $d/out.log | head -2)"
[ "$(cat $d/exit.log)" != "0" ] && grep -qiE 'purged|缺口|早于' $d/out.log && ok "C5a PURGED 拒启增量" || bad "C5a"
# 恢复：位点拨到现存最新文件头
$ETCDC put /gocdc/verify/sources/default/position "{\"v\":1,\"name\":\"$NEWFILE\",\"pos\":4,\"gtid_set\":\"$GTID\",\"seq\":1,\"leader_term\":1,\"node_id\":\"node1\"}" >/dev/null
stub_start node1 verify :9595 :8060
wait_ready 9595 60 || bad "C5b 恢复启动失败"
sleep 3
$SRC -e "INSERT INTO shop.orders (user_id,amount,remark) VALUES (555001,5.55,'c5-after-purge')" 2>/dev/null
B5OK=0
for i in $(seq 1 15); do
  sleep 2
  D=$($DST -N -e "SELECT COUNT(*) FROM shop.orders WHERE remark='c5-after-purge'" 2>/dev/null)
  [ "$D" = "1" ] && { B5OK=1; break; }
done
[ "$B5OK" = "1" ] && ok "C5b 恢复后增量流动" || bad "C5b dst=$D"

killall_gocdc
summary

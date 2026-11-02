# v2 验证公共库（source 引入，勿直接执行）
# 用户指令约束（本次全量重验的形态基线）：
#   1. 禁用全量配置文件模式：全量配置一律写入配置中心 etcd(cfg-etcd:12380)
#      的 /gocdc/{cluster}/config；gocdc 只允许两种启动形态——
#        A. 零参数 + 工作目录引导桩（config_source: etcd，stub_start）
#        B. 纯 flag：-cluster X -etcd 127.0.0.1:12379（同实例双角色，flag_start）
#   2. 基础环境 = WSL Ubuntu + Docker（vf-src/vf-dst/vf-etcd/cfg-etcd/rmqnamesrv/rmqbroker）
#   3. 每个 case 组开始前 wipe_all：杀进程 → 清协调面/配置中心 → 双端整库重建 → 播种
set -u
# 环境坑（2026-10-30 实证）：WSL 泄漏 http_proxy=127.0.0.1:7890，grpc-go 不识别
# NO_PROXY 的 127.* 通配 → RocketMQ v5 gRPC 全走系统代理 → context deadline exceeded。
# 本套件全链路无外网依赖，一律直连。
unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY all_proxy ALL_PROXY 2>/dev/null
export NO_PROXY="*"; export no_proxy="*"
# 全局互斥锁：严禁两个组脚本并发（2026-10-30 实证：并发实例的 E9 CREATE 落在
# 另一实例的 seed 窗口内，幽灵表导致节点启动核对拒启、全组连锁假失败）
mkdir -p /tmp/v2
exec 9>/tmp/v2/.lock
if ! flock -n 9; then
  echo "❌ 另一个 v2 验证脚本正在运行（/tmp/v2/.lock 被占），拒绝并发执行"
  ps -eo pid,etime,args | grep -E "v2/[0-9]" | grep -v grep
  exit 1
fi
# 仓库定位：默认相对本文件自定位（lib.sh 恒在 <repo>/.verify/v2/ 下），仓库挪动/
# 换机零配置；特殊布局可用环境变量 GOCDC_REPO 覆盖仓库根。
V2="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="${GOCDC_REPO:-$(cd "$V2/../.." && pwd)}"
V1="$REPO/.verify"
BIN="$V1/gocdc"
RMQCONS="$V1/rmqcons-bin"
export REPO V1 V2 BIN RMQCONS

SRC="docker exec vf-src mysql -uroot -p123456 -h127.0.0.1 --default-character-set=utf8mb4"
SRC2="docker exec vf-src2 mysql -uroot -p123456 -h127.0.0.1 --default-character-set=utf8mb4"
DST="docker exec vf-dst mysql -uroot -p123456 -h127.0.0.1 --default-character-set=utf8mb4"
DST2="docker exec vf-dst2 mysql -uroot -p123456 -h127.0.0.1 --default-character-set=utf8mb4"
CFGC="docker exec cfg-etcd etcdctl --endpoints=127.0.0.1:2379 --command-timeout=15s"
ETCDC="docker exec vf-etcd etcdctl --endpoints=127.0.0.1:2379 --command-timeout=15s"

PASS=0; FAIL=0; FAILED_CASES=""
ok(){  PASS=$((PASS+1)); echo "  ✅ $1 RESULT=PASS"; }
bad(){ FAIL=$((FAIL+1)); FAILED_CASES="$FAILED_CASES|$1"; echo "  ❌ $1 RESULT=FAIL"; }
grp(){ echo; echo "════════ $1 ════════"; }
summary(){ echo; echo "████ 组完成: PASS=$PASS FAIL=$FAIL $FAILED_CASES"; [ "$FAIL" = "0" ]; }

# ---- 进程管理 ----
# 只按 argv 第二段（可执行文件路径）匹配：/…/.verify/gocdc 或 ./gocdcbin（桩工作目录副本）。
# 严禁 grep '/gocdc' 宽匹配——会命中脚本自身路径（/gocdc-mysql/）导致自杀（2026-10-30 实证）。
killall_gocdc(){
  for p in $(ps -eo pid,args | awk '$2 ~ /\/gocdc$/ || $2 ~ /gocdcbin$/ {print $1}'); do
    kill -9 "$p" 2>/dev/null
  done
  sleep 2
  local n; n=$(ps -eo args | awk '$1 ~ /\/gocdc$/ || $1 ~ /gocdcbin$/' | wc -l)
  echo "  [kill] gocdc 残留=$n"
}
# 节点 pid（按 /proc/<pid>/cwd 引导桩工作目录定位）：pickpid node1
pickpid(){
  local p
  for p in $(ps -eo pid,args | awk '$2 ~ /gocdcbin$/ {print $1}'); do
    [ "$(readlink /proc/$p/cwd 2>/dev/null)" = "/tmp/v2/$1" ] && { echo "$p"; return; }
  done
}

# ---- 配置中心 ----
# ccput <cluster> <bodyfile>: 全量配置正文写入配置中心(12380)
ccput(){ $CFGC put "/gocdc/$1/config" "$(cat "$2")" >/dev/null && echo "  [cc] /gocdc/$1/config ← $(basename "$2")"; }
# flagput <cluster> <bodyfile>: 纯 flag 形态（配置中心=协调面同实例 vf-etcd）
flagput(){ $ETCDC put "/gocdc/$1/config" "$(cat "$2")" >/dev/null && echo "  [flag-cc] /gocdc/$1/config ← $(basename "$2")（vf-etcd 同实例双角色）"; }

# ---- 启动形态 A：零参数 + 工作目录引导桩 ----
# stub_start <name> <cluster> <metrics_addr> <web_addr> [extra_env_kv...]
stub_start(){
  local name=$1 cluster=$2 met=$3 web=$4; shift 4
  local d=/tmp/v2/$name
  rm -rf "$d"; mkdir -p "$d"
  cat > "$d/config.local.yml" <<EOF
cluster: $cluster
bootstrap:
  config_source: etcd
  config_center:
    etcd_addrs: [127.0.0.1:12380]
  etcd_addrs:
    - 127.0.0.1:12379
EOF
  cp "$BIN" "$d/gocdcbin"
  # 9>&-：节点不继承 flock fd（demo 长跑实证：节点活多久锁被占多久，
  # 后续组脚本全被「锁被占」拒启；组间互斥语义不变——孤儿节点由下组 killall 兜底）
  (cd "$d" && env WEB_PASSWORD=demo123 METRICS_ADDR=$met WEB_ADDR=$web "$@" \
     setsid nohup ./gocdcbin 9>&- > /tmp/v2/$name.log 2>&1 &)
  echo "  [start-A] $name cluster=$cluster metrics=$met web=$web（零参数引导桩）"
}
# ---- 启动形态 B：纯 flag（-cluster + -etcd，同实例双角色）----
# flag_start <name> <cluster> <metrics_addr> <web_addr> [extra_env_kv...]
flag_start(){
  local name=$1 cluster=$2 met=$3 web=$4; shift 4
  local d=/tmp/v2/$name
  rm -rf "$d"; mkdir -p "$d"
  cp "$BIN" "$d/gocdcbin"
  (cd "$d" && env WEB_PASSWORD=demo123 METRICS_ADDR=$met WEB_ADDR=$web "$@" \
     setsid nohup ./gocdcbin -cluster "$cluster" -etcd 127.0.0.1:12379 9>&- > /tmp/v2/$name.log 2>&1 &)
  echo "  [start-B] $name cluster=$cluster（纯 flag，配置中心=协调面 12379）"
}

# ---- 等待/查询 ----
wait_ready(){ # wait_ready <web_port> <timeout_s>
  local i=0
  while [ $i -lt "${2:-30}" ]; do
    [ "$(curl -s --max-time 3 --connect-timeout 2 --noproxy '*' localhost:$1/healthz 2>/dev/null)" = "ok" ] && return 0
    i=$((i+1)); sleep 1
  done
  return 1
}
jget(){ python3 -c "import sys,json;d=json.load(sys.stdin)['data'];print($1)" 2>/dev/null; }
roleof(){ curl -s --max-time 3 --connect-timeout 2 --noproxy '*' -u admin:demo123 "localhost:$1/api/v1/status" | jget 'd["role"]' 2>/dev/null; echo; }
leader_port(){ local p r; for p in 8060 8061; do r=$(roleof $p); [ "$r" = "leader" ] && { echo $p; return; }; done; echo ""; }
dumpconns(){ $SRC -N -e "SELECT COUNT(*) FROM information_schema.processlist WHERE command LIKE 'Binlog Dump%'" 2>/dev/null; }
lagof(){ curl -s --max-time 3 --connect-timeout 2 --noproxy '*' localhost:$1/metrics | grep '^gocdc_master_position_lag_bytes' | awk '{print $2}'; }

# ---- 环境恢复 ----
coord_reset(){ $ETCDC del --prefix "/gocdc/$1/" >/dev/null; echo "  [coord] /gocdc/$1/ 前缀已清"; }
# wipe_data: 双端整库 DROP 重建（DDL 级环境恢复，防跨组 schema 漂移）+ 播种
# 错误必须可见并中止（2026-10-30 实证：静默吞错导致整组跑在上一轮脏数据上，
# node2 启动核对撞到上轮 case 表直接拒启，全组连锁假失败）
wipe_data(){
  local err
  $SRC -e "DROP DATABASE IF EXISTS shop; DROP DATABASE IF EXISTS biz2; DROP DATABASE IF EXISTS types;
DROP DATABASE IF EXISTS rmqtest; DROP DATABASE IF EXISTS cluster_e2e; DROP DATABASE IF EXISTS kafka12; DROP DATABASE IF EXISTS \`edu.mall.admin\`" 2>&1 | grep -v "password on the command" && err=1
  $DST -e "DROP DATABASE IF EXISTS shop; DROP DATABASE IF EXISTS biz2; DROP DATABASE IF EXISTS types;
DROP DATABASE IF EXISTS rmqtest; DROP DATABASE IF EXISTS cluster_e2e; DROP DATABASE IF EXISTS \`edu.mall.merge\`" 2>&1 | grep -v "password on the command" && err=1
  docker exec -i vf-src mysql -uroot -p123456 -h127.0.0.1 --default-character-set=utf8mb4 < "$V1/init-src.sql" 2>&1 | grep -v "password on the command" && err=1
  docker exec -i vf-dst mysql -uroot -p123456 -h127.0.0.1 --default-character-set=utf8mb4 < "$V1/init-dst.sql" 2>&1 | grep -v "password on the command" && err=1
  # 基线补充表（双端）：new_table（D 系探测基线）/ rmqtest.msg / cluster_e2e.tab / 带点库
  # order（保留字表，ADV-03）/ adv13t（ADV-13 回放窗口）/ dying_table（E8b P0-8 过期放行）
  # ——三者均列入 body_main 白名单（组形态=白名单制：启动核对静态要求双端预建）
  local BASE="CREATE TABLE IF NOT EXISTS shop.new_table (id BIGINT NOT NULL AUTO_INCREMENT, val VARCHAR(64) NULL, PRIMARY KEY (id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE IF NOT EXISTS shop.\`order\` (id BIGINT NOT NULL AUTO_INCREMENT, \`key\` INT NULL, \`select\` VARCHAR(32) NULL, PRIMARY KEY (id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE IF NOT EXISTS shop.adv13t (id BIGINT NOT NULL AUTO_INCREMENT, v INT NULL, PRIMARY KEY (id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE IF NOT EXISTS shop.dying_table (id BIGINT NOT NULL AUTO_INCREMENT, v INT NULL, PRIMARY KEY (id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE DATABASE IF NOT EXISTS rmqtest;
CREATE TABLE IF NOT EXISTS rmqtest.msg (id BIGINT NOT NULL AUTO_INCREMENT, name VARCHAR(64) NOT NULL, qty INT NOT NULL DEFAULT 0, amount DECIMAL(12,2) NOT NULL DEFAULT 0, note VARCHAR(255) NULL, PRIMARY KEY (id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE DATABASE IF NOT EXISTS cluster_e2e;
CREATE TABLE IF NOT EXISTS cluster_e2e.tab (id BIGINT NOT NULL AUTO_INCREMENT, v VARCHAR(64) NULL, PRIMARY KEY (id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"
  $SRC -e "$BASE" 2>&1 | grep -v "password on the command" && err=1; $DST -e "$BASE" 2>&1 | grep -v "password on the command" && err=1
  $SRC -e "CREATE DATABASE IF NOT EXISTS \`edu.mall.admin\`;
CREATE TABLE IF NOT EXISTS \`edu.mall.admin\`.users (id BIGINT NOT NULL AUTO_INCREMENT, name VARCHAR(64) NOT NULL, phone VARCHAR(20) NULL, PRIMARY KEY (id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4" 2>&1 | grep -v "password on the command" && err=1
  $DST -e "CREATE DATABASE IF NOT EXISTS \`edu.mall.merge\`;
CREATE TABLE IF NOT EXISTS \`edu.mall.merge\`.users (id BIGINT NOT NULL AUTO_INCREMENT, name VARCHAR(64) NOT NULL, phone VARCHAR(20) NULL, PRIMARY KEY (id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4" 2>&1 | grep -v "password on the command" && err=1
  # 播种
  docker exec -i vf-src mysql -uroot -p123456 -h127.0.0.1 --default-character-set=utf8mb4 < "$V1/seed.sql" 2>&1 | grep -v "password on the command" && err=1
  $SRC -e "INSERT INTO \`edu.mall.admin\`.users (name,phone) VALUES ('dotdb-a','13900000001'),('dotdb-b','13900000002'),('dotdb-c',NULL)" 2>&1 | grep -v "password on the command" && err=1
  rm -f /tmp/rmq-*.jsonl /tmp/rmqcons-*.log
  if [ -n "${err:-}" ]; then
    echo "  [data] ❌ wipe_data 有 SQL 错误（见上行），环境未干净！"
    return 1
  fi
  # 收敛自检：行数不符立即暴露
  local n=$($SRC -N -e 'SELECT COUNT(*) FROM shop.orders' 2>/dev/null)
  [ "$n" = "100" ] || { echo "  [data] ❌ 播种自检失败 orders=$n（期待100）"; return 1; }
  # 交接自证：打印 shop 表清单（幽灵表防护——曾出现 wipe 后数秒 e9_table 复现致
  # node2 启动核对拒启，此清单用于对质 wipe 交接时刻的真实状态）
  local tl; tl=$($SRC -N -e 'SELECT GROUP_CONCAT(table_name) FROM information_schema.tables WHERE table_schema="shop"' 2>/dev/null)
  echo "  [data] 双端整库重建+播种完成（自检 orders=$n，shop: $tl）"
}
# wipe_all: 组级全清（进程/协调面/配置中心/数据）；数据清理失败直接终止脚本
# 并发防护唯一依赖 lib 顶部的 flock（ps 扫描天生不可靠：$() 子 shell 与父进程
# 命令行相同必然自计数+1，2026-10-30 实证）
wipe_all(){
  killall_gocdc
  $ETCDC del --prefix /gocdc/ >/dev/null
  $CFGC del --prefix /gocdc/ >/dev/null
  echo "  [wipe] 协调面+配置中心 /gocdc/ 全清"
  wipe_data || exit 1
}

# ---- 多源组（MS 系列）公共设施 ----
# ms_wipe: 双源+双目标整库重建。src-a=vf-src(13306) 的 msa 库、src-b=vf-src2(13406)
# 的 msb 库；聚合形态两源同写 vf-dst(13307) 的 ms_merged，分库形态各写各的
#（ms_merged_a→vf-dst、ms_merged_b→vf-dst2，body_ms_split.yml）。
ms_wipe(){
  local err
  $SRC -e "DROP DATABASE IF EXISTS msa; CREATE DATABASE msa CHARACTER SET utf8mb4;
CREATE TABLE msa.orders (id BIGINT NOT NULL AUTO_INCREMENT, val VARCHAR(64) NULL, amt DECIMAL(10,2) NOT NULL DEFAULT 0, PRIMARY KEY (id)) ENGINE=InnoDB;
CREATE TABLE msa.evlog (id BIGINT NOT NULL AUTO_INCREMENT, v INT NULL, PRIMARY KEY (id)) ENGINE=InnoDB;
INSERT INTO msa.orders (val,amt) SELECT CONCAT('a-seed-',n), n*1.5 FROM (WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM s WHERE n<50) SELECT n FROM s) t;
INSERT INTO msa.evlog (v) VALUES (1),(2),(3);" 2>&1 | grep -v "password on the command" && err=1
  $SRC2 -e "DROP DATABASE IF EXISTS msb; CREATE DATABASE msb CHARACTER SET utf8mb4;
CREATE TABLE msb.users (id BIGINT NOT NULL AUTO_INCREMENT, name VARCHAR(64) NULL, phone VARCHAR(20) NULL, PRIMARY KEY (id)) ENGINE=InnoDB;
CREATE TABLE msb.evlog (id BIGINT NOT NULL AUTO_INCREMENT, v INT NULL, PRIMARY KEY (id)) ENGINE=InnoDB;
CREATE TABLE msb.tlog (id BIGINT NOT NULL AUTO_INCREMENT, v INT NULL, PRIMARY KEY (id)) ENGINE=InnoDB;
INSERT INTO msb.users (name,phone) SELECT CONCAT('b-seed-',n), CONCAT('1390000',LPAD(n,4,'0')) FROM (WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM s WHERE n<60) SELECT n FROM s) t;
INSERT INTO msb.tlog (v) VALUES (10),(20),(30);" 2>&1 | grep -v "password on the command" && err=1
  for pair in "vf-dst:ms_merged" "vf-dst:ms_merged_a" "vf-dst2:ms_merged_b"; do
    local c=${pair%%:*} db=${pair##*:}
    docker exec "$c" mysql -uroot -p123456 -h127.0.0.1 -e \
      "DROP DATABASE IF EXISTS \`$db\`; CREATE DATABASE \`$db\` CHARACTER SET utf8mb4" 2>&1 | grep -v "password on the command" && err=1
  done
  # 聚合/分库两形态的目标表结构（同名同列约束：启动核对 fail-fast 要求预建；
  # 白名单内全部表都要建——evlog 虽 do_stock=false 也在核对清单）
  docker exec vf-dst mysql -uroot -p123456 -h127.0.0.1 -e "
CREATE TABLE IF NOT EXISTS ms_merged.orders (id BIGINT NOT NULL AUTO_INCREMENT, val VARCHAR(64) NULL, amt DECIMAL(10,2) NOT NULL DEFAULT 0, PRIMARY KEY (id)) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS ms_merged.users (id BIGINT NOT NULL AUTO_INCREMENT, name VARCHAR(64) NULL, phone VARCHAR(20) NULL, PRIMARY KEY (id)) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS ms_merged.evlog (id BIGINT NOT NULL AUTO_INCREMENT, v INT NULL, PRIMARY KEY (id)) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS ms_merged.tlog (id BIGINT NOT NULL AUTO_INCREMENT, v INT NULL, PRIMARY KEY (id)) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS ms_merged_a.orders (id BIGINT NOT NULL AUTO_INCREMENT, val VARCHAR(64) NULL, amt DECIMAL(10,2) NOT NULL DEFAULT 0, PRIMARY KEY (id)) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS ms_merged_a.users (id BIGINT NOT NULL AUTO_INCREMENT, name VARCHAR(64) NULL, phone VARCHAR(20) NULL, PRIMARY KEY (id)) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS ms_merged_a.evlog (id BIGINT NOT NULL AUTO_INCREMENT, v INT NULL, PRIMARY KEY (id)) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS ms_merged_a.tlog (id BIGINT NOT NULL AUTO_INCREMENT, v INT NULL, PRIMARY KEY (id)) ENGINE=InnoDB;" 2>&1 | grep -v "password on the command" && err=1
  docker exec vf-dst2 mysql -uroot -p123456 -h127.0.0.1 -e "
CREATE TABLE IF NOT EXISTS ms_merged_b.orders (id BIGINT NOT NULL AUTO_INCREMENT, val VARCHAR(64) NULL, amt DECIMAL(10,2) NOT NULL DEFAULT 0, PRIMARY KEY (id)) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS ms_merged_b.users (id BIGINT NOT NULL AUTO_INCREMENT, name VARCHAR(64) NULL, phone VARCHAR(20) NULL, PRIMARY KEY (id)) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS ms_merged_b.evlog (id BIGINT NOT NULL AUTO_INCREMENT, v INT NULL, PRIMARY KEY (id)) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS ms_merged_b.tlog (id BIGINT NOT NULL AUTO_INCREMENT, v INT NULL, PRIMARY KEY (id)) ENGINE=InnoDB;" 2>&1 | grep -v "password on the command" && err=1
  if [ -n "${err:-}" ]; then
    echo "  [ms-wipe] ❌ ms_wipe 有 SQL 错误（见上行）"
    return 1
  fi
  rm -f /tmp/rmq-msa.jsonl /tmp/rmq-msb.jsonl
  echo "  [ms-wipe] 双源(msa@13306/msb@13406)+双目标(ms_merged@13307/ms_merged_b@13407) 重建完成"
}

# src_state <web_port> <sid>：/status 里某源 pipeline_state（多源视图数据面）
src_state(){
  curl -s --max-time 3 --connect-timeout 2 --noproxy '*' -u admin:demo123 \
    "localhost:$1/api/v1/status" | jget "next((s['pipeline_state'] for s in d['sources'] if s['id']=='$2'), 'MISSING')" 2>/dev/null
  echo
}
# src_phase <web_port> <sid>：/status 里某源装配 phase（starting/ready/failed）
src_phase(){
  curl -s --max-time 3 --connect-timeout 2 --noproxy '*' -u admin:demo123 \
    "localhost:$1/api/v1/status" | jget "next((s['phase'] for s in d['sources'] if s['id']=='$2'), 'MISSING')" 2>/dev/null
  echo
}
# wait_src_state <web_port> <sid> <want> [timeout_s]：等某源到位（任一 phase 可前缀匹配 state）
wait_src_state(){
  local port=$1 sid=$2 want=$3 i=0
  while [ $i -lt "${4:-60}" ]; do
    [ "$(src_state "$port" "$sid")" = "$want" ] && return 0
    i=$((i+1)); sleep 1
  done
  return 1
}
# wait_row <dst_cmd> <sql> <want> [timeout_s]：轮询目标库标量值到位（PLAYBOOK §5
# 套路——MySQL 落地断言禁止裸 sleep 后一次判）
wait_row(){
  local dcmd=$1 sql=$2 want=$3 i=0 got=""
  while [ $i -lt "${4:-20}" ]; do
    got=$($dcmd -N -e "$sql" 2>/dev/null)
    [ "$got" = "$want" ] && return 0
    i=$((i+1)); sleep 1
  done
  echo "  [wait_row] 超时: got=$got want=$want sql=$sql"
  return 1
}
# wait_log <file> <pattern> [timeout_s]：轮询日志出现模式（源装配失败类断言——
# 装配与 healthz 并发，healthz 就绪 ≠ 装配已出结果）
wait_log(){
  local file=$1 pat=$2 i=0
  while [ $i -lt "${3:-30}" ]; do
    grep -q "$pat" "$file" 2>/dev/null && return 0
    i=$((i+1)); sleep 1
  done
  return 1
}
# wait_phase <web_port> <sid> <want> [timeout_s]：轮询某源装配 phase
wait_phase(){
  local port=$1 sid=$2 want=$3 i=0
  while [ $i -lt "${4:-30}" ]; do
    [ "$(src_phase "$port" "$sid" | tr -d '[:space:]')" = "$want" ] && return 0
    i=$((i+1)); sleep 1
  done
  return 1
}

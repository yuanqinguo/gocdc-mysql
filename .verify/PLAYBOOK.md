# gocdc 全量验证手册（PLAYBOOK）

> 用途：**可复用的完整验证流程**。改版/发布前全量跑一遍；也可只跑单个 case 组。
> 本轮沉淀：2026-10-31 全量重验（etcd 配置中心形态专项，11 组 94 断言全绿），
> 报告见 `docs/VERIFICATION-2026-10-31.md`，可执行资产在 `.verify/v2/`。
> 同日下午 r2 复验（零产品缺陷，上轮两缺陷回归确认不复现；L1 全程 -count=1）：
> 报告 `docs/VERIFICATION-2026-10-31-r2.md`，新增坑 12 与 run_group/archive_logs 脚本。
> 注意：`.verify/` 根目录的其余 `*.sh/*.sql/*.py` 是历史资产（10-29/10-30 轮），
> **当前有效套件是 `.verify/v2/`**；根目录的 `seed.sql / init-src.sql / init-dst.sql /
> workload1.sql / b7b10.sql / c2*_bigtx.sql / rmq_incr.sql / rmq_compare.py /
> rmqcons-bin` 仍被 v2 套件引用，勿删。
> `v2/debug/` 是本轮问题定位的活体检查脚本存档（27 个）：再遇「DDL 静默丢弃 /
> 流量不通 / RMQ 连不上 / 幽灵表」类问题时，先翻这里的 dbg_* 手法再写新的。

## 9. v2/ 套件清单

| 文件 | 用途 |
|------|------|
| `lib.sh` | 公共库：代理 unset + flock 互斥 + 精确进程管理 + 两种启动形态 + 配置中心 put + wipe/自检 + 断言计数（**改这里=改所有组**） |
| `00_reset.sh` | 环境从零重建（容器/卷全删 → 6 容器 → 编译 → 播种 → FIFO topic → 自检） |
| `restart_env.sh` | 容器全停后（隔夜 WSL 重启）的最小恢复：docker start + MySQL 就绪 + topic 路由等待 |
| `10_boot.sh` | A 系列：flag 契约/坏配置清单/${ENV}/binlog_format/缺列/多余列/三端点/下游不可达/打码 |
| `20_data.sh` | B 系列：stock/全类型/主键/联合主键/大事务/verify 注入/mask + F4/F5/H1/H2/H4 |
| `30_pos.sh` | C 系列：/position 实时/kill -9 大事务/回拨/GTID 一致/PURGED |
| `35_ms_core.sh` | MS 系列（多源核心，2026-11-02 新增）：双源端到端一致性/并发首启全量/跨源 DDL 冻结半径/逐源 pause-resume-回拨-restock/MQ 双源 topic+envelope source/配置校验脚本分支/API 兼容层/审计 source/指标 label |
| `40_ha.sh` | D1-D5：竞选/follower 三态/kill -9 接管/回归降级/resign（双节点共享正文） |
| `45_ha_outage.sh` | E9 follower 审批 + I5 单产出 + ADV-07 跨会话重扫 + D6 etcd 宕机（双节点） |
| `46_ms_ha.sh` | MS 系列（多源故障面，2026-11-02 新增）：全量中断续扫/源库宕机隔离与自愈/背压隔离（分目标库）/v1 升级+etcd 键迁移幂等/旧 leader 位点写拒绝 |
| `50_ddl.sh` | E 系列：auto 加列/扩容/pause 冻结/skip 补齐/approve 全自动/exclude 忽略/CREATE 接入/过期放行 |
| `55_op_pause.sh` | PP 系列（13 §3 人工暂停/恢复，2026-11-01 新增）：pause/resume confirm 门禁/受理态（paused+paused_by_op+保持 Leader）/重复拒绝/暂停期增量冻结/恢复续跑收敛/暂停期改位点（SET-POSITION 沿用 gtid_set 回归）/标志跨重启/审计留痕 |
| `60_api.sh` | G 系列：认证/confirm 门禁/SET-POSITION/审计/verify 契约/404 + H3 |
| `65_mq.sh` | I 系列：restock 快照双写/信封/三方对账/FIFO 保序/broker 宕机补投/崩溃重放 |
| `70_adv.sh` | ADV 对抗：无主键双层/UK/保留字/零日期/60KB/漂移/重复 skip/TRUNCATE/auth 裸奔/时间机器/restock 不重启 |
| `80_exceptions.sh` | 异常注入：目标库宕机/源库宕机/HA-10 严格版（双权限全摘） |
| `90_hiprio.sh` | 配置中心专项：桩+双 etcd 分离/带点库名全链/ha 单节点（纯 flag）/多集群共存/桩错误路径 |
| `tally.sh` | 汇总各组 `/tmp/v2-gNN.out` 的 PASS/FAIL |
| `run_group.sh` | 单组执行器：`run_group.sh 10_boot` → 跑组并落 `/tmp/v2-gNN.out`（Git Bash 侧免内联长命令） |
| `archive_logs.sh` | 组结果留档到 `.verify/v2/logs/`（/tmp 隔夜即失） |
| `chk_reset.sh` | 00_reset 后补充自检（带点库播种行数/FIFO topic 实存——reset 第 8 步 dotdb= 空为显示伪影） |
| `reseed.sh` | 仅重建数据不重建容器（调试用） |
| `debug/` | 本轮问题定位脚本存档（活体检查手法库；r2 新增 dbg_lag_race.sh=lag gauge 采样竞态复现手法） |

---

## 0. 硬性形态约束（每轮验证开始前确认）

1. **全量配置一律存配置中心 etcd**（cfg-etcd:12380）的 `/gocdc/{cluster}/config`，
   **任何 case 不得用全量配置文件模式**（`-config-file` 只允许指向引导桩）。
2. 节点只允许两种启动形态（lib.sh 已封装）：
   - **形态 A** `stub_start`：零参数 + 工作目录引导桩 `config.local.yml`
     （内容仅 `cluster` + `bootstrap.config_source: etcd` + 两个 etcd 地址）；
   - **形态 B** `flag_start`：纯 flag `-cluster X -etcd 127.0.0.1:12379`
     （配置中心=协调面同实例双角色，正文用 `flagput` 写到 12379）。
3. 基础环境 = **WSL Ubuntu + Docker**，全部命令经脚本文件执行（见 §6 坑 1）。
4. 每个 case 组开始前 `wipe_all`（杀进程 → 清两套 etcd 的 /gocdc/ 前缀 → 双端整库
   DROP 重建 → 播种），保证组间互不污染。

---

## 1. 前置条件

| 项 | 要求 |
|----|------|
| WSL | `wsl -d <发行版实名>`（`wsl -l -v` 查询，本机为 Ubuntu-22.04），内部 Docker 可用，`/usr/local/go/bin/go`（1.24.10） |
| Windows go | `D:/gopath/go1.24.10/bin/go.exe`（L1 Windows 侧用） |
| 镜像（本地须有，勿在线拉） | `mysql:8.0`、`apache/rocketmq:5.3.1`、`docker.cnb.cool/qingflyfei/edu.image.base:etcd-3.5.16` |
| 二进制 | `.verify/gocdc`（Linux），由 00_reset.sh 第 4 步从当前 HEAD 重编 |
| MQ 消费端 | `.verify/rmqcons-bin`（Go v5 SimpleConsumer，编译产物已入库） |

## 2. 环境拓扑（00_reset.sh 全自动重建）

| 容器 | 端口(宿主) | 角色 |
|------|-----------|------|
| vf-src | 13306 | 源库 mysql:8.0，`ROW+FULL+GTID ON`，`cte_max_recursion_depth=500000`，repl 用户（SELECT/REPLICATION/RELOAD/BACKUP_ADMIN） |
| vf-src2 | 13406 | 多源组第二源库（server_id=201，MS 系列双源的第二条 binlog 流） |
| vf-dst | 13307 | 目标库 mysql:8.0 |
| vf-dst2 | 13407 | 多源组第二目标库（MS-10 背压隔离：分目标库形态） |
| vf-etcd | 12379 | **协调面**（位点/选举/DDL 门/审计） |
| cfg-etcd | 12380 | **配置中心**（`/gocdc/{cluster}/config`，与协调面分离实例） |
| rmqnamesrv | namesrv:9876（容器网内） | RocketMQ nameserver |
| rmqbroker | 8081 | broker + proxy（gRPC 8081 映射宿主） |

Docker 网络 `gocdc-verify`；FIFO topic 预建：`CDC_rmqtest_msg`、`CDC_edu_mall_admin_users`、
`CDC_msa_msa_orders`、`CDC_msb_msb_users`（`mqadmin updateTopic -a +message.type=FIFO`，
v5 客户端只拉路由不建 topic）。

配置中心正文（`.verify/v2/body_*.yml`，节点共享同一份，端口走 `${ENV}`）：
- `body_main.yml`：cluster=verify，shop/types/biz2/rmqtest/cluster_e2e 五规则，
  `${METRICS_ADDR}/${WEB_ADDR}/${WEB_PASSWORD}` 区分多副本端口；
- `body_hiprio.yml`：带点库名 `edu.mall.admin.users` → `target_database: edu.mall.merge` + RMQ 双写；
- `body_single.yml`：`ha.enabled: false`，cluster=verify-single；
- `body_ms.yml`：多源双源聚合形态（src-a@13306 + src-b@13406 → ms_merged@13307，v2 sources[]）；
- `body_ms_split.yml`：多源分目标库形态（src-a→vf-dst、src-b→vf-dst2，MS-10 背压隔离用）。

## 3. 标准执行流程（全量）

```bash
# v2 脚本全部相对自身定位（lib.sh 推导仓库根；特殊布局可 export GOCDC_REPO 覆盖），
# 只有外层 wsl 调用需要真实路径；发行版实名按 wsl -l -v（本机 Ubuntu-22.04）。
REPO=/mnt/d/course/gocdc-mysql

# ① 环境从零重建（容器/卷全删重建 + 编译二进制 + 建库播种 + topic + 自检）
MSYS_NO_PATHCONV=1 wsl -d Ubuntu-22.04 -- bash $REPO/.verify/v2/00_reset.sh

# ② L1：Windows 侧（build/vet/test，go 用 PATH 上的）
go build ./... && go vet ./... && go test ./...

# ③ L1：WSL -race（强制 -count=1 禁缓存）
MSYS_NO_PATHCONV=1 wsl -d Ubuntu-22.04 -- bash $REPO/.verify/v2/l1_wsl_race.sh

# ④ L2：集成（testcontainers，分钟级；镜像默认本地已有 mysql:8.0 / etcd-3.5.16）
MSYS_NO_PATHCONV=1 wsl -d Ubuntu-22.04 -- bash $REPO/.verify/v2/l2_integration.sh

# ⑤ L3：14 组按序跑（每组内部先 wipe_all；run_group.sh 落 /tmp/v2-gNN.out）
for g in 10_boot 20_data 30_pos 35_ms_core 40_ha 45_ha_outage 46_ms_ha 50_ddl 55_op_pause 60_api 65_mq 70_adv 80_exceptions 90_hiprio; do
  MSYS_NO_PATHCONV=1 wsl -d Ubuntu-22.04 -- bash $REPO/.verify/v2/run_group.sh $g
done
```

组顺序依赖：40/45（集群）在 50（DDL）前后均可但**必须**各自 wipe 起步；65（MQ）的
topic 重置在组内完成；90（hiprio）放最后（会建 verify-single 第二集群）。
单组复跑：直接跑该组脚本即可，组内 wipe_all 保证环境自恢复。

## 4. lib.sh API 速查（写新 case 组必读）

| 函数 | 用途 |
|------|------|
| `wipe_all` | 组级全清（含 flock 互斥；wipe 失败直接 exit 1——**绝不带脏环境跑 case**） |
| `ccput <cluster> <bodyfile>` | 配置正文 → 配置中心(12380)（形态 A 用） |
| `flagput <cluster> <bodyfile>` | 配置正文 → 协调面(12379)（形态 B 用——**两种 put 不可混**） |
| `stub_start <name> <cluster> <metrics> <web> [ENV k=v...]` | 形态 A 启动；二进制副本 + 桩落在 `/tmp/v2/<name>/`，日志 `/tmp/v2/<name>.log` |
| `flag_start <...>` | 形态 B 启动（`-cluster -etcd 12379`） |
| `pickpid <name>` | 按 `/proc/<pid>/cwd=/tmp/v2/<name>` 定位节点 pid |
| `killall_gocdc` | 精确杀全部节点（argv 第二段匹配 `/gocdc$|gocdcbin$`，**严禁宽匹配**） |
| `wait_ready <metrics_port> <timeout>` | 等 `/healthz`（**metrics 端口无鉴权**；web 端口 8060 的 healthz 要 Basic 认证） |
| `roleof <web_port>` / `leader_port` / `dumpconns` / `lagof <metrics_port>` | 角色/单 Dump/追平口径 |
| `coord_reset <cluster>` / `wipe_data` | 单独清前缀 / 单独重建数据 |
| `ok <name>` / `bad <name>` / `grp` / `summary` | 断言计数（结果行约定 `RESULT=PASS/FAIL`） |

## 5. 写 case 的固定套路（稳定性关键）

1. **等待一律轮询**，禁止裸 sleep 后断言：
   - 节点就绪：`wait_ready 9595 60`；
   - stock 完成：**轮询 dst 行数**（stock 期间 lag 恒 0，不能拿 lag 判 stock 完成）；
   - 增量追平：`lagof` 轮询 `=0`；MySQL 落地类断言再追一层「行数出现」轮询（重启后
     队列消化有秒级延迟）；
   - DDL 挂起：`pending_id` 内置 8 次重试（API/事件到达抖动）。
2. **DDL pause 的清理**：skip 时必须排除刚 approve/skip 过的 id（Settled 短路会返回
   200 但真正的新事件永远 pending → 水位冻结整组后续增量）。
3. **case 结束把环境还原**：临时表双端 DROP（src 的 DROP 会触发 pause → skip 兜底）、
   权限/索引复原、组尾 `pending_id` 全 skip + `killall_gocdc`。
4. **建表类 case 的窗口期不要重启副本**：库级规则按 information_schema 展开当前表，
   窗口内入簇的副本会因「目标未预建临时表」拒启（§9 注 6 的正确表现）。
   需要双节点的组，第二个节点在基线就绪后再入簇（见 90 组）。
5. 每个断言输出 `❌/✅ <name> RESULT=PASS/FAIL` + 关键证据值，便于事后审计与 tally。

## 6. 坑与处置（本轮实战全部踩过，勿再踩）

| # | 坑 | 处置（已固化在 lib.sh/脚本） |
|---|----|------------------------------|
| 1 | Git Bash 把 `/mnt/...` 转成 Windows 路径 | 一律 `MSYS_NO_PATHCONV=1 wsl ...`；**复杂命令一律落脚本文件**再执行，禁止内联长命令 |
| 2 | `grep '/gocdc'` 杀进程会命中脚本自身路径（`/gocdc-mysql/`）**自杀** | argv 第二段精确匹配 `/gocdc$|gocdcbin$`；节点区分用 `/proc/<pid>/cwd` |
| 3 | WSL 泄漏 `http_proxy=127.0.0.1:7890`，grpc-go 不识别 NO_PROXY 的 `127.*` 通配 → RMQ v5 全走系统代理 → `context deadline exceeded` | lib.sh 顶部统一 unset 全部代理变量 + `NO_PROXY="*"` |
| 4 | wipe 吞错（`2>/dev/null`）→ 整组跑在上一轮脏数据上连环假失败（node2 启动核对撞上一轮的 case 表拒启） | wipe_data 响亮化：错误收集 + `orders=100` 自检 + shop 表清单交接自证；失败 exit 1 |
| 5 | 两个验证实例并发：A 的 case 表落进 B 的 seed 窗口 = 幽灵表连锁假失败 | lib.sh 顶部 `flock /tmp/v2/.lock`；ps 扫描防并发不可靠（`$()` 子 shell 与父同命令行自计数） |
| 6 | RMQ **新消费组从 EARLIEST 重放**（与 rmqcons 注释相反）→ 跨 run 积压污染计数/对账 | topic delete+recreate **必须在任何节点启动之前**；运行中 producer 路由缓存被 recreate 打失效 → RMQ 发送失败 → 管线 PAUSED → MySQL 增量连带冻结（两害取其时序） |
| 7 | broker 重启后 **proxy 路由缓存刷新是分钟级**：mqadmin（namesrv 9876）已就绪 ≠ v5 gRPC 客户端（proxy 8081）可用 | 消费端按 `TOPIC_NOT_FOUND` 重试（rmqcons 启动即退出语义），不按 namesrv 路由判断就绪 |
| 8 | DDL pause 清理 skip 到刚结清的旧 ID → Settled 短路 200，真新事件永远 pending → 水位冻结 | skip 时按 id 排除已决策事件（见 45/70 组写法） |
| 9 | WSL/Docker 隔夜重启：容器全停 + `/tmp` 清空（结果文件/桩目录全丢） | 跑 `v2/restart_env.sh`（docker start + MySQL 就绪 + topic 路由等待）；loud wipe 会拒绝在脏环境跑 case（正确行为） |
| 10 | 前台拒启类断言：节点 fatal 后 `wait_ready` 会误判 | 前台运行捕获 `exit.log`/`out.log`，断言 exit≠0 + 错误关键字 |
| 11 | bash 的 `$()` 子 shell 与父进程命令行相同 | 一切基于 ps 的自检都会自计数 +1，并发判断只信 flock |
| 12 | `master_position_lag_bytes` 是 **SaveInterval（默认 3s）周期采样**的 gauge：跨写入边界首探可读到上一周期陈旧 0 → 假「追平」→ 行数断言抢跑（2026-10-31 r2 组20 B2-B5 实证：types=5 假 FAIL 秒级自愈；3000 行实测 417ms 落地、正常链路无假 0） | 「追平→查行数」的 MySQL 落地断言必须跟**行数收敛轮询**（§5.1 第二层，期望值不变）；新写 case 组时 B 类行数断言逐条过一遍 |

## 7. 结果判读与报告

- 每组末尾 `summary` 打印 `████ 组完成: PASS=n FAIL=m |失败项`；退出码 FAIL>0 时为 1。
- 结果文件建议落 `/tmp/v2-gNN.out`（**注意 /tmp 隔夜即失**，需要留档就拷到 .verify/v2/logs/）。
- 全量报告写 `docs/VERIFICATION-YYYY-MM-DD.md`，固定结构：结论 → 产品缺陷与修复
  （含回归）→ 形态落实表 → 场景矩阵 → 基建教训 → §9 语义增补建议 → 覆盖声明。
- 修复产品代码后：**L1 双侧 + L2 必须重跑**（本轮 stock/ddlparse 修复均如此收口）。

## 8. 已知语义速查（验收时勿当 bug，详见 docs/TESTCASES §9）

verify 不感知 mask 列；复合主键 verify 需显式 `pk`；回拨必须停机后做；白名单外 DDL
approve=复验不自动执行；RENAME exclude 表新名不再命中 exclude（重启被核对拦）；
RMQ topic 须预建 FIFO；`event_lag_seconds` 活动口径、追平看 `master_position_lag_bytes=0`；
restock 只清标记须重启/接管触发；GTID 模式位点 file:pos 停 rotate 锚点=正常；
HA-10 严格验证须 RELOAD+BACKUP_ADMIN 双摘；auth_disabled=true 放行但 confirm 门禁独立。

# gocdc 实施进度（历史日志）

> 项目已交付完成，本文件保留为实施过程的历史记录。文中对 14-17 号设计文档的引用按写作时点原样保留（这些文档已移除）。

## 当前阶段：交付完成（M0-M7；T6.3 前端受阻、T7.4 专项 crash 注入挂后续）

## 依赖版本回填（17 §2 表）

| 依赖 | 实际版本 |
|------|---------|
| go-mysql-org/go-mysql | **v1.14.0**（2026-10-28；v1.15+ 需 Go≥1.25，见 16 §6） |
| go.etcd.io/etcd/client/v3 | **v3.6.8**（api/v3、client/pkg/v3 同步 v3.6.8；2026-10-28，见 16 §6 决策） |
| testcontainers-go（+etcd/mysql/kafka module，仅 integration 标签） | v0.38.0 |
| github.com/IBM/sarama | — |
| rocketmq-client-go/v2 | — |
| go-sql-driver/mysql | **v1.10.1**（2026-10-28） |
| gin / zap / prometheus | zap v1.28.0、prometheus/client_golang v1.23.2（2026-10-28）；lumberjack 已按用户决定移除（日志出 stdout）；gin 待引入（M6） |

## M0 骨架与配置（读 03 + 02§3）

- [x] T0.1 仓库初始化（go.mod go1.24 / main.go / Makefile / 目录骨架 / .golangci.yml）
- [x] T0.2 logging（zap 封装 + 敏感过滤，输出 stdout）
- [x] T0.3 etcdx（client/TLS/auth/重试/lease/注册；集成测试无 Docker 跳过，`-tags=integration` 编译检查通过）
- [x] T0.4 config（schema/加载/严格校验/ENV 解析；库级规则+表级覆盖，见 16 §6）
- [x] T0.5 metrics 框架（全指标 + healthz/readyz，冒烟通过）
- [x] T0.6 CI + Dockerfile + docker-compose.yml（集群演示/单节点，见 16 §6 修订）

### M0 里程碑验收（对照 15 §4，2026-10-28）

| 验收项 | 结论 | 证据 |
|--------|------|------|
| 仓库初始化 / CI | ✅ | 6 任务各一 commit；.cnb.yml：test(build/vet/race) + govulncheck(v1.1.4) + build/push（无 go mod tidy） |
| 日志/指标框架 | ✅ | logging 8 测试 + metrics 存在性/标签/端点测试全绿；冒烟 /healthz 200、/readyz ready、/metrics 9 条 gocdc_ 序列 |
| config schema/加载/校验 | ✅ | 50+ 表驱动用例 + tag 断言 + ENV/cluster 裁决；坏配置冒烟输出 6 条错误清单 exit=1 |
| etcdx client/注册 | ✅ | 9 单测（fake 注入）+ 集成测试已写（编译过） |
| `-h` 可用 | ✅ | 5 个 flag 契约输出正常；缺参 exit=2 |
| 配置从 etcd 加载 | ✅（2026-10-28 补验） | WSL Docker + testcontainers 真实 etcd 3.5.16：TestIntegrationEtcdX PASS（建连/KV 重试包装/lease session/节点注册注销/lease 过期清理，11.6s）；镜像默认换 cnb.cool 本地可达版（ghcr 被拒） |
| 容器可起 | ✅（2026-10-28 补验） | WSL Docker 构建镜像成功（gocdc:dev-test）；`docker run -version` 输出正常、缺参 fail-fast（exit≠0）；compose 语法 `config -q` 通过 |
| lint | ⚠️ 豁免 | golangci-lint 未装（用户决定），.golangci.yml(v2) 就位；CI 暂无 lint 段 |

## M1 单线程管线 + MySQL sink（读 04 + 05§5 + 01§5.3/§7）

- [x] T1.1 transform（04§7 全类型 + 脱敏 + Event 构建）
- [x] T1.2 schema（TableMeta 加载/版本化）
- [x] T1.3 source（BinlogSyncer 封装/监督重启）
- [x] T1.4 position（etcd + lease 门槛 + 节流）
- [x] T1.5 mysql sink（upsert/IN 删除/PK 变更/游程分组）
- [x] T1.6 管线粘合 N=1（Dispatcher/Worker/Watermark/drain）；规则编译（通配展开+库级/表级合并）落在此处，**须输出每表合并后生效的完整规则视图**（Web「查看配置」数据源，16 §6 方案 A）

### M1 里程碑验收（对照 15 §4，2026-10-28）

| 验收项 | 结论 | 证据 |
|--------|------|------|
| source/管线/位点/sink 全链 | ✅ | TestIntegrationPipelineE2E（WSL Docker mysql:8=8.4，单容器双库，N=1）PASS 17.5s |
| DML 全类型对账一致（JSON/decimal/BIT/unsigned/联合主键） | ✅ | E2E 断言：decimal(12,4) 字符串无损、bigint unsigned uint64max（有符号回绕重解释）、json 嵌套提取、bit(8)=0b1010、datetime、varbinary HEX；联合主键在 sink 集成测试覆盖（批量 IN 删除/PK 变更无残留） |
| kill -9 重放无丢失 | ✅ | E2E 第二段：从 e2eStore 最后已存位点重启重放 + 新写入 → 4 行收敛、零重复（幂等 upsert 消化） |
| 位点协议 | ✅ | etcd 位点 = Watermark.base（lease 门槛事务，TestIntegrationStoreGate：Revoke 后写入被拒）；位点不超前断言（TestWatermark_NeverAhead） |
| DDL 策略 | ⚠️ M1 简化 | DDL=pause 即停管线（08 §2 默认策略的最小实现），barrier/白名单同步在 M5（T5.1-T5.3） |
| lint | ⚠️ 豁免 | golangci-lint 未装（用户决定） |

## M2 并行化 + MQ sink（读 04§2-5 + 05§2-4/§7）

- [x] T2.1 分片并行 + rotate 屏障
- [x] T2.2 watermark 强化
- [x] T2.3 kafka sink
- [x] T2.4 rocketmq sink
- [x] T2.5 SinkGroup 双写

## M3 HA 选举（读 03§4）

- [x] T3.1 elector（退避 campaign/Resign/term）
- [x] T3.2 follower watch + leader 复核
- [x] T3.3 当选/落选装配 + 写门槛全覆盖

### M2/M3 里程碑验收（对照 15 §4，2026-10-28）

| 验收项 | 结论 | 证据 |
|--------|------|------|
| M2 乱序 ack 下位点正确 | ✅ | TestWatermark_ConcurrentAcks（万级并发乱序）+ 4 worker E2E |
| M2 双写语义（单边故障全暂停） | ✅ | SinkGroup/SinkRouter 单边故障单测 + Kafka 容器集成（双写链路单边语义同构） |
| M2 kafka 收消息 key 断言 | ✅ | TestIntegrationKafkaSink（confluent-local 镜像；见 T2.3 记录的镜像结论）——运行依赖镜像拉取，本机已验 |
| M3 kill leader 接管 < TTL+10s | ✅ | TestIntegrationElector_Takeover（Resign 路径 <1s；lease 死亡路径 ≤TTL） |
| M3 分区无双写窗口扩大 / 旧 P0-8 回归 | ✅ | 在位复核 + 写门槛全覆盖（follower/旧 leader 位点写被拒断言） |

## M4 全量与交接（读 06，重点 §4.1）

- [x] T4.1 快照会话 + 锁降级 + P 先行持久化
- [x] T4.2 PK 游标扫描 + 进度标记 + 跨会话重扫
- [x] T4.3 交接协议 + PURGED 检测 + restock

## M5 DDL（读 08）

- [x] T5.1 barrier + 三策略
- [x] T5.2 白名单 DDL 同步
- [x] T5.3 SchemaManager 刷新 + 审批流程（etcd 审批契约；Web/CLI 界面挂 M6.2）

## M6 可观测 + Web（读 11 + 13）

- [x] T6.1 指标全集接线
- [x] T6.2 admin API + auth + 审计；含 `GET /rules/effective`（每表合并生效规则视图，对接 T1.6 编译产物，13 §3/16 §6 方案 A）
- [x] T6.3 React 前端 + embed（React 18 + TypeScript + Vite + Ant Design 5；实现 Dashboard、规则、DDL 审批、位点、只读配置/生效规则、审计；Basic 凭据仅内存持有、写操作确认门禁、Go embed 同源提供静态壳（不含敏感信息），`/api/v1` 仍受 Basic 鉴权。业务对接复核：effective rules 脱敏摘要不泄露 runtime salt/固定值、列表空值归一为数组、dotted schema restock 有无歧义路由、verify 提醒唯一键/PK、DDL 确认展示 SQL 与审批语义、浏览器 fetch 正确绑定以保证登录/API 请求可用；Chrome+临时 API stub 冒烟覆盖 login/dashboard、restock、verify、DDL approve、position set、config、audit。Nodes/Full Sync/overview 指标/规则运行态等因 API 未实现，本轮不展示伪造数据，详见 docs/arch/13-web-admin.md §4）
- [x] T6.4 Grafana dashboard + 告警规则

## M7 加固交付（读 14）

- [x] T7.1 启动检查器
- [x] T7.2 verify 对账工具
- [x] T7.3 position import + old2new
- [~] T7.4 混沌/不变式测试（不变式测试已随里程碑就位：位点不超前断言/乱序 ack 万级并发/kill 重放收敛/elector takeover/门禁拒绝——专项随机 crash 注入（07 §6）挂后续会话）
- [x] T7.5 runbook
## 2026-11-04 配置面统一 sources[]：移除 v1 顶层形态兼容（用户决策）

- **决策**：normalizeTree（v1 顶层 source/targets/rules 自动包装为
  sources[default]）整体移除——配置面统一 `sources[]`，单源=数组一条；
  顶层旧字段命中即报错并给改写指引（rejectV1Form；静默忽略不可接受）。
  rawedit.SpliceSourceRules 的 v1 顶层 rules 特判同步删除（正文无
  sources 键即报错）。推翻 PRD README D4 的「自动包装」半边；etcd 键
  迁移（migrate_keys，落点恒 sources/default/）保留不动。
- **升级语义变化**：v1 正文升级从「零修改」变为「一步改写」——包一层
  `sources: [{id: "default", ...}]`；id 必须填 default 与键迁移落点对齐
  （GUIDE §11 / RUNBOOK §9.4 / PRD 03 §4）。dev61 生产正文（v1 形态）
  需一次性改写后再重启。
- **测试**：multi_source_test 三个包装测试（V1Wrap/DualWrite/V1Partial）
  换为拒绝测试（V1FormRejected/V1FormDualWriteRejected）；rawedit
  V1_TopLevel 换 V1Rejected；其余 v1 形态夹具（config_test/bootstrap_test/
  rule_group_test）改写为 sources[] 形态。L1 config/api/cmd 全绿。
- **正文资产一次性迁移**（yaml.Node 手术保留注释/键序，产物全过
  LoadFromRaw 真链）：examples/single.yml、examples/cluster.yml、
  deploy/config.example.yml、deploy/config.edu-mall-dev.yml、
  config.local.yml、.verify/v2/body_{main,single,hiprio,extra}.yml。
- **顺带修 2 个存量问题**：① deploy/config.example.yml 密码裸数字
  （`password: 123456`）从未通过加载链（JSON 桥 number→string 拒绝）→
  加引号；② .verify/v2 组 95 资产相对 11-03 topic 命名改动过期——
  body_extra.yml `topic_prefix: "cdc."` 会被 kafkaTopicRe 拒（且
  TopicName 已改下划线分隔，脚本期望的 cdc.kafka12.big 派生不出来）→
  对齐为 cdc_ / cdc_kafka12_big（95_extra.sh 三处 topic 引用同步）。

### 2026-11-04 补：RulesManage 在线编辑与 v1 移除的冲突复核（全绿）

并行会话落地「规则视图（按库）在线编辑」（RulesManage.tsx，包装既有
PUT /sources/{sid}/rules 三道护栏，无新后端）。与本次 v1 移除的冲突面
复核结论：唯一薄弱点 = 配置中心正文为 v1/坏正文时 GET/PUT rules 的 500
只有笼统文案。已修：config_edit.go 三处错误响应带具体加载链错误明细
（errsText）——v1 形态指引直达 UI。测试补齐：Go TestRouter_ConfigEdit_
V1Body（raw 透传/GET/PUT 拒写矩阵）；前端纯逻辑抽 rulesManageLogic.ts
+ 10 用例（sink 双形态保留/diff 摘要）。验证：Windows L1 18 包 +
WSL -race 三包 + api 集成（真 etcd）+ web typecheck/53 vitest/build
全绿；dist 产物已重建一致。

### 2026-11-04 补：TESTCASES 全层级验收跑通（L1/L2/L3 全绿，最终口径 111 PASS / 0 FAIL）

用户问「docs 的所有测试 case 都跑了吗」→ 如实回答没跑全，随即补齐并修复一路
暴露的测试资产腐化（均为多源改造（11-03）后未跟进的存量问题，产品代码零缺陷）：
- **L2 全量 18 包**：testcontainers 全绿（L2_EXIT=0，此前只跑过 api 单包）。
- **L3 第一轮 12 组**：8 绿 4 挂。修复清单：
  ① `.verify/v2` 8 个脚本 CRLF 行尾污染（Windows 侧工具覆写；.gitattributes 只救
  git 管的检出，run_group.sh 等 8 文件带 CR 秒挂全部组——`file *.sh | grep CRLF`
  扫描 + sed 归一）；
  ② 10_boot A2 坏配置正文还是 v1 形态 → 改写 sources[] 形态（新语义 rejectV1Form
  短路 1 错是预期；预验 19 错 ≥15 达标）；
  ③ 20_data H2 单发读采样 gauge（3s tick）竞态 → 轮询收敛窗口（r2 教训的漏网点）；
  ④ body_mqonly.yml 从未存在（65_mq I7 灌空正文拒启 4 连挂）→ 按 body_extra 重建
  sources[] 形态 + 凭据对齐 root/123456；
  ⑤ body_extra 陈旧凭据 repl/src123（vf-src 只配 root）→ 对齐；
  ⑥ 95_extra.sh 三重过时：旧顶层路由（/position/migrate-gtid、/verify，11-03 路由
  收敛后一律 404）→ /sources/default/...；v1 位点键 /gocdc/verify/position →
  sources/default 子树；本地 stateof 读旧版 /status 顶层形状（pipeline_state 已迁
  sources[]）→ 按 id 过滤。
- **运维事故一支**：上个会话退出只杀了 Windows 侧 wsl.exe，VM 里的重跑存活成孤儿
  实例继续跑，与新一轮重跑并发争 flock、双写同一 .out 文件（tee 截断交错）——教训：
  **重跑前必须 ps 查 VM 侧残留实例，flock 拦截信息「另一个脚本正在运行」时要先分清
  是孤儿还是误报**。最终单实例补跑取证。
- **POS-09b-3 断言口径**：追平判据改为数据实证（迁移重启后插入的探针行送达 =
  重放收敛），lag gauge（3s 采样）降级为参考输出——与 B 系列第二层轮询同口径。
- 终局：**12 组全绿（10:13/20:14/30:6/40:5/45:4/50:8/60:7/65:10/70:13/80:4/90:14/95:13，
  合计 111 PASS / 0 FAIL）**。教训沉淀：**组 95 与键迁移演练类资产在每次路由/键空间
  改造后必须实跑一轮**（这次 95 自 11-03 后从未真跑过，四类腐化叠加）。

### 2026-11-04 补：启动默认值改造正确落地（用户意图 + 协议修复，定向验收绿）

用户在 main.go 把四个 flag 默认值直接改为非空（cluster=gocdc-mysql、
etcd=ETCD_HOSTS env、config-key=/config/edu.mall.gocdc/system、
config-file=config.local.yml）——意图是公司约定零 flag 启动，但 flag 默认值
非空让 ResolveStartup 无法区分「显式传参/默认」，三形态全炸（引导桩读错键、
compose 纯 flag 因默认文件名非空直接 loadBootstrap 报错、ETCD_HOSTS 静默覆盖
引导文件 etcd_addrs）。正确落地：
- **pflag 默认值全部恢复空串**；回退链下沉 ResolveStartup：
  `flag > env(仅 etcd: ETCD_HOSTS) > 引导文件 > 终极默认 > 报错`；
- 终极默认：cluster=gocdc-mysql（config.DefaultClusterName）、配置键=
  /config/edu.mall.gocdc/system（config.DefaultConfigCenterKey，公司约定
  固定键）——**集群名不再参与配置键派生**（只定协调面前缀 /gocdc/{cluster}），
  多集群共享配置中心 etcd 须桩 key / -config-key 显式区分；DefaultConfigKey
  删除；
- 帮助文本修正（用户原文 /configs 拼写与代码 /config 不一致）；
- 联动：bootstrap_test（ETCD_HOSTS 防护 + env 回退新用例 + 新键断言 +
  cluster 缺失改取默认）、lib.sh stub_start/flag_start 显式带
  /gocdc/{cluster}/config、10/30/70/80/90 手写桩补 key、
  deploy/docker-compose.edu-mall-dev.yml 补 -config-key（dev61 不补会在
  下次重启读错键拒启）、stub 示例/compose 注释；
- 文档：main.go 头注释、config README §1/§2.1/§2.3、GUIDE §11 前配置条、
  arch03 §3.1 两段（键与集群分工）、RUNBOOK §3/§9.1、prd07 表、deploy
  README/compose 注释；
- 验收：L1 18 包 + WSL -race 两包绿；L3 定向 10_boot 13/13（桩形态）+
  40_ha 5/5（flag_start 形态）绿。运维注意：dev61 若拉新镜像须同步本次
  compose 改动（-config-key），或把 104 的正文迁到默认键后去掉该 flag。

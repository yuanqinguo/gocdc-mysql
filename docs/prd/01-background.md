# 01 · 背景与目标

## 1. 现状基线：单源是三层硬边界

gocdc 当前是一个成熟的单源 CDC 系统：binlog 拉取（go-mysql 协议层）→
按表分片并行管线 → MySQL/Kafka/RocketMQ 下游，配 etcd 选主 HA、一致性
快照全量 + 无缝交接增量、DDL 审批门禁、字段脱敏、内嵌 Web 控制台。这些
能力的协议与语义由 `docs/arch/02~11、13` 固化，本 PRD 全部原样继承。

「单源」不是某个开关，而是从配置到存储到运行时的三层结构性约束：

**第一层：配置结构**。`Config.Source` 是单数结构体
（`internal/config/config.go:22`），全配置只有一份源库连接参数（addr /
server_id / use_gtid / exclude_schemas / timezone…）。`Targets` 与
`Rules` 挂在顶层，隐含「全部规则都在这一个源连接里寻址」——校验器
`schemaSeen` 的「一组一库」去重（`internal/config/validate.go:228`）
正是按这个前提写的。

**第二层：etcd 键空间**。协调面前缀 `/gocdc/{cluster}` 下，状态键全部
**无源维度**：

```
/gocdc/{cluster}/position                          # 全局唯一位点（cmd/gocdc/bootstrap.go:180）
/gocdc/{cluster}/stock/{db}.{table}                # 全量表级标记（internal/stock/marker.go:68）
/gocdc/{cluster}/stock/_global_position            # 全量全局快照位点（marker.go:73）
/gocdc/{cluster}/events/ddl/{db}.{table}.{file}.{pos}   # DDL 审批（internal/pipeline/ddlgate.go:67）
/gocdc/{cluster}/pipeline-pause                    # 人工暂停标志（cmd/gocdc/lifecycle.go:93）
```

两个源库若有同名库表，这些 key 直接冲突。`model.Position`
（`internal/model/position.go:9`）同样没有源字段——一份位点只可能属于
「那一个」源。

**第三层：运行时单管线**。进程级单例（`cmd/gocdc/state.go`）：
`pipelineUp`、`pipelineStateRef`、`currentPipeline` 三个全局原子量只描述
一条管线；`app`（`cmd/gocdc/lifecycle.go:38-76`）持有单 `srcDB`、单
`schemaMgr`、单 `transformer`、单 `router`、单 `store`；`runLeaderFlow`
用 `pipeCancel` 单槽互斥（lifecycle.go:149-157）。管线内部才是并行的
（N worker），但「拉取→分片→投递→水位→位点」整个源是进程内唯一实例。

连带的单源暗礁（多源化时必须逐一处理，详见 [05](05-runtime.md)）：

| 位置 | 暗礁 |
|------|------|
| `internal/schema/schema.go:48-50` | TableMeta 缓存键 = 小写 `db.table`，无源前缀，跨源同名表互踩 |
| `cmd/gocdc/components.go:50-60` | Transformer 绑定单一 `source.timezone`，多源不同时区即错值 |
| `cmd/gocdc/identity.go:26-36` | `server_id` 按 node.id 派生一次，多源连同一源库会互踢 |
| `internal/pipeline/worker.go:232` | 增量 event_id = `file:pos:rowIdx`，仅在同一源的 binlog 流内唯一 |
| `cmd/gocdc/lifecycle.go:95-103` | `resumeFn` 闭包绑死唯一位点 store |
| `internal/api/status.go` / 13 §4 | `/status`、位点页、规则页、verify 的请求/视图全部隐含唯一源 |

## 2. 现有的「多」能力与它们的边界

系统已有四个维度的「多」，容易与多源混淆，先划清边界：

1. **多库多表规则**：`rules[]` 组形态白名单，一组一库、可多组——但全部
   规则共享同一个 `source.addr`。edu-mall-dev 部署
   （`deploy/config.edu-mall-dev.yml`）就是极限用例：一个源实例里 7 个库
   118 张表，靠 `targets.mysql.target_database` 聚合进
   `dev_edu.mall.merge`。**源侧永远是一个连接**。
2. **多下游扇出**：一条规则可同时投 MySQL + Kafka + RocketMQ
   （`SinkRouter`/`SinkGroup`，05 §6 双写语义）。扇出维度，不是源维度。
3. **多集群共存**：`/gocdc/{cluster}` 前缀天然隔离，「一台 etcd 跑多套
   同步」（03 §2）。这是现行的多源答案——部署 N 个 cluster。本 PRD 要
   替代的正是这个形态的运维成本（见 §3）。
4. **多副本 HA**：同 cluster 内 2+ 节点选主，standby 热备。同一时刻仍
   只有 leader 的那一条管线在跑。

## 3. 需求：为什么要单进程多源

来自实际部署形态的诉求（edu-mall 类聚合场景的推广）：

- **源是多个 MySQL 实例**，不是一个实例里的多个库。多个源实例的数据要
  汇聚到一个目标库（或分发到各自下游）。现状只能一个实例部署一套
  cluster：N 个源 = N 个 gocdc 集群 = N 份配置 key、N 个 Web 端口、
  N 套告警面板、N 次滚动重启编排。
- **运维面收敛**：一个控制台看到全部同步源的健康度、待决 DDL、位点、
  延迟；一个发布动作升级全部源；一套凭据、一个端口。
- **等价性要求（验收基石）**：多源化的每个源在数据正确性、全量/增量
  协议、HA 接管、投递语义、DDL 审批、暂停/恢复、位点回拨、对账等所有
  现有行为上，**与单独部署一个现有单源集群完全一致**。多源化不允许引入
  任何单源部署下不存在的数据风险。

## 4. 目标与非目标

### 4.1 目标

1. 单进程、单 cluster 承载 N（1 ≤ N ≤ 64）条同步源；每个源拥有
   独立的源连接、规则、下游、位点、全量标记、DDL 审批、暂停标志、
   管线状态与生命周期。
2. 全量 + 增量协议逐源完整保留：一致性快照（06 §2）、表级标记与断点
   （06 §4）、交接与 PURGED 检查（06 §7）、幂等 upsert / event_id 去重
   （07）——全部按源独立实例化。
3. HA 语义等价：cluster 级选主不变，leader 同时承担全部源；leader
   切换时每个源都从各自 etcd 位点续传，零丢失承诺逐源成立。
4. 失败隔离到源级：某个源的源库宕机/校验失败/致命错误，只影响该源
   （进入 failed/paused 并周期重试或等人工），其余源不受影响。
5. 存量部署平滑升级：v1 配置自动映射为单源；etcd 键一次性自动迁移；
   旧 API 在单源部署下行为不变。
6. Web 控制台与 API 按源切分视图与操作，并提供全部源聚合总览。

### 4.2 非目标（明确不做，及理由）

| 非目标 | 理由 |
|--------|------|
| 每源独立选主（源级 leader 分布到不同节点） | 需要 N 套选举/门键/复核，防脑裂面 ×N；且与「单 Web 入口看全部」的诉求弱相关。列为后续扩展（[04 §6](04-etcd-ha.md)）。 |
| 跨源事务/一致性 | 各源 binlog 流彼此独立，不存在跨源全序；本 PRD 不制造任何跨源位点（与部署 N 个集群一致）。 |
| 表名/列名映射、分库分表合并 | 现行「目标表 = 源表同名同列」是 schema 校验层强制的无错配设计（05 §5），多源化不改变该边界；聚合靠 `target_database`。 |
| 配置热更新 | 维持「改 etcd + 滚动重启」的既定决策（03 §3.1：不做 watch 不热更）。 |
| MQ topic 跨源聚合投递（多源写同一 topic） | 同 key 跨源交错无法保序，消费端无法重建源内序；改为强制命名空间不相交（D6），聚合交给下游消费。 |
| Web 端配置编辑器 | 13 §4 的遗留缺口，与多源正交；多源让该需求变紧迫，列为开放问题（[08 §5](08-acceptance-rollout.md)）。 |

## 5. 等价性定义（验收基石）

「与部署 N 个单源集群等价」按下列维度逐条验收（完整清单与测试映射见
[08 §1](08-acceptance-rollout.md)）：

- **数据面等价**：任一源的端到端数据结果（MySQL 终态 / MQ 消息集与
  序）、全量交接正确性、at-least-once + 幂等收敛、event_id 去重可用性，
  与该源单独部署一个现有集群完全一致。
- **控制面等价**：DDL 审批（pending/approve/skip 幂等）、人工暂停/恢复、
  位点回拨与 GTID 迁移、restock、verify 对账、审计——逐源可用且语义
  不变。
- **HA 等价**：leader kill -9 后每个源 ≤ TTL+10s 接管、窗口期零丢失、
  旧 leader 写入被租约门槛拒绝。
- **资源面等价（声明式）**：每源独立的有界队列/连接池/批参数，一条
  源的背压不传导到其他源。

**两处显式的不完全等价**（如实声明，验收不视为缺陷）：

1. **资源集中**：N 个源共享一个进程的 CPU/内存（多集群则是 N 份）。
   容量规划按 [05 §5](05-runtime.md) 的乘数公式做。
2. **切换/重启同时性**：多集群可逐集群滚动重启（各集群切换窗口错开）；
   单进程方案的 leader 切换/重启让全部源同时进入切换窗口。每个源的
   中断时长仍 ≈ 单次切换窗口（≤ TTL+10s），只是同时发生。需要错峰的
   场景仍可拆多个 cluster 部署——两种形态共存。

## 6. 术语

| 术语 | 定义 |
|------|------|
| **cluster（集群）** | etcd 键空间与选主的单位，`/gocdc/{cluster}` 前缀。多源化后一个 cluster 含 N 个源。 |
| **node（节点）** | 一个 gocdc 进程，`{prefix}/nodes/{node_id}` 注册。角色 leader/follower 仍是 cluster 级。 |
| **source（源）** | 本 PRD 引入的一等实体：`源库连接（现行 Source 块）+ 规则 + 下游 + 管线参数 + 状态面（位点/全量标记/DDL 审批/暂停标志）`。配置段名 `sources[]`，标识 `id`（即 source_id，行文与键空间中简写 sid）。一个源 ≈ 现行一个单源集群的全部数据面与状态面。「源」的日常语义偏指输入端连接，本 PRD 将其外延为完整同步单元，与配置段/键空间/API/指标用词一词到底（命名取舍见 [02 §1](02-overall-design.md)）。 |
| **sink（下游）** | MySQL / Kafka / RocketMQ 三类投递端，语义不变。 |
| **规则（rule）** | 组形态白名单（一组一库一组 sinks/ddl_policy + 表清单），作用域从 cluster 收敛到源。 |

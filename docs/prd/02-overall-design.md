# 02 · 总体方案

## 1. 核心抽象：同步源（source）

把现行「一个 cluster 的全部数据面与状态面」打包为可复制的单元，称**源（source）**：

```
源 = {
  id            源标识（cluster 内唯一，etcd key / 指标 label / API 路径 / 日志字段共用）
  source        源库连接（现行 Source 块原样：addr/user/flavor/server_id/use_gtid/timezone…）
  rules         规则组（作用域收敛到该源：schema 在该源内唯一即可）
  targets       该源下游（mysql/kafka/rocketmq，独立连接池/生产者）
  pipeline      管线参数（可继承 cluster 级默认）
  stock         全量参数（可继承 cluster 级默认）
  状态面        位点 / 全量表级标记 / DDL 审批门 / 暂停标志 / 管线状态机
}
```

进程内每个源是一个自治的运行时单元（组件矩阵见
[05 §1](05-runtime.md)）；源之间**不共享任何数据面状态**——没有全局
位点、没有跨源水位、没有跨源屏障。这个「不共享」是等价性的构造性
保证：每个源的正确性论证与 04/06/07 篇对单管线的论证逐字相同。

**命名说明**：实体统一命名 `source`（源），与配置段 `sources[]`、
etcd 键段 `sources/{sid}`、API 路由段 `/sources/{sid}`、指标 label 与
消息 envelope 的 `source` 字段**一词到底**——运维排查时同一个东西只
有一个名字。曾评估 `channel`（与 MQ sink 语境、Go 语言原语双重撞名）
与 `link`（英文实体名偏弱，且会与上述已定命名脱节），均不采用。

## 2. 八个核心决策

### D1 · 配置升级为 `sources[]` 数组，源成为一等实体

单源假设的根在 `Config.Source` 单数结构（config.go:22），修叶子不修根
只会把单源假设摊薄成更多隐式约定。配置 v2 把 `source/targets/rules`
下沉到数组元素，`pipeline/stock` 支持 cluster 级默认 + 源级覆盖。
v1 顶层形态已移除（2026-11 用户决策）：配置面统一 `sources[]`，单源即
数组一条，顶层旧字段命中即报错并指引改写（[03 §4](03-config-schema.md)）。

**被否方案**：用多个 cluster 前缀在单进程内模拟多源（进程内跑 N 套
coordination）。否决理由：N 套选主/N 个 Web 端口/N 份 metrics server，
运维面没有收敛，只是把 docker-compose 的多容器换成多 goroutine，失去
本需求的主要价值。

### D2 · 保持 cluster 级单 leader，leader 同时承担全部源

选主、租约、在位复核、防脑裂四件套（03 §4.2）**零改动**：仍是一个
`{prefix}/election`、一个 `{prefix}/leader` 指针、一个 session 租约。
差别只在 leader 当选后启动的不是一条管线而是 N 条（每源一个
`runSourceFlow` goroutine，全部派生自同一个 leadCtx）。

**关键收益——写门槛天然覆盖全部源**：位点/全量标记的 lease 门槛事务
`Compare(LeaseValue({prefix}/leader)) == 本届 lease`
（`internal/position/store.go:77-101`、`internal/stock/marker.go:92-114`）
以 cluster 级 leader key 为锚。每源独立的 position store / marker store
实例只是 key 不同，锚相同——旧 leader 失位后对**任何**源的位点写入都
被拒，防脑裂论证不用重写。

**被否方案**：每源独立选举（`{prefix}/sources/{sid}/leader`）。收益是
源可分布到不同节点（负载分摊、爆炸半径更小），代价是选举/复核/门键/
注册全部 ×N，etcd 心跳与 watch 数量 ×N，且「谁是 leader」从一个问题变成
N 个问题（Web、告警、runbook 全部复杂化）。列为后续扩展（[04 §6](04-etcd-ha.md)），
本 PRD 不做。

### D3 · etcd 键空间下沉一层：`/gocdc/{cluster}/sources/{sid}/...`

源私有状态下沉到 `sources/{sid}/` 前缀下（position、pipeline-pause、
stock/*、events/ddl/*）；cluster 共性状态（config、leader、election/、
nodes/、events/audit）保持原位。键空间全图与迁移协议见
[04](04-etcd-ha.md)。

### D4 · 存量平滑升级：正文一步改写 + etcd 键一次性自动迁移

- 配置：v1 YAML（顶层 `source/targets/rules`）升级时人工包一层
  `sources: [{id: "default", ...}]`（2026-11 起加载链不再自动包装，顶层
  旧形态报错拒绝）；id 必须填 `default` 与键迁移落点对齐，校验、装配、
  API 全走 v2 路径。
- 键迁移：进程启动装配期执行「legacy key → `sources/default/`」的 CAS
  搬家（目标不存在才写，成功后删旧键），幂等、并发安全、记审计；
  迁移与回滚细则见 [04 §4](04-etcd-ha.md)。

### D5 · 失败隔离到源级

现行语义：单源启动检查失败 → 进程退出（fail-fast，`checkSource` 全项
必须过）。多源下「一坏全停」不等价于 N 个集群（一个集群坏，其他集群照
跑），必须改为：

- **进程级 fail-fast 保留**：etcd 连不上、配置非法、Web/metrics 端口
  占用等——这些是全部源的公共依赖，仍拒绝启动。
- **源级 fail-fast 收敛为源 failed + 周期重试**：某源的源库连不上/
  启动检查不过/目标 Ping 失败 → 该源进入 `failed`（状态机见
  [05 §3](05-runtime.md)），30s 起指数退避重试（封顶 5min），其余源
  正常启动运行；指标 `gocdc_pipeline_paused{source, reason="startup"}` +
  告警。这比现行单源行为（进程退出靠 systemd 拉起）更精细，且对单源
  v1 部署是严格改进（不再整进程退出）。
- **运行期致命错误**：沿用现行语义——管线熔断置 PAUSED 保持 leader、
  等人工（07 §6 宁可停不带缺口），只是作用域变成单源；其他源不受
  影响，这是与多集群等价的关键行为。

### D6 · 跨源目标命名空间强制不相交

两个源写同一个投递目标 = 数据互覆或乱序，属于配置错误，在**校验期**
拦死（[03 §3](03-config-schema.md)）：

- **MySQL**：全部源编译后 effective 目标三元组
  `(addr, target_database, table)` 必须全局唯一。两个源解析到同一
  张目标表 → 报错（各自 upsert 互覆，静默错数据）。
- **Kafka**：`(brokers 集合, topic)` 全局唯一——即跨源 effective
  topic 不得相同（含 topic_prefix 拼接后的最终名）。同 topic 跨源同 key
  交错，消费端无法重建源内序，禁止。
- **RocketMQ**：`(name_servers 集合, topic)` 同理（message group =
  key 折叠，同 topic 内跨源组冲突同样乱序）。

比对用规范化字符串精确匹配（brokers/name_servers 排序后拼接）；不做
「同实例不同写法」的智能归一（如 127.0.0.1 vs localhost）——宁可漏报
（漏报的后果是多占用一个连接池，不是错数据）。跨源写**不同**目标库
的**同名表**不受影响（三元组不同）。

### D7 · 消息契约：envelope 加 `source` 字段，event_id 不改格式

- envelope JSON 增加 `"source": "{sid}"`，`schema_version` 1 → 2
  （05 §2 消息契约）。老消费者忽略未知字段即兼容，不破坏现役消费。
- **event_id 保持现行格式**（增量 `file:pos:rowIdx`，快照
  `db.table.snap.P.chunk.idx`）。理由：D6 保证了任一 topic / 目标表只
  属于一个源 ⇒ event_id 只需源内唯一 ⇒ 现行格式已满足；消费端去重
  键 (topic, event_id) 或 (source, event_id) 均成立。改格式会让全部
  现役消费者的去重表在升级瞬间失效，无收益不做。

### D8 · 指标与操作面按源切分

受影响的 gocdc_ 指标统一增加 `source` label（清单见
[07 §1](07-observability-ops.md)）；Grafana 面板与告警规则同步按
`source` 维度聚合/切分。Web 控制台引入顶栏源选择器 + 全部源聚合总览
（[06](06-api-web.md)）。单源（v1 升级）部署升级后指标多出
`source="default"` label，属可预期的破坏性变更，在升级说明中标注。

## 3. 改造后的进程结构

```
                          ┌──────────────────────── 一个 gocdc 进程（node）────────────────────────┐
                          │                                                                        │
  etcd ◄── 选主/注册 ────┤  elector（cluster 级，不变）                                            │
                          │     │ 当选                                                             │
                          │     ▼                                                                  │
                          │  leader flow                                                          │
                          │     ├─ GC 循环（遍历全部源的 DDL 门/死标记）                          │
                          │     ├─ src[order]       srcDB① → source① → dispatcher① → workers①    │
                          │     │                     → watermark① → persister① → sinkrouter①     │
                          │     │                     stock①（快照会话①/标记①） │ DDLGate①       │
                          │     ├─ src[user]         srcDB② → source② → dispatcher② → workers②    │
                          │     │                     → watermark② → persister② → sinkrouter②     │
                          │     │                     stock② │ DDLGate②                            │
                          │     └─ src[log]          …（每源独立实例，互不共享数据面状态）        │
                          │                                                                        │
                          │  共享：etcd session / registrar / metrics registry / audit / Web(gin)   │
                          │  失位（onLost）：cancel leadCtx → 全部源并行 drain → 等待退出         │
                          └────────────────────────────────────────────────────────────────────────┘
```

要点：

- 每源的组件清单与现行 `buildComponents` 产物一一对应
  （`cmd/gocdc/components.go:43-199`），只是从「进程装配一次」变成
  「源装配一次」；共享组件（etcd 客户端、注册器、指标、Web、审计）
  仍装配一次。逐组件归属见 [05 §1](05-runtime.md)。
- leader 切换协议不变：`onLost` 先 cancel leadCtx（全部源同时开始优雅
  drain：停拉取 → 排空 → 刷 sink → 位点终值落盘），并行等待退出（每源
  上限 10s，总等待上限 15s，语义与现行 lifecycle.go:353-359 一致），再
  让位。新 leader 逐源从各自 etcd 位点续传。

## 4. 失败隔离模型

| 故障 | 波及范围 | 行为 |
|------|---------|------|
| 某源的源库宕机 | 仅该源 | source 重连退避（现行 run.go:22-65 语义）；超时熔断 → 源 PAUSED |
| 某源的源库 binlog PURGED | 仅该源 | 拒绝启动增量，报错要求该源 restock（06 §7），其余源不受影响 |
| 某源目标库宕机 | 仅该源 | sink 熔断 → 水位冻结 → 该源队列打满反压该源拉取；其他源队列独立，不受背压传导 |
| 某源致命转换错误/毒消息 | 仅该源 | 源 PAUSED 等人工（07 §6），其他源正常 |
| 某源 DDL 等待审批 | 仅该源 | 该源位点冻结；其他源正常推进（多集群下本就如此） |
| etcd 不可达 | 全部源 | lease 到期 → onLost → 全部源停（安全态）；恢复后重新当选全部源续传。与多集群各自停等价 |
| 本进程崩溃/被杀 | 全部源 | 全部源按各自水位重放恢复（at-least-once + 幂等收敛） |
| 一个源配置非法 | 进程拒绝启动 | 配置校验是进程级的（全部错误一次报出，load.go:79-84），非法配置不允许带病运行——与多集群「那个集群配错了起不来」等价，其余「集群」在修正该源配置段前共同不可用。这是显式取舍：配置正确性优先于局部可用性。 |

## 5. 与「N 个单源集群」的等价性映射

| 维度 | N 个单源集群（现状） | 单进程 N 源（本方案） | 等价？ |
|------|---------------------|------------------------|--------|
| 位点/快照标记/DDL 门/暂停标志 | 每集群独立前缀 | 每源独立 `sources/{sid}/` 子前缀 | ✅ |
| leader 选举与接管 | 每集群独立选举 | 一个选举，leader 承担全部源；切换时全部源同时接管 | ✅（切换同时性差异见 01 §5） |
| 写门槛/防脑裂 | 每集群各自 lease 门 | 同一 lease 门锚保护全部源 | ✅ |
| 管线并行度/背压/内存上界 | 每集群独立 | 每源独立（队列/批/池均独立） | ✅ |
| 全量快照与交接 | 每集群独立 | 每源独立 | ✅ |
| DDL 审批冻结半径 | 冻结该集群管线 | 冻结该源管线 | ✅ |
| Web/metrics/凭据 | N 套端口 N 套面板 | 1 套，按源切分 | 超 集（改进） |
| 配置变更生效 | 改该集群 key + 滚动重启该集群 | 改同一 key（含全部源）+ 滚动重启（全部源同时进入切换窗口） | ⚠️ 见 01 §5 |
| 资源 | N 份进程资源 | 1 份进程资源承载 N 源 | ⚠️ 见 01 §5，容量公式见 05 §5 |

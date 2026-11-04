# 05 · 运行时与生命周期

## 1. 组件矩阵：共享 vs 每源

装配改造的核心是把现行 `buildComponents`（`cmd/gocdc/components.go:43-199`）
的产物按归属拆开：

| 组件 | 归属 | 说明 |
|------|------|------|
| etcd client / session / registrar | 共享 | 进程一个协调面连接与注册 |
| elector | 共享 | cluster 级，一个 |
| metrics registry / metrics server / readyz | 共享 | 端口一个；指标实例带 source label（[07](07-observability-ops.md)） |
| Web admin server（gin + embed SPA） | 共享 | 一个端口，API 按源路由（[06](06-api-web.md)） |
| 审计 store | 共享 | `{prefix}/events/audit` 一个环，条目带 source |
| GC 循环 | 共享 | 遍历全部源 |
| **源库连接池 srcDB** | 每源 | `openSourceDB` 逐源：`SetMaxOpenConns(4 + spec.stock.workers)`（沿用现行公式，bootstrap.go:200） |
| **schema.Manager** | 每源 | 缓存键仍为 `db.table`——**实例隔离替代键前缀**，零改动复用（源内本就一组一库不冲突） |
| **Transformer** | 每源 | 绑定该源 `source.timezone`；maskers 缓存随实例 |
| **sink（按 targets 启用装配）** | 每源 | 独立连接池/生产者（= N 个集群各自的池）；mysql/kafka/rocketmq 任一可缺（纯 MQ 下游合法），`router.Ping` 只 ping 已启用项，fail-fast 逐源 |
| **SinkRouter** | 每源 | 规则→SinkGroup 映射随源 |
| **verifyTargetTables（启动目标表核对）** | 每源 | 启动期逐源执行（D5：失败只停该源） |
| **DDLGate + DDLHooks** | 每源 | key 在源子树；审批 API 逐源 |
| **position.Store / MarkerStore** | 每源 | key 在源子树，gate 锚共享（PRD 04 §3） |
| **source.Source / Pipeline（dispatcher+workers+watermark+persister）** | 每源/每任期 | 现行「每任期新建」语义逐源（lifecycle.go:107） |
| **stock.Service（快照会话/扫描）** | 每源/每任期 | 快照会话只存在于该源源库 |
| **暂停标志读写 / resumeFn** | 每源 | `resumeFn` 闭包绑定该源 store（改 lifecycle.go:95-103 为按源构造） |

## 2. 装配流程改造

```
run()
 ├─ 解析启动与配置加载（config.Load；顶层 v1 形态报错拒绝，统一 sources[]，PRD 03 §4）
 ├─ 日志 / metrics server                       [共享，不变]
 ├─ 协调面 etcd + session + 注册 + server_id 逐源派生（§6）
 ├─ ★ 键迁移 v1→v2（幂等，PRD 04 §4）
 ├─ ★ 逐源 buildSourceComponents(sid)：srcDB → checkSource(sid) →
 │      schema/transform/sinks/router.Ping/verifyTargetTables/DDLGate
 │      —— 任一步失败：该源标记 failed，进入重试循环；不影响其他源
 ├─ admin server（Deps 持 按源闭包表，PRD 06）
 └─ 选举 / 单节点 leader
       当选 → runLeaderFlow：
         ├─ 全部源 store/markers SetGate（同一 leader key + 本届 lease）
         ├─ GC 循环（遍历源）
         └─ for each source（goroutine）：
              ├─ 暂停闸检查（该源 pipeline-pause）
              ├─ runStock（该源全量 → 该源增量起点）
              └─ newPipeline（该源 source + 组件）→ Run(leadCtx)
```

对应现行代码的改造点：

- `cmd/gocdc/components.go`：`buildComponents` → `buildSourceComponents(spec)`，
  产物 `*sourceComponents`；`cmd/gocdc/state.go` 的进程级单例
  （`pipelineUp/pipelineStateRef/currentPipeline`）改为
  `sources *sourceTable`（`map[sid]*sourceState` + RWMutex，含各自
  `pipeCancel/pipeDone/stockSvc/opPaused` 现行字段的逐源版本，
  对应 lifecycle.go:38-76 的 app 结构按源拆分）。
- `checkSource`（bootstrap.go:205-219）从「失败退出进程」改为「失败标记
  源 failed + 重试」，重试循环在源装配监督 goroutine 内（30s 起指数
  退避封顶 5min，成功即进入待 leader 启动状态；期间该源在 `/status`
  显示 `failed(startup)`）。
- 懒装配与 leader 的衔接：源装配成功但当前非 leader → 待命（不启动
  管线）；leader 当选后 `runLeaderFlow` 只启动**已就绪**源，failed
  源由其监督循环在装配成功后自动加入（需要与当选流程握手：监督循环
  装配成功后调用 `launchSource`，若当前 leading 则直接启动）。

## 3. 源状态机

现行管线五态（idle/stock/running/paused/stopped，`api/dto.go:43-49`）
保留，新增装配期状态与错误分类：

```
config-loaded ──build──► starting ──ok──► ready ─(leader)─► stock → running ⇄ paused
                             │                                  （DDL 等待在 running 上
                             ╰─fail─► failed ──重试成功──► ready    叠加，现行语义不变）
                                        （退避重试，30s→5min）
```

- `failed` 带 `error` 与 `since`，`/status` 与指标 `gocdc_source_up{source}=0`
  + `gocdc_pipeline_paused{source, reason="startup"}` 暴露。
- 运行期致命错误（管线熔断）仍为 `paused`（保 leader 等人工），与
  failed(startup) 区分。
- leader 失位：全部运行中源进入 `stopped`（等待新 leader），现行语义。

## 4. 全量与增量（协议不变，逐源实例化）

全部协议条款引用现行文档，逐源成立，本节只写多源带来的编排差异：

1. **快照会话独立**：每源在自己的源库上执行
   FTWRL/LOCK INSTANCE FOR BACKUP → CONSISTENT SNAPSHOT → 位点 P → P 先行
   持久化（06 §2、§4；`internal/stock/session.go:90-161`）。源 A 的
   快照锁不涉及源 B 的源库（不同实例）；**同一源实例被两个源订阅**
   的场景（合法但少见，server_id 必须显式互异）下两个 FTWRL 可能互相
   等待——毫秒级短锁，实测无死锁风险（FTWRL 与一致性快照读不互斥），
   文档如实标注。
2. **增量起点逐源**：`max(该源已存位点, 该源 stock_position)`，
   首启取该源 `SHOW MASTER STATUS`，PURGED 检查逐源
   （lifecycle.go:279-327 的 runStock 逐源实例化）。
3. **首启全量并发**：N 个源同时首启 = N × stock.workers 个扫描连接、
   N 份 chunk 吞吐打向各自目标。这与 N 个集群同时部署的场面一致（等价
   性），不做全局节流；容量建议见 §5。错峰启动是部署手段（先起一条
   追平再上下一条），runbook 提示。
4. **restock / do_stock 语义**：逐源逐表，`POST /sources/{sid}/rules/.../restock`
   只清该源标记；重启后仅该源重扫。
5. **交接时刻差异**：各源全量完成时刻不同，各自独立进入增量——不存
   在「等最慢源」的屏障（与 N 集群一致）。

## 5. 背压与资源

### 5.1 隔离

每源独立的有界队列（`worker_queue_size`）、独立 worker 池、独立
sink 池与退避状态。源 A 的下游变慢 → A 的队列打满 → A 的拉取阻塞
（TCP 反压到 A 的源库）；源 B 的队列与拉取完全不受影响。内存上界：

```
单源上界 ≈ 现行单集群上界（04 §4 ≈ 100MB 级）
进程上界  ≈ N × 单源 + 共享面（etcd/web/metrics ≈ 数十 MB）
```

### 5.2 连接数预算（部署 checklist 项）

| 资源 | 每源 | N 个源合计 |
|------|--------|-------------|
| 源库连接 | `4 + stock.workers`（扫描/采样/拉取） | Σ 每源（打向**各自**源库） |
| 目标 MySQL 连接 | `max_open_conns`（显式必填） | Σ 每源（若指向同一实例，全部落同一实例——`max_connections` 要按合计规划） |
| Kafka 生产者 | 1（franz-go 客户端） | N |
| RocketMQ 生产者 | 1 | N |
| etcd session | 1（进程共享） | 1 |
| binlog dump 线程（源库侧） | 1/源 | N（分散在各自源库） |

**GOMEMLIMIT 建议**：`N × 150MB + 100MB` 起步（07 §2 现行 4C8G 基线
的单集群口径推广），压测修正。

### 5.3 上限与告警

- 配置校验硬上限 64 个源（PRD 03 V1）。
- 新增指标 `gocdc_sources`（Gauge，就绪源数/配置源数）与
  `gocdc_source_up{source}`；告警：任一源 `source_up=0` 持续 1min。

## 6. server_id 派生

现行：`cfg.Source.ServerID == 0` → `fnv32a(node_id)`（bootstrap.go:159-163）。
v2：逐源 `fnv32a(node_id + ":" + sid)`。性质：

- 同进程内各源互异（sid 不同）；同源多副本（共享配置的 HA 节点）
  互异（node_id 不同）——与现行派生的两条性质同构。
- 派生冲突（跨集群碰巧同值且同源实例）表现与现行一致：MySQL 互踢，
  可观测非静默。显式配置优先。

## 7. 优雅停机

信号 → ctx 取消 → elector 让位（Resign + 删 leader 指针，仅当在任）→
`onLost` → cancel leadCtx → **全部源并行**执行现行 drain 序列
（pipeline.go:204-243：停拉取 → 关队列 → 排空刷 sink（≤10s）→ 位点
终值强制落盘）→ 等全部退出（15s 封顶）→ defer LIFO 关闭组件
（web → 每源 sink → 每源 srcDB → 注销+session → metrics → log → etcd）。

与现行差异仅两处：管线退出等待从「一条」变「全部并行退出」；组件关闭
多一层源循环。单源部署下行为与现状一致。

## 8. 已识别暗礁的处理（对应 01 §1 清单）

| 暗礁 | 处理 |
|------|------|
| schema.Manager 缓存键无源维度 | 实例隔离（每源一个 Manager），键格式不动 |
| Transformer 单时区 | 每源一个 Transformer，绑定源时区 |
| event_id 源内唯一性 | 不改格式；跨源靠目标不相交（D6/D7）保证语义 |
| resumeFn 绑死单 store | 逐源构造闭包 |
| server_id 单值 | §6 派生规则推广 |
| state.go 进程级单例 | sourceTable（§2） |
| checkSource fail-fast 进程退出 | 源级 failed + 重试（D5） |

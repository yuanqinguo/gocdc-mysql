# 04 · etcd 键空间 v2 与 HA

## 1. 键空间全图

```
/gocdc/{cluster}/
├── config                                  # 不变：全量配置 YAML（v2 结构，含 sources[]）
├── election/{...}                          # 不变：cluster 级选举（concurrency.Election 内部）
├── leader                                  # 不变：leader 指针（值 = nodeID，挂当选 lease）
│                                           #   —— 兼任全部源写门槛的锚点 key（§3）
├── nodes/{node_id}                         # 不变：节点注册（进程级，role 仍 leader/follower）
├── events/audit                            # 不变：审计环（条目结构加 source 字段，§5）
├── migration/sources/v1tov2                # ★ 新增：一次性迁移完成标记（§4）
└── sources/{sid}/                          # ★ 新增：源私有状态子树（每源一份）
    ├── position                            #   位点（结构不变：v/name/pos/gtid_set/seq/term/…）
    ├── pipeline-pause                      #   人工暂停持久标志（无 lease，跨重启/失位保持）
    ├── stock/{db}.{table}                  #   全量表级标记（结构不变）
    ├── stock/_global_position              #   全量全局快照位点
    └── events/ddl/{db}.{table}.{file}.{pos}   # DDL 审批记录（结构见 §5 的 ID 变更）
```

设计规则：

- **共性上提、个性下沉**：配置/选举/注册/审计是 cluster 的公共面，留原位
  ——多集群共享 etcd 时的跨 cluster 隔离方式（前缀第一段）不变；源
  私有状态统一挂在 `sources/{sid}/` 下，sid 是新的第二级命名空间。
- 全部路径仍从 cluster 配置派生 + sid 拼接，禁止硬编码（沿用
  `internal/etcdx/register.go:23-24` 的纪律）。
- 单源（v1 升级）部署的键空间 = `sources/default/` 子树，与其他源
  无任何形态差异。

## 2. 各 store 的 key 变化

| Store | 现行 key | v2 key | 构造处 |
|-------|---------|--------|--------|
| `position.Store` | `{prefix}/position` | `{prefix}/sources/{sid}/position` | 每源 `position.NewStore(kv, sourcePrefix+"/position", gateKey, 0)` |
| `stock.MarkerStore` | `{prefix}/stock` | `{prefix}/sources/{sid}/stock` | 每源 `stockf.NewMarkerStore(kv, sourcePrefix+"/stock")` |
| `DDLGate` | `{prefix}/events/ddl` | `{prefix}/sources/{sid}/events/ddl` | 每源 `pipeline.NewDDLGate(kv, sourcePrefix+"/events/ddl")` |
| 暂停标志 | `{prefix}/pipeline-pause` | `{prefix}/sources/{sid}/pipeline-pause` | lifecycle 源流 |
| `Registrar` / elector / audit | — | 不变 | — |

注意 `position.NewStore` 的签名现状（`internal/position/store.go:36`，
gateKey 已是参数）与 `MarkerStore`（`internal/stock/marker.go`，
`SetGate` 独立方法）都已经以「实例持自己的 key + gate」组织，**每源
实例化不需要改这两个包的内部实现**，只是多 new 几个实例。

## 3. 写门槛复用论证

现行 lease 门槛事务（03 §4.2-2）：

```
Txn: If Compare(LeaseValue("/gocdc/{cluster}/leader")) == 本届 leaseID
     Then Put(<任意源的 position/stock 键>)
```

v2 不改锚点 key：全部源的 store 实例 `SetGate` 传入**同一个**
`{prefix}/leader` 与本届 leaseID（`runLeaderFlow` 现行逻辑的逐源重复，
`cmd/gocdc/lifecycle.go:172-173`）。由此继承的性质：

- 旧 leader 分区后失联，lease 过期 → leader key 消失 → 它对**任何**源
  的位点/标记写入全部 `ErrGateRejected` → 立即熔断该源 persister
  （现行 persister.go:113-149 语义）。防脑裂不需要按源重新论证，
  锚点唯一 ⇒ 结论对所有子树成立。
- follower 从不 `SetGate`（leaseID=0 写全拒，store.go:87-89）——对全部
  源成立。
- leaseID=0（未当选/无门槛期）写全拒的启动期保护同样逐源生效。

**审计环写入门槛**（CAS ModRevision，`internal/api/auth.go:120-124`）
与源无关，不动。

## 4. 一次性迁移协议（v1 → v2 键搬家）

### 4.1 触发与范围

升级后的进程**首次启动装配期**（`setupCoordination` 之后、源装配之前）
执行。搬迁清单（全部在 `{prefix}` 下）：

```
position                          → sources/default/position
pipeline-pause                    → sources/default/pipeline-pause
stock/*（含 _global_position）    → sources/default/stock/*
events/ddl/*                      → sources/default/events/ddl/*
```

`leader`、`election/*`、`nodes/*`、`config`、`events/audit` **不迁**。
配置已先行规范化为 v1 → `sources:[{id:"default"}]`（PRD 03 §4），
迁移目标源 id 恒为 `default`。

### 4.2 算法（幂等 + 并发安全）

对每个键执行 etcd 事务：

```
Txn: If   targets/<new> 不存在            （Version == 0）
     Then Put(<new>, <old 的值>)           （不带 lease，普通持久键——原键就是持久键，
                                            位点等键的「lease 保护」在写门槛而非键本身）
          Delete(<old>)
     Else  (不 Then 分支)
          若 <new> 已存在 → Delete(<old>)   （幂等收尾：上次迁移写到一半重跑）
```

旧键不存在则跳过（未部署过/已迁完）。两节点同时启动执行同一批事务时，
etcd 线性一致保证每个键恰有一个事务的 If 成立——天然互斥，无需额外锁。
全部键处理完后写完成标记 `migration/sources/v1tov2 = {时间, node_id, 搬迁键数}`。
任何一步 etcd 错误：重试（etcdx 现行重试语义），迁移未完成不阻塞启动——
因为新代码只读新键，旧键残留无副作用；完成标记只在「清单全空或全部搬完」
时写入。

审计：迁移开始/完成各记一条审计事件（`config.migrate_sources_v1tov2`），
日志输出搬迁明细（旧 key → 新 key 计数）。

### 4.3 回滚（降级到旧版本二进制）

旧版本代码只认旧键；迁移后降级会「丢」位点（旧键已删）。回滚流程
（写入 RUNBOOK，[07 §5](07-observability-ops.md)）：

1. 停进程；
2. `etcdctl get /gocdc/{cluster}/sources/default/position --print-value-only`
   确认现行位点；
3. `etcdctl put /gocdc/{cluster}/position <上述值>`（手工搬回；stock 与
   DDL 门同理，DDL 门通常量少可只搬 pending 记录）；
4. 换回旧二进制启动。

位点值结构（model/position.go）与快照标记结构跨版本不变，搬回即可用。

## 5. 结构微调

- **DDLRecord.ID**：`{db}.{table}.{file}.{pos}` → 前缀源段。由于 key
  本身已含 `{sid}` 路径段，**ID 字段保持原格式即可全局不冲突**（key 不
  同 = 记录不同）；但 API 返回与前端展示需要可读的来源，`DDLRecord`
  增加 `Source string` 字段（创建时填 sid）。同理 `AuditEntry` 增加
  `Source string`（源级操作填，cluster 级操作留空）。
- **Marker**：结构不变；key 已按源隔离，两源同名库表标记互不干扰。
- **NodeInfo**：不变（role 是 cluster 级概念）。

## 6. HA 行为

### 6.1 不变的部分

- 选举协议、lease TTL、在位复核（含 checkTimeout 有界超时）、自愈重写
  leader 指针、follower 绝不删指针——全部原样（`internal/elector/elector.go`）。
- 失位动作链顺序：`leading=false → cancel(leadCtx) → 角色更新 → 等管线
  退出`（lifecycle.go:331-360）。v2 中 cancel 的是**全部源共享的**
  leadCtx，每源在自己的 goroutine 里感知并执行现行 drain 序列
  （停拉取 → 排空 → 刷 sink → 位点终值）。
- etcd 不可达（outage）：lease 到期 → 全部源停（安全态）→ 恢复后重选，
  全部源从各自 etcd 位点续传。与「N 个集群各自 etcd 故障全停」等价。

### 6.2 变化的部分

- `onLost` 等待「全部源退出」：并行 drain（每源上限 10s），总等待
  15s 封顶（数值沿用现行 lifecycle.go:353-359 的 15s，语义从「等一条」
  变为「等全部并行退出」）。
- 单节点模式（`ha.enabled: false`）：门键创建逻辑不变，源装配同上。
- `resign`（主动让位）：cluster 级操作，全部源一起切换；API 不变。

### 6.3 后续扩展（本 PRD 不做）：源级选主

若未来需要源分布到不同节点，键空间已为其预留自然位置
（`sources/{sid}/leader`、`sources/{sid}/election/`），写门槛锚点从
cluster leader key 换成源 leader key 是局部改动。本 PRD 不实现，
避免第一版引入 N 倍选举面。

## 7. GC 循环

现行 `gcLoop`（lifecycle.go:246-274，每小时）清理 stock 死标记与已决
DDL（7 天）。v2 改为遍历全部源，逐源调用各自的 marker GC 与 gate
GC；单源 GC 失败只告警不中断其他源的清理。

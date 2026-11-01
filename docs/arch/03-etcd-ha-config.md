# 03 · etcd 设计：配置中心、高可用选举、位点存储

> etcd 是 gocdc 唯一的协调面。本文给出完整设计：key 布局、配置 schema、选举协议、位点协议、客户端治理。

## 1. etcd 职责总览

| 职责 | 说明 |
|------|------|
| 配置中心 | 全量配置存单一 key（正文 YAML，唯一格式，不做 JSON 兼容），启动拉取，重启生效（无 watch，按需求不做动态生效） |
| Leader 选举 | `clientv3/concurrency` Session+Election，lease TTL 可配（默认 10s） |
| 位点存储 | 单 key JSON，路径可配置；先投递后推进 |
| 节点注册 | 带 lease 的临时 key，供 Web 展示集群成员与角色 |
| 全量标记 | 每表完成标记，重启不重跑全量 |

**单集群单 etcd**：配置中心与集群协调统一一个 client（支持多 endpoint、TLS、用户认证）。

## 2. Key 布局

所有 key 在可配置前缀下（默认 `/gocdc/{cluster}`，cluster 名必填，用于一台 etcd 跑多套同步）：

```text
/gocdc/{cluster}/
├── config                          # 全量配置（YAML 正文，唯一格式）
├── config.lock                     # 可选：配置写入互斥（web 端修改时用事务）
├── position                        # 位点（路径可用 position_key 覆盖，见 §5）
├── election/                       # concurrency.Election 内部前缀
├── leader                          # 当选指针（值为选举 key 路径，选举服务写入）
├── nodes/{node_id}                 # 节点注册（lease 临时 key）
│     值: {"node_id":"...","advertise_addr":"ip:port","version":"...","started_at":...,"role":"leader|follower"}
├── stock/{db}.{table}              # 全量完成标记
│     值: {"status":"done","done_at":...,"rows":123456,"snapshot_position":{...}}
└── events/ddl                      # 可选：最近 N 条 DDL 事件日志（web 展示），环形覆盖
```

命名规则：所有 etcd 路径从 `cluster` 配置派生，禁止硬编码——一套环境一个前缀，多集群可共存于同一 etcd。

## 3. 配置中心

### 3.1 加载协议

```text
启动（零参数优先）:
  gocdc                                    # 零参数：读工作目录引导文件 config.local.yml（GoLand 调试形态）
  gocdc -config-file <yml>                 # 显式引导/配置文件
  gocdc -cluster <name> -etcd <addrs> [-config-key <path>] [-config-file <yml>]   # 显式 flag 形态
```

**启动裁决（config.ResolveStartup）**：优先级 **flag > 引导文件 > 报错**。引导文件同一文件两种形态（`bootstrap.config_source` 区分）：`file`（默认）= 文件即全量配置（开发）；`etcd` = 引导桩，全量配置从配置中心拉取。**两个 etcd 角色可分离**：配置中心（`bootstrap.config_center.etcd_addrs`，一个 etcd 按 `/gocdc/{cluster}/config` 前缀隔离多环境）与协调面（位点/选举，`bootstrap.etcd_addrs`，缺省回退配置中心）不必是同一实例。

**先有鸡先有蛋的裁决（实现必须遵守）**：启动裁决后的 `cluster` 值（flag 覆盖 > 引导文件顶层 `cluster` 字段）是 etcd key 前缀（`/gocdc/{cluster}/config`）的**唯一来源**；加载到的配置体内的 `cluster` 字段**必须与裁决值相等**，不等即 fail-fast 报「配置属于别的集群」——配置字段只是冗余校验，绝不参与 key 推导。

1. 优先级：启动裁决 → `file` 形态读引导文件本体（跳过 etcd 配置源）→ `etcd` 形态/纯 flag 读配置中心 `config` key。
2. 配置读取失败或 key 不存在 → **启动失败退出**（fail-fast，绝不带着空/旧配置运行）。
3. 读取后解析为配置树（YAML 唯一格式，不兼容 JSON 配置）+ **严格校验**（必填、类型、值域、规则冲突如「MySQL sink 必须同名表」「主键列不可 exclude」），校验失败打印全部错误并退出。
4. **不做 watch、不做热更新**（需求明确不需要动态生效）。Web 端提供「查看配置」与「写回 etcd」（写回仅存 etcd，提示需重启生效）。

### 3.2 配置 schema（完整示例）

```yaml
cluster: edu-merge-prod            # 必须等于 -cluster flag（见 §3.1 裁决）

node:                              # 节点身份（选举/注册/Web 展示用）
  id: ""                           # 空 = 自动派生 "{hostname}-{8位随机}"，多副本 k8s 也可用 POD_NAME 注入
  advertise_addr: ""               # 空 = 自动 "本机IP:web端口"；供集群页面定位节点

source:
  addr: "10.0.0.1:3306"
  user: repl
  password: "***"
  charset: utf8mb4
  flavor: mysql                    # mysql | mariadb
  server_id: 123456                # 可省略：缺省按 node.id 派生（fnv32）；显式则须全网唯一
  timezone: Asia/Shanghai          # DATETIME 解释时区（显式声明，不依赖进程 Local）
  use_gtid: false                  # true 时位点含 gtid_set
  heartbeat_interval_ms: 3000      # binlog 心跳（保活 + 空转探活）
  read_timeout_ms: 60000
  exclude_schemas: [mysql, sys, information_schema, performance_schema]
                                   # 系统库硬排除（配置可追加，不可移除这四个）
  tls: { enabled: false, skip_verify: false, ca: "", cert: "", key: "" }

ha:
  lease_ttl_seconds: 10            # 选举 lease
  leader_check_interval_ms: 2000   # leader 在位复核周期
  resign_on_shutdown: true         # 优雅停机时主动让位

pipeline:
  workers: 4                       # 解析/发送并行度（按表分片）
  worker_queue_size: 8192          # 每 worker 有界队列（事件数）
  batch_size: 500                  # 攒批条数
  flush_interval_ms: 200           # 攒批时间
  position_save_interval_ms: 3000  # 位点节流
  on_convert_error: "null"         # 值转换失败策略（04 §7）：null|zero|fail_event
  decimal_as: string               # DECIMAL 承载（04 §7）：string|float
  zero_datetime_as: "null"         # 零日期策略（04 §7）：null|string

targets:
  mysql:
    enabled: true
    addr: "10.0.1.1:3306"
    user: writer
    password: "***"
    charset: utf8mb4
    max_open_conns: 20             # 显式必填（0 = 无上限，防连接失控）
    max_idle_conns: 5
    conn_max_lifetime_min: 30
    tx_chunk_rows: 500             # 单事务最大行数
    target_database: edu_merge     # 默认目标库（05 §5.1.1）；空 = 与源库同名
    ddl_sync: true                 # 允许 auto 策略同步白名单 DDL 到目标
  kafka:
    enabled: true
    brokers: ["10.0.2.1:9092"]
    topic_prefix: "cdc_"           # 实际 topic = prefix + db_table（Kafka/RocketMQ 统一规范，点号不收；或按 rule 覆盖）
    order_by: row                  # row: key=pk（同行有序）| table: key=db.table（表级有序）
    compression: snappy
    idempotent_producer: true
    ack: all
    send_timeout_ms: 5000
    max_message_bytes: 1048576     # 客户端侧消息上限，须 ≤ broker/topic max.message.bytes
    sasl: { enabled: false, mechanism: "", user: "", password_env: "" }
    tls: { enabled: false, skip_verify: false, ca: "", cert: "", key: "" }
  rocketmq:
    enabled: false
    name_servers: ["10.0.3.1:9876"]
    group: gocdc-producer
    topic_prefix: "CDC_"
    order_by: row                  # 顺序消息 selector 按 key 哈希
    send_timeout_ms: 5000
    acl: { access_key: "", secret_key: "" }

rules:
  # 组形态白名单（唯一形态）：一组一库；未列入 tables 的表（含未来新表）不同步
  - schema: edu.mall.admin
    ddl_policy: auto               # auto | pause | ignore（见 08）；省略 = pause（安全默认）
    sinks: { mysql: true, kafka: true }
                                   # bool 简写；需覆盖 topic/tag 时写对象：
                                   # kafka: {enabled: true, topic: "cdc.custom"}；MySQL sink 配 topic/tag 拒绝
    tables:
      - { table: admin_user, order_by_column: id, do_stock: true,
          mask_columns: { phone: phone } }   # 全量快照排序键见 06 §4；do_stock 表级必填
      - { table: tmp_log, order_by_column: id, do_stock: false }  # 只增量不快照
      remark: { type: truncate, keep: 32 }

observability:
  web: { enabled: true, addr: ":8060", username: admin, password: "***" }
  metrics_addr: ":9595"
  log: { level: info, json: false }   # 输出统一标准 stdout（容器收集），不做文件轮转
```

设计要点：

- **targets 显式 enabled**：同一规则可同时启用 mysql+kafka+rocketmq（多下游双写/多写）。
- MySQL sink 规则**没有**目标表名/列映射字段——约束「同名同表、列一一对应」在 schema 校验层强制（05 文档），配置层面不提供错配空间。
- 敏感字段（password）允许 `${ENV_VAR}` 占位，加载时解析，避免明文进 etcd（配合 etcd auth/TLS）。

## 4. Leader 选举设计

**单节点模式（`ha.enabled: false`）**：跳过选举，进程直接以 leader 启动管线——适用调试/单机场景。写门槛语义不变：以 session 租约在 `{prefix}/leader` 创建门键，位点/标记写照常受租约保护（session 死亡 → 门键消失 → 位点写被拒 → 管线停，与失位语义一致；恢复需重启进程）。/status 的 role 显示 `single`，resign 接口无效。

### 4.1 协议

基于 `concurrency.Election`（内部：lease 临时 key + revision 排序 + watch，等待者在/key 删除后自动当选）：

```text
Elect(ctx):                                    // ctx = 应用生命周期 context
  loop:
    session = NewSession(lease_ttl)            // 失败: 退避重试(100ms*2^n 上限10s)，绝不退出 goroutine
    election = NewElection(session, "{prefix}/election")
    go session.KeepAlive()                     // clientv3 内置，TTL/3 周期
    err = election.Campaign(ctx, nodeValue)    // nodeValue = {node_id, advertise_addr, started_at}
    if err: 退避 continue
    beLeader():
      atomic role=leader; Put("{prefix}/leader", election.Key())
      informCh <- Elected{term: election.Key() revision}   // 带修订号作为 term
      watch session.Done():
        on fired → beFollower(); 重新 campaign（lease 已被 etcd 删除）
  优雅停机 / 收到 Resign 请求:
    election.Resign(ctxWithTimeout(3s)); Delete("{prefix}/leader"); beFollower()
```

follower 侧：

- 持续 **watch `{prefix}/leader`**：值变化 → 更新 Web 展示的 leader 信息；key 被删 → 标记「无主」。
- follower **不启动管线**；watchdog 每 `leader_check_interval_ms` 复核 `IsLeader()`（lease 存活 + election key 仍为本节点），失败立即 `stopPipeline()`。

### 4.2 防脑裂与双写

1. **leadership 复核**：leader 的 watchdog 周期性确认 lease 存活与 election key 归属；与 etcd 分区 → 复核失败 → **立即停止管线**（先于新 leader 接任，TTL 10s 内完成）。双写窗口收敛为「最长 TTL 的投递重叠」，由下游幂等（MySQL upsert；MQ 重复消息）兜底——这是文档化的明确语义，不声称重叠窗口为零。
2. **位点写门槛**：PositionPersister 每次写 etcd 前用事务 `If(lease alive)` 语义（Compare lease ID）；lease 失效则位点写不进去，新 leader 的位点推进不被旧 leader 覆盖。**同样的 lease 门槛也适用于全量标记 `stock/*` 的写入**——防止分区旧 leader 在失去领导权后继续推进/覆盖全量状态（06 文档 §5）。
3. **term 记录**：每次当选以 election key 的 revision 为 term，写入节点注册值与日志；位点值附带 `leader_term`，便于排障时判断是否跨主。
4. **停机让位**：SIGTERM 处理顺序 = 停拉取 → 排空队列 → 刷 sink → 保存位点 → `Resign`。配合 `resign_on_shutdown`，滚动发布时切换时间 ≈ 排空时间，而非等 TTL。

### 4.3 选举客户端实现要求

| 要求 | 实现 |
|---|---|
| 会话失败不退出 | 退避重试循环，goroutine 永不退出 |
| 不热自旋 | Campaign 失败指数退避 |
| 停机显式让位 | 应用 ctx + 停机显式 Resign |
| follower 持续观测 | 持续 watch leader key，不只看一次 |
| 无主态显式化 | Resign 删除指针；watch 容忍暂缺并显示「无主」 |
| 路径零硬编码 | 列举 `{prefix}/election/` 前缀 + nodes 注册，全部从配置派生 |
| term 可追溯 | revision 作 term，落日志与位点 |
| 事件不乱序 | 状态机由选举 goroutine 独占驱动 + atomic role，不共享 chan |

## 5. 位点存储设计

### 5.1 位点模型

```jsonc
// value of {prefix}/position（或 position_key 指定路径）
{
  "v": 1,                           // 位点格式版本（格式演进时向前兼容判断）
  "name": "mysql-bin.000123",       // file
  "pos": 45678,                     // position
  "gtid_set": "",                   // use_gtid=true 时填充
  "seq": 987654321,                 // 对应 watermark 序号（排障用，重启不依赖）
  "leader_term": 123456,
  "updated_at": "2026-10-28T12:00:00+08:00",
  "node_id": "node-1"
}
```

- `position_key` 配置项（默认 `{prefix}/position`）满足「可配置 key 存储」需求；不同集群可指向同一路径做迁移。
- **兼容迁移**：位点播种经 Web `POST /api/v1/position`（confirm=SET-POSITION）或 etcdctl 写位点 key。无 CLI 迁移命令（运维统一走 Web）。

### 5.2 保存协议

- 触发：XID 事件（节流 3s）、rotate（强制）、DDL barrier 完成（强制）、优雅停机（强制）、全量交接完成（强制）。
- 写法：etcd `Put`（etcd 本身线性一致，无需 CAS version）；Put 前复核 leadership（§4.2-2）。
- 失败处理：写失败 **不关闭管线**，指数退避重试 + `position_save_failed_total` 计数 + 告警；连续失败超过 `position_save_max_fail`（默认 30 次/1min）才熔断停管线——因为位点写不进去继续消费会放大重放窗口，停机反而是正确行为，但要先重试并告警。
- 崩溃窗口：位点至多落后已投递数据 `position_save_interval_ms` → 重启重放该窗口（at-least-once，幂等消化）。

### 5.3 客户端治理

- 单一 import：`go.etcd.io/etcd/client/v3`，**版本固定 v3.6.x，实施期锁 v3.6.8**（v3.6.10+ 要求 Go ≥1.25，与 Go 1.24 硬锁冲突；版本策略：不追最新 minor，取其后 1~2 个 minor；v3.6 客户端兼容 v3.5+ 服务端，服务端升级不受客户端绑定牵制，未来升 Go 后可平移到 v3.6 最新 patch）；`client.KV`（内置 balancer/failover），**禁用** `clientv3.NewKV`。
- 多 endpoint、`DialTimeout` 5s、每 op `Timeout` 3s + `MaxCallSendMsgSize` 合理值。
- TLS（`--cacert/--cert/--key`）与用户名/密码支持，配置项齐全。
- 读写均封装「重试 3 次 + 指数退避」，避免瞬时错误直接上抛。

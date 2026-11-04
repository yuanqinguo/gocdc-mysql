# 03 · 配置模型 v2

## 1. 结构定义

### 1.1 顶层

```yaml
cluster: mall-aggregate          # 不变：flag 裁决优先，etcd key 前缀唯一来源（03 §3.1）

node: { ... }                    # 不变：cluster 级节点身份
ha: { ... }                      # 不变：cluster 级选主

pipeline: { ... }                # 集群级默认（源可覆盖，见 §1.3）
stock: { ... }                   # 集群级默认（源可覆盖）

sources:                         # ★ 新增：同步源数组，1..64 条
  - id: order-center             # ★ 源标识，规则见 §2.1
    source: { ... }              # 现行 Source 块原样下沉
    targets: { ... }             # 现行 Targets 块原样下沉（该源独立）
    rules: [ ... ]               # 现行规则组原样下沉（作用域收敛到该源）
    pipeline: { ... }            # 可选：覆盖集群级默认（未给字段逐项继承）
    stock: { ... }               # 可选：覆盖集群级默认

observability: { ... }           # 不变：web/metrics/log 是进程级端点，cluster 共享
```

Go 结构草案（`internal/config/config.go` 改造）：

```go
type Config struct {
    Cluster   string `json:"cluster"`
    Node      Node   `json:"node"`
    HA        HA     `json:"ha"`
    Pipeline  Pipeline  `json:"pipeline"`   // 集群级默认
    Stock     Stock     `json:"stock"`      // 集群级默认
    Sources   []SourceSpec `json:"sources"` // ★ 替代原 Source/Targets/Rules 三字段
    Observability Observability `json:"observability"`
}

// SourceSpec 一条同步源的配置（数据面）。
type SourceSpec struct {
    ID       string   `json:"id"`
    Source   Source   `json:"source"`
    Targets  Targets  `json:"targets"`
    Rules    []Rule   `json:"rules"`
    Pipeline *Pipeline `json:"pipeline,omitempty"` // nil = 全继承集群默认；非 nil 时未给字段逐项继承
    Stock    *Stock    `json:"stock,omitempty"`
}

// ResolvePipeline/ResolveStock 合并解析：源覆盖 > 集群默认（指针字段区分「未设」与「设为零值」）。
func (s SourceSpec) ResolvePipeline(base Pipeline) Pipeline { ... }
```

**为什么 targets/rules 不做集群级共享**：两个源共享一个 targets 段会
诱导「共享连接池」的实现倾向，而连接共享需要引用计数与关闭协调，收益
（省几十个连接）不抵复杂度；且「每源独立下游」正是多集群等价性的一部分。
指向同一目标实例的两个源各建各的池，容量乘数在 [05 §5](05-runtime.md)
显式管理。

### 1.2 源 id 规则

- `id` 即 source_id（行文与键空间中简写 sid）。
- 字符集：`^[a-z][a-z0-9_-]{1,31}$`（小写字母开头，总长 2~32，与
  cluster/topic 前缀字符集风格一致）。
- cluster 内唯一（校验）。
- `default` 为 etcd 旧键迁移的落点 id（§4；2026-11 起配置正文不再自动
  包装，id 一律显式声明，`default` 无保留语义但升级场景须用它与键迁移
  对齐）。
- 用途：etcd key 段、指标 `source` label、API 路径段、日志字段、
  envelope `source` 字段——同一个值贯穿全部面。

### 1.3 pipeline/stock 的继承语义

集群级 `pipeline:`/`stock:` 是默认模板；源级同名段是**覆盖补丁**：

- 段整体省略 → 全继承。
- 段存在但只给部分字段 → 该部分覆盖，其余继承集群默认，最后统一过
  `ApplyDefaults` 填周期类缺省。实现上源级用指针结构体（区分
  「未设」与「显式 0」），合并发生在 `Load` 之后的规范化阶段，校验与
  运行时看到的每个源都是**已合并的完整值**（与现行单源视角一致）。

示例——重账单源给更高并行度，其余源用默认：

```yaml
pipeline: { workers: 4, batch_size: 500 }   # 集群默认
sources:
  - id: billing
    pipeline: { workers: 8 }                # 只覆盖 workers，其余继承
  - id: user
    # 省略段 → 4 workers
```

## 2. 校验规则（validate.go 增改清单）

`Validate` 维持「全部错误一次报出」的现行风格（load.go:79-84）。

### 2.1 新增校验

| # | 规则 | 错误信息要点 |
|---|------|-------------|
| V1 | `sources` 非空且 ≤ 64 条 | 「sources 至少 1 条、至多 64 条（容量规划见 PRD 05 §5）」 |
| V2 | 每个源 id 匹配 `^[a-z][a-z0-9_-]{1,31}$` 且 cluster 内唯一 | 指明冲突的两个下标 |
| V3 | 现行全部**源内**校验逐源执行：source 必填项、targets 至少启用一个、rules 非空、一组一库（源内）、表级字段、mask/exclude、TLS 成对、sinks↔targets 交叉（规则声明 mysql sink 须启用 targets.mysql；ddl_sync 仅 MySQL 直写下游）…… | 错误信息带 `[sources/{sid}]` 前缀定位 |
| V4 | **跨源目标不相交**（D6）：MySQL `(addr, target_database, table)` 三元组、Kafka `(排序后 brokers, topic)`、RocketMQ `(排序后 name_servers, topic)` 各自全局唯一 | 报出冲突的两个源与目标名 |
| V5 | 跨源 `server_id` 冲突提示：两个源 `source.addr` **完全相同**且 server_id 相同 → 报错（同实例互踢，必然踩）；addr 不同则无法判定，文档提示人工确认（§5） | — |
| V6 | 源级 pipeline/stock 覆盖后的合并值逐源过现行范围校验（workers 1..64 等） | — |

### 2.2 修改的校验

- 原「cluster 字段必须等于 flag 裁决值」不变。
- 原 `rules` 校验循环外层套 `for each source`；`schemaSeen` 去重集合
  **每源独立**（两个源各自同步自己的 `mall.orders` 是合法的——只要
  目标三元组不冲突，V4 把关）。

### 2.3 放宽/不变的校验

- `exclude_schemas` 四系统库必须在列：逐源不变。
- `targets.mysql.max_open_conns` 必须显式：逐源不变（每源一个池，
  各自声明自己的上限）。

## 3. 跨源目标不相交校验（V4）的判定算法

加载后、校验时对全部源编译 effective 规则（复用
`internal/pipeline/rulecompile.go` 的编译产物，表级 `target_database`
解析链「表级 > 组级 > targets.mysql.target_database > 同名」不变，
`cmd/gocdc/components.go:70-78`）：

```
mysql 目标键  = lower(addr) + "/" + lower(resolveTargetDB(src, rule, table)) + "/" + lower(table)
kafka 目标键  = join(sort(brokers), ",") + "/" + topic   // topic = 规则覆盖或 prefix+{db}_{table}（点折叠）
rocketmq 目标键 = join(sort(name_servers), ",") + "/" + topic
```

三类各自建 set，重复即收集为错误。注意与现行行为的差异：**单源部署
下 V4 恒不触发**（集合大小 = 源数 = 1），零回归风险。

## 4. v1 顶层形态移除（2026-11 用户决策）

落地之初的「v1 自动包装为 `sources[default]`」兼容策略已移除：配置面
统一 `sources[]`，单源即数组一条——统一一个处理逻辑，不保留双形态。

- `Load` 在 ${ENV} 解析后检测顶层 `source/targets/rules` 任一键 → 报错
  并给改写指引（`sources: [{id: ..., source/targets/rules: ...}]`），
  绝不静默忽略（根结构体无这些字段，无检查会被无声丢弃）。
- 升级路径：旧正文人工包一层
  `sources: [{id: "default", source/targets/rules 原样内嵌}]`——id 须填
  `default` 与键迁移落点（PRD 04 §4）对齐，位点无感续用；改用其他 id
  须先用 etcdctl 手工搬键（RUNBOOK §9.4/§9.3）。
- v1 顶层 `pipeline/stock` 段改写后保持顶层位置（即集群默认模板），被
  唯一源继承，值不变。

## 5. 完整示例（两源聚合 + 混合下游）

```yaml
# 场景：订单中心 + 用户中心两个源实例，聚合进一个 MySQL 目标库，
# 订单源另发 Kafka；用户源带宽小用默认管线参数。
cluster: mall-aggregate

node: { id: "", advertise_addr: "" }
ha: { lease_ttl_seconds: 10, leader_check_interval_ms: 2000, resign_on_shutdown: true }

pipeline: { workers: 4, worker_queue_size: 8192, batch_size: 500,
            flush_interval_ms: 200, position_save_interval_ms: 3000 }
stock: { lock_mode: auto, chunk_rows: 5000, throttle_ms: 0, workers: 4 }

sources:
  - id: order-center
    source:
      addr: "10.0.0.11:3306"
      user: repl
      password: ${ORDER_SRC_PASSWORD}
      charset: utf8mb4
      flavor: mysql
      server_id: 99001                 # 显式：与该实例上并存的其他订阅者错开
      timezone: Asia/Shanghai
      use_gtid: true
      exclude_schemas: [mysql, sys, information_schema, performance_schema]
    targets:
      mysql:
        enabled: true
        addr: "mysql-dst:3306"
        user: writer
        password: ${TGT_PASSWORD}
        max_open_conns: 20             # 每源独立池：总连接 = Σ各源
        tx_chunk_rows: 500
        target_database: mall_merged   # 聚合库（表名不可改，V4 保证不与 user-center 撞表）
        ddl_sync: true
      kafka:
        enabled: true
        brokers: [ "kafka-1:9092", "kafka-2:9092" ]
        topic_prefix: cdc_oc_          # 命名空间隔离：不同源前缀不同
        order_by: row
        max_message_bytes: 4MB
    rules:
      - schema: order_db
        sinks: { mysql: true, kafka: true }
        ddl_policy: auto
        tables:
          - { table: orders,     order_by_column: id, do_stock: true }
          - { table: order_items, order_by_column: id, do_stock: true,
              mask_columns: { buyer_phone: phone } }
    pipeline: { workers: 8 }           # 重负载源覆盖

  - id: user-center
    source:
      addr: "10.0.0.12:3306"
      user: repl
      password: ${USER_SRC_PASSWORD}
      charset: utf8mb4
      flavor: mysql
      server_id: 99002
      timezone: Asia/Shanghai
      use_gtid: true
      exclude_schemas: [mysql, sys, information_schema, performance_schema]
    targets:
      mysql:
        enabled: true
        addr: "mysql-dst:3306"         # 同一目标实例：独立连接池（等价两个集群各写各的池）
        user: writer
        password: ${TGT_PASSWORD}
        max_open_conns: 10
        tx_chunk_rows: 500
        target_database: mall_merged
        ddl_sync: true
    rules:
      - schema: user_db
        sinks: { mysql: true }
        ddl_policy: pause
        tables:
          - { table: users, order_by_column: id, do_stock: true }
          - { table: user_profiles, order_by_column: id, do_stock: true }

observability:
  metrics_addr: ":9595"
  web: { enabled: true, addr: ":8060", username: admin, password: ${WEB_PASSWORD} }
  log: { level: info, json: false }
```

**server_id 说明**：源 `server_id` 缺省（0）时按
`fnv32a(node_id + ":" + sid)` 派生（现行 `deriveServerID` 的推广，
`cmd/gocdc/identity.go:26-36`）——同进程多源天然互异；同一源实例被
多个源订阅（同 addr）时 V5 强制显式配置且互异。跨进程/跨集群的
server_id 协调仍属部署责任（现行语义不变）。

## 6. 敏感项管理

多源后一份配置里出现 N 份源密码 + 每源目标密码。现行机制原样适用，
数量增加不改变机制：

- `${ENV}` 占位注入（`internal/config/env.go`），未设置的变量在加载期
  带路径报错——多源下错误信息同样带 `[sources/{sid}]` 前缀定位；
- `/api/v1/config` 输出打码（`password`/`secret_key` → `***`，
  `cmd/gocdc/admin.go:87-122`）——递归遍历天然覆盖数组元素，无需改动；
- Kafka `password_env`、mask `salt_env` 存环境变量名不存值的策略不变。

建议（非强制）：每源用独立环境变量名（如 `ORDER_SRC_PASSWORD` /
`USER_SRC_PASSWORD`），避免一处泄密全线失守。

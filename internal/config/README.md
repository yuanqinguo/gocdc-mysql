# gocdc 配置使用指南

本目录是 gocdc 的全量配置实现。本文面向**使用者**：配置放在哪、怎么写、每个字段
什么含义、校验失败怎么修。

- 权威设计：[docs/arch/03-etcd-ha-config.md](../../../docs/arch/03-etcd-ha-config.md) §3
- 代码地图：`config.go` 结构体模型 · `load.go` 加载与默认值 · `validate.go` 严格校验 · `env.go` `${ENV}` 解析 · `mask.go` 脱敏算法

> 配置正文为 **YAML**（唯一格式，不做 JSON 兼容）。
> YAML 支持 `#` 注释，下文样例可直接复制。

---

## 1. 配置如何被加载（fail-fast 全流程）

```text
gocdc                        # 零参数（GoLand 调试形态）
gocdc -config-file <yml>     # 指定引导/配置文件
ETCD_HOSTS=<addrs> gocdc     # 公司约定形态：env 给 etcd，零 flag
gocdc -cluster <n> -etcd <addrs> [-config-key <path>] [-config-file <yml>]   # flag 覆盖/纯 flag 形态
  │
  ├─ ⓪ 启动裁决（flag > 环境变量 > 引导文件 > 终极默认 > 报错，§2.3）：
  │     定位引导文件（-config-file 或工作目录 config.local.yml）→ 得出
  │     cluster（终极默认 gocdc-mysql）/ 配置来源 / 协调面 etcd 地址
  │     （-etcd > ETCD_HOSTS > bootstrap.etcd_addrs；与配置中心可不同实例）
  │
  ├─ file 形态（引导文件即全量配置，开发）
  └─ etcd 形态 / 纯 flag     → 读配置中心 etcd key：
        │                       默认 /config/edu.mall.gocdc/system（公司约定固定键）；
        │                       多集群共享 etcd 时以桩 key / -config-key 显式区分
        ① 解析为配置树（YAML 唯一格式，不做 JSON 兼容）
           + ${ENV} 占位替换（引用了未设置的环境变量 → 报错）
        ② cluster 裁决：配置体里的 "cluster" 必须 == 裁决后的 cluster，否则拒绝启动
        ③ 填默认值（数值字段填 0/省略 → 使用默认值，见 §8）
        ④ 严格校验：收集全部问题，一次性打印错误清单后退出
        │
        └─ 全部通过才继续启动。不做 watch、不做热更新：改配置必须重启进程生效。
```

三条铁律（设计文档 03 §3.1）：

1. **fail-fast**：etcd 读取失败、key 不存在、任何校验错误 → 启动即退出，绝不带
   空/旧/部分配置运行。
2. **键与集群分工**：`-cluster`（终极默认 `gocdc-mysql`）决定**协调面** key 前缀
   `/gocdc/{cluster}/`（位点/选举）；**配置中心** key 终极默认
   `/config/edu.mall.gocdc/system`（公司约定固定键，不随集群变）——多集群共享
   同一配置中心 etcd 时，各集群以引导桩 `config_center.key` 或 `-config-key`
   显式区分。配置体里的 `cluster` 字段只是冗余校验（防止把 A 集群的配置错装给
   B 集群）。
3. **错误一次报全**：校验不做短路，一次列出所有问题，改完一处不用反复重启试错。

---

## 2. 配置放哪：etcd（生产）vs 本地文件（开发）

### 2.1 生产：配置写入 etcd，单 key

```bash
# 默认键 = 公司约定固定键 /config/edu.mall.gocdc/system
etcdctl put /config/edu.mall.gocdc/system < config.yaml

# 公司约定形态：ETCD_HOSTS 给 etcd，零 flag 启动（cluster 默认 gocdc-mysql）
ETCD_HOSTS=10.0.5.1:2379 gocdc

# 或显式 flag（多副本用同一份配置，靠 etcd 选举决定谁是 leader）
gocdc -etcd 10.0.5.1:2379,10.0.5.2:2379
```

- 单环境单服务：默认键即用，零 flag（有 ETCD_HOSTS 时）或只给 `-etcd`。
- **多集群共享同一配置中心 etcd**：各集群显式区分配置键——引导桩
  `config_center.key` 或 `-config-key`（惯例 `/gocdc/{cluster}/config`）；
  协调面前缀 `/gocdc/{cluster}/` 仍按集群天然隔离。
- 配置里含密码等敏感信息时用 `${ENV}` 占位（§3），配合 etcd 自身的 auth/TLS。

### 2.2 开发：本地文件（零命令行参数）

把全量配置放工作目录 `config.local.yml`（file 形态，加一段 `bootstrap` 指协调面
etcd），然后**零参数启动**——GoLand 里 Working directory 设为仓库根，直接 Run：

```yaml
# config.local.yml（file 形态：本文件即全量配置；*.local.yml 已 gitignore）
cluster: dev
bootstrap:
  etcd_addrs: ["127.0.0.1:2379"]   # 协调面 etcd（位点/选举仍走 etcd，不可省）
# ……其余全量配置字段
```

注意两点：

- `bootstrap.etcd_addrs`（或 `-etcd`）仍必配——选举、位点存储仍依赖 etcd，
  本地文件只是替代**配置来源**；
- `cluster` 必须与裁决值一致（flag 覆盖 > 文件字段）。

### 2.3 引导文件与多环境（一个配置中心 etcd 服务多环境）

启动裁决优先级：**flag > 环境变量（仅 etcd：`ETCD_HOSTS`）> 引导文件 > 终极默认
（cluster=`gocdc-mysql`、配置键=`/config/edu.mall.gocdc/system`）> 报错**
（`config.ResolveStartup`，启动方式：零参数读 `config.local.yml`；引导桩；
`ETCD_HOSTS=<addrs> gocdc` 公司约定形态；显式 flag 形态）。

引导文件同一文件两种形态，`bootstrap.config_source` 区分（空 = 自动推断：
有 config_center 则 etcd，否则 file）：

```yaml
# ── 形态一：file（开发，见 §2.2）——文件即全量配置 ──
cluster: dev
bootstrap:
  etcd_addrs: ["127.0.0.1:2379"]

# ── 形态二：etcd（生产引导桩，完整示例见 deploy/config.etcd-stub.example.yml）──
cluster: edu-merge-prod
bootstrap:
  config_source: etcd
  etcd_addrs: ["10.0.5.11:2379"]          # 协调面（位点/选举）——缺省回退配置中心
  config_center:
    etcd_addrs: ["cfg.etcd.internal:2379"] # 配置中心
    # key: "/gocdc/edu-merge-prod/config"  # 多集群共享 etcd 时显式区分；缺省 = 公司固定键
```

**两个 etcd 角色可分离**：

| 角色 | 配置项 | 承担 |
|------|--------|------|
| 配置中心 | `bootstrap.config_center.etcd_addrs`（+ 可选 `key`） | 全量配置存放；key 默认 `/config/edu.mall.gocdc/system`，多集群/多环境显式给 `key` 区分 |
| 协调面 | `bootstrap.etcd_addrs`（缺省回退配置中心） | 位点/选举/DDL 门/审计；前缀 `/gocdc/{cluster}/` 按集群隔离 |

纯 flag 形态（无引导文件）：`-etcd`（或 `ETCD_HOSTS`）必填（`-cluster` 缺省
`gocdc-mysql`），同一 etcd 兼任配置中心与协调面（docker-compose/CI 即此形态）。

---

## 3. `${ENV}` 占位符：敏感信息不进 etcd

任意字符串字段的值都可以写 `${VAR}`，加载时替换为同名环境变量：

```yaml
password: "${GOCDC_SOURCE_PWD}"        # 启动前 export GOCDC_SOURCE_PWD=xxx
web:
  password: "${GOCDC_WEB_PASSWORD}"
```

- 引用了**未设置**的环境变量 → 启动失败，错误信息会给出字段路径（绝不静默替换为空）。
- 支持 K8s Secret 挂载：`envFrom: secretRef` 后配置里直接 `${...}` 引用。
- Kafka SASL 密码更进一步：字段本身就是环境变量**名**（`password_env`，§5.6）。

---

## 4. 最小可用配置（可直接复制）

只包含全部必填项，其余走默认值，可通过所有校验。配置面统一 `sources[]`
（§4.5）：单源部署就是数组一条：

```yaml
cluster: dev
ha: { lease_ttl_seconds: 10 }
pipeline: { workers: 4, worker_queue_size: 8192, batch_size: 500 }
sources:
  - id: main                     # 源标识：^[a-z][a-z0-9_-]{1,31}$，cluster 内唯一
    source:
      addr: "10.0.0.1:3306"
      user: repl
      password: "${GOCDC_SOURCE_PWD}"
      charset: utf8mb4
      flavor: mysql
      server_id: 123456
      timezone: Asia/Shanghai
      exclude_schemas: [mysql, sys, information_schema, performance_schema]
    targets:
      mysql:
        enabled: true
        addr: "10.0.1.1:3306"
        user: writer
        password: "${GOCDC_TARGET_PWD}"
        charset: utf8mb4
        max_open_conns: 20
        tx_chunk_rows: 500
    rules:
      - schema: edu.mall.admin
        sinks: { mysql: true }
        tables:
          - { table: admin_user, order_by_column: id, do_stock: true }
observability:
  metrics_addr: ":9595"
  log: { level: info }
```

必填项速查：

| 字段 | 要求 |
|------|------|
| `cluster` | == `-cluster` flag |
| `sources` | ≥1 条（单源部署同样是数组一条）；顶层 `source/targets/rules` 旧形态已移除，命中即报错 |
| `sources[].id` | ^[a-z][a-z0-9_-]{1,31}$，cluster 内唯一 |
| `sources[].source.addr/user/charset/flavor/timezone` | 非空；`flavor` ∈ mysql/mariadb |
| `sources[].source.server_id` | **可省略**：缺省按 node.id:sid 自动派生（fnv32，etcd 配置中心共享一份配置时各副本天然不同）；显式配置须全网唯一（与所有从库、canal 等订阅者不冲突） |
| `sources[].source.exclude_schemas` | 必须包含四个系统库，只能追加不能移除 |
| `ha.lease_ttl_seconds` | ≥2（建议 10） |
| `pipeline.workers / worker_queue_size / batch_size` | ≥1（workers ≤64） |
| `sources[].targets.*` | 每源至少启用一个下游 |
| `sources[].targets.mysql.max_open_conns / tx_chunk_rows` | 启用 mysql 时必须显式 ≥1 |
| `sources[].rules` | ≥1 组；每组 `schema` 唯一、`sinks` 至少开一个、`tables` 非空且逐表显式 `do_stock` |
| `observability.metrics_addr` | 合法 `addr:port` |

---

## 4.5 源级结构（`sources[]`）与多源

`source/targets/rules` 一律写在 `sources[]` 数组元素内（2026-11 用户决策：
统一一个处理逻辑，单源即数组一条，不再支持/自动包装顶层旧形态）。
单进程可承载多条 MySQL 同步源（1..64）：每源独立的源连接、规则、下游、
位点子树（`/gocdc/{cluster}/sources/{sid}/…`）、全量标记与 DDL 审批；
顶层 `pipeline/stock` 变为**集群级默认模板**，源级同名段是覆盖补丁：

```yaml
cluster: mall-aggregate
ha: { lease_ttl_seconds: 10 }
pipeline: { workers: 4, worker_queue_size: 8192, batch_size: 500 }   # 集群默认
stock: { lock_mode: auto, chunk_rows: 5000 }

sources:                       # 1..64 条
  - id: order-center           # 源标识：^[a-z][a-z0-9_-]{1,31}$，cluster 内唯一
    source: { ... }            # §4 的 source 块（该源独立的源库连接）
    targets: { ... }           # §5 的 targets 块（该源独立连接池/生产者）
    rules: [ ... ]             # §6 的规则组（schema 源内唯一即可）
    pipeline: { workers: 8 }   # 可选覆盖：只覆盖 workers，其余继承集群默认
  - id: user-center
    source: { ... }            # 省略 pipeline/stock 段 → 全继承集群默认
    targets: { ... }
    rules: [ ... ]
```

要点：

| 主题 | 规则 |
|------|------|
| **id** | 小写字母开头，`[a-z0-9_-]`，总长 2~32，cluster 内唯一；etcd 键段/指标 label/API 路径（`/api/v1/sources/{sid}/…`）共用。id 一经部署不可随手改（位点子树跟着 sid 走） |
| **继承** | 源级段省略 = 全继承；段内字段省略 = 该字段继承集群默认；合并后统一填默认值——校验与运行时看到的是每源的完整值 |
| **跨源目标不相交**（校验期拦死） | MySQL：全部源的 effective `(addr, target_database, table)` 三元组全局唯一；Kafka：`(排序后 brokers, effective topic)`；RocketMQ：`(排序后 name_servers, effective topic)`。两源写同一 topic/目标表 = 数据互覆/乱序。跨源写**不同**目标库的同名表不受影响 |
| **同实例多源** | 两个源 `source.addr` 完全相同且 `server_id` 相同 → 报错（互踢必然发生）；必须显式配置互异 server_id。`server_id` 缺省（0）按 `fnv32a(node_id:sid)` 派生，同进程天然互异 |
| **每源独立资源** | targets 是每源独立的连接池/生产者（指向同一实例也各建各的池）；目标库 `max_connections` 按全部源合计规划 |
| **敏感项** | `${ENV}` 占位与 `/config` 打码递归覆盖 `sources[]` 数组元素，机制不变；建议每源独立变量名（如 `ORDER_SRC_PASSWORD`） |

---

## 5. 完整配置样例（逐段解释）

```yaml
cluster: edu-merge-prod

# ── 节点身份：可整体省略，空值运行时自动派生 ──
node:
  id: ""                    # 空 = "{hostname}-{8位随机}"；k8s 建议注入 POD_NAME
  advertise_addr: ""        # 空 = "本机IP:web端口"；Web 页面用它定位节点

# ── 选举：多副本部署时谁拉 binlog ──
ha:
  enabled: true                     # 集群模式开关（缺省开启）：false=单节点，跳过选举
                                    # 直接以 leader 启动管线；位点/标记仍走协调面 etcd
                                    # 租约门槛（session 死亡→管线停，恢复需重启进程）
  lease_ttl_seconds: 10             # 切换时长上限≈TTL；建议 10（过短如 2s 会在网络
                                    # 抖动时误判 leader 死亡、频繁切主）；
                                    # 单节点模式下是 leader 门键/节点注册的租约时长
  leader_check_interval_ms: 2000    # 0 = 默认 2000
  resign_on_shutdown: true          # 优雅停机主动让位，standby 立即接管

# ── 管线：攒批语义 = 攒满 batch_size 条 或 超 flush_interval_ms，先到先发 ──
# （集群级默认模板；源级 pipeline/stock 段可逐项覆盖，见 §4.5）
pipeline:
  workers: 4                        # 1..64；按表分片，同表串行保序、异表并行
  worker_queue_size: 8192           # 每 worker 有界队列；打满反压阻塞拉取
  batch_size: 500
  flush_interval_ms: 200            # 0 = 默认 200
  position_save_interval_ms: 3000   # 0 = 默认 3000；越大 etcd 写越少、重启重复越多
  on_convert_error: "null"          # 值转换失败策略（04 §7）：null | zero | fail_event；
                                    # 空 = null（置 NULL + 计数）。fail_event = 规则 PAUSED + 告警
  decimal_as: string                # DECIMAL 承载：string（无损，默认）| float（有精度风险）
  zero_datetime_as: "null"          # 零日期（0000-00-00…）：null（默认，+计数）| string（原样透传）

# ── 同步源：sources[] 统一形态（单源=数组一条；多源在数组里追加条目）──
sources:
  - id: main                        # 源标识（位点子树/指标 label/API 路径共用）
    # ── 源库：binlog 从哪来 ──
    source:
      addr: "10.0.0.1:3306"
      user: repl                    # 需 REPLICATION SLAVE/CLIENT + SELECT 权限
      password: "${GOCDC_SOURCE_PWD}"
      charset: utf8mb4
      flavor: mysql                 # mysql | mariadb
      server_id: 123456             # 伪从库 ID，可省略：缺省按 node.id:sid 自动派生（共享配置的多副本天然不同）；显式配置须全网唯一，冲突会互相踢下线
      timezone: Asia/Shanghai       # DATETIME 值的解释时区，必须显式声明
      use_gtid: false               # true 时位点含 GTID 集合（源库需开 GTID）
      heartbeat_interval_ms: 3000   # 0 = 默认 3000；低流量库靠心跳推进位点防误判掉线
      read_timeout_ms: 60000        # 0 = 默认 60000
      exclude_schemas: [mysql, sys, information_schema, performance_schema]
                                    # 四系统库必须保留；可追加如 shard_tmp
      tls: { enabled: false, skip_verify: false, ca: "", cert: "", key: "" }
    # ── 下游：至少 enabled 一个；同一规则可同时写多个（每源独立连接池）──
    targets:
      mysql:
        enabled: true
        addr: "10.0.1.1:3306"
        user: writer
        password: "${GOCDC_TARGET_PWD}"
        charset: utf8mb4
        max_open_conns: 20          # 必须显式 ≥1（不设默认：无上限连接会打爆目标库）
        max_idle_conns: 5           # 0 = 默认 5
        conn_max_lifetime_min: 30   # 0 = 默认 30
        tx_chunk_rows: 500          # 单事务最大行数，大事务切批
        ddl_sync: true              # 允许 ddl_policy=auto 的白名单 DDL 在目标端执行
      kafka:
        enabled: true
        brokers: ["10.0.2.1:9092", "10.0.2.2:9092"]
        topic_prefix: "cdc_"        # 默认 topic = 前缀 + "{db}_{table}"（Kafka/RocketMQ 统一规范，点号不收）
        order_by: row               # row: key=主键，同行有序（推荐）；table: 整表全有序
        compression: snappy         # none|gzip|snappy|lz4|zstd
        idempotent_producer: true   # 幂等 producer，重试不重复不乱序
        ack: all                    # all 不丢 | one 低延迟
        send_timeout_ms: 5000       # 0 = 默认 5000
        max_message_bytes: 1048576  # 必填 [1KB,100MB]，且 ≤ broker max.message.bytes
        sasl: { enabled: false, mechanism: "", user: "", password_env: "" }
        tls: { enabled: false, skip_verify: false, ca: "", cert: "", key: "" }
      rocketmq:
        enabled: false              # false 时不校验、不连接
        name_servers: ["10.0.3.1:9876"]
        group: gocdc-producer
        topic_prefix: "CDC_"
        order_by: row
        send_timeout_ms: 5000
        send_concurrency: 32        # 0 = 默认 32；组间并发发送上限（组内仍严格串行）
        acl: { access_key: "", secret_key: "${ROCKETMQ_SECRET}" }
    # ── 规则：组形态白名单——哪个库的哪些表写到哪些下游（详见 §6）──
    rules:
      - schema: edu.mall.admin      # 一组一库
        ddl_policy: auto            # 组级：auto | pause | ignore；省略 = pause
        sinks: { mysql: true, kafka: true }
        tables:                     # 白名单：未列入的表（含未来新表）不同步
          - { table: admin_user, order_by_column: id, do_stock: true,
              mask_columns: { phone: phone } }
          - { table: subjects,   order_by_column: id, do_stock: true }
          - { table: tmp_log,    order_by_column: id, do_stock: false }  # 只增量不快照

# ── 可观测：端口与日志 ──
observability:
  web:
    enabled: true
    addr: ":8060"
    username: admin                 # enabled 时必填，admin API 必须鉴权（HTTP Basic 单凭据）
    password: "${GOCDC_WEB_PASSWORD}"
    auth_disabled: false            # 内网裸奔必须显式 true（配置校验告警）
  metrics_addr: ":9595"             # /metrics /healthz /readyz
  log: { level: info, json: false } # 统一 stdout，容器收集；json=true 便于采集器
```

### 5.1 `source` 的两个易错点

- **`server_id` 冲突**：MySQL 把相同 server_id 的订阅者视为同一从库，互相踢下线、
  位点错乱。**缺省自动按 node.id 派生（fnv32），多副本共享一份配置时天然不同**；
  只有与外部订阅者（canal/datax 等）共源时才需显式分配全网唯一号。
- **`timezone` 为什么必须显式**：MySQL DATETIME 不带时区，若隐式按进程本地时区
  解释，容器默认 UTC 会造成 8 小时漂移。这里强制写清楚，如
  `Asia/Shanghai`。

### 5.2 `targets.kafka.order_by` 怎么选

| 值 | 消息 key | 有序性 | 吞吐 | 适用 |
|----|----------|--------|------|------|
| `row`（推荐） | 主键 | 同一行严格有序 | 高（按行打散到分区） | 绝大多数场景：下游按行 upsert |
| `table` | 库名.表名 | 整表全有序 | 低（单表单分区） | 下游需要表级全局顺序（如按表回放） |

### 5.3 `targets.*` 与 `rules[].sinks` 的关系

`targets` 声明「有哪些下游可用」（连接信息），`rules[].sinks` 决定「每条规则的
数据实际流向哪几个」。规则里打开的 sink 应在 `targets` 中 enabled，否则该规则
等于没有这个出口。

---

## 6. `rules` 详解：组形态白名单（唯一形态，重点）

### 6.1 形态与字段归属

一条规则 = 一个库（组）：组级统一下游与 DDL 策略，`tables` 逐表白名单列出该库
要同步的表；**未列入 `tables` 的表（含未来新建的表）不同步**：

```yaml
- schema: edu.mall.admin
  sinks: { mysql: true }        # 组级必填，至少启用一个
  ddl_policy: auto              # 组级；省略 = pause（安全默认）
  target_database: edu.mall.merge  # 组级可选；省略 = targets.mysql.target_database
  tables:                       # 必填非空（白名单）
    - { table: admin_user, order_by_column: id, do_stock: true }
    - { table: users,      order_by_column: id, do_stock: false,
        mask_columns: { phone: phone } }
```

| 层级 | 可配字段 |
|------|----------|
| 组级 | `schema`、`sinks`（必填）、`ddl_policy`、`target_database` |
| 表级 | `table`、`order_by_column`、`do_stock`（**必填**）、`target_database`（覆盖组级）、`exclude_columns`、`mask_columns` |

约束：

- 一组一库（schema 不可重复）；组内表名不可重复；表名禁 `"*"`/通配符/正则。
- 表级不可写 `sinks`/`ddl_policy`（整组统一）。
- 解码期严格校验未知字段——`table`/`do_stock` 等表级字段写在组条目上
  会被拒绝并指引到本节。
- **Web 编辑走同一校验器**（2026-11）：规则页「编辑规则」保存 =
  `PUT /api/v1/sources/{sid}/rules`——服务端在原始正文上做 yaml.Node 手术
  （只替换该源 rules 段，`${ENV}` 占位/注释/键序保留），随后走与启动完全
  相同的加载链（含本文件全部校验），再做活体预检（源/目标表结构核对本次
  变更表）与 CAS 写；保存后需重启生效（`POST /config/apply-restart`）。

### 6.2 典型场景

**例 1：一个库全量+增量同步，写 MySQL 和 Kafka**

```yaml
- schema: edu.mall.admin
  sinks: { mysql: true, kafka: true }
  ddl_policy: auto
  tables:
    - { table: admin_user, order_by_column: id, do_stock: true }
    - { table: subjects,   order_by_column: id, do_stock: true }
```

**例 2：个别表有差异——admin_user 追加脱敏；tmp_log 只跟增量不做全量；
goal 表单独指定目标库**

```yaml
- schema: edu.mall.admin
  sinks: { mysql: true }
  tables:
    - { table: admin_user, order_by_column: id, do_stock: true,
        mask_columns: { remark: { type: truncate, keep: 32 } } }
    - { table: tmp_log,    order_by_column: id, do_stock: false }
    - { table: goal,       order_by_column: id, do_stock: true,
        target_database: edu.mall.other }
```

**例 3：只同步该库一张表（白名单里只写一行即可）**

```yaml
- schema: edu.mall.log
  ddl_policy: ignore
  sinks: { kafka: true }
  tables:
    - { table: access_log, order_by_column: id, do_stock: false }
```

**要接入新表**：在 `tables` 里加一行，重启生效（白名单语义——新表不自动同步）。

### 6.4 `sinks` 的两种写法

```yaml
sinks: { mysql: true, kafka: true }                        # bool 简写（常用）
sinks: { kafka: { enabled: true, topic: "cdc_custom" } }   # 对象：覆盖 topic/tag（覆盖值同样不收点）
```

- MySQL sink **没有** topic/表名映射字段——目标表固定为源库**同名同表**，从配置
  层面杜绝错配；给它配 `topic`/`tag` 会被校验拒绝。
- topic/tag 只对 Kafka/RocketMQ 有意义；不覆盖时 topic = `topic_prefix + "{db}_{table}"`（两个下游统一规范，点号不收）。

### 6.5 `ddl_policy` 三种策略（08 §2）

| 值 | 行为 | 适用 |
|----|------|------|
| `auto` | 白名单 DDL（ADD COLUMN、varchar 放大等）自动同步目标库继续跑；白名单外自动转 pause。需 `targets.mysql.ddl_sync=true` | 生产默认 |
| `pause`（**省略时默认**） | 任何 DDL → 该表管线暂停 + 告警，人工处理后 resume | 强管控表 |
| `ignore` | 跳过 DDL 继续跑 | 目标结构另有管理人；显式配置 |

---

## 7. `mask_columns` 脱敏算法（10 §1）

值可以是**算法名字符串**或**对象**：

```yaml
mask_columns:
  phone: phone                                   # 字符串简写
  email: { type: email }                         # 对象形式，等价
  remark: { type: truncate, keep: 32 }
  id_card: { type: hash_sha256, salt_env: MASK_SALT }
```

| 算法 | 对象参数 | 效果 |
|------|----------|------|
| `fixed` | `value`（默认 `***`） | 替换为固定值 |
| `hash_md5` / `hash_sha256` | `salt_env`（可选，盐的环境变量名） | 摘要哈希 |
| `phone` | — | `138****1234` |
| `email` | — | `a***@example.com` |
| `truncate` | `keep`（必填 ≥1） | 保留前 keep 字符 + `...` |
| `null` | — | 置为 NULL |

约束：

- `exclude_columns` 与 `mask_columns` 语义互斥，同一列只能出现一边；
- `do_stock=true` 时 `order_by_column` 不可脱敏、不可排除（会破坏快照分块游标）；
- **YAML 注意**：算法名 `null` 必须带引号写作 `"null"`——裸 `null` 会被 YAML
  解析成空值而不是算法名（同理 `"true"/"false"` 等字面量作字符串时建议引号）。

---

## 8. 默认值一览（省略/填 0 时）

| 字段 | 默认 |
|------|------|
| `source.heartbeat_interval_ms` | 3000 |
| `source.read_timeout_ms` | 60000 |
| `ha.leader_check_interval_ms` | 2000 |
| `pipeline.flush_interval_ms` | 200 |
| `pipeline.position_save_interval_ms` | 3000 |
| `pipeline.on_convert_error` | `null`（枚举 null/zero/fail_event，04 §7） |
| `pipeline.decimal_as` | `string`（枚举 string/float） |
| `pipeline.zero_datetime_as` | `null`（枚举 null/string） |
| `targets.mysql.max_idle_conns` | 5 |
| `targets.mysql.conn_max_lifetime_min` | 30 |
| `targets.kafka.send_timeout_ms` / `targets.rocketmq.send_timeout_ms` | 5000 |
| `targets.rocketmq.send_concurrency` | 32 |
| `rule.ddl_policy`（库级省略时） | `pause` |
| `observability.log.level` | `info` |

结构性/安全性字段**没有默认值**（如 `max_open_conns`、`lease_ttl_seconds`、
`max_message_bytes`），必须显式配置——校验会拦截。

---

## 9. 常见校验报错与修法

| 报错关键句 | 原因 | 修法 |
|------------|------|------|
| `cluster=… 与 -cluster flag=… 不一致` | 配置装错了集群 | 检查 key 路径 / `-cluster` 参数 |
| `exclude_schemas 缺少系统库 "…"` | 四系统库被移除 | 补回 mysql/sys/information_schema/performance_schema |
| `max_open_conns=0 必须显式 ≥1` | 省略了连接上限 | 显式配置（安全字段不设默认） |
| `do_stock=true 但 order_by_column 为空` | 全量缺排序键 | 填主键列名 |
| `order_by_column … 不可同时出现在 exclude_columns / 不可脱敏` | 破坏快照游标 | 从 exclude/mask 中移除该列 |
| `列 … 同时出现在 exclude_columns 与 mask_columns` | 语义互斥 | 二选一 |
| `sinks.mysql 不接受 topic/tag` | MySQL 同名同表无映射 | 删掉，改用 kafka/rocketmq 的 topic 覆盖 |
| `不支持的规则组形态（…）` | table/do_stock 等写在了组条目上 | 按组形态重写：表级字段移入 `tables`（§6.1） |
| `… tables 不能为空` | 组缺表白名单 | 逐表列出要同步的表 |
| `… do_stock 必填` | 表条目缺全量声明 | 显式写 `do_stock: true/false` |
| `… schema 重复（一组一库）` | 同库写了两组 | 合并为一组 |
| `… 表级规则重复` | 组内表名重复 | 去重 |
| `max_message_bytes=… 须在 [1KB,100MB]` | 超 broker 上限风险 | 对齐 broker `max.message.bytes` |
| `引用的环境变量 … 未设置` | `${ENV}` 缺值 | 启动前 export / K8s envFrom 注入 |
| `配置 YAML 解析失败` | 正文语法错误 | 按错误里的行/列定位 |

排查方法：直接启动进程即可——校验**不短路**，一次打印全部错误清单，改完再启动。

---

## 10. FAQ

**Q：改了配置怎么生效？**
A：重启进程。设计上明确不做 watch/热更新（03 §3.1-5）；Web 端写回 etcd 后也会
提示需重启。

**Q：多套环境共用一个 etcd？**
A：key 前缀 `/gocdc/{cluster}/` 天然隔离；`-cluster` 不同、配置体 `cluster` 对应
即可。配置体 `cluster` 与 flag 不一致会拒绝启动，防误装。

**Q：想让不同表进不同 Kafka topic？**
A：sinks 整组统一（组形态设计取舍）——topic 覆盖写在组级
`sinks.kafka.topic`；确需按表独立 topic 时，把该表拆成单独一组（同 schema
拆两组不允许，可为其单独建组或调整 topic_prefix + 表名派生）。

**Q：密码不想明文进 etcd？**
A：所有敏感字段支持 `${ENV}`（§3）；Kafka SASL 密码固定走 `password_env`。

**Q：只想要增量、不要全量？**
A：该表 `do_stock: false` 即可（此时无需 `order_by_column`；§6.2 例 2 的
tmp_log）。全量表则必须显式 `do_stock: true` 并给 `order_by_column`。

**Q：配置能用 JSON 写吗？**
A：不支持。配置正文仅接受 YAML（JSON 值多数恰好是合法 YAML flow 语法，
但这不是承诺的支持面）。

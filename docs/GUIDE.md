# gocdc 使用指导（从零上手）

> 面向读者：第一次接触 gocdc 的运维/开发。照本文可以完成：源库/目标库准备 → 单点或集群启动 → MySQL/Kafka/RocketMQ
> 下游配置 → DDL 审批 → 常用运维。
>
> 配套文档：[RUNBOOK.md](RUNBOOK.md)（故障处置动作）、[TESTCASES.md](TESTCASES.md)（验收清单）、docs/arch/（架构设计）。

---

## 1. 一句话架构

```
MySQL(ROW binlog) → gocdc（etcd 协调：选主/位点/全量标记/DDL 审批）
                        ├→ 目标 MySQL（upsert 幂等直写，同名库或指定库）
                        ├→ Kafka（envelope JSON，key=主键）
                        └→ RocketMQ（envelope JSON，message group=主键保序）
```

- **二进制 = 纯服务**，没有运维子命令。全部运维动作走 Web API（`/api/v1`，HTTP Basic + confirm 门禁）。
- 单点跑（一个 gocdc 进程）或集群跑（2+ 进程，etcd 选主，一主多备）都支持，配置只差 `node.id`/端口。

---

## 2. 前置要求

### 2.1 源库（MySQL，必须满足）

```sql
-- ① 参数要求（my.cnf 或启动参数）
--    binlog_format=ROW          # 必须，否则启动直接拒绝
--    binlog_row_image=FULL      # 必须，UPDATE 前像依赖
--    log_bin=ON                 # 必须
--    server_id=1                # 与 gocdc 的 server_id 不同即可
--    gtid_mode=ON + enforce_gtid_consistency=ON   # 强烈建议（主从切换免手工修位点）

-- ② 复制账号（三权 + 快照锁两权）
CREATE USER 'repl'@'%' IDENTIFIED BY '<密码>';
GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'repl'@'%';
-- 全量快照需要（二选一，推荐都给）：
GRANT RELOAD, BACKUP_ADMIN ON *.* TO 'repl'@'%';
--    RELOAD      → FTWRL 毫秒级锁快照（默认，最一致）
--    BACKUP_ADMIN→ LOCK INSTANCE FOR BACKUP（不阻塞 DML，8.0+）
--    都不给      → 启动报错，要求显式配 stock.lock_mode="none"（无锁近似一致，不推荐）
```

### 2.2 目标库（MySQL，必须满足）

- **同名库表必须预先存在**（列集一一对应）。gocdc 不替你建首次表——但增量期的
  `ADD COLUMN`/扩容/注释，以及白名单表的目标缺表补建（CREATE TABLE），可自动同步
  或 Web 审批（见 §6 DDL）；白名单外表的建表事件一律过滤。
- 账号要有目标库表的 INSERT/UPDATE/DELETE（+开 ddl_sync 时的 DDL 权限）。

### 2.3 协调面

- **etcd ≥3.5**：单点也要（存位点/标记），3 节点集群用于生产。
- 启动参数里 `-etcd` 指向它；所有状态在 key 前缀 `/gocdc/{cluster}/` 下。

### 2.4 MQ 下游（可选）

| 下游 | 要求 |
|------|------|
| Kafka | broker ≥0.11（幂等 producer）；topic 可自动创建或预建 |
| RocketMQ v5 | **gRPC proxy 端口**（默认 8081，不是 9876！）；**topic 必须预建且带 FIFO 属性**（见 §7.2） |

---

## 3. 十分钟最快上手（单节点 MySQL→MySQL）

### 3.1 docker compose 一键（体验）

```bash
cd deploy
docker compose up -d        # 起 etcd + 源库 + 目标库 + gocdc 单节点
# 集群演示： docker compose --profile cluster up -d
```

### 3.2 手工三步

> **GoLand 调试（零命令行参数）**：把配置放工作目录 `config.local.yml`
> （文件末尾加一段 `bootstrap: { etcd_addrs: ["127.0.0.1:2379"] }` 指协调面 etcd），
> Working directory 设为仓库根，直接 Run main.go——进程自动读该文件，位点/选举
> 仍走 `bootstrap.etcd_addrs` 的 etcd。生产引导桩（配置在配置中心、多环境隔离）
> 见 `deploy/config.etcd-stub.example.yml` 与 internal/config/README.md §2.3。

**① 写配置文件** `config.yml`（完整字段见 §4，能跑的最小集如下）：

```yaml
cluster: prod
node: { id: node1, advertise_addr: "" }
ha: { lease_ttl_seconds: 10, leader_check_interval_ms: 2000, resign_on_shutdown: true }
pipeline: { workers: 4, worker_queue_size: 8192, batch_size: 500, flush_interval_ms: 200, position_save_interval_ms: 3000 }
sources:                          # 统一 sources[]：单源=数组一条
  - id: main
    source:
      addr: "10.0.0.1:3306"
      user: repl
      password: ${SRC_PWD}
      charset: utf8mb4
      flavor: mysql
      server_id: 99001
      timezone: Asia/Shanghai
      exclude_schemas: [mysql, sys, information_schema, performance_schema]
    targets:
      mysql:
        enabled: true
        addr: "10.0.0.2:3306"
        user: gocdc
        password: ${DST_PWD}
        charset: utf8mb4
        max_open_conns: 20
        tx_chunk_rows: 500
        ddl_sync: true
    rules:                        # 组形态白名单：组级统一，表清单逐表列出
      - schema: order_db
        sinks: { mysql: true }
        tables:
          - { table: orders, order_by_column: id, do_stock: true }
observability:
  metrics_addr: ":9595"
  web:
    enabled: true
    addr: ":8060"
    username: admin
    password: ${WEB_PASSWORD}
    auth_disabled: false
  log: { level: info, json: false }
```

要点：
- `${SRC_PWD}` 这类占位从环境变量解析，缺失启动报错——**密码不进配置文件明文**。
- `timezone` **必须等于源库实际写入时区**（`SELECT @@system_time_zone`）。配错 TIMESTAMP 会漂移（UTC 库配 Asia/Shanghai = 差 8 小时）。启动检查器会告警。
- `cluster` 是 key 前缀 `/gocdc/prod/` 的来源，必须与 `-cluster` 参数一致。
- 配置正文为 YAML（唯一格式，不做 JSON 兼容）；含 `#`、`:` 等特殊字符的值（如密码）建议加引号。

**② 起服务**：

```bash
export SRC_PWD=xxx DST_PWD=xxx WEB_PASSWORD=admin123
./gocdc -cluster prod -etcd 10.0.0.10:2379 -config-file ./config.yml
```

**③ 验证**：

```bash
curl http://127.0.0.1:9595/healthz        # ok
curl http://127.0.0.1:9595/readyz         # ready（管线运行中才 ready）
curl -u admin:admin123 http://127.0.0.1:8060/api/v1/status
# 写入源库 → 目标库出现数据；对账：
curl -u admin:admin123 -X POST -H 'Content-Type: application/json' \
  -d '{"database":"order_db","table":"orders","sample":100}' \
  'http://127.0.0.1:8060/api/v1/verify?confirm=true'
```

---

## 4. 配置详解（全字段）

### 4.1 source（源库，从哪读 binlog）

| 字段 | 必填 | 说明 |
|------|------|------|
| `addr` | ✅ | `host:port` |
| `user` / `password` | ✅ | 复制账号（权限见 §2.1）；password 支持 `${ENV}` |
| `charset` | ✅ | 一般 `utf8mb4` |
| `flavor` | ✅ | `mysql` \| `mariadb` |
| `server_id` | 可省略 | 缺省按 node.id 自动派生（多副本共享一份配置时天然不同）；与外部订阅者（canal 等）共源时才需显式分配全网唯一号，撞了互踢 |
| `timezone` | ✅ | **必须=源库写入时区**（TIMESTAMP 正确性的关键） |
| `use_gtid` | 建议 true | 源库开 GTID 时配 true，位点带 gtid_set，主从切换免手工修位点 |
| `heartbeat_interval_ms` / `read_timeout_ms` | 可省 | 0=默认 3000/60000 |
| `exclude_schemas` | 建议 | 额外排除的库；四个系统库强制排除不可移除 |
| `tls` | 可选 | `{enabled,skip_verify,ca,cert,key}` |

### 4.2 targets.mysql（MySQL 直写）

可选下游：`enabled: false` 即纯 MQ 部署（事件只进 Kafka/RocketMQ，无目标
库——对账/DDL diff/verify 随之不可用）；规则声明 `sinks.mysql` 的源必须
启用本段（配置校验拦死）。

| 字段 | 说明 |
|------|------|
| `addr/user/password/charset` | 目标库连接；目标**同名库表须预建** |
| `target_database` | 空=与源库同名（推荐）；填了则该实例所有规则写这个库。**规则级同名覆盖**：`rules[].target_database`（库级/表级）解析优先于全局 |
| `max_open_conns` | 必须 ≥1（连接池上限）；`max_idle_conns` 默认 5 |
| `tx_chunk_rows` | 单事务最大行数（500），大事务切块提交 |
| `ddl_sync` | true 时允许 auto/审批策略在目标执行白名单 DDL（仅 MySQL 直写下游；mysql 未启用时配置校验拒绝） |

### 4.3 targets.kafka（Kafka 下游）

```json
"kafka": {
  "enabled": true,
  "brokers": ["10.0.1.10:9092", "10.0.1.11:9092"],
  "topic_prefix": "cdc_",
  "order_by": "row",              // row=按主键分区内有序（推荐）；table=整表有序
  "compression": "snappy",
  "idempotent_producer": true,    // broker≥0.11 建议开
  "ack": "all",                   // all 不丢；one 低延迟
  "send_timeout_ms": 5000,
  "max_message_bytes": 1048576,   // 必填 1KB~100MB，须 ≤ broker max.message.bytes
  "sasl": { "enabled": true, "mechanism": "scram-sha256", "user": "u", "password_env": "KAFKA_SASL_PWD" },
  "tls": { "enabled": true, "ca": "/path/ca.pem" }
}
```

topic 命名：`topic_prefix + "{库}_{表}"`（如 `cdc_order_db_orders`；Kafka/RocketMQ 统一规范，点号一律不收，见 §7.1）；规则级覆盖见 §7.3。

### 4.4 targets.rocketmq（RocketMQ v5 下游，gRPC）

```json
"rocketmq": {
  "enabled": true,
  "name_servers": ["10.0.2.10:8081"],   // v5 proxy 的 gRPC 端口（8081），不是 namesrv 9876！
  "group": "gocdc-producer",
  "topic_prefix": "CDC_",
  "order_by": "row",
  "send_timeout_ms": 5000,
  "acl": { "access_key": "ak", "secret_key": "${RMQ_SK}" }
}
```

**topic 必须预建 + FIFO**（客户端只拉路由不建 topic），见 §7.2。
topic 命名：`topic_prefix + "{库}_{表}"`（如 `CDC_order_db_orders`；与 Kafka 同一套规范，点号一律不收）。

### 4.5 rules（同步规则：哪个库表 → 哪些下游 → 怎么处理）

**组形态白名单**（唯一形态；一组一库，未列入 tables 的表不同步）：

```yaml
rules:
  - schema: order_db
    sinks: { mysql: true, kafka: true }   # 组级统一（bool 简写或对象覆盖 topic/tag）
    ddl_policy: pause                      # pause(默认)/auto/ignore
    tables:
      - { table: users,    order_by_column: id, do_stock: true,
          mask_columns: { phone: phone, id_card: hash_sha256 } }
      - { table: orders,   order_by_column: id, do_stock: true }
      - { table: tmp_log,  order_by_column: id, do_stock: false }   # 只增量不快照
```

| 字段 | 层级 | 取值/语义 |
|------|------|----------|
| `schema` | 组 | 源库名（允许点） |
| `sinks` | 组 | 至少启用一个下游；整组统一 |
| `ddl_policy` | 组 | `pause`（默认，DDL 挂起等 Web 审批）/ `auto`（白名单 DDL 自动同步：加列/varchar 扩容/注释）/ `ignore` |
| `target_database` | 组/表 | 目标库覆盖；省略 = targets.mysql.target_database |
| `table` | 表 | 白名单表名（禁 `*`/通配符） |
| `do_stock` | 表 | **必填**：true 首次全量快照；false 只增量不快照 |
| `order_by_column` | 表 | do_stock=true 时必填（全量分块排序键） |
| `exclude_columns` / `mask_columns` | 表 | 排除列 / 脱敏列 |
| `exclude_columns` | 列不同步（目标表不应有该列，或该列可空有默认） |
| `mask_columns` | 值脱敏：`"phone"` 短格式=内置算法名；对象格式 `{"type":"truncate","keep":32}`（字段：`type/value/keep/salt_env`）；内置算法：`fixed:<v>` / `hash_md5` / `hash_sha256` / `phone` / `email` / `truncate:<n>` / `null` |
| `sinks` | bool 简写或对象，整体替换（表级写哪个哪个生效） |
| `sinks.{kafka,rocketmq}.topic` | 表级 topic 覆盖（对象形式）：`"sinks": {"kafka": {"enabled": true, "topic": "cdc_custom"}}`（覆盖值同样不收点）；`rocketmq` 另有 `tag` 覆盖 |

**规则校验红线**（启动即拒）：exclude∩mask 互斥；mask/exclude 撞目标唯一键告警或拒启；无主键表必须配 `order_by_column`。

### 4.6 ha / pipeline / stock / observability

```json
"ha":       { "lease_ttl_seconds": 10, "leader_check_interval_ms": 2000, "resign_on_shutdown": true },
"pipeline": { "workers": 4, "worker_queue_size": 8192, "batch_size": 500,
              "flush_interval_ms": 200, "position_save_interval_ms": 3000,
              "on_convert_error": "null",   // null|zero|fail_event（值转换失败策略）
              "decimal_as": "string",       // string(推荐，无损)|float
              "zero_datetime_as": "null" }, // null|string
"stock":    { "lock_mode": "auto", "chunk_rows": 5000, "throttle_ms": 0, "workers": 4 },
```

- `ha.lease_ttl_seconds` ≥2（建议 10）：leader 死亡后接管时延上界 = TTL+10s。
- `stock.lock_mode`：`auto`（按权限探测）| `ftwrl` | `backup` | `none`（无锁必须显式声明）。
- `observability.web`：HTTP Basic 单凭据；`auth_disabled: true` 显式裸奔（内网）时凭据可空。

---

## 5. 部署形态

### 5.1 单节点

- 一个 gocdc 进程 + etcd。进程挂了不会自动恢复（systemd/k8s 拉起即可自动续接——位点在 etcd）。
- etcd 短暂不可达：管线在 TTL 内停止（安全态），etcd 恢复后自动恢复。

### 5.2 集群（推荐生产：2+ 节点）

**每个节点**：同一份配置（`cluster` 相同），不同的 `node.id` / `source.server_id` / 端口：

```
node1: server_id=99001, metrics :9595, web :8060
node2: server_id=99002, metrics :9596, web :8061
```

**启动**（每台各自执行）：

```bash
./gocdc -cluster prod -etcd etcd1:2379,etcd2:2379,etcd3:2379 -config-file ./config-node1.yml
```

**行为**：自动选主（恰一 leader 抓 binlog），follower 热备不投递；leader 挂（kill -9）→
follower ≤ TTL+10s 接管；接管从 etcd 位点续传，**窗口期数据零丢失**（binlog 在源库，幂等 upsert）。
etcd 生产用 3 节点集群（quorum 丢失=全停安全态，恢复自动重选）。

**滚动发布**：新版本先发 follower → 对 leader 调 `POST /api/v1/leader/resign?confirm=true`
优雅让位 → 升级原 leader。发布前检查（RUNBOOK §8）：无暂停规则、lag 正常、server_id 不冲突。

**配置生产实践**：配置放 etcd（`etcdctl put /gocdc/prod/config - < config.yml`），
启动只给 `-cluster prod -etcd ...`（不给 -config-file）——**改配置须重启**（不热更）。

---

## 6. DDL：表变化零人工 / 仅 Web 审批

| 场景 | 行为 | 人工动作 |
|------|------|---------|
| auto + 加列/varchar 扩容/注释/白名单表缺表补建 | 目标自动同步 | **零** |
| pause（默认）+ 任何 DDL | 挂起等审批（位点冻结，防止带缺口运行） | Web 一次点击 |
| DROP/RENAME/缩容/改类型（任何策略） | 挂起等审批；**approve 不自动执行**——人工先在目标端执行同样 DDL，approve 只做复验+resume；或 skip（目标保旧结构继续） | 改目标 + 一次点击 |

```bash
# 看挂起
curl -u admin:xxx http://127.0.0.1:8060/api/v1/ddl-events
# 批准（目标自动执行，resume）
curl -u admin:xxx -X POST 'http://127.0.0.1:8060/api/v1/ddl/{event_id}/approve?confirm=true'
# 跳过（审计留痕）
curl -u admin:xxx -X POST 'http://127.0.0.1:8060/api/v1/ddl/{event_id}/skip?confirm=true'
```

审批在**任意节点**的 Web 都能操作（gate 走 etcd）。挂起期间位点冻结是**文档化语义**：
宁可停，不带缺口。

---

## 7. MQ topic 配置

### 7.1 命名规则

| 下游 | 默认 topic（Kafka/RocketMQ 统一规范） | 规则级覆盖 |
|------|-----------|-----------|
| Kafka | `{topic_prefix}{库}_{表}` → `cdc_order_db_orders` | `rules[].sinks.kafka.topic`（对象形式） |
| RocketMQ | `{topic_prefix}{库}_{表}` → `CDC_order_db_orders` | `rules[].sinks.rocketmq.topic` |

两个下游同一套命名：库名含点（如 `edu.mall.admin`）替换为下划线，连接符为下划线。
点号一律不收——RocketMQ topic 字符集本就非法（带点报 ILLEGAL_TOPIC），Kafka 官方
建议 topic 名不混用 `.` 与 `_`（metrics 聚合按 `.`→`_` 折叠，两种写法会同名串味）。
前缀字符集 `[a-zA-Z0-9_-]`。

### 7.2 RocketMQ 必做三步（不做必翻车）

```bash
# ① 预建 topic（v5 客户端只拉路由不建！）
mqadmin updateTopic -n 10.0.2.10:9876 -b 10.0.2.10:10911 -t CDC_order_db_orders
# ② 顺序消息必须 FIFO 属性（message group=key，同主键有序）
mqadmin updateTopic -n 10.0.2.10:9876 -b 10.0.2.10:10911 -t CDC_order_db_orders -a +message.type=FIFO
# ③ broker 若关了 autoCreateTopicEnable，每张表一条命令批量建
```

不做：producer 启动报 TOPIC_NOT_FOUND（启动失败，报错里带本提示）；
FIFO 缺失：发送报 message type not match → 管线 PAUSED。

### 7.3 消息格式（消费端对接）

```json
{
  "event_id": "db.tbl.binlog.000007:1000.0.3",   // 全局唯一，消费端幂等去重键
  "type": "INSERT|UPDATE|DELETE|DDL",
  "phase": "binlog|snapshot",                     // 全量/增量
  "database": "order_db", "table": "orders",
  "pk": {"id": 42},
  "before": {...}, "after": {...},                // UPDATE 成对像（均脱敏后）
  "columns": {"amount": {"type":"decimal","precision":12,"scale":2,"masked":null}, ...},
  "position": {"file":"binlog.000007","pos":1000},
  "gtid": "uuid:1-5", "xid": 99, "ts": 1761624000, "exec_ts": "2026-10-29T..."
}
```

key/message group = `{库}.{表}.{主键值}`（order_by=row）——同主键严格有序。
RocketMQ 侧 message group 同样不收点：组键里的点替换为下划线（保序语义不变，
点→下划线折叠只会把不同行并成同组串行，方向安全）。

---

## 8. 常用运维速查

```bash
B="-u admin:xxx "; H="http://127.0.0.1:8060/api/v1"
curl $B $H/status                 # 角色/term/leader/版本
curl $B $H/position               # 当前位点
curl $B $H/rules/effective        # 每表合并生效规则（不用脑跑继承）
curl $B $H/ddl-events             # DDL 挂起/处置记录
curl $B $H/audit                  # 操作审计（尾部 100 条）
curl -X POST "$H/verify?confirm=true" -H 'Content-Type: application/json' \
     -d '{"database":"db","table":"t","sample":100}'          # 抽样对账
curl -X POST "$H/rules/db.t/restock?confirm=true"             # 单表重跑全量（清标记）
curl -X POST "$H/leader/resign?confirm=true"                  # 优雅让位（滚动发布）
curl -X POST -H 'Content-Type: application/json' \
     -d '{"name":"binlog.000123","pos":100,"confirm":"SET-POSITION"}' \
     "$H/position"                                            # 位点修改（危险）
curl http://127.0.0.1:9595/metrics                            # Prometheus 指标
```

**位点回拨必须停机后做**（运行中回拨会被优雅停机的水位终值覆盖）——见 RUNBOOK §1。

---

## 9. 生产最佳实践 checklist

1. **GTID 开启**：源库 gtid_mode=ON + `use_gtid:true`——主从切换免手工修位点。
2. **timezone 对齐**：`source.timezone` = 源库 `@@system_time_zone`；目标库会话时区与其一致。错了 TIMESTAMP 漂移 8 小时。
3. **server_id 唯一**：每节点独立（99001/99002/…），与任何真实从库不同。
4. **复制账号最小授权**：SELECT + REPLICATION SLAVE + REPLICATION CLIENT + RELOAD/BACKUP_ADMIN；不给 SUPER。
5. **密码全走 `${ENV}`**：配置文件/etcd 里无明文；`auth_disabled` 只允许纯内网且显式声明。
6. **集群 ≥2 节点 + etcd 3 节点**：etcd 是协调面单点，quorum 丢失=全停（安全态）。
7. **配置进 etcd**：默认键 `/config/edu.mall.gocdc/system`（多集群共享 etcd 时以引导桩 `config_center.key` 或 `-config-key` 显式区分，如 `/gocdc/{cluster}/config`），改配置滚动重启（不热更）。
8. **RocketMQ topic 预建+FIFO**；Kafka `max_message_bytes` ≤ broker 限制。
9. **ddl_sync 谨慎开**：auto 策略只自动执行白名单（加列/扩容/注释/CREATE）；DROP 类任何策略都要人工。
10. **验证/对账常态化**：每日低峰 `POST /verify` 抽样核对（被 mask 表除外——脱敏列必然 DIFF）。
11. **升级前**：确认无 pending DDL、无暂停规则、lag 归零；滚动 = 先备后主 + resign。
12. **告警接入**：Grafana dashboard（deploy/grafana）+ 关键指标：`gocdc_leader_state`（无主）、
    `gocdc_pipeline_paused`（暂停）、`gocdc_master_position_lag_bytes`（积压）、`gocdc_position_save_failures_total`（位点写失败）。

## 10. FAQ（对照真实验证结论）

| 现象 | 原因与处置 |
|------|-----------|
| 启动报「binlog_format 非 ROW」 | 源库参数不满足，改配置并重启会话（检查器 fail-fast） |
| 启动报「目标表 xxx 不存在」 | 目标同名库表须预建；或列集不一致（缺列/NOT NULL 无默认的 exclude 列） |
| 启动报「无快照锁权限」 | 复制账号缺 RELOAD/BACKUP_ADMIN；或显式 `stock.lock_mode:"none"` 接受近似一致 |
| 启动报「下游目标不可达」 | 目标库连接失败；先 telnet 目标端口 |
| RMQ 启动报 TOPIC_NOT_FOUND | topic 未预建——mqadmin updateTopic（§7.2） |
| RMQ 报 message type not match | topic 缺 FIFO 属性——`-a +message.type=FIFO` |
| 管线 paused{reason="ddl"} | 有 DDL 挂起等审批——/ddl-events 查看 → approve/skip |
| 数据对不上但 verify 显示 DIFF 全是 phone 列 | 正常——verify 不感知脱敏，被 mask 列必然 DIFF |
| 位点回拨后数据没重放 | 运行中回拨会被停机水位覆盖——**停机 → 回拨 → 启动**（RUNBOOK §1） |
| etcd 挂了会怎样 | TTL 内全节点停管线（安全态，不丢数据）；恢复后自动重选续传 |
| leader 挂了会怎样 | follower ≤ TTL+10s 接管；窗口期数据零丢失（binlog 重放+幂等 upsert） |

## 11. 多源部署（配置 v2 sources[]，2026-11）

单进程一个 cluster 承载最多 64 条 MySQL 同步源：每源独立的源连接、规则、
下游、位点、全量标记、DDL 审批、暂停标志；一个 Web 控制台、一个指标端口、
一次选主覆盖全部源。完整设计见 `docs/prd/`。

**升级一步**：顶层 `source/targets/rules` 旧形态已移除（2026-11 统一
`sources[]`，单源=数组一条）——升级时把配置正文包一层 `sources: [{id: "default"}]`
（内嵌原三段），其余零改动。etcd 存量键（位点/标记）启动时仍自动迁移到
`sources/default/` 子树（幂等，日志 `etcd_keys_migrated`）。
**升级正文的 id 必须填 `default`**：键迁移固定落到该子树，id 改名即对不上
位点——等于弃位点从头全量（要走别的 id 就先用 etcdctl 手工搬键）。

```yaml
pipeline: {workers: 4, worker_queue_size: 8192, batch_size: 500}   # 集群默认
sources:
  - id: order-center          # ^[a-z][a-z0-9_-]{1,31}$，cluster 内唯一
    source: {addr: "10.0.0.11:3306", ...}     # 原 source 块原样下沉
    targets: {mysql: {..., target_database: mall_merged}, ...}
    rules: [...]
    pipeline: {workers: 8}    # 可选：只覆盖 workers，其余继承集群默认
  - id: user-center
    source: {addr: "10.0.0.12:3306", ...}
    ...
```

**三条硬约束**（启动校验期拦死）：

1. 跨源目标不相交：全部源的 effective `(目标实例, 目标库, 表)` 三元组与
   `(brokers, topic)` / `(name_servers, topic)` 全局唯一——两源写同一目标表/
   topic = 数据互覆，属配置错误。
2. 同一源实例被多个源订阅时 `server_id` 必须显式互异（否则 binlog 订阅互踢）；
   缺省按 `node_id:sid` 派生，同进程天然互异。
3. 源数 1..64（容量规划口径：目标连接=Σ各源 max_open_conns，
   GOMEMLIMIT ≈ N×150MB+100MB，详见 RUNBOOK §9.5）。

**失败隔离**：某源源库宕机/启动检查不过 → 该源 `failed` + 30s 起指数退避
重试（上限 5min），其余源不受影响；运行期致命错误仍为该源 `paused` 等
人工。运维操作（位点回拨/restock/DDL 审批/暂停恢复）按源走
`/api/v1/sources/{sid}/...`（单源部署同样走 `/sources/default/...`）。

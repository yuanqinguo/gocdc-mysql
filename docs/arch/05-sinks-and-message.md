# 05 · 下游设计：MQ 消息格式、Kafka/RocketMQ、MySQL 直写

> 行变更解析后可进 MQ、也可直接写另一个 MySQL 库的表，**两者可同时存在**；MySQL sink 强制同名同表、不支持字段映射（从配置层面杜绝错配）；MQ 消息体为完整描述事件的 JSON。

## 1. Sink 接口

```go
type Sink interface {
    Name() string                                  // "mysql" | "kafka" | "rocketmq"
    Ping(ctx context.Context) error
    Send(ctx context.Context, batch []*Event) error // 同步发送，全部成功才返回 nil
    Close(ctx context.Context) error
}

// SinkGroup：一条规则的启用 sink 集合（≥1 个），Worker 按 batch 调用
type SinkGroup struct{ sinks []Sink }

// Send 语义：逐个 sink 发送；某 sink 失败 → 该 sink 进入退避重试（见 §7），
// 最终失败则整批 ack 失败 → 水位线不推进 → 反压/重放（at-least-once）。
```

每条规则独立配置各 sink 的启用（`rules[].sinks`），同一批事件按规则路由到对应 SinkGroup。

## 2. MQ 消息格式规范（核心契约）

### 2.1 事件消息（增量）

```jsonc
{
  "event_id": "mysql-bin.000123:45678:3",      // 全局唯一：file:pos:row_index；GTID 模式下为 gtid:seq
  "type": "UPDATE",                             // INSERT | UPDATE | DELETE
  "phase": "binlog",                            // binlog | snapshot（全量消息标记，见 06）
  "database": "edu.mall.admin",
  "table": "admin_user",
  "ts": 1761624000,                             // binlog 事件时间戳（秒）
  "exec_ts": "2026-10-28T12:00:00+08:00",       // 上字段的可读形式
  "position": { "file": "mysql-bin.000123", "pos": 45678 },
  "gtid": "",                                   // use_gtid 时
  "xid": 9812345,                               // 事务号
  "pk": { "id": 12345 },                        // 主键列名→值（脱敏前的主键值；主键不允许脱敏）
  "before": null,                               // UPDATE/DELETE 的前像；INSERT 为 null。
                                                //   UPDATE：仅变更列（binlog row image=MINIMAL）或全列（FULL，推荐）
  "after": {                                    // INSERT/UPDATE 后像（脱敏后；DELETE 为 null）
    "id": 12345,
    "name": "***masked***",
    "created_at": "2026-10-28T12:00:00+08:00"
  },
  "columns": {                                  // 列元信息（自描述，消费方无需查询 schema）
    "id":         { "type": "int", "pos": 1 },
    "name":       { "type": "varchar", "masked": "sha256" },
    "amount":     { "type": "decimal", "precision": 10, "scale": 2, "as_string": true },
    "ext":        { "type": "json" }
  },
  "schema_version": 7                           // 该表结构缓存版本（DDL barrier 时 +1）
}
```

设计要点：

| 旧问题 | 新设计 |
|---|---|
| 无位点 → 下游无法去重/追溯 | `event_id` + `position`/`gtid` |
| `date` 拼写错误 | `after`/`before` 明确命名 |
| UPDATE 前像按全局开关 | UPDATE 一律带 `before`（依赖源端 `binlog_row_image=FULL`，启动检查）；DELETE 可选带前像（配置 `delete_with_before`） |
| 无 schema 自描述 | `columns` 元信息（脱敏标记、decimal 标度） |
| 全量/增量不可区分 | `phase` 字段 |

### 2.2 DDL 消息（`ddl_notify: true` 时）

```jsonc
{ "event_id": "mysql-bin.000123:45900:0", "type": "DDL", "phase": "binlog",
  "database": "edu.mall.admin", "table": "admin_user", "ts": ..., "position": {...},
  "sql": "ALTER TABLE admin_user ADD COLUMN remark varchar(255)", "result": "synced" }
```

### 2.3 Topic 与分区

| 项 | Kafka | RocketMQ |
|----|-------|----------|
| Topic | `topic_prefix + db_table`（库名/连接符的 `.`→`_`，两个下游统一命名，点号一律不收），或 rule 覆盖 | 同左，或 rule 覆盖 |
| Key / ShardingKey | `order_by: row` → `{db}.{table}.{pk值拼接}`；`table` → `{db}.{table}` | 同 key 语义，组键 `.`→`_`（message group 字符集与 topic 同，不收点），`SelectMessageQueueByHash` |
| Tag | `INSERT/UPDATE/DELETE/DDL` | 同 |
| 效果 | 同行（或同表）落同分区，**消费侧有序** | 同 key 落同队列，顺序消费 |

## 3. Kafka sink（twmb/franz-go）

- **同步确认**：`ProduceSync` + `FirstErr`，全部成功才返回 nil，错误必须返回给 worker（不允许异步漏错）。
- **幂等 producer**：kgo 生产端默认开启（要求 broker ≥0.11），重试不乱序；`ack: all`（可配 `one` 降级 LeaderAck）。
- 压缩可配（默认 snappy）；`send_timeout_ms` 同时约束连接与单条 Record 超时。
- `max_message_bytes` 发送前预检：序列化超限 → poison 永久错误（§7），不进发送路径。
- 重试：kgo 内部重试（幂等开启下不乱序）+ sink 层退避双层兜底。
- `order_by: row` 时同一行 key 固定 → 分区内严格有序；key 策略见 §2.3。

## 4. RocketMQ sink（rocketmq-clients/golang/v5，gRPC）

- **同步发送**：`Producer.Send` 返回同步结果，错误上抛给 worker，不吞错。
- **顺序消息**：v5 无 selector，用 **message group**——group = key（`{db}.{table}.{pk}`）中的点替换为下划线（message group 字符集与 topic 同，不收点；同 key 语义保序），同 group 派同队列、broker 按到达序落盘；topic 必须预建且带 FIFO 属性（预建步骤见 GUIDE §7.2）。
- **topic 预声明**：producer 启动时预声明全部 topic，否则卡 settings 同步（v5 客户端实战约束）；客户端只拉路由不建 topic。
- Tag = 事件类型；Keys = `event_id`（消息查询/轨迹用）。
- `Ping()` 只做 nameServer/producer 状态检查，**绝不发测试消息**。
- **组间并发发送（`Send` 同步全确认语义不变）**：v5 Go 客户端 `Producer.Send` 仅单条（协议层 `send0` 支持多条但未公开批量 API），同步逐条时单发送流吞吐 ≈ 1/RTT，峰值 1W 行/s 会积压（09 §7 单热表基线）。`Send` 批内按 (topic, 顺序组) 分组：**组内严格串行**——同组消息到达序 = 发送序，broker 按到达序落盘，这是顺序消息保序的唯一来源，不变式不放松；**组间经 `send_concurrency`（默认 32，进程级全局信号量，09 §2）并发**。任一组失败 → 整批错误 → worker 退避重试整批（at-least-once，已发出消息的重复由 event_id 幂等消化）。**组级串行断言**：同一顺序组跨 Send 在途冲突（stock 重扫与增量并发同表时可能出现）→ 返回可重试错误退避重试，绝不静默乱序。消息构建前置到任何发送之前，Permanent 失败零部分批。

## 5. MySQL sink（同名同表约束与实现）

### 5.1 硬约束（配置校验层强制）

1. **目标表 = 源表同名**；目标库 = `targets.mysql` 可配默认库 + 规则可覆盖 `target_database`（支持多源库聚合进一个目标库，表名不可改）。
2. **列一一对应、无映射**：默认全列复制；`exclude_columns` 生效时目标表不得包含被排除列（启动时对比目标表结构校验，违规则 fail-fast）；`mask_columns` 生效时写入脱敏值。
3. 主键必须存在且明确：优先表真实主键；无主键表必须配置 `order_by_column` 且该列有唯一索引（启动校验 information_schema），否则拒绝该规则（放行无主键表会让 DELETE 无法寻址）。
4. **唯一键治理**：
   - 幂等不只靠主键——upsert 的 `ON DUPLICATE KEY UPDATE` 对**任何唯一键冲突**都走 UPDATE 路径，at-least-once 重复与全量断点重扫的重复块同样被吸收；
   - 启动结构核对三条（information_schema，T1.5/T7.1 落地）：**exclude 列参与主键/唯一键 → fail-fast**（10 §3）；**mask 列参与唯一键 → 告警**（脱敏碰撞合并行风险）；无主键表见第 3 条；
   - 目标库若被 DBA 手工加了源库没有的唯一键 → 同步冲突 → 失败重试后 PAUSED+告警（正确语义：目标结构被外部改动，人工介入），绝不静默跳过；
   - 全量-增量交接的重复由 06 快照会话协议（增量起点=max(快照对)）+ 上述幂等共同收敛。

### 5.2 SQL 生成

```text
INSERT/UPDATE  →  多行 upsert（攒批按表分组）:
  INSERT INTO `db`.`tbl` (`c1`,`c2`,...) VALUES (?,?,...),(?,?,...),...
  ON DUPLICATE KEY UPDATE `c1`=VALUES(`c1`),...
  # 联合主键：主键列照常进列清单

DELETE  →  批量:
  DELETE FROM `db`.`tbl` WHERE (`pk1`,`pk2`) IN ((?,?),(?,?),...)
  # 联合主键生成正确的多列谓词

主键值变更的 UPDATE  →  同一事务内:
  DELETE FROM `db`.`tbl` WHERE (pk) IN ((old_pk,));
  INSERT ... ON DUPLICATE KEY UPDATE ...（新行）
  # PK 变更行：先删旧行（依赖 UPDATE 的 before 像，binlog_row_image=FULL）
```

### 5.3 批量与事务

- Worker 攒批内**按表分组**聚合多行 SQL；单语句行数上限 `tx_chunk_rows`（默认 500），超限切块。
- **批内顺序——游程分组，禁止全局重排**：把批内同表**连续的同类型操作**合并为一条多行语句（run-length grouping），但绝不允许「先所有 DELETE 后所有 INSERT/UPDATE」的全局重排——同一 PK 出现 `UPDATE→DELETE→INSERT` 序列时，全局重排会让最后的 INSERT（旧行）复活已删除的行。正确示例：

  ```text
  事件序: U(k1) U(k2) D(k3) U(k3) I(k4)
  生成:   INSERT..VALUES(k1,k2) ODKU → DELETE WHERE pk=k3 → INSERT..VALUES(k3,k4) ODKU
  ```
- 事务粒度 = 语句级（单条多行语句自身原子，不长持锁）。同 PK 的旧→新次序由上述行序保证；崩溃中断由幂等重放收敛（07 文档）。
- 连接池：`max_open_conns` 必填（默认 20）、超时（connect/read/write 5s）、`interpolateParams=true`。
- 类型绑定按 §04-7 规格（decimal 字符串、JSON []byte、BIT uint64…），从根上消灭「map 绑参失败」「精度丢失」。
- 目标端存在**其他唯一索引**时的语义（文档明示 + 启动检测警告）：upsert 可能命中唯一键冲突行——建议目标表仅保留与源一致的主键/唯一键；检测到多余唯一索引时告警。
- 目标表结构同步：白名单 DDL 自动同步见 08 文档。

### 5.4 幂等论证

- INSERT/UPDATE → upsert on PK：重放任意次结果一致（last-write-wins）。
- DELETE by PK：幂等（不存在即无操作）。
- 主键变更 UPDATE：delete(old)+upsert(new) 在重放时同样收敛（old 已不在，delete 无操作）。
- 唯一前提：**同表事件有序**（§04-2 分片保证）。

## 6. 双写（MQ + MySQL 同时启用）语义

- Worker 将同一批事件发给 SinkGroup 内每个启用的 sink，**先 MySQL 后 MQ**（MySQL 失败概率高、且是终态存储，先保它）。
- 语义承诺：**两边都是 at-least-once，互相独立重试**；位点推进要求两边都确认（SinkGroup 全成功才 ack）。
- 单边长期失败：该 sink 熔断（09 文档）→ **整条规则暂停推进位点**（不能只推进另一边，否则故障侧永久丢数据），告警提示人工介入；熔断恢复后自动继续。
- 明示不做「2PC/事务消息」：跨 MySQL/Kafka 的强一致超出 CDC 工具定位，at-least-once + 幂等消费是文档化的承诺。

## 7. 失败处理与重试统一规范

| 层 | 策略 |
|----|------|
| 可重试错误（网络超时、broker not available、lock wait timeout…） | sink 内指数退避重试（100ms×2^n，上限 30s，持续到 `sink_retry_max` 默认 10 次或恢复） |
| 不可重试错误（SQL 语法、消息超限、认证拒绝） | 立即熔断：规则置 PAUSED + 指标/告警，位点停推，等待人工修复后 resume（Web/CLI）——**绝不静默跳过** |
| sink 长时间不可用 | 熔断期间 worker 停 ack → 队列填满 → 反压（09）；恢复后从断点续投 |
| **消息超限**（事件序列化后 > `max_message_bytes`，如巨型 BLOB 行） | MQ sink 侧预检：**不可重试 poison**——整规则 PAUSED + 告警等人工处理（修复后 resume 或调整预检配置） |
| DDL/结构导致的持续失败（目标列缺失等） | 不可重试 → PAUSED + 告警（与 08 pause 语义一致） |

Poison 事件没有 DLQ 逃生通道：放行 = 接受该事件不落目标，MySQL sink 语义下破坏终态一致——统一走 PAUSED + 人工介入（修复数据/结构后 resume），绝不静默跳过。

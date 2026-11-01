# gocdc 设计文档（架构决策记录）

> 这组文档记录 gocdc 的核心设计决策：**遇到什么问题、为什么这样解**。
> 修改对应模块前先读文档；文档与实现不一致时，先改文档对齐，再改代码。
> 上手与运维入口：[GUIDE.md](../GUIDE.md)（配置/使用）、[RUNBOOK.md](../RUNBOOK.md)（故障处置）。
>
> 编号即文档 ID，互相按编号引用（历史演进保留缺口，不影响阅读）。

## 文档索引

| 文档 | 解决什么问题 / 核心决策 |
|------|------------------------|
| [02 目标架构](02-target-architecture.md) | 总览：四条设计原则（正确性优先 / 有界资源 / etcd 唯一协调面 / 明确语义承诺）、模块与线程模型、技术选型与依赖边界 |
| [03 etcd 协调面](03-etcd-ha-config.md) | etcd 三大职责：配置中心、leader 选举（lease + 防脑裂）、位点存储；key 布局与客户端治理 |
| [04 管线并行化](04-pipeline-parallel.md) | binlog 单线程拉取 + 按 `schema.table` 分片并行，表内严格有序，水位线位点推进；值转换规格 |
| [05 下游设计](05-sinks-and-message.md) | MQ 消息格式规范（envelope JSON，`event_id`/before/after）、Kafka/RocketMQ 生产者治理、MySQL 幂等直写、多下游双写 |
| [06 全量-增量交接](06-full-sync-handover.md) | 一致性快照 + 快照时位点捕获 + 表级完成标记：不重不漏收敛到源库终态，幂等、可中断、可续跑 |
| [07 投递语义](07-delivery-semantics.md) | at-least-once 承诺、幂等设计（upsert + `event_id` 去重）、故障场景矩阵 |
| [08 DDL 策略](08-ddl-strategy.md) | `pause/auto/ignore` 三策略、Web 审批流、DDL barrier（先排空该表积压再换 schema） |
| [09 背压与资源治理](09-backpressure.md) | 全链路有界队列、阻塞传导（TCP 反压到源库 dump 线程）、sink 熔断与自动恢复、内存预算 |
| [10 字段过滤与脱敏](10-field-masking.md) | `exclude_columns`/`mask_columns` 列级策略，MQ 与 MySQL sink、全量与增量行为一致 |
| [11 可观测性](11-observability.md) | 指标体系（吞吐 / 事件时间延迟 / 位点滞后 / 队列深度 / HA 状态）、健康端点、Grafana 面板与告警规则 |
| [13 Web 管理端](13-web-admin.md) | API 优先、React 单页 go:embed 内嵌分发、HTTP Basic + confirm 门禁、DDL 审批与对账入口 |

## 阅读路径

- **只想用起来**：[GUIDE](../GUIDE.md) → `deploy/` 演示 → 需要调参再回来查对应篇。
- **要改某个模块**：先读索引表里对应的一篇（每篇开头就是它解决的问题），再读相邻两篇（数据面 04→05，控制面 03）。
- **评估可靠性**：07（语义承诺）+ 09（背压）+ 03（故障接管）三篇构成可靠性论证的闭环。

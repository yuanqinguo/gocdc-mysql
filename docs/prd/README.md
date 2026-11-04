# 多源 MySQL 同步 PRD（Multi-Source）

本目录是「单进程承载多条 MySQL 同步源」的产品需求文档（PRD）。目标读者：
实现本特性的工程师、评审设计的人、以及验收的人。阅读前建议先过一遍
`docs/arch/README.md` 的三条阅读路径——本 PRD 建立在现有 11 篇架构文档
（02~11、13）所描述的单源系统之上，不重复论证已定决策，只做增量设计。

## 一页速览

**现状**：一个 cluster = 一个源库 = 一条管线 = 一套 etcd 状态。要同步 N 个
源库实例，就得部署 N 个 cluster（各自前缀共存于同一 etcd）、N 套 Web 端口、
N 份运维面。

**目标**：单进程、单 cluster、单 Web 控制台承载 N 条**同步源**（source），
每个源 = 一个源库 + 一组规则 + 一组下游 + 自己的位点/全量标记/DDL 审批/
暂停标志。**最终效果与部署 N 个现有单源集群等价**（等价性定义见
[01 §5](01-background.md)，验收清单见 [08 §1](08-acceptance-rollout.md)）。

**核心决策**（详见 [02 §2](02-overall-design.md)）：

| # | 决策 | 一句话理由 |
|---|------|-----------|
| D1 | 引入「源」一等实体，配置升级为 `sources[]` 数组 | 单源假设的根在 `Config.Source` 单数结构，从根上改 |
| D2 | 保持 cluster 级单 leader，leader 同时承担全部源 | 租约写门槛/防脑裂机制零改动，一个 `{prefix}/leader` 锚点保护全部源 |
| D3 | etcd 键空间下沉一层：`/gocdc/{cluster}/sources/{sid}/...` | 位点/标记/DDL 门/暂停标志全部按源分片；cluster 级键（leader/nodes/config/audit）不动 |
| D4 | 存量键一次性自动迁移（CAS 幂等）；v1 顶层正文一步改写为 `sources:[{id:"default"}]`（2026-11 起不再自动包装） | 键迁移可审计、可回滚；正文改写一次性、升级路径见 RUNBOOK §9.4 |
| D5 | 失败隔离到源级：某个源的源库故障只停该源并周期重试 | 「一坏全停」与部署 N 个集群的效果不等价，必须改 |
| D6 | 跨源目标命名空间强制不相交（MQ topic、MySQL 目标表），冲突即配置错误 | 两个源写同一个 topic/表 = 数据互覆/乱序，校验期拦死 |
| D7 | 消息 envelope 增加 `source` 字段（schema_version 升 2）；event_id 保持源内唯一 | topic 不相交 ⇒ 消费端按 (topic, event_id) 去重依然成立，老消费者忽略新字段即兼容 |
| D8 | 受影响指标统一加 `source` label；Web 引入源选择器与聚合总览 | 可观测与操作面按源切分，Grafana 面板同步更新 |

**非目标**：每源独立选主、跨源事务一致性、表名/列名映射、配置热更新
（仍重启生效）、MQ topic 跨源聚合投递。理由见 [01 §4](01-background.md)。

## 文档导航

| 编号 | 文档 | 内容 |
|------|------|------|
| 01 | [background](01-background.md) | 现状基线（单源硬边界的三层根因）、需求、目标与非目标、等价性定义、术语 |
| 02 | [overall-design](02-overall-design.md) | 总体方案：源抽象、八个核心决策及被否方案、进程结构、失败隔离模型 |
| 03 | [config-schema](03-config-schema.md) | 配置模型 v2：`sources[]` 结构、字段与继承、校验规则、v1 兼容映射、完整示例 |
| 04 | [etcd-ha](04-etcd-ha.md) | etcd 键空间 v2、写门槛复用论证、一次性迁移协议与回滚、HA 行为 |
| 05 | [runtime](05-runtime.md) | 运行时与生命周期：共享/每源组件矩阵、装配改造、全量与增量编排、背压与资源、优雅停机 |
| 06 | [api-web](06-api-web.md) | REST API 变更与兼容策略、Web 控制台信息架构（源作用域/URL 设计）、逐页改造、前端实现适配面 |
| 07 | [observability-ops](07-observability-ops.md) | 指标 label 化清单、告警与 Grafana、日志规范、部署形态、RUNBOOK 增补 |
| 08 | [acceptance-rollout](08-acceptance-rollout.md) | 等价性验收清单、MS 测试组、分阶段实施计划、风险与对策、开放问题 |

## 阅读约定

- 引用现行实现一律给出 `文件路径:行号`（写作时点为 master@80f9d35，行号可能随
  演进漂移，以符号名为准）。
- 引用既有架构决策写作（`03 §4.2`）即 `docs/arch/03-etcd-ha-config.md` §4.2。
- 本目录编号 01~08 自成体系，不与 `docs/arch` 的稳定编号（02~13）混用。

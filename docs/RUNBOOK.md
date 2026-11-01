# gocdc 运维手册（Runbook）

> 完整 SLA/拓扑/参数速查见 docs/arch/；本文只写故障处置动作。**全部运维操作
> 经 admin Web（`/api/v1`，HTTP Basic 单用户名密码 + confirm）**——不提供 CLI
> 子命令（Docker 部署下不应要求进节点执行命令；`-h` 只剩服务模式 flag）。
> 个别无 Web 界面的操作（如手工播种位点）用 etcdctl 直接写对应 key。

## 0. 日常检查
- Grafana `gocdc-overview`：leader 状态、事件延迟、位点滞后、暂停规则、DLQ 计数。
- 告警通道：Critical（无 leader / 管线暂停 / 位点保存失败 / DLQ）必报到人。

## 1. 位点回拨（目标端污染需重放）
1. Grafana/DBA 确认污染窗口起点（binlog file+pos）。
2. **先停进程**（运行中回拨的位点会被优雅停机的水位终值覆盖）。
3. Web `POST /api/v1/position`（confirm=SET-POSITION）回拨位点到窗口起点
   （进程已停时也可 etcdctl 直接写 `{prefix}/position`）。
4. 重启进程 → 从新位点重放 → MySQL upsert / MQ 消费端按 event_id 去重消化重复。

## 2. 强制重跑全量（restock）
- Web `POST /api/v1/rules/{db.table}/restock?confirm=true` → 清标记。
- 重启进程 → onElected 检测无标记自动重跑全量（快照对 + 增量衔接）。

## 3. 新增表接入
1. 改 etcd 配置（rules 加库级/表级规则）。
2. 重启（配置不热更）。新表无 stock 标记 → 自动全量 → 增量接管。

## 4. 源库主从切换
- GTID 模式：自动续接（推荐生产开启）。
- file:pos 模式：停实例 → 从新主 SHOW BINARY LOG STATUS 取位点 →
  Web `POST /api/v1/position`（confirm=SET-POSITION）写入 → 启动。
  切换间隙丢失不可恢复——强烈建议 GTID。

## 5. DDL 挂起处理（ddl_policy=pause / 白名单外）
1. 告警 `gocdc_pipeline_paused{reason="ddl"}=1`；Web `GET /api/v1/ddl-events` 查看 SQL。
2. 评估：安全 → `POST /api/v1/ddl/{id}/approve?confirm=true`（执行目标 DDL+resume）；
   危险 → 人工改目标表后 approve（复验通过即 resume）；跳过 → skip（按新 schema 继续，审计留痕）。

## 6. etcd 故障
- 单节点宕机：无影响。quorum 丢失：lease 全过期 → 全节点停管线（安全态）→
  恢复后自动重选（≤ TTL+10s）。写进 SLA 说明。

## 7. 下游迁移（换 Kafka/目标库）
sink 配置变更 + 暂停 → 切换 → 恢复。位点不受影响。

### RocketMQ 下游前置
- topic 须**预先在 broker 创建**（v5 客户端只拉路由不建 topic）：
  `mqadmin updateTopic -n <namesrv> -b <broker:10911> -t <topic> -a +message.type=FIFO`
  （顺序消息必须 FIFO 属性，否则发送报 message type not match → 管线 PAUSED）。
- 默认 topic 名 = `{topic_prefix}{db}_{table}`（点号非法，不得复用 Kafka 命名）。

## 8. 发布前置检查
无暂停规则；lag < 阈值；旧实例已停（server_id 冲突互踢）。

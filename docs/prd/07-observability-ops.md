# 07 · 可观测性与运维

## 1. 指标变更（label 化清单）

原则：**源私有指标加 `source` label，cluster 级指标不加**。实现方式为
把现行 `Metrics` 结构中的 Gauge/Counter 单值字段改造为 `*Vec`（注册时
带 label 名），调用点传 sid。

**加 `source` label（源私有）：**

| 指标 | 现行形态 | v2 形态 |
|------|---------|---------|
| `gocdc_events_in_total` | CounterVec(db,table,type) | + source |
| `gocdc_events_out_total` | CounterVec(db,table,type,sink) | + source |
| `gocdc_events_failed_total` | CounterVec(sink,reason) | + source |
| `gocdc_bytes_total` | CounterVec(sink) | + source |
| `gocdc_convert_errors_total` | CounterVec(db,table,column,action) | + source |
| `gocdc_deadletter_total` / `gocdc_message_oversize_total` | Vec | + source |
| `gocdc_event_lag_seconds` | Gauge | GaugeVec(source) |
| `gocdc_position_lag_seconds` | Gauge | GaugeVec(source) |
| `gocdc_master_position_lag_bytes` | Gauge | GaugeVec(source) |
| `gocdc_worker_queue_depth` | GaugeVec(worker) | + source |
| `gocdc_worker_queue_oldest_seconds` | Gauge | GaugeVec(source, worker) |
| `gocdc_watermark_seq` / `gocdc_last_saved_seq` | Gauge | GaugeVec(source) |
| `gocdc_pipeline_paused` | GaugeVec(db,table,reason) | + source |
| `gocdc_sink_batch_duration_seconds` | HistogramVec(sink,db,table) | + source |
| `gocdc_sink_retry_total` | CounterVec(sink) | + source |
| `gocdc_position_save_failures_total` | Counter | CounterVec(source) |
| `gocdc_stock_progress_ratio` / `gocdc_stock_rows_total` | GaugeVec(db,table) | + source |

**不加 label（cluster 级）：** `gocdc_leader_state`、`gocdc_leader_term`、
`gocdc_etcd_request_duration_seconds`、go_*/process_*。

**新增：**

| 指标 | 类型 | 说明 |
|------|------|------|
| `gocdc_sources` | GaugeVec(state) | 按 ready/failed/stopped 计数的源数 |
| `gocdc_source_up` | GaugeVec(source) | 1=该源就绪且非 failed |

**破坏性说明**：单源升级后上述指标多出 `source="default"`——现有
PromQL（不带 `source` 的选择器）仍然命中，但 `sum()` 类聚合结果不受
影响、`by (...)` 不含 source 的查询自动聚合。Grafana 面板随本特性一并
更新（每表吞吐/全量进度/sink 健康页签加 source 维度筛选变量）。

## 2. 告警（deploy/grafana/gocdc-alerts.yml 增补）

沿用现有 6 条规则（label 选择器自动适配），新增：

| 告警 | 表达式要点 | 级别 |
|------|-----------|------|
| GocdcSourceDown | `max_over_time(gocdc_source_up{source=...}[1m]) == 0` | Critical |
| GocdcSourcesFailed | `gocdc_sources{state="failed"} > 0` | Warning |

## 3. 日志规范

- 源作用域的全部日志（管线/全量/DDL/重连/熔断）**必须**携带
  `zap.String("source", sid)` 字段——与现行 `event=` 结构化字段并列，
  排障按 source 过滤是一等操作。
- 生命周期事件（源启动/装配失败/重试/暂停/恢复/迁移完成）沿用
  `internal/logging/events.go` 事件命名风格，新增：
  `source_components_failed`、`source_recovered`、`etcd_keys_migrated`。
- 行数据脱敏红线（logging/redact.go）逐源适用，机制不变。

## 4. 部署形态

### 4.1 形态选择

| 形态 | 适用 |
|------|------|
| 单节点多源（`ha.enabled: false`） | 开发/小规模：一个进程 + 一个 etcd 同步 N 源（edu-mall-dev 的自然演进） |
| 集群多源（推荐生产） | 2+ 节点共享同一份 v2 配置（cluster 相同），选主后 leader 承担全部源；滚动重启 = follower 先升级 → resign 切换 → 升级原 leader（流程不变，切换窗口覆盖全部源） |
| 多 cluster 共存 | 需要错峰切换/故障域隔离的超大规模场景：按业务域拆多个 cluster，各自 N 个源——与本特性正交可叠加 |

### 4.2 演示样例（deploy/ 新增）

新增 `deploy/docker-compose.multi-source.yml`（演示拓扑）：

```
etcd ── mysql-src-a（订单源）──┐
     ├─ mysql-src-b（用户源）──┼─► gocdc（单进程 2 源，聚合写入）──► mysql-dst
     └─ mysql-dst（聚合目标）
profile cluster：gocdc-2 热备（选主演示覆盖全部源）
```

验证脚本（README 演示命令）覆盖：双源写入 → 聚合目标出现两路数据；
杀 leader → 两个源一起接管续传；单源宕机 → 另一源不受影响。

### 4.3 edu-mall-dev 演进

现部署单源 7 库聚合（`deploy/config.edu-mall-dev.yml`）升级路径：
配置原样（v1 自动包装 default）→ 行为不变；未来源拆多实例时改写为
`sources[]` 多源即可，目标库与聚合表不变（V4 校验保证新源不与
default 源撞目标表）。

## 5. RUNBOOK 增补条目（docs/RUNBOOK.md）

| 条目 | 内容 |
|------|------|
| 多源配置变更 | 改 etcd `/gocdc/{cluster}/config`（v2 YAML）→ 滚动重启生效（不热更，现行口径）；新增源 = 配置加一项 + 重启（新源首启自动全量，其余源照常） |
| 单源故障处置 | `/status` 或 `gocdc_source_up` 定位源 → 单源视图按现行单集群 runbook 处置（DDL 审批/暂停恢复/位点回拨/restock），操作路由带 `/sources/{sid}` |
| 某源源库主从切换 | 现行 §4 流程逐源执行：GTID 模式自动续传；file:pos 停该源（其他源不停——整进程重启仍会全部源切换窗口，如需绝对隔离见多 cluster 形态） |
| etcd 键迁移回滚 | PRD 04 §4.3 的三步手工搬回流程 |
| 升级检查单 | ① 新二进制 + 现行 v1 配置启动一次（自动迁移完成日志 `etcd_keys_migrated`）② 指标出现 `source="default"` label（告警查询同步更新）③ Web 顶栏出现源选择器 |
| 容量规划 | 连接数预算表（PRD 05 §5.2）+ GOMEMLIMIT 建议值 |

## 6. verify 对账

`POST /api/v1/sources/{sid}/verify` 语义与现行完全一致
（`internal/verify/verify.go`：COUNT + 随机抽样逐列比对，mismatch>0
返回 200 数据差）。实现层装配闭包按源取 `srcDB/tgtDB/targetDB`
（`cmd/gocdc/admin.go:48-56` 的逐源版本）。

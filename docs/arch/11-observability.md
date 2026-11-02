# 11 · 可观测性设计

> 解决「同步黑盒」：吞吐、事件时间延迟、位点滞后、队列深度、全量进度、HA 状态全程有指标，故障可以从 Grafana 直接定位到环节。

## 1. 指标体系（Prometheus，路径固定 `/metrics`）

### 1.1 吞吐与处理量

| 指标 | 类型 | 标签 | 说明 |
|------|------|------|------|
| `gocdc_events_in_total` | Counter | db, table, type(insert/update/delete/ddl) | binlog 侧进入事件数（与下游侧计数对称，可算积压） |
| `gocdc_events_out_total` | Counter | db, table, type, sink | 各 sink 成功投递数（全量 phase=snapshot 另计标签） |
| `gocdc_events_failed_total` | Counter | sink, reason | sink 投递失败数（含重试耗尽） |
| `gocdc_bytes_total` | Counter | sink | 下发字节量（容量规划） |
| `gocdc_convert_errors_total` | Counter | db, table, column, action | 值转换异常数（§04-7 on_convert_error） |
| `gocdc_message_oversize_total` | Counter | db, table, sink | 消息超限事件数（05 §7 预检） |

### 1.2 延迟

| 指标 | 类型 | 说明 |
|------|------|------|
| `gocdc_event_lag_seconds` | Gauge | **端到端延迟** = now − watermark 处事件的 binlog 时间戳（事件时间视角） |
| `gocdc_position_lag_seconds` | Gauge | 位点滞后 = now − 已保存位点对应事务时间（低频兜底视角） |
| `gocdc_master_position_lag_bytes` | Gauge | 与源库 `SHOW MASTER STATUS` 的差距（字节，追平进度） |
| `gocdc_sink_batch_duration_seconds` | Histogram | sink 批次耗时（labels: sink, db, table），观察下游健康 |
| `gocdc_sink_retry_total` | Counter | sink 重试次数 |

### 1.3 管线与资源

| 指标 | 类型 | 说明 |
|------|------|------|
| `gocdc_worker_queue_depth` | GaugeVec(worker) | 分片队列深度（背压前兆） |
| `gocdc_watermark_seq` / `gocdc_last_saved_seq` | Gauge | 水位与已落盘位点（差值=未落盘窗口） |
| `gocdc_pipeline_paused` | GaugeVec(source, db, table, reason) | 暂停状态（ddl/pause 策略/sink 熔断/致命退出）；置 1 后由恢复路径清零（DDL 决策返回、管线重启），不残留旧标记 |
| `gocdc_position_save_failures_total` | Counter | etcd 位点写失败（03 §5.2 告警源） |

### 1.4 全量 / HA / 进程

| 指标 | 类型 | 说明 |
|------|------|------|
| `gocdc_stock_progress_ratio` | GaugeVec(db, table) | 全量进度 0~1 |
| `gocdc_stock_rows_total` | CounterVec | 全量已扫描行数 |
| `gocdc_leader_state` | Gauge | 1=leader |
| `gocdc_leader_term` | Gauge | election revision（切主可见） |
| 标准进程指标 | — | go_*, process_*（自动） |

## 2. 健康检查

| 端点 | 语义 |
|------|------|
| `GET /healthz` | 进程存活（liveness）——不检查外部依赖，防级联重启 |
| `GET /readyz` | 就绪：配置已加载、etcd 可达、角色确定；leader 附加：管线运行中 |
| `GET /metrics` | Prometheus |

## 3. 日志规范

- zap 结构化，控制台(开发)/JSON(生产) 可选；级别由 `log.level` 启动时配置。
- 关键生命周期必打事件（固定字段 `event`）：leader_elected/lost、pipeline_start/stop、position_saved(lag)、sink_paused/resumed、ddl_detected/approved、stock_start/done、config_loaded。
- **敏感数据纪律**：任何行数据日志经 exclude/mask 视图过滤（10 §5）；位点/seq 打印不带列值。
- 采样：正常行级日志默认关闭（`log.sample_rows` 调试用），错误全量。

## 4. Grafana 与告警（deploy/ 提供 dashboard + rules JSON）

建议告警规则（阈值可按业务调整）：

| 告警 | 条件（示例） | 级别 |
|------|-------------|------|
| 同步延迟过高 | `gocdc_event_lag_seconds > 60` 持续 5m | Warning；>600s Critical |
| 位点不推进 | `gocdc_watermark_seq` 5m 无增长且队列非空 | Critical |
| 位点落盘失败 | `increase(gocdc_position_save_failures_total[5m]) > 0` | Critical |
| sink 熔断/规则暂停 | `gocdc_pipeline_paused > 0` | Critical |
| 全量失败 | `gocdc_stock_progress_ratio` 出现 stalled 或失败事件 | Warning |
| 队列接近反压 | `gocdc_worker_queue_depth / worker_queue_size > 0.8` 持续 10m | Warning |
| 转换异常 | `increase(gocdc_convert_errors_total[10m]) > 0` | Warning |
| 进程重启/切主 | `gocdc_leader_term` 变化 / uptime 重置 | Info~Warning |

Dashboard 页签：总览（lag/吞吐/暂停）、每表吞吐、sink 健康（批量耗时直方图/重试）、全量进度、HA 与位点。

## 5. 与 Web 管理端的关系

Web（13 文档）负责**操作**（pause/resume、位点、restock、DDL 审批），Grafana 负责**看图**；Web 首页内嵌只读的关键指标摘要（来自同一 metrics registry），不做重复图表系统。

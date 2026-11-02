# 09 · 背压（Backpressure）与资源治理

> 解决「下游消费不动导致 OOM / 静默丢批」：全链路有界队列 + 阻塞传导（TCP 反压到源库 dump 线程），sink 故障一律显式暴露，绝不丢弃。

## 1. 背压模型：全链路有界 + 阻塞传导

```text
源 MySQL dump 线程 ←TCP 反压— BinlogSyncer ←阻塞— Dispatcher ←阻塞— [worker 有界队列] ←不ack— Worker ←慢— Sink
```

- 每一环都是**有界**的：队列容量 `worker_queue_size`（默认 8192 事件）、批量 `batch_size`、sink 在途请求数。
- 任何下游变慢 → worker 不 ack → 队列填满 → Dispatcher 阻塞 → Syncer 停止读取 → 源库 binlog dump 线程被 TCP 窗口卡住。**数据停留在源库 binlog，不占用本进程内存**——这是 MySQL 复制协议天然赋予的背压通道，工具端不制造无界缓冲。
- **没有丢弃路径**：不可重试错误走 pause（07 §7），不走丢弃。

## 2. 各环节资源上限（全部显式可配，无隐式无界）

| 环节 | 上限 | 默认 |
|------|------|------|
| worker 队列 | `worker_queue_size` × workers | 8192×N |
| 攒批缓冲 | `batch_size` | 500 |
| MySQL sink 单语句行数 | `tx_chunk_rows` | 500 |
| MySQL sink 连接池 | `max_open_conns`（必填校验） | 20 |
| MQ 在途批量 | 每 worker 1 批在途（同步发送） | — |
| RocketMQ 组间并发 RPC | `send_concurrency`（进程级全局信号量，组内仍串行） | 32 |
| 全量单块行数 | `stock.chunk_rows` | 5000 |
| 全量并行表数 | `stock.workers` | 4 |
| 内存总预算 | `GOMEMLIMIT`（deploy 默认设为容器 limit 的 80%） | — |

内存估算公式（容量规划用）：`峰值 ≈ workers × queue_size × avg_row + workers × batch_size × avg_row × 2`；1KB 行、8 worker 默认配置 ≈ 100MB，另加 schema 缓存与连接池，容器建议 512MB~1GB。

## 3. Sink 治理（防「慢下游拖死」与「坏下游烧内存」）

1. **超时**：每个 sink 调用带 `send_timeout_ms`（默认 5s）；MySQL 语句带执行超时。杜绝无超时等待。
2. **退避重试**：可重试错误指数退避（100ms→30s 封顶），重试不占用队列（事件留在 worker 批内）。
3. **熔断**：连续失败或单次失败为不可重试类 → 该规则 PAUSED + 告警；位点停推 → 上游自然反压。**熔断是安全态不是故障态**：它把「OOM 风险」转化为「延迟风险 + 明确告警」。
4. **恢复**：PAUSED 规则由 watchdog 周期探活（或人工 resume）自动续投，从断点继续，无丢失。
5. 反压可见性：队列深度、最老事件滞留时长成为一等指标与告警项（11 文档），「慢」在演变成「停」之前就被看见。
6. **poison 事件治理**：消息超限（05 §7）、持续不可重试失败这类事件不允许无限重试占用队列——预检 + 快速归类为 PAUSED（07 §6），把「坏事件」的代价从「烧内存重试」降级为「明确告警」。

## 4. 源库侧的配套要求（运维文档明确）

- binlog 保留期必须 ≥ 最长停机/反压时间（建议 ≥ 3 天或按容量评估）：反压期间同步暂停，恢复后从位点续拉；若位点被 PURGED，启动检测直接报错（06 §7），绝不跳过。
- 反压期间源库 dump 线程挂起与 binlog 积压对源库影响极小（服务端文件页缓存），但**长反压 → 大量积压 → 恢复后追平期间吞吐是瓶颈**，监控 lag 指标（11 文档）。
- `slave_server_id` 唯一、`binlog_row_image=FULL`（before 像依赖）等源端要求汇总在 14 文档。

## 5. 进程级资源治理

- `GOMEMLIMIT` 软上限 + 常规 GOGC；容器 memory limit ≥ 1.5×GOMEMLIMIT（留 GC 与堆外余量）。
- goroutine 泄漏防护：所有后台 goroutine 挂应用 ctx + 退出清理（`goleak` 纳入测试）。
- watchdog 检测：worker 心跳超时、watermark 长时间（> `pipeline_stall_timeout` 默认 60s 且队列非空）不推进 → 重建管线（从位点重放）。
- 优雅停机 drain 超时（默认 30s）防止停机卡死。

## 6. 全量阶段的资源约束

- 全量块走与增量相同的有界队列与 sink 治理——全量打满下游时同样反压，不会 OOM。
- 全量对源库的压力控制：chunk 行数 + 表间并行度 + 可选每块间隔（`stock.throttle_ms`），文档给出「低峰执行」建议。

## 7. 容量与压测基线（验收门槛建议）

| 场景 | 目标 |
|------|------|
| 均匀 8 表写入 20k row/s（1KB/行） | workers=8，队列深度 < 25%，lag < 5s |
| 单热表 10k row/s | 表内串行上限验证（MySQL sink 批量 upsert 吞吐） |
| 下游 kill 60s 后恢复 | 无丢失、无 OOM、恢复追平 |
| 4C8G 容器极限 | 持续运行 24h RSS 稳定（±10%） |

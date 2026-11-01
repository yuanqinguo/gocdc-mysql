// Package metrics 注册全部 Prometheus 指标（吞吐/事件延迟/位点滞后/队列深度/
// sink 批次直方图/全量进度/HA 状态，11 §1 全清单）并提供 /metrics、/healthz、
// /readyz 三个端点。
//
// 使用独立 Registry（非全局 DefaultRegisterer），便于测试与多实例隔离；
// 调用方直接持有 *Metrics 结构体字段打点。
//
// 权威设计：docs/arch/11-observability.md。
package metrics

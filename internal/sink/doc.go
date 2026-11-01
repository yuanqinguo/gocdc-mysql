// Package sink 定义下游 Sink 接口与双写 SinkGroup（先 MySQL 后 MQ，全确认才 ack，
// 单边熔断全停）。
//
// 红线：任何 sink 失败不允许跳过该事件继续；只有 05 §7 定义的两条出路
// （重试 → 熔断 PAUSED / DLQ 且仅 MQ sink 可用）。
// 失败语义（05 §7）：可重试 → sink 内指数退避；不可重试 →
// 立即 PAUSED + 告警（包装 ErrPermanent，见 errors.go）；结果未知（写后断连）
// 按可重试处理——at-least-once 允许重复，幂等收敛。
//
// 权威设计：docs/arch/05-sinks-and-message.md。
package sink

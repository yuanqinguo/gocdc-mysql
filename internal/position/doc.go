// Package position 实现位点模型与 etcd 持久化：lease 门槛事务写入（16/03 §4.2-2）、
// 节流器（默认 3s）、失败指数退避重试 + 连续失败熔断（03 §5.2）。
//
// 红线：位点只能由 Watermark.base 推进（04 §5），不提供任何旁路写位点函数。
package position

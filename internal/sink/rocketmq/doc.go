// Package rocketmq 实现 RocketMQ sink（05 §4，客户端 rocketmq-clients/golang/v5
// gRPC 协议）。
//
// 语义（05 §4 为准，API 按 v5）：
//   - 同步 Send（SendSync 语义 = v5 同步 Send 返回 receipt，不吞错）；
//   - 顺序消息：message group = 分区/顺序键（05 §2.3 key 策略，同组派同队列保序）；
//   - Tag = 事件类型；Keys = event_id（消息查询/轨迹用）；
//   - Ping 无副作用：仅检查 producer 启动状态，绝不发测试消息；
//   - producer 启动必须预声明全部 topic，否则卡 settings 同步（团队经验）
//     ——Start 后 topic 集合不可追加，故 New 要求显式 topics，或走全局
//     前缀派生的动态集合由调用方保证已声明。
//
// 权威设计：docs/arch/05-sinks-and-message.md §3/§7。
package rocketmq

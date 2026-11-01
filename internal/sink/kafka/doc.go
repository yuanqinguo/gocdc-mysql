// Package kafka 实现 Kafka sink（05 §3，客户端 twmb/franz-go）。
//
// 语义（05 §3 为准，API 按 kgo）：
//   - 同步确认（ProduceSync + FirstErr，不漏错）；
//   - 幂等 producer 默认开启（kgo 默认要求 broker ≥0.11）；
//   - RequiredAcks=all（配置 ack=one 降级 LeaderAck）；
//   - key = 05 §2.3（row=主键 / table=表），同 key 分区内严格有序；
//   - 消息超限发送前预检（max_message_bytes）→ poison 永久错误（05 §7）。
//
// 权威设计：docs/arch/05-sinks-and-message.md §2/§7。
package kafka

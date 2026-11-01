// 下游投递统一接口（05 §1）。实现见 internal/sink/mysql（终态存储）、
// internal/sink/kafka、internal/sink/rocketmq（MQ 下游）与本包 SinkGroup。

package sink

import (
	"context"

	"gocdc/internal/model"
)

// Sink 下游投递统一接口（05 §1）。
//
// 失败语义（05 §7）：Send/Ping 返回非 nil 即本批失败，事件不跳过、推进停止；
// 错误必须经错误分类体系标注（见 errors.go：不可重试用 Permanent 包装，结果
// 未知按可重试，at-least-once 允许重复、幂等收敛）。
type Sink interface {
	// Name 标识名（诊断/日志/SinkGroup 组名拼接），如 "mysql"、"kafka"。
	Name() string
	// Ping 存活检查（worker 启动检查器周期调用；不判可用性只判连通性）。
	Ping(ctx context.Context) error
	// Send 同步投递一批事件，全部成功才返回 nil（组内位点推进以此为准）。
	Send(ctx context.Context, batch []*model.Event) error
	// Close 释放底层资源（连接/producer）；ctx 取消时也应尽快返回。
	Close(ctx context.Context) error
}

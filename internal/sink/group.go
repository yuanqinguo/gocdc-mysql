package sink

import (
	"context"
	"fmt"
	"strings"

	"gocdc/internal/model"
)

// SinkGroup 一条规则启用的 sink 集合（05 §1/§6）：
//   - 顺序发送：装配层按「先 MySQL 后 MQ」排列（MySQL 失败概率高且是终态存储，
//     先保它）；前一个失败则不再投递后续 sink（05 §6 单边熔断全停语义）；
//   - 位点推进要求组内全部 sink 确认（Send 返回 nil 才 ack，05 §6）；
//   - 每边 at-least-once 独立重试（sink 内部退避），组层面不做额外重试
//     （worker → 反压 → 重放兜底）。
type SinkGroup struct {
	sinks []Sink
}

// NewSinkGroup 构造（至少一个 sink）。
func NewSinkGroup(sinks ...Sink) (*SinkGroup, error) {
	var real []Sink
	for _, s := range sinks {
		if s != nil {
			real = append(real, s)
		}
	}
	if len(real) == 0 {
		return nil, fmt.Errorf("sink: SinkGroup 至少需要一个 sink")
	}
	return &SinkGroup{sinks: real}, nil
}

func (g *SinkGroup) Name() string {
	names := make([]string, 0, len(g.sinks))
	for _, s := range g.sinks {
		names = append(names, s.Name())
	}
	return "group(" + strings.Join(names, "+") + ")"
}

// Ping 全部 sink 存活检查（任一失败即失败）。
func (g *SinkGroup) Ping(ctx context.Context) error {
	for _, s := range g.sinks {
		if err := s.Ping(ctx); err != nil {
			return fmt.Errorf("sink %s ping: %w", s.Name(), err)
		}
	}
	return nil
}

// Send 按序投递（先 MySQL 后 MQ）；单边失败立即上抛（位点停推 → 反压 →
// 恢复后从断点续投，05 §6/§7）。
func (g *SinkGroup) Send(ctx context.Context, batch []*model.Event) error {
	for _, s := range g.sinks {
		if err := s.Send(ctx, batch); err != nil {
			return fmt.Errorf("sink 组成员 %s 失败: %w", s.Name(), err)
		}
	}
	return nil
}

// Close 逆序关闭（后启的依赖先关）。
func (g *SinkGroup) Close(ctx context.Context) error {
	var firstErr error
	for i := len(g.sinks) - 1; i >= 0; i-- {
		if err := g.sinks[i].Close(ctx); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

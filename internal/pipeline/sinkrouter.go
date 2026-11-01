package pipeline

import (
	"context"
	"fmt"
	"sync"

	"gocdc/internal/model"
	"gocdc/internal/sink"
)

// SinkRouter 按规则的 sink 启用集路由批次事件（05 §1：同一批事件按规则路由到
// 对应 SinkGroup；先 MySQL 后 MQ，05 §6）。
//
// 组合缓存：每「sink 启用组合」建一个 SinkGroup（同名同规则的表必然同组合，
// 段内顺序保持——游程分组语义不受影响）。Router 是全部 worker 共享的单例，
// groups 懒加载必须持锁——并发写 map 会触发 runtime fatal 崩进程。
type SinkRouter struct {
	rules    *CompiledRules
	mysql    sink.Sink
	kafka    sink.Sink
	rocketmq sink.Sink

	mu     sync.Mutex
	groups map[string]sink.Sink // 规则 Key → SinkGroup
}

// NewSinkRouter 构造（任一 sink 可 nil = 未启用）。
func NewSinkRouter(rules *CompiledRules, mysql, kafka, rocketmq sink.Sink) *SinkRouter {
	return &SinkRouter{
		rules: rules, mysql: mysql, kafka: kafka, rocketmq: rocketmq,
		groups: map[string]sink.Sink{},
	}
}

// group 按规则 Key 取（或懒建）SinkGroup（并发安全）。
func (r *SinkRouter) group(key string) (sink.Sink, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if g, ok := r.groups[key]; ok {
		return g, nil
	}
	rule := r.rules.MatchByKey(key)
	if rule == nil {
		return nil, fmt.Errorf("sinkrouter: 规则 %s 未匹配", key)
	}
	g, err := r.groupFor(rule)
	if err != nil {
		return nil, err
	}
	r.groups[key] = g
	return g, nil
}

// Send 按规则分组路由；组内顺序 = 批内原序（保序），组间 = 批内首次出现序。
func (r *SinkRouter) Send(ctx context.Context, batch []*model.Event) error {
	if len(batch) == 0 {
		return nil
	}
	// ① 批内首次出现序的规则组
	var order []string
	seen := map[string]bool{}
	for _, ev := range batch {
		rule := r.rules.Match(ev.Database, ev.Table)
		if rule == nil {
			// 无规则事件不应到达（Dispatcher 已过滤）；保守上抛不静默跳过
			return fmt.Errorf("sinkrouter: %s.%s 无规则（过滤层失守）", ev.Database, ev.Table)
		}
		if !seen[rule.Key] {
			seen[rule.Key] = true
			order = append(order, rule.Key)
		}
	}
	// ② 逐组投递（组内保持批内顺序；组间按出现序；组合缓存）
	for _, key := range order {
		g, err := r.group(key)
		if err != nil {
			return err
		}
		var sub []*model.Event
		for _, ev := range batch {
			if rule := r.rules.Match(ev.Database, ev.Table); rule != nil && rule.Key == key {
				sub = append(sub, ev)
			}
		}
		if err := g.Send(ctx, sub); err != nil {
			return err
		}
	}
	return nil
}

func (r *SinkRouter) groupFor(rule *model.Rule) (sink.Sink, error) {
	var group []sink.Sink
	if rule.Sinks.MySQL {
		if r.mysql == nil {
			return nil, fmt.Errorf("sinkrouter: 规则 %s 启用 mysql sink 但 targets.mysql 未配置", rule.Key)
		}
		group = append(group, r.mysql)
	}
	if rule.Sinks.Kafka {
		if r.kafka == nil {
			return nil, fmt.Errorf("sinkrouter: 规则 %s 启用 kafka sink 但 targets.kafka 未配置", rule.Key)
		}
		group = append(group, r.kafka)
	}
	if rule.Sinks.RocketMQ {
		if r.rocketmq == nil {
			return nil, fmt.Errorf("sinkrouter: 规则 %s 启用 rocketmq sink 但 targets.rocketmq 未配置", rule.Key)
		}
		group = append(group, r.rocketmq)
	}
	return sink.NewSinkGroup(group...)
}

func (r *SinkRouter) Name() string { return "router" }

// Ping 全部底层 sink。
func (r *SinkRouter) Ping(ctx context.Context) error {
	for _, s := range []sink.Sink{r.mysql, r.kafka, r.rocketmq} {
		if s == nil {
			continue
		}
		if err := s.Ping(ctx); err != nil {
			return err
		}
	}
	return nil
}

// Close 全部底层 sink。
func (r *SinkRouter) Close(ctx context.Context) error {
	var first error
	for _, s := range []sink.Sink{r.mysql, r.kafka, r.rocketmq} {
		if s == nil {
			continue
		}
		if err := s.Close(ctx); err != nil && first == nil {
			first = err
		}
	}
	return first
}

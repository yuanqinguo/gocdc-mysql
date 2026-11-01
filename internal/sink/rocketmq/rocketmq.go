package rocketmq

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	rmq "github.com/apache/rocketmq-clients/golang/v5"
	"github.com/apache/rocketmq-clients/golang/v5/credentials"

	"gocdc/internal/config"
	"gocdc/internal/metrics"
	"gocdc/internal/model"
	"gocdc/internal/sink"
)

// Options 装配参数（config.Targets.RocketMQ + 规则覆盖解析闭包）。
type Options struct {
	NameServers     string // v5 gRPC proxy endpoint（如 "10.0.3.1:8081"，多地址 ; 分隔）
	Group           string
	TopicPrefix     string
	OrderBy         string // sink.OrderByRow | OrderByTable
	SendTimeout     time.Duration
	SendConcurrency int // 组间并发发送上限；<=0 = 默认 32（组内仍严格串行，05 §4）
	ACL             config.ACLOptions
	Metrics         *metrics.Metrics
	Topics          []string // 预声明 topic 集合（装配层按规则展开；实战坑必须 Start 前给全）
	TopicOverride   func(db, table string) string
}

// defaultSendConcurrency 组间并发默认值：8 worker 共享全局信号量时约 4k~2W 条/s
// （1~2ms RTT），覆盖 09 §7 单热表 1W 行/s 峰值基线。
const defaultSendConcurrency = 32

// producer rmq.Producer 的收窄子集：v5 客户端 Producer 含未导出方法（isClient），
// 包外无法实现 fake——收窄后单测可注入（Send/Start/GracefulStop 语义与真实一致）。
type producer interface {
	Send(ctx context.Context, msg *rmq.Message) ([]*rmq.SendReceipt, error)
	Start() error
	GracefulStop() error
}

// Sink RocketMQ 下游（sink.Sink 实现）。
type Sink struct {
	producer   producer
	opts       Options
	started    bool
	mu         sync.RWMutex
	metrics    *metrics.Metrics
	sem        chan struct{} // 组间并发信号量（全局有界，09 §2「无隐式无界」）
	inflightMu sync.Mutex
	inflight   map[string]int // (topic\0group) → 在途 Send 计数（组级串行断言）
}

// New 构造（不 Start；Start 经 Start 显式调用以便 fail-fast 序列在装配层控制）。
func New(opts Options) (*Sink, error) {
	if opts.NameServers == "" {
		return nil, sink.Permanent(fmt.Errorf("rocketmq sink: name_servers 不能为空"))
	}
	if opts.SendTimeout <= 0 {
		opts.SendTimeout = 5 * time.Second
	}
	if opts.OrderBy == "" {
		opts.OrderBy = sink.OrderByRow
	}
	if opts.SendConcurrency <= 0 {
		opts.SendConcurrency = defaultSendConcurrency
	}
	cfg := &rmq.Config{
		Endpoint:  opts.NameServers, // v5 proxy endpoint（分号分隔多地址）
		NameSpace: "",
		Credentials: &credentials.SessionCredentials{
			AccessKey:    opts.ACL.AccessKey,
			AccessSecret: opts.ACL.SecretKey,
		},
	}
	// 发送超时由每次 Send 的 ctx（SendTimeout）控制；客户端内部重试默认 3 次
	producerOpts := []rmq.ProducerOption{}
	if len(opts.Topics) > 0 {
		producerOpts = append(producerOpts, rmq.WithTopics(opts.Topics...))
	}
	producer, err := rmq.NewProducer(cfg, producerOpts...)
	if err != nil {
		return nil, fmt.Errorf("rocketmq sink: 构造 producer: %w", err)
	}
	return &Sink{
		producer: producer, opts: opts, metrics: opts.Metrics,
		sem:      make(chan struct{}, opts.SendConcurrency),
		inflight: make(map[string]int),
	}, nil
}

// Start 启动 producer（预取 topic 路由；实战坑：未声明的 topic 会卡 settings 同步）。
//
// 注意 v5 客户端 WithTopics 只做路由预取，不会创建 topic：broker 未预先建 topic
// 时启动报 TOPIC_NOT_FOUND/ILLEGAL_TOPIC。报错里给出
// 运维动作提示，避免用户对着 CODE:17 猜。
func (s *Sink) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.started {
		return nil
	}
	if err := s.producer.Start(); err != nil {
		return fmt.Errorf("rocketmq sink: producer 启动: %w"+
			"（若为 TOPIC_NOT_FOUND：topic 需先在 broker 预先创建——"+
			"mqadmin updateTopic -n <namesrv> -b <broker:10911> -t <topic>，"+
			"或 broker 开 autoCreateTopicEnable）", err)
	}
	s.started = true
	return nil
}

// GracefulStop 停止 producer（等待在途请求）。
func (s *Sink) GracefulStop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.started {
		s.producer.GracefulStop()
		s.started = false
	}
}

func (s *Sink) Name() string { return "rocketmq" }

// Ping 无副作用：仅检查启动状态，不向真实 topic 发测试消息。
func (s *Sink) Ping(_ context.Context) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.started {
		return fmt.Errorf("rocketmq sink: producer 未启动")
	}
	return nil
}

// Close 满足 sink.Sink（等价 GracefulStop）。
func (s *Sink) Close(_ context.Context) error { s.GracefulStop(); return nil }

// sendUnit 一条消息的发送前产物（构建一次，组间并发时复用）。
type sendUnit struct {
	group string // 顺序组 = 05 §2.3 key（同行/同表派同队列保序）
	msg   *rmq.Message
	ev    *model.Event
	size  int
}

// Send 批量同步发送：一事件一消息，全部成功才返回 nil（05 §1）。
//
// 并发模型（05 §4）：v5 客户端 Producer.Send 仅单条
// （无批量 API，send0 内部多条路径未公开），同步逐条时单发送流吞吐 ≈ 1/RTT，
// 峰值 1W 行/s 会积压（09 §7 单热表基线）。批内按 (topic, 顺序组) 分组：
//   - 组内严格串行——同组消息的到达序 = 发送序，broker 按到达序落盘，
//     这是顺序消息保序的唯一来源；
//   - 组间经全局信号量（send_concurrency）并发——不同组路由互不相干，可并行。
//
// 任一组失败 → 整批错误 → worker 退避重试整批（at-least-once，已发出消息的
// 重复由下游 event_id 幂等消化）。首个错误触发 cancel 让其余组尽早收尾。
func (s *Sink) Send(ctx context.Context, batch []*model.Event) error {
	s.mu.RLock()
	started := s.started
	s.mu.RUnlock()
	if !started {
		return sink.Permanent(fmt.Errorf("rocketmq sink: producer 未启动（装配层须先 Start）"))
	}
	if len(batch) == 0 {
		return nil
	}

	// 构建阶段前置：BuildMessage 失败 = Permanent，且发生在任何发送之前
	// （逐条边构建边发会在中途 Permanent 留下部分已发批——at-least-once 下
	// 无害，但前置后失败面更小）。
	units := make([]sendUnit, 0, len(batch))
	for _, ev := range batch {
		topic := sink.TopicName(s.opts.TopicPrefix, s.topicOverride(ev), ev.Database, ev.Table)
		msg, err := sink.BuildMessage(ev, s.opts.OrderBy)
		if err != nil {
			return sink.Permanent(err)
		}
		m := &rmq.Message{Topic: topic, Body: msg.Value}
		m.SetKeys(ev.EventID)     // Keys = event_id（消息查询/轨迹）
		m.SetTag(string(ev.Type)) // Tag = 事件类型（INSERT/UPDATE/DELETE/DDL）
		// 顺序组 = 05 §2.3 key。message group 字符集与 topic 同（^[%|a-zA-Z0-9_-]+$，
		// 不收点）——key 里的点（库名/表名分隔）替换为下划线，否则带点库名在发送时
		// 被 broker 拒绝。点→下划线非单射（理论上两个 key 折叠同组），只会把两行并成
		// 一组串行（保序方向安全），不会乱序。
		group := strings.ReplaceAll(msg.Key, ".", "_")
		m.SetMessageGroup(group)
		units = append(units, sendUnit{group: group, msg: m, ev: ev, size: len(msg.Value)})
	}

	// 组分组：组键 (topic, group) 保序去重（topic 才是 group 的作用域）；
	// 组内下标升序 = 批内原序。
	groupOf := make(map[string][]int, len(units))
	gkeys := make([]string, 0, len(units))
	for i := range units {
		gk := units[i].msg.Topic + "\x00" + units[i].group
		if _, ok := groupOf[gk]; !ok {
			gkeys = append(gkeys, gk)
		}
		groupOf[gk] = append(groupOf[gk], i)
	}

	// 组级串行断言：同一顺序组跨 Send 在途冲突 → 可重试错误（stock 重扫与
	// 增量 worker 并发同表时可能出现，退避后冲突自然消解），绝不静默乱序。
	if err := s.registerGroups(gkeys); err != nil {
		return err
	}
	defer s.unregisterGroups(gkeys)

	sctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var (
		mu       sync.Mutex
		firstErr error
	)
	fail := func(err error) {
		mu.Lock()
		if firstErr == nil {
			firstErr = err
		}
		mu.Unlock()
		cancel()
	}
	var wg sync.WaitGroup
	for _, gk := range gkeys {
		idxs := groupOf[gk]
		wg.Add(1)
		go func(idxs []int) {
			defer wg.Done()
			select {
			case s.sem <- struct{}{}:
			case <-sctx.Done():
				return // 首错已记录/停机取消：无需重复报错
			}
			defer func() { <-s.sem }()
			for _, i := range idxs {
				if err := s.sendOne(sctx, &units[i]); err != nil {
					fail(err)
					return
				}
			}
		}(idxs)
	}
	wg.Wait()
	if firstErr != nil {
		return firstErr
	}
	return sctx.Err() // 全部发完但被外部取消（停机）：如实上抛，重复由幂等消化
}

func (s *Sink) sendOne(ctx context.Context, u *sendUnit) error {
	sctx, cancel := context.WithTimeout(ctx, s.opts.SendTimeout)
	defer cancel()
	receipts, err := s.producer.Send(sctx, u.msg)
	if err != nil {
		return classify(err)
	}
	if len(receipts) == 0 {
		// 无回执 = 投递结果未知：按可重试处理重发（at-least-once 幂等收敛）
		return fmt.Errorf("rocketmq sink: %s 无发送回执（结果未知，将重发）", u.ev.EventID)
	}
	if s.metrics != nil {
		s.metrics.EventsOut.WithLabelValues(u.ev.Database, u.ev.Table, string(u.ev.Type), s.Name()).Inc()
		s.metrics.BytesOut.WithLabelValues(s.Name()).Add(float64(u.size))
	}
	return nil
}

// registerGroups 组级串行断言：全部组键一次性登记；任一已在途 → 回滚已登记项
// 并返回可重试错误。同组并发 = 到达序不可控 = 顺序违约，宁可失败重试。
func (s *Sink) registerGroups(keys []string) error {
	s.inflightMu.Lock()
	defer s.inflightMu.Unlock()
	added := make([]string, 0, len(keys))
	for _, k := range keys {
		if s.inflight[k] > 0 {
			for _, a := range added {
				s.inflight[a]--
			}
			return sink.Retryable(fmt.Errorf("rocketmq sink: 顺序组 %q 已有在途发送（组级串行冲突，退避重试即收敛）",
				strings.ReplaceAll(k, "\x00", "/")))
		}
		s.inflight[k]++
		added = append(added, k)
	}
	return nil
}

func (s *Sink) unregisterGroups(keys []string) {
	s.inflightMu.Lock()
	defer s.inflightMu.Unlock()
	for _, k := range keys {
		if s.inflight[k] <= 1 {
			delete(s.inflight, k) // 归零即清理，map 不随历史组键无限增长
		} else {
			s.inflight[k]--
		}
	}
}

func (s *Sink) topicOverride(ev *model.Event) string {
	if s.opts.TopicOverride != nil {
		return s.opts.TopicOverride(ev.Database, ev.Table)
	}
	return ""
}

// classify 发送错误分类：topic 不存在/认证拒绝为永久；
// remoting 超时与 broker 不可达为可重试。未识别保守可重试（重发幂等收敛）。
func classify(err error) error {
	if err == nil {
		return nil
	}
	msg := err.Error()
	switch {
	case strings.Contains(msg, "TOPIC_NOT_EXIST"),
		strings.Contains(msg, "TOPIC_NOT_FOUND"),
		strings.Contains(msg, "No topic route info"),
		strings.Contains(msg, "ILLEGAL_TOPIC"),
		strings.Contains(msg, "illegal characters"),
		strings.Contains(msg, "TOPIC_ALREADY_EXISTS"),
		// 顺序消息发到非 FIFO topic（broker 属性 message.type 不匹配）——
		// 配置错误，重试无意义
		strings.Contains(msg, "message type not match"),
		strings.Contains(msg, "authentication"),
		strings.Contains(msg, "permission"),
		strings.Contains(msg, "AUTHORIZATION"):
		return sink.Permanent(fmt.Errorf("rocketmq sink: %w", err))
	default:
		return fmt.Errorf("rocketmq sink: %w", err)
	}
}

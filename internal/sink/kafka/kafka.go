package kafka

import (
	"context"
	"crypto/tls"
	"fmt"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"

	"gocdc/internal/config"
	"gocdc/internal/metrics"
	"gocdc/internal/model"
	"gocdc/internal/sink"
)

// Options 装配参数（config.Targets.Kafka + 规则级覆盖解析闭包）。
type Options struct {
	Brokers         []string
	TopicPrefix     string
	OrderBy         string // sink.OrderByRow | OrderByTable
	Compression     string // none|gzip|snappy|lz4|zstd
	Idempotent      bool
	Ack             string // all | one
	SendTimeout     time.Duration
	MaxMessageBytes int
	SASL            config.SASLOptions
	TLS             config.TLSOptions
	Metrics         *metrics.Metrics
	SourceID        string // 指标 source label（多源；空 = 单源/测试）

	// TopicOverride(db, table) → 规则级 topic 覆盖（rules[].sinks.kafka.topic；
	// 空 = prefix + "{db}_{table}"，与 RocketMQ 统一规范）。
	TopicOverride func(db, table string) string
}

// Sink Kafka 下游（sink.Sink 实现）。
type Sink struct {
	cl      *kgo.Client
	opts    Options
	metrics *metrics.Metrics
}

// New 构造并建连（不阻塞等待 broker——Ping 时验证）。
func New(opts Options) (*Sink, error) {
	if len(opts.Brokers) == 0 {
		return nil, sink.Permanent(fmt.Errorf("kafka sink: brokers 不能为空"))
	}
	if opts.SendTimeout <= 0 {
		opts.SendTimeout = 5 * time.Second
	}
	if opts.MaxMessageBytes <= 0 {
		opts.MaxMessageBytes = 1048576
	}
	if opts.OrderBy == "" {
		opts.OrderBy = sink.OrderByRow
	}

	clientOpts := []kgo.Opt{
		kgo.SeedBrokers(opts.Brokers...),
		kgo.RecordDeliveryTimeout(opts.SendTimeout),
		kgo.ProduceRequestTimeout(opts.SendTimeout),
	}
	if !opts.Idempotent {
		clientOpts = append(clientOpts, kgo.DisableIdempotentWrite())
	}
	switch opts.Ack {
	case "one":
		clientOpts = append(clientOpts, kgo.RequiredAcks(kgo.LeaderAck()))
	default: // all（默认，不丢）
		clientOpts = append(clientOpts, kgo.RequiredAcks(kgo.AllISRAcks()))
	}
	if cc, err := compression(opts.Compression); err != nil {
		return nil, err
	} else if cc != nil {
		clientOpts = append(clientOpts, kgo.ProducerBatchCompression(*cc))
	}
	if saslMech, err := saslMechanism(opts.SASL); err != nil {
		return nil, err
	} else if saslMech != nil {
		clientOpts = append(clientOpts, kgo.SASL(saslMech))
	}
	if tlsCfg, err := tlsConfig(opts.TLS); err != nil {
		return nil, err
	} else if tlsCfg != nil {
		clientOpts = append(clientOpts, kgo.DialTLSConfig(tlsCfg))
	}

	cl, err := kgo.NewClient(clientOpts...)
	if err != nil {
		return nil, fmt.Errorf("kafka sink: 构造客户端: %w", err)
	}
	return &Sink{cl: cl, opts: opts, metrics: opts.Metrics}, nil
}

func (s *Sink) Name() string { return "kafka" }

// Ping 集群可达性（元数据请求，无副作用）。
func (s *Sink) Ping(ctx context.Context) error { return s.cl.Ping(ctx) }

// Close 关闭客户端（等待在途生产完成）。
func (s *Sink) Close(_ context.Context) error { s.cl.Close(); return nil }

// Send 批量同步发送（05 §1：全部成功才返回 nil）。
// 一事件一消息；消息超限在发送前预检（05 §7 poison）。
func (s *Sink) Send(ctx context.Context, batch []*model.Event) error {
	if len(batch) == 0 {
		return nil
	}
	records := make([]*kgo.Record, 0, len(batch))
	for _, ev := range batch {
		topic := sink.TopicName(s.opts.TopicPrefix, s.topicOverride(ev), ev.Database, ev.Table)
		msg, err := sink.BuildMessage(ev, s.opts.OrderBy, s.opts.SourceID)
		if err != nil {
			return sink.Permanent(err)
		}
		if len(msg.Value) > s.opts.MaxMessageBytes {
			if s.metrics != nil {
				s.metrics.MessageOversize.WithLabelValues(s.opts.SourceID, ev.Database, ev.Table, s.Name()).Inc()
			}
			return sink.Permanent(&sink.OversizeError{Topic: topic, Size: len(msg.Value), Limit: s.opts.MaxMessageBytes})
		}
		records = append(records, &kgo.Record{Topic: topic, Key: []byte(msg.Key), Value: msg.Value})
	}

	results := s.cl.ProduceSync(ctx, records...)
	if err := results.FirstErr(); err != nil {
		return classify(err)
	}
	if s.metrics != nil {
		for i, ev := range batch {
			s.metrics.EventsOut.WithLabelValues(s.opts.SourceID, ev.Database, ev.Table, string(ev.Type), s.Name()).Inc()
			s.metrics.BytesOut.WithLabelValues(s.opts.SourceID, s.Name()).Add(float64(len(records[i].Value)))
		}
	}
	return nil
}

func (s *Sink) topicOverride(ev *model.Event) string {
	if s.opts.TopicOverride != nil {
		return s.opts.TopicOverride(ev.Database, ev.Table)
	}
	return ""
}

// classify 生产错误分类：kgo 的超时/不可达类为可重试；
// RecordTooLarge/消息过大与认证拒绝为永久。未识别错误保守按可重试
// （RecordDeliveryTimeout 内部重试耗尽后的超时属于 Ambiguous → 重发幂等收敛）。
func classify(err error) error {
	if err == nil {
		return nil
	}
	msg := err.Error()
	switch {
	case strings.Contains(msg, "KAFKA_ERR__MSG_SIZE_TOO_LARGE"),
		strings.Contains(msg, "MSG_SIZE_TOO_LARGE"),
		strings.Contains(msg, "SASL authentication"),
		strings.Contains(msg, "topic authorization"),
		strings.Contains(msg, "group authorization"):
		return sink.Permanent(fmt.Errorf("kafka sink: %w", err))
	default:
		return fmt.Errorf("kafka sink: %w", err)
	}
}

func compression(name string) (*kgo.CompressionCodec, error) {
	switch name {
	case "", "snappy":
		cc := kgo.SnappyCompression()
		return &cc, nil
	case "none":
		return nil, nil
	case "gzip":
		cc := kgo.GzipCompression()
		return &cc, nil
	case "lz4":
		cc := kgo.Lz4Compression()
		return &cc, nil
	case "zstd":
		cc := kgo.ZstdCompression()
		return &cc, nil
	default:
		return nil, sink.Permanent(fmt.Errorf("kafka sink: 未知压缩 %q", name))
	}
}

func saslMechanism(o config.SASLOptions) (sasl.Mechanism, error) {
	if !o.Enabled {
		return nil, nil
	}
	switch o.Mechanism {
	case "plain":
		return plain.Plain(func(context.Context) (plain.Auth, error) {
			return plain.Auth{User: o.User, Pass: osGetenv(o.PasswordEnv)}, nil
		}), nil
	case "scram-sha256":
		return scram.Sha256(func(context.Context) (scram.Auth, error) {
			return scram.Auth{User: o.User, Pass: osGetenv(o.PasswordEnv)}, nil
		}), nil
	case "scram-sha512":
		return scram.Sha512(func(context.Context) (scram.Auth, error) {
			return scram.Auth{User: o.User, Pass: osGetenv(o.PasswordEnv)}, nil
		}), nil
	default:
		return nil, sink.Permanent(fmt.Errorf("kafka sink: 未知 SASL 机制 %q", o.Mechanism))
	}
}

func tlsConfig(o config.TLSOptions) (*tls.Config, error) {
	if !o.Enabled {
		return nil, nil
	}
	if o.SkipVerify {
		return &tls.Config{InsecureSkipVerify: true}, nil
	}
	if o.CA != "" {
		caCert, err := osReadFile(o.CA)
		if err != nil {
			return nil, sink.Permanent(fmt.Errorf("kafka sink: 读取 CA: %w", err))
		}
		pool := certPool()
		if !pool.AppendCertsFromPEM(caCert) {
			return nil, sink.Permanent(fmt.Errorf("kafka sink: CA 证书无效 %s", o.CA))
		}
		return &tls.Config{RootCAs: pool}, nil
	}
	return &tls.Config{}, nil
}

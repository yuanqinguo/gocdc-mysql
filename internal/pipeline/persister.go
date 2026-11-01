package pipeline

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"gocdc/internal/metrics"
	"gocdc/internal/model"
	"gocdc/internal/position"
	"gocdc/internal/sink"
)

// PositionStore 位点存储接口（*position.Store 满足；测试可注入内存实现）。
type PositionStore interface {
	Load(ctx context.Context) (model.Position, bool, error)
	Save(ctx context.Context, p model.Position) error
}

// Persister 位点落盘（03 §5.2）：
//   - 周期（save interval，默认 3s）检查水位推进 → 写 etcd（节流由周期天然承担，
//     Throttler 供外部强制路径共享节流窗口）；
//   - 强制路径（rotate/DDL/停机/全量交接）经 Force 立即写；
//   - 失败不关管线：退避重试 + 计数 + 告警；连续失败 ≥ maxFailures 熔断（回调停管线）——
//     一次抖动不该永久停摆且无告警，而位点写不进还继续消费会放大重放窗口，
//     故先重试、超限才停机。
//   - ErrGateRejected（lease 失效/失主）→ 立即熔断：继续消费必属无主写入。
type Persister struct {
	wm          *Watermark
	store       PositionStore
	interval    time.Duration
	maxFailures int
	met         *metrics.Metrics
	log         *zap.Logger

	nodeID      string
	term        int64
	lastEventTs func() int64 // 位点滞后口径：save 时刻最近事件的 binlog 秒

	mu        sync.Mutex
	forceCh   chan struct{}
	failures  int
	onCircuit func(reason error) // 熔断回调（装配层停管线）
}

// NewPersister 构造。onCircuit 可 nil（仅记录）。
func NewPersister(wm *Watermark, store PositionStore, interval time.Duration,
	maxFailures int, nodeID string, term int64, met *metrics.Metrics,
	log *zap.Logger, onCircuit func(error), lastEventTs func() int64) *Persister {
	if interval <= 0 {
		interval = 3 * time.Second
	}
	if maxFailures <= 0 {
		maxFailures = 30
	}
	if log == nil {
		log = zap.NewNop()
	}
	return &Persister{
		wm: wm, store: store, interval: interval, maxFailures: maxFailures,
		met: met, log: log, nodeID: nodeID, term: term,
		forceCh: make(chan struct{}, 1), onCircuit: onCircuit, lastEventTs: lastEventTs,
	}
}

// Force 请求立即落盘（rotate/DDL/停机；非阻塞，去重）。
func (p *Persister) Force() {
	select {
	case p.forceCh <- struct{}{}:
	default:
	}
}

// Run 阻塞循环直至 ctx 取消。
func (p *Persister) Run(ctx context.Context) error {
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			// 停机兜底：无条件保存当前水位（04 §8 步骤 4）。persistCtx 已取消，
			// 用独立超时 ctx 执行最后一次写；失败仅告警——位点落后由重启重放消化。
			saveCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_, pos := p.wm.Current()
			err := p.saveOnce(saveCtx, pos)
			cancel()
			if err != nil {
				p.log.Warn("停机位点保存失败（重启将重放）", zap.Error(err))
			}
			return ctx.Err()
		case <-ticker.C:
			if _, pos, ok := p.wm.Advance(); ok {
				if err := p.saveWithRetry(ctx, pos); err != nil {
					return err
				}
			}
		case <-p.forceCh:
			// 强制保存不受推进判据限制（rotate 可能无新 ack 但文件已切换）
			_, pos := p.wm.Current()
			if err := p.saveWithRetry(ctx, pos); err != nil {
				return err
			}
		}
	}
}

// saveWithRetry 指数退避重试（03 §5.2：失败不关管线，重试至上限熔断）。
// 返回非 nil = 已熔断（调用方结束 Persister → 装配层停管线）。
func (p *Persister) saveWithRetry(ctx context.Context, pos model.Position) error {
	for attempt := 1; ; attempt++ {
		err := p.saveOnce(ctx, pos)
		if err == nil {
			p.mu.Lock()
			p.failures = 0
			p.mu.Unlock()
			return nil
		}
		// 计数与熔断判定（gate 拒绝立即熔断；其余累计至 maxFailures）
		p.mu.Lock()
		p.failures++
		n := p.failures
		p.mu.Unlock()
		if errors.Is(err, position.ErrGateRejected) {
			p.log.Error("位点写入被 lease 门槛拒绝（失主/分区），立即熔断", zap.Error(err))
			if p.onCircuit != nil {
				p.onCircuit(err)
			}
			return err
		}
		if n >= p.maxFailures {
			p.log.Error("位点连续保存失败达上限，熔断停管线",
				zap.Int("failures", n), zap.Error(err))
			cerr := sink.Permanent(fmt.Errorf("位点连续保存失败 %d 次: %w", n, err))
			if p.onCircuit != nil {
				p.onCircuit(cerr)
			}
			return cerr
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(sink.Backoff(attempt)):
		}
	}
}

// saveOnce 单次写入（位点字段补全：seq/term/node/时间）。
func (p *Persister) saveOnce(ctx context.Context, pos model.Position) error {
	base, _ := p.wm.Current()
	pos.Seq = base
	pos.Term = p.term
	pos.NodeID = p.nodeID
	if err := p.store.Save(ctx, pos); err != nil {
		if p.met != nil {
			p.met.PositionSaveFailures.Inc()
		}
		p.log.Warn("位点保存失败，将退避重试", zap.Error(err),
			zap.String("file", pos.Name), zap.Uint32("pos", pos.Pos))
		return err
	}
	if p.met != nil {
		p.met.LastSavedSeq.Set(float64(pos.Seq))
		if p.lastEventTs != nil {
			if ts := p.lastEventTs(); ts > 0 {
				p.met.PositionLagSeconds.Set(float64(time.Now().Unix() - ts))
			}
		}
	}
	return nil
}

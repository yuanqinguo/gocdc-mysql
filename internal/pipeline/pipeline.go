package pipeline

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"go.uber.org/zap"

	"gocdc/internal/logging"
	"gocdc/internal/metrics"
	"gocdc/internal/model"
	"gocdc/internal/schema"
	"gocdc/internal/sink"
	"gocdc/internal/source"
	"gocdc/internal/transform"
)

// Options 管线装配参数（main 装配层构造）。
type Options struct {
	Source      *source.Source
	SchemaMgr   *schema.Manager
	Transformer *transform.Transformer
	Sink        sink.Sink
	Store       PositionStore
	Rules       *CompiledRules
	Metrics     *metrics.Metrics
	Log         *zap.Logger

	Workers       int           // 分片并行度（0/1 = N=1；04 §2 按表分片）
	QueueSize     int           // worker 有界队列（0=8192）
	BatchSize     int           // 0=500
	FlushInterval time.Duration // 0=200ms
	SaveInterval  time.Duration // 0=3s
	NodeID        string
	Term          int64         // 单节点=0；选主部署为当选 revision
	MaxSaveFails  int           // 位点连续失败熔断阈值（0=30）
	DrainTimeout  time.Duration // 优雅停机排空上限（0=30s，04 §8）

	ExcludeSchemas []string // 库过滤（系统库+配置追加）

	DDLHooks *DDLHooks // DDL 策略外部依赖（nil = pause 停管线兜底）

	// MasterPosFn 源库当前位点（SHOW BINARY LOG STATUS 的 Name/Pos/Executed_Gtid_Set；
	// nil 跳过 master_position_lag 指标）。
	MasterPosFn func(ctx context.Context) (model.Position, error)

	// Flavor 源库类型（mysql|mariadb，GTID 集合解析用；空=mysql）。
	Flavor string
}

// Pipeline 增量同步管线（N worker 按表分片并行，04 §1/§2）。
//
// goroutine 拓扑：source.Run（拉取）→ Dispatcher（seq/过滤/控制事件，按表
// 路由到 N 个 worker 有界队列，退出时关队列=DrainMark）→ 各 Worker
// （transform/攒批/发送/ack）→ Watermark（跨 worker 连续水位）→
// Persister（节流+强制落盘）。
type Pipeline struct {
	opts Options
	log  *zap.Logger

	lastEventTs atomic.Int64 // 最近事件的 binlog 秒级时间戳（11 §1.2 lag 指标口径）
}

// LastEventTs 最近事件时间戳（Persister 位点滞后口径复用）。
func (p *Pipeline) LastEventTs() int64 { return p.lastEventTs.Load() }

// DDLPause 当前等待人工决策的 DDL（nil=无；/status running→paused 覆盖）。
func (p *Pipeline) DDLPause() *DDLPauseState { return p.opts.DDLHooks.Pause() }

// New 装配。
func New(opts Options) *Pipeline {
	if opts.QueueSize <= 0 {
		opts.QueueSize = 8192
	}
	if opts.DrainTimeout <= 0 {
		opts.DrainTimeout = 30 * time.Second
	}
	if opts.Log == nil {
		opts.Log = zap.NewNop()
	}
	return &Pipeline{opts: opts, log: opts.Log}
}

// Run 启动管线并阻塞：从 start 位点拉取增量，直到 ctx 取消（优雅停机 drain）
// 或致命错误（sink 不可重试/转换 fail_event/位点熔断/DDL pause）。
//
// 停机协议（04 §8）：①停拉取 → ②Dispatcher 退出关闭队列（DrainMark）→
// ③Worker 排空队列并冲刷 → ④水位终值强制保存。任一步超时记录日志进入下一步
// （位点可能落后 → 重启重放，at-least-once 兜底）。
func (p *Pipeline) Run(ctx context.Context, start model.Position, resume source.ResumeFunc) error {
	// ── N worker 有界队列（04 §1/§2：按表分片；workers 配置）──
	n := p.opts.Workers
	if n < 1 {
		n = 1
	}
	queues := make([]chan *EventUnit, n)
	for i := range queues {
		queues[i] = make(chan *EventUnit, p.opts.QueueSize)
	}
	wm := NewWatermark(start)

	// ── 拉取侧 ctx：独立于主 ctx，先于 worker 停止（排空期间不再有新事件）──
	srcCtx, stopSrc := context.WithCancel(context.WithoutCancel(ctx))

	// ── 熔断通道：任一致命错误记录首因并停拉取（队列随后关闭 → worker 排空）──
	// 先于拉取侧 goroutine 定义：拉取返回即熔断（下方位点持久化共用同一闭包）。
	var (
		fatalMu sync.Mutex
		fatal   error
	)
	abort := func(reason error) {
		fatalMu.Lock()
		if fatal == nil {
			fatal = reason
		}
		fatalMu.Unlock()
		stopSrc() // 队列随后关闭 → worker 排空退出
	}

	var srcWG sync.WaitGroup
	srcCh := make(chan *source.Event, 1024)
	srcWG.Add(1)
	go func() {
		defer srcWG.Done()
		if err := p.opts.Source.Run(srcCtx, start, resume, srcCh); err != nil && !errors.Is(err, context.Canceled) {
			// 拉取侧返回 = 起始位点非法（validateStart 拒绝）/GTID 集合解析失败——
			// 重试无意义，必须熔断全管线。若只 Warn：dispatcher 阻塞在永远无数据的
			// srcCh 上、队列不关、水位冻结——进程存活、Web 正常、同步静默死亡。
			p.log.Error("binlog 拉取终止，熔断管线", zap.Error(err))
			abort(fmt.Errorf("binlog 拉取终止: %w", err))
		}
	}()
	dispatcher := NewDispatcher(p.opts.Rules, p.opts.ExcludeSchemas, queues, p.log)
	srcWG.Add(1)
	go func() {
		defer srcWG.Done()
		defer close(srcCh)
		// Dispatcher.Run 退出时自行 closeQueues（= DrainMark，广播到全部队列）
		if err := dispatcher.Run(srcCtx, srcCh, wm, p.opts.NodeID); err != nil && !errors.Is(err, context.Canceled) {
			p.log.Warn("分发退出", zap.Error(err))
		}
	}()

	// ── 位点持久化：熔断回调触发全管线中止（03 §5.2）──
	persistCtx, stopPersist := context.WithCancel(context.WithoutCancel(ctx))
	persister := NewPersister(wm, p.opts.Store, p.opts.SaveInterval, p.opts.MaxSaveFails,
		p.opts.NodeID, p.opts.Term, p.opts.Metrics, p.log, abort, p.lastEventTs.Load)
	var persistWG sync.WaitGroup
	persistWG.Add(1)
	go func() {
		defer persistWG.Done()
		_ = persister.Run(persistCtx)
	}()

	// ── Workers：独立于主 ctx（停机靠队列关闭驱动排空）；任一致命错误全停 ──
	workerCtx, stopWorker := context.WithCancel(context.WithoutCancel(ctx))
	workerErr := make(chan error, n) // 每 worker 一发；首个错误驱动停机
	var workerWG sync.WaitGroup
	for i := range queues {
		workerWG.Add(1)
		w := NewWorker(queues[i], i, p.opts.Rules, p.opts.SchemaMgr, p.opts.Transformer,
			p.opts.Sink, wm, persister.Force, p.opts.DDLHooks, &p.lastEventTs, p.opts.Metrics,
			p.opts.BatchSize, p.opts.FlushInterval, p.log)
		go func() {
			defer workerWG.Done()
			workerErr <- w.Run(workerCtx)
		}()
	}
	// 队列深度/水位采样（04 §4/§5：背压前兆与洞检测，11 §1.3）
	sampleCtx, stopSample := context.WithCancel(context.WithoutCancel(ctx))
	var sampleWG sync.WaitGroup
	sampleWG.Add(1)
	go func() {
		defer sampleWG.Done()
		p.sampleLoop(sampleCtx, queues, wm)
	}()

	p.log.Info("管线启动",
		logging.Event(logging.EventPipelineStart),
		zap.String("file", start.Name), zap.Uint32("pos", start.Pos),
		zap.String("gtid", start.GTIDSet), zap.Uint64("seq", start.Seq),
		zap.Int("workers", n))

	// ── 生命周期：等主 ctx 或任一 worker 先退出 ──
	var (
		runErr    error
		workerRes error // 记录首个非 nil 的 worker 错误
		got       int   // 已收到的 worker 退出结果数
	)
	select {
	case werr := <-workerErr:
		got++
		workerRes = werr
		runErr = werr
	case <-ctx.Done():
		runErr = ctx.Err()
	}

	// 停机序列（04 §8）
	drain := time.NewTimer(p.opts.DrainTimeout)
	defer drain.Stop()
	stopSrc()
	srcWG.Wait() // dispatcher 退出 → 关闭全部队列（DrainMark）
	timedOut := false
	for got < n {
		select {
		case werr := <-workerErr:
			got++
			if workerRes == nil {
				workerRes = werr
			}
		case <-drain.C:
			timedOut = true
		}
		if timedOut {
			break
		}
	}
	if timedOut {
		p.log.Warn("排空超时，强制中止 worker（位点可能落后，重启重放兜底）",
			zap.Int("exited", got), zap.Int("workers", n))
		stopWorker() // 强停后 worker 的 select ctx 分支必然退出
		for got < n {
			werr := <-workerErr
			got++
			if workerRes == nil {
				workerRes = werr
			}
		}
	}
	workerWG.Wait()
	runErr = firstFatal(&fatalMu, &fatal, workerRes, runErr)
	stopSample()
	sampleWG.Wait()
	stopPersist()
	persistWG.Wait()
	stopWorker() // 释放 worker ctx（所有路径最终都到达此处）

	p.log.Info("管线退出", logging.Event(logging.EventPipelineStop), zap.Error(runErr))
	return runErr
}

// sampleLoop 周期采样队列深度与水位（11 §1.3：gocdc_worker_queue_depth /
// gocdc_watermark_seq；洞检测告警——pending 非空且队列全空 = worker 卡死前兆）。
func (p *Pipeline) sampleLoop(ctx context.Context, queues []chan *EventUnit, wm *Watermark) {
	interval := p.opts.SaveInterval
	if interval <= 0 {
		interval = 3 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	var holeSince time.Time
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if p.opts.Metrics == nil {
				continue
			}
			depths := 0
			for i := range queues {
				d := len(queues[i])
				depths += d
				p.opts.Metrics.WorkerQueueDepth.WithLabelValues(fmt.Sprint(i)).Set(float64(d))
			}
			base, _ := wm.Current()
			p.opts.Metrics.WatermarkSeq.Set(float64(base))
			// 洞检测：有 pending（乱序未连续）且全部队列已空且持续 > 2 个周期
			// → worker 卡死前兆，告警日志
			// lag 三口径（11 §1.2）：事件延迟 = now − 最近事件 binlog 时间戳；
			// 位点滞后由 Persister 在 save 时写入（复用 lastEventTs）
			if ts := p.lastEventTs.Load(); ts > 0 {
				p.opts.Metrics.EventLagSeconds.Set(float64(time.Now().Unix() - ts))
			}
			if p.opts.MasterPosFn != nil {
				if mp, err := p.opts.MasterPosFn(ctx); err == nil {
					_, cur := wm.Current()
					p.opts.Metrics.SetMasterPositionLagBytes(computeLagBytes(cur, mp, p.opts.Flavor))
				}
			}
			if holes := wm.Pending(); holes > 0 && depths == 0 {
				if holeSince.IsZero() {
					holeSince = time.Now()
				} else if time.Since(holeSince) > 2*interval {
					p.log.Warn("水位存在持续未连续确认（worker 卡死前兆）",
						zap.Int("holes", holes), zap.Duration("age", time.Since(holeSince)))
				}
			} else {
				holeSince = time.Time{}
			}
		}
	}
}

// computeLagBytes master_position_lag_bytes 口径（11 §1.2）。
//
// GTID 口径优先：use_gtid 重启后位点停在 rotate 锚点——file:pos 是 binlog dump
// 流起点（无业务意义，已执行事件被 GTID 过滤不上抛，无 XID 锚点修正），字节差
// 恒为大值假滞后。位点集合覆盖 master executed（无未消费事务）才是追平语义。
// 非 GTID（file:pos）维持字节差：同文件可比，跨文件视为已追平至文件尾。
func computeLagBytes(cur, master model.Position, flavor string) float64 {
	if cur.GTIDSet != "" && master.GTIDSet != "" {
		if ps, perr := mysql.ParseGTIDSet(flavor, cur.GTIDSet); perr == nil {
			if ms, merr := mysql.ParseGTIDSet(flavor, master.GTIDSet); merr == nil && ps.Contain(ms) {
				return 0
			}
		}
		// 集合未覆盖（真有未消费事务）：回落字节差——rotate 锚点场景偏大但
		// 方向正确、非 0 可告警；追平后由 XID 锚点推进归 0。
	}
	if cur.Name == master.Name { // 同文件才可比；跨文件置 0（已追平至文件尾）
		return float64(master.Pos) - float64(cur.Pos)
	}
	return 0
}

// firstFatal 致命错误优先于 worker 错误、再优先于普通退出原因。
func firstFatal(fatalMu *sync.Mutex, fatal *error, workerRes, runErr error) error {
	fatalMu.Lock()
	f := *fatal
	fatalMu.Unlock()
	if f != nil {
		return fmt.Errorf("管线熔断: %w", f)
	}
	if workerRes != nil {
		return workerRes
	}
	return runErr
}

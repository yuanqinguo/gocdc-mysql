// 选举驱动的管线生命周期（03 §4）：follower 不启管线；当选 → SetGate
// 位点门槛 + 全量初始化 + 启动增量管线；失联/落选 → 立即停管线。
//
// 锁纪律：pipeMu 只保护 pipeCancel/pipeDone 指针赋值——全量扫描
// （小时级）绝不持锁（OnLost 抢锁会阻塞 elector 状态机：失联后不重新参选、
// SIGTERM 挂死）；stock 与增量管线共用 leadership 派生 ctx，OnLost 取消即中断。
package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/zap"

	"gocdc/internal/api"
	"gocdc/internal/config"
	electorf "gocdc/internal/elector"
	"gocdc/internal/etcdx"
	"gocdc/internal/logging"
	"gocdc/internal/metrics"
	"gocdc/internal/model"
	"gocdc/internal/pipeline"
	"gocdc/internal/position"
	"gocdc/internal/schema"
	"gocdc/internal/source"
	stockf "gocdc/internal/stock"
	"gocdc/internal/transform"
)

// app 进程运行期共享组件（装配一次，onElected/onLost 方法消费）。
type app struct {
	opts *options
	cfg  *config.Config
	ctx  context.Context // 应用生命周期 ctx（reg.Update / leadership 派生父 ctx）

	cli    *clientv3.Client
	prefix string
	sess   *concurrency.Session
	reg    *etcdx.Registrar
	nodeID string

	srcDB       *sql.DB
	schemaMgr   *schema.Manager
	transformer *transform.Transformer
	router      *pipeline.SinkRouter
	rules       *pipeline.CompiledRules
	ddlHooks    *pipeline.DDLHooks
	ddlGate     *pipeline.EtcdDDLGate
	tz          *time.Location
	met         *metrics.Metrics

	store       *position.Store
	markers     *stockf.MarkerStore
	ruleSchemas []string
	resumeFn    source.ResumeFunc // 源端断线重连一律回到 etcd 已持久化位点

	stockSvc atomic.Pointer[stockf.Service] // Web restock 读取（onElected 重建）

	pipeMu     sync.Mutex
	pipeCancel context.CancelFunc
	pipeDone   chan struct{}

	// 人工暂停管线（13 §3）：pause 持久标志在 etcd {prefix}/pipeline-pause
	//（无 lease——暂停是持久意图，跨重启/失位，直到显式 resume）。
	pauseKey   string                           // etcd 暂停标志 key
	leading    atomic.Bool                      // 本进程承担管线（onElected 置位/onLost 清位）
	opPaused   atomic.Bool                      // 人工暂停态（/status paused_by_op 展示）
	curElected atomic.Pointer[electorf.Elected] // 当选信息（resume 重启流程复用）
}

// newApp 汇集装配产物并构造 resume 回调（04 §5 位点不变式：断线重连
// 回到 etcd 已确认位点，at-least-once 重放）。
func newApp(opts *options, cfg *config.Config, ctx context.Context,
	coord *coordination, comp *components, srcDB *sql.DB, met *metrics.Metrics) *app {

	a := &app{
		opts: opts, cfg: cfg, ctx: ctx,
		cli: coord.cli, prefix: coord.prefix, sess: coord.sess, reg: coord.reg,
		nodeID: coord.nodeID,
		srcDB:  srcDB, schemaMgr: comp.schemaMgr, transformer: comp.transformer,
		router: comp.router, rules: comp.rules, ddlHooks: comp.ddlHooks,
		ddlGate: comp.ddlGate,
		tz:      comp.tz, met: met,
		store: coord.store, markers: coord.markers,
		ruleSchemas: comp.ruleSchemas,
		pauseKey:    coord.prefix + "/pipeline-pause",
	}
	a.resumeFn = func() model.Position {
		rctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		p, ok, err := a.store.Load(rctx)
		if err != nil || !ok {
			return model.Position{}
		}
		return p
	}
	return a
}

// newPipeline 按任期构造增量管线（source 每任期新建；其余组件跨任期复用）。
func (a *app) newPipeline(term int64) *pipeline.Pipeline {
	return pipeline.New(pipeline.Options{
		Source: source.New(source.Config{
			Addr: a.cfg.Source.Addr, User: a.cfg.Source.User, Password: a.cfg.Source.Password,
			Charset: a.cfg.Source.Charset, Flavor: a.cfg.Source.Flavor, ServerID: a.cfg.Source.ServerID,
			Timezone: a.tz, UseGTID: a.cfg.Source.UseGTID,
			HeartbeatInterval: time.Duration(a.cfg.Source.HeartbeatIntervalMS) * time.Millisecond,
			ReadTimeout:       time.Duration(a.cfg.Source.ReadTimeoutMS) * time.Millisecond,
		}, logging.L()),
		SchemaMgr: a.schemaMgr, Transformer: a.transformer,
		Sink: a.router, Store: a.store, Rules: a.rules,
		DDLHooks: a.ddlHooks,
		Metrics:  a.met, Log: logging.L(),
		Workers:       a.cfg.Pipeline.Workers,
		QueueSize:     a.cfg.Pipeline.WorkerQueueSize,
		BatchSize:     a.cfg.Pipeline.BatchSize,
		FlushInterval: time.Duration(a.cfg.Pipeline.FlushIntervalMS) * time.Millisecond,
		SaveInterval:  time.Duration(a.cfg.Pipeline.PositionSaveIntervalMS) * time.Millisecond,
		NodeID:        a.nodeID, Term: term, MaxSaveFails: 30,
		DrainTimeout:   10 * time.Second, // 让位/失联切换 ≈ 排空时间（03 §4.2-4）
		ExcludeSchemas: a.cfg.Source.ExcludeSchemas,
		MasterPosFn: func(mctx context.Context) (model.Position, error) {
			return showMasterStatus(mctx, a.srcDB)
		},
		Flavor: a.cfg.Source.Flavor,
	})
}

// onElected 当选回调（elector 转发；本方法内转 goroutine，不阻塞选举循环）：
// 位点/标记写门槛切换 → 全量初始化（06）→ 增量管线。
func (a *app) onElected(info electorf.Elected) {
	go a.runLeaderFlow(info)
}

// runLeaderFlow leader 侧主流程：占槽（pipeCancel/pipeDone）→ 写门槛 →
// 人工暂停检查 → 全量初始化 → 增量管线。当选与 /pipeline/resume 共用；
// 退出时机 = leadCtx 取消（失位/让位/停机/人工暂停）或致命错误。
func (a *app) runLeaderFlow(info electorf.Elected) {
	// leadership 生命周期 ctx：全量 + 增量共用；OnLost/停机即取消
	leadCtx, cancel := context.WithCancel(a.ctx)
	done := make(chan struct{})
	a.pipeMu.Lock()
	if a.pipeCancel != nil { // 已有任期在运行（重复当选回调）
		a.pipeMu.Unlock()
		cancel()
		return
	}
	a.pipeCancel = cancel
	a.pipeDone = done
	a.pipeMu.Unlock()
	defer func() {
		cancel()
		a.pipeMu.Lock()
		a.pipeCancel = nil
		a.pipeDone = nil
		a.pipeMu.Unlock()
		close(done)
	}()

	a.leading.Store(true)
	a.curElected.Store(&info)
	defer a.leading.Store(false)

	// 位点/全量标记写门槛 → leader 指针 key + 当选 lease（06 §4.2-2）
	a.store.SetGate(info.LeaderKey, info.LeaseID)
	a.markers.SetGate(info.LeaderKey, info.LeaseID)

	// ── 人工暂停闸（13 §3）：持久 pause key 存在 → 不启动全量/增量；
	// 保持 Leader（DDL 审批 gate 继续服务），恢复走 /pipeline/resume ──
	if paused, perr := a.pauseFlagExists(leadCtx); perr != nil {
		logging.L().Warn("读取人工暂停标志失败（按未暂停处理）", zap.Error(perr))
	} else if paused {
		a.opPaused.Store(true)
		logging.L().Info("人工暂停标志生效，本任期不启动管线",
			zap.Int64("term", info.Term))
		setPipelineState(api.PipePaused, "管线已被人工暂停（恢复入口：总控台或位点管理页 /pipeline/resume）")
		return
	}
	a.opPaused.Store(false)

	// ── etcd 治理 GC：stock 死标记 + 已决 DDL 记录，低频后台循环 ──
	go a.gcLoop(leadCtx)

	// ── 全量初始化（06）：do_stock 表无 done 标记 → 先全量后增量；
	// done → 跳过（重启不重跑全量）。失联取消不按失败记账 ──
	setPipelineState(api.PipeStock, "")
	startPos, err := a.runStock(leadCtx)
	if err != nil {
		// 状态对外可见：人工暂停取消 = paused（恢复闭环可见）；失位/停机 =
		// 正常停止；真失败 = paused 待人工介入
		if a.opPaused.Load() {
			setPipelineState(api.PipePaused, "管线已被人工暂停（恢复入口：总控台或位点管理页 /pipeline/resume）")
		} else if leadCtx.Err() != nil {
			setPipelineState(api.PipeStopped, "")
		} else {
			setPipelineState(api.PipePaused, "全量初始化失败: "+err.Error())
		}
		return // runStock 内部已按 取消/失败 分别记账
	}

	met := a.met
	met.LeaderState.Set(1)
	met.LeaderTerm.Set(float64(info.Term))
	// 角色尽力更新（有界超时：etcd 抖动不拖慢管线启动；失败不影响当选）
	uctx, ucancel := context.WithTimeout(a.ctx, 2*time.Second)
	_ = a.reg.Update(uctx, a.sess, "leader")
	ucancel()

	pipelineUp.Store(true)
	setPipelineState(api.PipeRunning, "")
	pl := a.newPipeline(info.Term)
	currentPipeline.Store(pl)
	logging.L().Info("leader 管线启动",
		logging.Event(logging.EventPipelineStart),
		zap.Int64("term", info.Term),
		zap.String("file", startPos.Name), zap.Uint32("pos", startPos.Pos))
	perr := pl.Run(leadCtx, startPos, a.resumeFn)
	pipelineUp.Store(false)
	if perr != nil && !errors.Is(perr, context.Canceled) && leadCtx.Err() == nil {
		// 管线致命错误（sink 不可重试 / 转换 fail_event / 位点熔断 / 拉取侧
		// 位点非法）：记录并保留 leadership 复核（DDL 审批 gate 仍需本节点
		// 服务）；人工修复后重启进程恢复
		logging.L().Error("管线退出（非停机）", zap.Error(perr))
		met.PipelinePaused.WithLabelValues("*", "*", "fatal").Set(1)
		setPipelineState(api.PipePaused, perr.Error())
	} else if a.opPaused.Load() {
		// 人工暂停（/pipeline/pause 取消 leadCtx）：位点终值已落盘，
		// 展示为可恢复的暂停态而非失位停机
		setPipelineState(api.PipePaused, "管线已被人工暂停（恢复入口：总控台或位点管理页 /pipeline/resume）")
	} else {
		setPipelineState(api.PipeStopped, "") // 失位/让位/停机：正常退出
	}
}

// gcLoop leader 侧低频 etcd 治理：① stock 死标记——白名单外表的标记
// 永远不会再被 runStock 读取（done 保留语义不受影响，白名单内不动）；
// ② 已决 DDL 记录——超过 DDLGCRetention 删除，pending 永不删。
// 当选后立即跑一次，之后每小时一次；失位/停机随 leadCtx 退出。
func (a *app) gcLoop(ctx context.Context) {
	run := func() {
		gctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		if n, err := a.markers.GC(gctx, func(dbTable string) bool {
			return a.rules.MatchByKey(dbTable) != nil
		}); err != nil {
			logging.L().Warn("stock 死标记 GC 失败", zap.Error(err))
		} else if n > 0 {
			logging.L().Info("stock 死标记 GC 完成", zap.Int("removed", n))
		}
		if n, err := a.ddlGate.GC(gctx, pipeline.DDLGCRetention); err != nil {
			logging.L().Warn("DDL 已决记录 GC 失败", zap.Error(err))
		} else if n > 0 {
			logging.L().Info("DDL 已决记录 GC 完成", zap.Int("removed", n))
		}
	}
	run()
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			run()
		}
	}
}

// runStock 全量初始化（06）：无标记表快照 + 增量起点推导（max(位点, 全局
// stock_position)，06 §4.1）+ PURGED 检查（06 §7）。返回增量起点。
// ctx 取消 = 失联/停机（静默退出，defer 清理）；其余失败记错误日志。
func (a *app) runStock(leadCtx context.Context) (model.Position, error) {
	stockSvc := stockf.New(a.srcDB, a.schemaMgr, a.transformer, a.router,
		a.markers, a.cfg.Stock, a.rules, a.met, a.nodeID, logging.L())
	stockSvc.SetSchemas(a.ruleSchemas)
	a.stockSvc.Store(stockSvc)
	sres, serr := stockSvc.Run(leadCtx)
	if serr != nil {
		if leadCtx.Err() != nil {
			return model.Position{}, leadCtx.Err() // 失联/停机取消：defer 清理即可
		}
		logging.L().Error("全量初始化失败，本轮不启动管线（修复后重启/resume）",
			logging.Event(logging.EventStockStart), zap.Error(serr))
		return model.Position{}, serr
	}
	if leadCtx.Err() != nil {
		return model.Position{}, leadCtx.Err()
	}

	// 增量起点 = max(已存位点, 全局 stock_position)（06 §4.1）
	startPos, ok, err := a.store.Load(leadCtx)
	if err != nil {
		logging.L().Error("当选后读取位点失败，本轮不启动管线", zap.Error(err))
		return model.Position{}, err
	}
	if sres.StockPos.Name != "" && (!ok || laterBinlog(sres.StockPos, startPos)) {
		startPos = sres.StockPos
	}
	if !ok && sres.StockPos.Name == "" {
		sp, serr := showMasterStatus(leadCtx, a.srcDB)
		if serr != nil {
			logging.L().Error("首启无位点且读取源库位点失败，本轮不启动管线", zap.Error(serr))
			return model.Position{}, serr
		}
		startPos = sp
		logging.L().Info("首启位点取自源库当前 master 位点（无全量表）",
			zap.String("file", startPos.Name), zap.Uint32("pos", startPos.Pos))
	}
	// PURGED 检查（06 §7）：binlog 被清理 → 拒绝启动增量，绝不带缺口运行
	if perr := stockf.CheckPurged(leadCtx, a.srcDB, startPos); perr != nil {
		logging.L().Error("binlog 保留检查失败，本轮不启动增量",
			logging.Event(logging.EventStockDone), zap.Error(perr))
		return model.Position{}, perr
	}
	logging.L().Info("全量完成/跳过",
		logging.Event(logging.EventStockDone),
		zap.Int64("rows", sres.Scanned), zap.Int("skipped", sres.Skipped),
		zap.String("start_file", startPos.Name), zap.Uint32("start_pos", startPos.Pos))
	return startPos, nil
}

// onLost 失去 leader：只读指针后立即放锁（goroutine 侧 defer 负责置 nil
// 清理）——绝不持锁等待重型操作，elector 状态机同步调用本回调（03 §4）。
func (a *app) onLost(reason string) {
	a.leading.Store(false)
	// 失位即移交恢复责任：接管节点当选时读暂停标志重新置位（暂停闸）；
	// 本节点若保留 opPaused 会在失位后误响应 /pipeline/resume（删标志+
	// 带死租约重启流程，gate 失效兜底但徒增双拉窗口）
	a.opPaused.Store(false)
	a.pipeMu.Lock()
	cancel := a.pipeCancel
	done := a.pipeDone
	a.pipeMu.Unlock()
	a.met.LeaderState.Set(0)
	// 先停管线（安全性最高优先级）：etcd 失联时注册更新会阻塞在 KV 重试上，
	// 若先更新角色，03 §4.2-1 的「TTL 内停管线」会被拖成 TTL+，窗口内
	// 旧 leader 仍在投递——故停管线永远先于注册更新
	if cancel != nil {
		logging.L().Warn("失去 leader，停止管线", zap.String("reason", reason))
		cancel()
	}
	// 节点角色尽力更新（有界超时：etcd 不可达时不拖慢让位/重选）
	uctx, ucancel := context.WithTimeout(a.ctx, 2*time.Second)
	_ = a.reg.Update(uctx, a.sess, "follower")
	ucancel()
	if done != nil {
		select {
		case <-done:
		case <-time.After(15 * time.Second):
			logging.L().Warn("停管线等待超时（位点可能落后，重启重放兜底）")
		}
	}
}

// runSingleLeader 单节点模式（ha.enabled: false）：
// 跳过选举，进程直接以 leader 身份启动管线。写门槛语义不变——以 session
// 租约在 {prefix}/leader 创建门键（与选举当选后的 leader 键同位），位点/标记
// 写照常被租约门槛保护：session 死亡（etcd 不可达）→ 门键随租约消失 →
// 位点写被拒 → 管线停，与集群模式失位语义一致；恢复需重启进程重走启动检查。
func (a *app) runSingleLeader(ctx context.Context) error {
	gateKey := a.prefix + "/leader"
	if _, err := a.cli.Put(ctx, gateKey, a.nodeID, clientv3.WithLease(a.sess.Lease())); err != nil {
		return fmt.Errorf("单节点 leader 门键创建失败: %w", err)
	}
	logging.L().Info("单节点模式（ha.enabled=false）：跳过选举，直接启动管线",
		zap.String("gate_key", gateKey))
	a.onElected(electorf.Elected{Term: 1, LeaseID: a.sess.Lease(), LeaderKey: gateKey})
	<-ctx.Done()
	return nil
}

// waitPipelineExit 停机等待管线完全退出（位点终值落盘）。
func (a *app) waitPipelineExit() {
	a.pipeMu.Lock()
	done := a.pipeDone
	a.pipeMu.Unlock()
	if done != nil {
		select {
		case <-done:
		case <-time.After(20 * time.Second):
			logging.L().Warn("停机等待管线退出超时（位点可能落后）")
		}
	}
}

// pauseFlagExists 读 etcd 人工暂停标志（false, nil = 未暂停/读空）。
// 读失败按未暂停处理（fail-open）：暂停标志是人工写入的低频意图，当选本身
// 依赖 etcd 可用，抖动窗口内两者撞上的概率可忽略；fail-close 会让节点在
// etcd 间歇抖动时永远无法启动管线，代价不成比例。
func (a *app) pauseFlagExists(ctx context.Context) (bool, error) {
	pctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	resp, err := a.cli.Get(pctx, a.pauseKey)
	if err != nil {
		return false, err
	}
	return resp.Count > 0, nil
}

// pausePipeline 人工暂停（/pipeline/pause 闭包）：写持久标志 → 取消管线
// ctx（复用优雅停机：排空 + 位点终值落盘）。runLeaderFlow 退出分支看到
// opPaused 置 paused 态。非 leader / 重复暂停返回错误。
func (a *app) pausePipeline(ctx context.Context) error {
	if a.opPaused.Load() {
		return errors.New("管线已处于人工暂停状态")
	}
	if !a.leading.Load() {
		return errors.New("本节点当前不承担管线（热备/未当选），无需暂停")
	}
	if !a.opPaused.CompareAndSwap(false, true) {
		return errors.New("管线已处于人工暂停状态")
	}
	pctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if _, err := a.cli.Put(pctx, a.pauseKey, time.Now().Format(time.RFC3339)); err != nil {
		a.opPaused.Store(false)
		return fmt.Errorf("写入暂停标志失败: %w", err)
	}
	a.pipeMu.Lock()
	cancelFn := a.pipeCancel
	a.pipeMu.Unlock()
	logging.L().Warn("人工暂停管线（保持 Leader，恢复走 /pipeline/resume）")
	if cancelFn != nil {
		cancelFn() // 优雅停机协议接管：位点终值落盘后 runLeaderFlow 置 paused
	}
	return nil
}

// resumePipeline 恢复人工暂停（/pipeline/resume 闭包）：等暂停排空完全退出
// （位点终值落盘）→ CAS 清暂停态 → 删持久标志 → 复用当选信息重走
// runLeaderFlow（从 etcd 位点重启——支持暂停期间改位点）。
//
// 护栏用 opPaused 而非 leading：暂停即 runLeaderFlow 退出（leading 已清位），
// 但节点在选举层面仍是 Leader（gate/lease 存活）——「暂停中的 Leader」与
// 「热备/失位」在 leading 上不可区分（55 组 PP-05 两轮实证：用 leading 做
// 入口护栏会让恢复永远被自己拒绝，用它做重启前复核同样误杀——暂停态的
// leading 恒为 false）。失位裁决信号 = session 存活 + onLost 清 opPaused：
// 排空等待期间失位则 CAS 必然失败（零状态变更可直接重试）；CAS/删标志
// 之后到 go 之间的毫秒级失位窗口由 gate 失效兜底（位点写被拒→熔断），
// 新任期当选读不到标志自然启动，状态自收敛。
func (a *app) resumePipeline(ctx context.Context) error {
	if !a.opPaused.Load() {
		return errors.New("管线未处于人工暂停状态，无需恢复")
	}
	if a.sess != nil {
		select {
		case <-a.sess.Done(): // lease 已失效（etcd 失联）——恢复由接管节点执行
			return errors.New("本节点 etcd 会话已失效，恢复请由接管节点执行（或重启本进程）")
		default:
		}
	}
	// 先等暂停触发的排空完全退出（位点终值落盘）再动状态——失败可直接重试，
	// 不产生「标志已删但流程未起」的中间态（前序实现在此之前已清 CAS+删标志，
	// 超时后重试撞「未处于人工暂停状态」死路）
	a.pipeMu.Lock()
	done := a.pipeDone
	a.pipeMu.Unlock()
	if done != nil {
		select {
		case <-done:
		case <-time.After(15 * time.Second):
			return errors.New("暂停排空未完成（位点落盘中），请稍后重试恢复")
		}
	}
	if !a.opPaused.CompareAndSwap(true, false) { // 失位（onLost 清位）/并发恢复在此兜底
		return errors.New("管线未处于人工暂停状态，无需恢复")
	}
	rctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if _, err := a.cli.Delete(rctx, a.pauseKey); err != nil {
		a.opPaused.Store(true)
		return fmt.Errorf("删除暂停标志失败: %w", err)
	}
	info := a.curElected.Load()
	if info == nil {
		return errors.New("任期信息缺失，无法恢复（请重启进程）")
	}
	logging.L().Info("恢复人工暂停的管线",
		zap.Int64("term", info.Term))
	go a.runLeaderFlow(*info)
	return nil
}

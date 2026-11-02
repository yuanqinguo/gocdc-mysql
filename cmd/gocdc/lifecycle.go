// 选举驱动的多源管线生命周期（03 §4 + 多源 PRD 05）：
// follower 不启管线；当选 → 逐源 SetGate 写门槛 → GC → 每源一个 goroutine
// （暂停闸 → 全量 → 增量管线）；失联/落选 → cancel 共享 leadCtx → 全部源
// 并行 drain → 等待退出。
//
// 锁纪律（沿袭单源版）：pipeMu 只保护指针赋值——全量扫描（小时级）绝不持锁
// （OnLost 抢锁会阻塞 elector 状态机）；stock 与增量管线共用该源流程 ctx，
// leadCtx 取消即级联中断。
//
// 每源单槽互斥（src.pipeCancel）防同一源重复启动；leadMu 单槽防重复当选回调
// 重入 leader 流程。人工暂停只取消该源的派生 ctx，其余源与 leader 任期不受影响。
package main

import (
	"context"
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
	"gocdc/internal/source"
	stockf "gocdc/internal/stock"
)

// app 进程运行期共享组件（cluster 级一个 + 源表；装配一次，onElected/onLost 消费）。
type app struct {
	opts *options
	cfg  *config.Config
	ctx  context.Context // 应用生命周期 ctx（reg.Update / leadership 派生父 ctx）

	cli    *clientv3.Client
	kv     kvClient
	prefix string
	sess   *concurrency.Session
	reg    registrarClient
	nodeID string
	met    metricsClient

	sources *sourceTable

	// ── leader 任期（cluster 级单槽）──
	leadMu     sync.Mutex
	leadCancel context.CancelFunc
	leadDone   chan struct{}
	leadCtxRef atomic.Pointer[leadContext] // 当任 ctx（resume 重启源流程取用）
	leading    atomic.Bool                 // 本进程承担管线（onElected 置位/onLost 清位）
}

// leadContext 当任任期快照（resume 等延迟启动路径取用；失位后置 nil）。
type leadContext struct {
	ctx  context.Context
	info electorf.Elected
}

// kvClient/registrarClient/metricsClient 依赖别名（测试可替换）。
type (
	kvClient        = *etcdx.KV
	registrarClient = *etcdx.Registrar
	metricsClient   = *metrics.Metrics
)

// newApp 构造：源表（etcd 状态面逐源实例化）+ 共享协调面引用。
func newApp(opts *options, cfg *config.Config, ctx context.Context,
	cli *clientv3.Client, kv kvClient, prefix string,
	sess *concurrency.Session, reg registrarClient, nodeID string, met metricsClient) *app {

	return &app{
		opts: opts, cfg: cfg, ctx: ctx,
		cli: cli, kv: kv, prefix: prefix, sess: sess, reg: reg, nodeID: nodeID, met: met,
		sources: newSourceTable(cfg, prefix, kv),
	}
}

// newPipeline 按任期构造单源增量管线（source 每任期新建；其余组件跨任期复用）。
func (a *app) newPipeline(src *sourceState, term int64) *pipeline.Pipeline {
	b := src.built.Load()
	spec := src.spec
	return pipeline.New(pipeline.Options{
		Source: source.New(source.Config{
			Addr: spec.Source.Addr, User: spec.Source.User, Password: spec.Source.Password,
			Charset: spec.Source.Charset, Flavor: spec.Source.Flavor, ServerID: spec.Source.ServerID,
			Timezone: b.comp.tz, UseGTID: spec.Source.UseGTID,
			HeartbeatInterval: time.Duration(spec.Source.HeartbeatIntervalMS) * time.Millisecond,
			ReadTimeout:       time.Duration(spec.Source.ReadTimeoutMS) * time.Millisecond,
		}, logging.L()),
		SchemaMgr: b.comp.schemaMgr, Transformer: b.comp.transformer,
		Sink: b.comp.router, Store: src.store, Rules: b.comp.rules,
		DDLHooks: b.comp.ddlHooks,
		Metrics:  a.met, Log: logging.L(),
		Workers:       spec.Pipeline.Workers,
		QueueSize:     spec.Pipeline.WorkerQueueSize,
		BatchSize:     spec.Pipeline.BatchSize,
		FlushInterval: time.Duration(spec.Pipeline.FlushIntervalMS) * time.Millisecond,
		SaveInterval:  time.Duration(spec.Pipeline.PositionSaveIntervalMS) * time.Millisecond,
		NodeID:        a.nodeID, Term: term, MaxSaveFails: 30,
		DrainTimeout:   10 * time.Second, // 让位/失联切换 ≈ 排空时间（03 §4.2-4）
		ExcludeSchemas: spec.Source.ExcludeSchemas,
		SourceID:       src.sid,
		MasterPosFn: func(mctx context.Context) (model.Position, error) {
			return showMasterStatus(mctx, b.srcDB)
		},
		Flavor: spec.Source.Flavor,
	})
}

// onElected 当选回调（elector 转发；本方法内转 goroutine，不阻塞选举循环）：
// leader 流程 = 逐源启动（每源自己的 goroutine：写门槛 → 暂停闸 → 全量 → 增量）。
func (a *app) onElected(info electorf.Elected) {
	go a.runLeaderFlow(info)
}

// runLeaderFlow leader 侧主流程（占 leadMu 单槽）：
// 写门槛与全量/增量全部下沉到每源的 runSourceFlow；本流程只负责
// 任期 ctx、leader 指标/角色、GC 循环、就绪源并发启动。
func (a *app) runLeaderFlow(info electorf.Elected) {
	// leadership 生命周期 ctx：全部源共用；OnLost/停机即取消
	leadCtx, cancel := context.WithCancel(a.ctx)
	done := make(chan struct{})
	a.leadMu.Lock()
	if a.leadCancel != nil { // 已有任期在运行（重复当选回调）
		a.leadMu.Unlock()
		cancel()
		return
	}
	a.leadCancel = cancel
	a.leadDone = done
	a.leadMu.Unlock()
	defer func() {
		cancel()
		a.leadCtxRef.Store(nil)
		a.leadMu.Lock()
		a.leadCancel = nil
		a.leadDone = nil
		a.leadMu.Unlock()
		close(done)
	}()

	a.leading.Store(true)
	a.leadCtxRef.Store(&leadContext{ctx: leadCtx, info: info})
	defer a.leading.Store(false)

	met := a.met
	met.LeaderState.Set(1)
	met.LeaderTerm.Set(float64(info.Term))
	// 角色尽力更新（有界超时：etcd 抖动不拖慢管线启动；失败不影响当选）
	uctx, ucancel := context.WithTimeout(a.ctx, 2*time.Second)
	_ = a.reg.Update(uctx, a.sess, "leader")
	ucancel()

	// ── etcd 治理 GC：遍历全部源（stock 死标记 + 已决 DDL，低频后台循环）──
	go a.gcLoop(leadCtx)

	// ── 就绪源并发启动；failed/starting 源由装配监督循环在就绪后补入
	// （launchIfLeading，PRD 05 §2 握手）。leader 流程随后阻塞至任期结束
	// （leadCtx 取消 = 失位/让位/停机）——任期态（leading/leadCtxRef）必须
	// 存活整届：延迟就绪源的补入与 pause→resume 重启都依赖当任 ctx ──
	ready := a.sources.ready()
	pending := len(a.sources.all()) - len(ready)
	if pending > 0 {
		logging.L().Warn("部分源未就绪，本届任期先启动就绪源（其余源装配成功后自动加入）",
			zap.Int("ready", len(ready)), zap.Int("pending", pending))
	}
	for _, src := range ready {
		go a.runSourceFlow(leadCtx, src, info)
	}
	<-leadCtx.Done()
}

// runSourceFlow 单源任期流程（现行单源 runLeaderFlow 的逐源版）：
// 占源单槽 → 写门槛（gate 锚 = cluster leader key）→ 人工暂停闸 →
// 全量初始化 → 增量管线。退出时机 = 源 ctx 取消（失位/让位/停机/人工暂停）
// 或该源致命错误（paused 保持 leader 等人工，其余源不受影响——D5）。
func (a *app) runSourceFlow(leadCtx context.Context, src *sourceState, info electorf.Elected) {
	// 源流程 ctx：leadCtx 派生——人工暂停/该源致命退出只影响本源
	srcCtx, cancel := context.WithCancel(leadCtx)
	done := make(chan struct{})
	src.pipeMu.Lock()
	if src.pipeCancel != nil { // 该源已有任期流程在跑（重复启动）
		src.pipeMu.Unlock()
		cancel()
		return
	}
	src.pipeCancel = cancel
	src.pipeDone = done
	src.pipeMu.Unlock()
	defer func() {
		cancel()
		src.pipeMu.Lock()
		src.pipeCancel = nil
		src.pipeDone = nil
		src.pipeMu.Unlock()
		close(done)
	}()

	src.curElected.Store(&info)

	// 位点/全量标记写门槛 → 同一 cluster leader key + 当选 lease（PRD 04 §3：
	// 锚点唯一 ⇒ 防脑裂结论对全部源子树成立）
	src.store.SetGate(info.LeaderKey, info.LeaseID)
	src.markers.SetGate(info.LeaderKey, info.LeaseID)

	log := logging.L().With(zap.String("source", src.sid))

	// ── 人工暂停闸（13 §3 逐源版）：持久 pause key 存在 → 不启动全量/增量；
	// 保持 Leader（DDL 审批 gate 继续服务），恢复走该源 /pipeline/resume ──
	if paused, perr := a.pauseFlagExists(src, srcCtx); perr != nil {
		log.Warn("读取人工暂停标志失败（按未暂停处理）", zap.Error(perr))
	} else if paused {
		src.opPaused.Store(true)
		log.Info("人工暂停标志生效，本任期不启动该源管线", zap.Int64("term", info.Term))
		src.setState(api.PipePaused, "管线已被人工暂停（恢复入口：总控台或位点管理页该源的 /pipeline/resume）")
		return
	}
	src.opPaused.Store(false)

	// ── 全量初始化（06 逐源）：do_stock 表无 done 标记 → 先全量后增量 ──
	src.setState(api.PipeStock, "")
	startPos, err := a.runStock(srcCtx, src)
	if err != nil {
		// 状态对外可见：人工暂停取消 = paused（恢复闭环可见）；失位/停机 =
		// 正常停止；真失败 = paused 待人工介入（只停该源，D5）
		if src.opPaused.Load() {
			src.setState(api.PipePaused, "管线已被人工暂停（恢复入口：总控台或位点管理页该源的 /pipeline/resume）")
		} else if srcCtx.Err() != nil {
			src.setState(api.PipeStopped, "")
		} else {
			src.setState(api.PipePaused, "全量初始化失败: "+err.Error())
		}
		return // runStock 内部已按 取消/失败 分别记账
	}

	pipelineRunningSources.Add(1)
	src.running.Store(true)
	src.setState(api.PipeRunning, "")
	pl := a.newPipeline(src, info.Term)
	src.curPipeline.Store(pl)
	log.Info("leader 源管线启动",
		logging.Event(logging.EventPipelineStart),
		zap.Int64("term", info.Term),
		zap.String("file", startPos.Name), zap.Uint32("pos", startPos.Pos))
	perr := pl.Run(srcCtx, startPos, src.resumeFn)
	src.running.Store(false)
	pipelineRunningSources.Add(-1)
	if perr != nil && !errors.Is(perr, context.Canceled) && srcCtx.Err() == nil {
		// 管线致命错误（sink 不可重试 / 转换 fail_event / 位点熔断 / 拉取侧
		// 位点非法）：记录并保留 leadership 复核（DDL 审批 gate 仍需本节点
		// 服务）；人工修复后重启进程恢复。只停该源（D5 失败隔离）。
		log.Error("源管线退出（非停机）", zap.Error(perr))
		a.met.PipelinePaused.WithLabelValues(src.sid, "*", "*", "fatal").Set(1)
		src.setState(api.PipePaused, perr.Error())
	} else if src.opPaused.Load() {
		// 人工暂停（该源 /pipeline/pause 取消源 ctx）：位点终值已落盘，
		// 展示为可恢复的暂停态而非失位停机
		src.setState(api.PipePaused, "管线已被人工暂停（恢复入口：总控台或位点管理页该源的 /pipeline/resume）")
	} else {
		src.setState(api.PipeStopped, "") // 失位/让位/停机：正常退出
	}
}

// gcLoop leader 侧低频 etcd 治理（PRD 04 §7）：遍历全部源，逐源执行
// ① stock 死标记——白名单外表的标记永远不会再被 runStock 读取；
// ② 已决 DDL 记录——超过 DDLGCRetention 删除，pending 永不删。
// 单源失败只告警不中断其他源清理。当选后立即跑一次，之后每小时一次。
func (a *app) gcLoop(ctx context.Context) {
	run := func() {
		for _, src := range a.sources.all() {
			b := src.built.Load()
			if b == nil {
				continue // 未装配完成：无规则/gate 可查
			}
			gctx, cancel := context.WithTimeout(ctx, 30*time.Second)
			if n, err := src.markers.GC(gctx, func(dbTable string) bool {
				return b.comp.rules.MatchByKey(dbTable) != nil
			}); err != nil {
				logging.L().Warn("stock 死标记 GC 失败", zap.String("source", src.sid), zap.Error(err))
			} else if n > 0 {
				logging.L().Info("stock 死标记 GC 完成", zap.String("source", src.sid), zap.Int("removed", n))
			}
			if n, err := b.comp.ddlGate.GC(gctx, pipeline.DDLGCRetention); err != nil {
				logging.L().Warn("DDL 已决记录 GC 失败", zap.String("source", src.sid), zap.Error(err))
			} else if n > 0 {
				logging.L().Info("DDL 已决记录 GC 完成", zap.String("source", src.sid), zap.Int("removed", n))
			}
			cancel()
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

// runStock 单源全量初始化（06 逐源）：无标记表快照 + 增量起点推导
// （max(该源位点, 该源 stock_position)，06 §4.1）+ PURGED 检查（06 §7）。
// 返回该源增量起点。ctx 取消 = 失联/停机/人工暂停（静默退出，defer 清理）。
func (a *app) runStock(srcCtx context.Context, src *sourceState) (model.Position, error) {
	b := src.built.Load()
	log := logging.L().With(zap.String("source", src.sid))
	stockSvc := stockf.New(b.srcDB, b.comp.schemaMgr, b.comp.transformer, b.comp.router,
		src.markers, *src.spec.Stock, b.comp.rules, a.met, src.sid, a.nodeID, logging.L())
	stockSvc.SetSchemas(b.comp.ruleSchemas)
	src.stockSvc.Store(stockSvc)
	sres, serr := stockSvc.Run(srcCtx)
	if serr != nil {
		if srcCtx.Err() != nil {
			return model.Position{}, srcCtx.Err() // 失联/停机/暂停取消：defer 清理即可
		}
		log.Error("全量初始化失败，本轮不启动该源管线（修复后重启/resume）",
			logging.Event(logging.EventStockStart), zap.Error(serr))
		return model.Position{}, serr
	}
	if srcCtx.Err() != nil {
		return model.Position{}, srcCtx.Err()
	}

	// 增量起点 = max(已存位点, 全局 stock_position)（06 §4.1）
	startPos, ok, err := src.store.Load(srcCtx)
	if err != nil {
		log.Error("当选后读取位点失败，本轮不启动该源管线", zap.Error(err))
		return model.Position{}, err
	}
	if sres.StockPos.Name != "" && (!ok || laterBinlog(sres.StockPos, startPos)) {
		startPos = sres.StockPos
	}
	if !ok && sres.StockPos.Name == "" {
		sp, serr := showMasterStatus(srcCtx, b.srcDB)
		if serr != nil {
			log.Error("首启无位点且读取源库位点失败，本轮不启动该源管线", zap.Error(serr))
			return model.Position{}, serr
		}
		startPos = sp
		log.Info("首启位点取自源库当前 master 位点（无全量表）",
			zap.String("file", startPos.Name), zap.Uint32("pos", startPos.Pos))
	}
	// PURGED 检查（06 §7）：binlog 被清理 → 拒绝启动增量，绝不带缺口运行
	if perr := stockf.CheckPurged(srcCtx, b.srcDB, startPos); perr != nil {
		log.Error("binlog 保留检查失败，本轮不启动增量",
			logging.Event(logging.EventStockDone), zap.Error(perr))
		return model.Position{}, perr
	}
	log.Info("全量完成/跳过",
		logging.Event(logging.EventStockDone),
		zap.Int64("rows", sres.Scanned), zap.Int("skipped", sres.Skipped),
		zap.String("start_file", startPos.Name), zap.Uint32("start_pos", startPos.Pos))
	return startPos, nil
}

// onLost 失去 leader（cluster 级）：cancel 共享 leadCtx → 全部源并行执行现行
// drain 序列（停拉取 → 排空 → 刷 sink → 位点终值落盘）→ 并行等待退出
// （每源 drain 上限 10s，总等待 15s 封顶，PRD 04 §6.2）。
// 只读指针后立即放锁——绝不持锁等待重型操作（elector 状态机同步调用本回调）。
func (a *app) onLost(reason string) {
	a.leading.Store(false)
	a.leadCtxRef.Store(nil)
	// 失位即移交恢复责任：接管节点当选时读暂停标志重新置位（暂停闸）；
	// 保留 opPaused 会让失位节点误响应该源 /pipeline/resume
	for _, src := range a.sources.all() {
		src.opPaused.Store(false)
	}
	a.leadMu.Lock()
	cancel := a.leadCancel
	a.leadMu.Unlock()
	a.met.LeaderState.Set(0)
	// 先停管线（安全性最高优先级）：etcd 失联时注册更新会阻塞在 KV 重试上，
	// 若先更新角色，03 §4.2-1 的「TTL 内停管线」会被拖成 TTL+，窗口内
	// 旧 leader 仍在投递——故停管线永远先于注册更新
	if cancel != nil {
		logging.L().Warn("失去 leader，停止全部源管线", zap.String("reason", reason))
		cancel()
	}
	// 节点角色尽力更新（有界超时：etcd 不可达时不拖慢让位/重选）
	uctx, ucancel := context.WithTimeout(a.ctx, 2*time.Second)
	_ = a.reg.Update(uctx, a.sess, "follower")
	ucancel()
	a.waitSourcesDrain(15*time.Second, "停管线等待超时（位点可能落后，重启重放兜底）")
}

// waitSourcesDrain 等全部源流程退出（共享 deadline 封顶；每源并行 drain）。
func (a *app) waitSourcesDrain(timeout time.Duration, warnMsg string) {
	deadline := time.After(timeout)
	for _, src := range a.sources.all() {
		src.pipeMu.Lock()
		done := src.pipeDone
		src.pipeMu.Unlock()
		if done == nil {
			continue
		}
		select {
		case <-done:
		case <-deadline:
			logging.L().Warn(warnMsg, zap.String("source", src.sid))
			return
		}
	}
}

// runSingleLeader 单节点模式（ha.enabled: false）：
// 跳过选举，进程直接以 leader 身份启动管线。写门槛语义不变——以 session
// 租约在 {prefix}/leader 创建门键（与选举当选后的 leader 键同位），全部源的
// 位点/标记写照常被租约门槛保护：session 死亡（etcd 不可达）→ 门键随租约
// 消失 → 位点写被拒 → 管线停，与集群模式失位语义一致。
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

// waitPipelineExit 停机等待全部源管线完全退出（位点终值落盘）。
func (a *app) waitPipelineExit() {
	a.waitSourcesDrain(20*time.Second, "停机等待源管线退出超时（位点可能落后）")
}

// pauseFlagExists 读该源 etcd 人工暂停标志（false, nil = 未暂停/读空）。
// 读失败按未暂停处理（fail-open）：暂停标志是人工写入的低频意图，当选本身
// 依赖 etcd 可用，抖动窗口内两者撞上的概率可忽略；fail-close 会让节点在
// etcd 间歇抖动时永远无法启动管线，代价不成比例。
func (a *app) pauseFlagExists(src *sourceState, ctx context.Context) (bool, error) {
	pctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	resp, err := a.cli.Get(pctx, src.pauseKey)
	if err != nil {
		return false, err
	}
	return resp.Count > 0, nil
}

// pauseSourcePipeline 单源人工暂停（该源 /pipeline/pause 闭包）：写持久标志 →
// 取消该源流程 ctx（复用优雅停机：排空 + 位点终值落盘；其余源不受影响）。
// runSourceFlow 退出分支看到 opPaused 置 paused 态。非承担/重复暂停返回错误。
func (a *app) pauseSourcePipeline(ctx context.Context, src *sourceState) error {
	if src.opPaused.Load() {
		return errors.New("该源管线已处于人工暂停状态")
	}
	src.pipeMu.Lock()
	cancelFn := src.pipeCancel
	src.pipeMu.Unlock()
	if cancelFn == nil {
		return errors.New("本节点当前不承担该源管线（热备/未当选/源未就绪），无需暂停")
	}
	if !src.opPaused.CompareAndSwap(false, true) {
		return errors.New("该源管线已处于人工暂停状态")
	}
	pctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if _, err := a.cli.Put(pctx, src.pauseKey, time.Now().Format(time.RFC3339)); err != nil {
		src.opPaused.Store(false)
		return fmt.Errorf("写入暂停标志失败: %w", err)
	}
	logging.L().Warn("人工暂停源管线（保持 Leader，恢复走该源 /pipeline/resume）",
		zap.String("source", src.sid))
	cancelFn() // 优雅停机协议接管：位点终值落盘后 runSourceFlow 置 paused
	return nil
}

// resumeSourcePipeline 恢复单源人工暂停（该源 /pipeline/resume 闭包）：
// 等暂停排空完全退出（位点终值落盘）→ CAS 清暂停态 → 删持久标志 →
// 复用当任任期信息重走 runSourceFlow（从该源 etcd 位点重启——支持暂停期间
// 改位点）。
//
// 护栏用 opPaused 而非 leading（沿袭单源版 PP-05 实证教训）：暂停即源流程
// 退出（leading 不变但源槽已空），「暂停中的 Leader 源」与「热备/失位」在
// 源槽上不可区分。失位裁决信号 = session 存活 + onLost 清 opPaused：
// 排空等待期间失位则 CAS 必然失败（零状态变更可直接重试）；CAS/删标志之后
// 到 go 之间的毫秒级失位窗口由 gate 失效兜底（位点写被拒→熔断）。
func (a *app) resumeSourcePipeline(ctx context.Context, src *sourceState) error {
	if !src.opPaused.Load() {
		return errors.New("该源管线未处于人工暂停状态，无需恢复")
	}
	if a.sess != nil {
		select {
		case <-a.sess.Done(): // lease 已失效（etcd 失联）——恢复由接管节点执行
			return errors.New("本节点 etcd 会话已失效，恢复请由接管节点执行（或重启本进程）")
		default:
		}
	}
	// 先等暂停触发的排空完全退出（位点终值落盘）再动状态——失败可直接重试
	src.pipeMu.Lock()
	done := src.pipeDone
	src.pipeMu.Unlock()
	if done != nil {
		select {
		case <-done:
		case <-time.After(15 * time.Second):
			return errors.New("暂停排空未完成（位点落盘中），请稍后重试恢复")
		}
	}
	if !src.opPaused.CompareAndSwap(true, false) { // 失位（onLost 清位）/并发恢复在此兜底
		return errors.New("该源管线未处于人工暂停状态，无需恢复")
	}
	rctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if _, err := a.cli.Delete(rctx, src.pauseKey); err != nil {
		src.opPaused.Store(true)
		return fmt.Errorf("删除暂停标志失败: %w", err)
	}
	lc := a.leadCtxRef.Load()
	if lc == nil {
		return errors.New("任期信息缺失（已失位或未当选），无法恢复（请重启进程）")
	}
	logging.L().Info("恢复人工暂停的源管线",
		zap.String("source", src.sid), zap.Int64("term", lc.info.Term))
	go a.runSourceFlow(lc.ctx, src, lc.info)
	return nil
}

// 源装配监督（多源 PRD 05 §2 / D5 失败隔离）：
// 现行「任一启动检查失败 → 进程退出」收敛为「该源 failed + 退避重试」
// （30s 起、指数翻倍、封顶 5min），其余源不受影响；对单源 v1 部署是严格
// 改进（不再整进程退出，靠周期重试自愈）。
//
// 运行期致命错误不走此循环：管线熔断置 PAUSED 保持 leader 等人工（07 §6），
// 作用域为单源——装配监督只负责「把源装配到可启动」，不管运行期。
package main

import (
	"context"
	"time"

	"go.uber.org/zap"

	"gocdc/internal/logging"
)

// superviseSource 单源装配监督循环：build → ready（若在任则启动）/ failed →
// 退避重试。装配成功即返回（源此后常驻就绪，由 leader 生命周期驱动启停；
// 运行期致命错误不回装配循环）。
func (a *app) superviseSource(ctx context.Context, src *sourceState) {
	const (
		baseBackoff = 30 * time.Second
		maxBackoff  = 5 * time.Minute
	)
	backoff := baseBackoff
	retried := false
	for {
		src.phase.Store(srcStarting)
		err := a.buildSource(ctx, src)
		if err == nil {
			src.failInfo.Store(nil)
			src.phase.Store(srcReady)
			event := logging.EventSourceReady
			msg := "源装配完成"
			if retried { // failed 后重试成功（自愈），事件与首轮区分
				event, msg = logging.EventSourceRecovered, "源装配完成（failed 后重试恢复）"
			}
			logging.L().Info(msg,
				zap.String("source", src.sid),
				zap.String("src_addr", src.spec.Source.Addr),
				logging.Event(event))
			a.launchIfLeading(src)
			return
		}
		retried = true
		src.failInfo.Store(&failInfo{Err: err.Error(), Since: time.Now()})
		src.phase.Store(srcFailed)
		logging.L().Error("源装配失败（源级隔离，其余源不受影响；退避重试）",
			zap.String("source", src.sid),
			zap.String("event", logging.EventSourceComponentsFailed),
			zap.Error(err),
			zap.Duration("next_retry", backoff))
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

// buildSource 装配单源数据面组件：源库连接 → 启动检查 → 组件装配 + 目标表
// 核对（05 §5.1.4）。任一步失败即返回（源保持 failed；已建连接关闭）。
func (a *app) buildSource(ctx context.Context, src *sourceState) error {
	db, err := openSourceDB(src.spec)
	if err != nil {
		return err
	}
	if err := checkSource(ctx, src.spec, db); err != nil {
		db.Close()
		return err
	}
	comp, closeSinks, err := buildComponents(ctx, a.cfg, src.spec, a.kv,
		src.sourcePrefix, db, a.met)
	if err != nil {
		db.Close()
		return err
	}
	src.built.Store(&sourceBuilt{srcDB: db, comp: comp, closeSinks: closeSinks})
	return nil
}

// launchIfLeading 装配就绪源与 leader 任期的握手（PRD 05 §2）：
// 当前在任 → 直接启动该源流程（leadCtx 取当任任期）；非 leader → 待命，
// 当选时 runLeaderFlow 会拉起全部就绪源。
// 失位竞态窗口由 gate 兜底：leadCtx 已取消 → 源流程立即退出（写门槛拒写）。
func (a *app) launchIfLeading(src *sourceState) {
	if !a.leading.Load() {
		return
	}
	lc := a.leadCtxRef.Load()
	if lc == nil {
		return
	}
	go a.runSourceFlow(lc.ctx, src, lc.info)
}

// closeSourceComponents 关闭单源数据面组件（sink 路由 → 源库连接池），
// 停机 defer 序列逐源调用（装配未完成则跳过）。
func (a *app) closeSourceComponents(src *sourceState) {
	if b := src.built.Swap(nil); b != nil {
		b.closeSinks()
		b.srcDB.Close()
	}
}

// metricsLoop 多源聚合指标（PRD 07 §1）：gocdc_sources{state} 按装配态计数、
// gocdc_source_up{source} 逐源就绪度。5s 周期（/status 轮询同量级）。
func (a *app) metricsLoop(ctx context.Context) {
	update := func() {
		counts := map[string]int{"ready": 0, "failed": 0, "starting": 0}
		for _, src := range a.sources.all() {
			phase := srcPhaseName(src.phase.Load())
			counts[phase]++
			a.met.SourceUp.WithLabelValues(src.sid).Set(map[bool]float64{true: 1, false: 0}[phase == "ready"])
		}
		for state, n := range counts {
			a.met.Sources.WithLabelValues(state).Set(float64(n))
		}
	}
	update()
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			update()
		}
	}
}

// 进程级管线运行状态枢纽：lifecycle 唯一写方（setPipelineState/pipelineUp/stock
// 引用），admin /status 与 readyz 探针读方。
package main

import (
	"sync/atomic"
	"time"

	"gocdc/internal/api"
	"gocdc/internal/pipeline"
)

// pipelineUp 管线运行标记（readyz 探针消费；由 leader 生命周期置位）。
var pipelineUp atomic.Bool

// pipelineStateRef 管线运行状态快照（lifecycle 生命周期写入、admin /status 读取；
// nil = 进程启动后从未参与同步 → 视为 idle）。
var pipelineStateRef atomic.Pointer[pipelineSnapshot]

// currentPipeline 当前增量管线实例（/status 事件延迟读取 LastEventTs；可 nil）。
var currentPipeline atomic.Pointer[pipeline.Pipeline]

// pipelineSnapshot 管线运行状态视图（cmd 内部；线上契约见 api.PipelineSnapshot）。
type pipelineSnapshot struct {
	State string    // api.PipeIdle / PipeStock / PipeRunning / PipePaused / PipeStopped
	Error string    // paused 时的首因错误（人工介入依据）
	Since time.Time // 进入该状态的时刻
}

// setPipelineState 原子更新管线状态。
func setPipelineState(state, errMsg string) {
	pipelineStateRef.Store(&pipelineSnapshot{State: state, Error: errMsg, Since: time.Now()})
}

// pipelineSnapshot /status 管线状态视图（原子读取 lifecycle 生命周期写入的
// 快照与当前管线实例；running 态附带事件延迟与追平口径）。
func (a *app) pipelineSnapshot() api.PipelineSnapshot {
	s := pipelineStateRef.Load()
	if s == nil {
		return api.PipelineSnapshot{State: api.PipeIdle}
	}
	snap := api.PipelineSnapshot{State: s.State, Error: s.Error, Since: s.Since, PausedByOp: a.opPaused.Load()}
	if s.State == api.PipeRunning {
		if pl := currentPipeline.Load(); pl != nil {
			// DDL 决策等待覆盖：running 基态叠加 paused（水位冻结等待人工，
			// 决策后自动恢复——生命周期的 fatal paused 语义不经过此处）
			if dp := pl.DDLPause(); dp != nil {
				snap.ApplyDDLPause(dp)
				return snap
			}
			// lag = now - 最近业务写入秒（与 sampleLoop 指标同口径）；ts=0（尚无事件）不给出。
			// 稀疏写入下该值会自然老化（=距上次写入多久），「是否追平」以 master lag 为准
			if ts := pl.LastEventTs(); ts > 0 {
				snap.EventLag, snap.HasLag = time.Now().Unix()-ts, true
			}
		}
		if a.met != nil {
			if lagBytes, ok := a.met.MasterPositionLagBytesValue(); ok {
				snap.MasterLag, snap.HasCatchup = lagBytes, true
			}
		}
	}
	return snap
}

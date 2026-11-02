// 源级运行状态枢纽（多源 PRD 05 §2/§3）：现行进程级单例
// （pipelineUp/pipelineStateRef/currentPipeline）的逐源版本。
//
// sourceTable 在装配期固定（配置不热更 ⇒ 进程生命周期内源集合不变），
// 并发安全靠各源内部的原子字段与每源单槽互斥。
package main

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	electorf "gocdc/internal/elector"
	"gocdc/internal/api"
	"gocdc/internal/config"
	"gocdc/internal/etcdx"
	"gocdc/internal/metrics"
	"gocdc/internal/model"
	"gocdc/internal/pipeline"
	"gocdc/internal/position"
	"gocdc/internal/source"
	stockf "gocdc/internal/stock"
)

// ── 源装配期状态机（PRD 05 §3）──
//
//	config-loaded ──build──► starting ──ok──► ready ─(leader)─► stock → running ⇄ paused
//	                             ╰─fail─► failed ──重试成功──► ready
//	                                       （退避 30s→5min，D5 失败隔离）
const (
	srcStarting int32 = iota
	srcReady
	srcFailed
)

// srcPhaseName 装配期状态的对外名称（/status 源摘要）。
func srcPhaseName(p int32) string {
	switch p {
	case srcReady:
		return "ready"
	case srcFailed:
		return "failed"
	default:
		return "starting"
	}
}

// failInfo 装配失败诊断（failed 态对外展示）。
type failInfo struct {
	Err   string
	Since time.Time
}

// sourceBuilt 源装配产物（supervisor 成功后一次写入；运行期只读、关闭期消费）。
type sourceBuilt struct {
	srcDB      *sql.DB
	comp       *components
	closeSinks func()
}

// sourceState 单源运行态。etcd 状态面（store/markers/pauseKey）在装配期构造，
// 不依赖源库连通；数据面组件（srcDB/comp）由监督循环装配、原子发布。
type sourceState struct {
	sid  string
	spec *config.SourceSpec

	// ── etcd 状态面（键空间 v2：{prefix}/sources/{sid}/…）──
	sourcePrefix string                  // {prefix}/sources/{sid}
	store        *position.Store         // 位点（gate 锚 = cluster leader key）
	markers      *stockf.MarkerStore     // 全量表级标记（同 gate）
	pauseKey     string                  // {sourcePrefix}/pipeline-pause
	resumeFn     source.ResumeFunc       // 断线重连回到该源 etcd 已持久化位点

	// ── 数据面（supervisor 装配产物；nil = 未就绪）──
	built    atomic.Pointer[sourceBuilt]
	stockSvc atomic.Pointer[stockf.Service] // Web restock 读取（源任期重建）

	// ── 装配监督状态（/status 源摘要与 readyz 读）──
	phase    atomic.Int32 // srcStarting | srcReady | srcFailed
	failInfo atomic.Pointer[failInfo]

	// ── 任期内源流程生命周期（每源单槽互斥，现行 app 单槽的逐源版）──
	pipeMu     sync.Mutex
	pipeCancel context.CancelFunc // 源流程 ctx（leadCtx 派生；人工暂停只停该源）
	pipeDone   chan struct{}
	opPaused   atomic.Bool                      // 人工暂停态（跨任期保持到 resume/失位）
	curElected atomic.Pointer[electorf.Elected] // 任期信息（resume 重启源流程复用）

	// ── 管线运行快照 ──
	stateRef   atomic.Pointer[pipelineSnapshot]
	running    atomic.Bool // 增量管线运行中（readyz 聚合计数）
	curPipeline atomic.Pointer[pipeline.Pipeline]
}

// pipelineSnapshot 管线运行状态视图（cmd 内部；线上契约见 api.PipelineSnapshot）。
type pipelineSnapshot struct {
	State string    // api.PipeIdle / PipeStock / PipeRunning / PipePaused / PipeStopped
	Error string    // paused 时的首因错误（人工介入依据）
	Since time.Time // 进入该状态的时刻
}

// setState 原子更新该源管线状态。
func (s *sourceState) setState(state, errMsg string) {
	s.stateRef.Store(&pipelineSnapshot{State: state, Error: errMsg, Since: time.Now()})
}

// pipelineRunningSources 运行中源计数（readyz pipeline_running 探针口径：
// 任一源在跑即就绪——失败隔离下个别源 failed 不应打红整个进程探针）。
var pipelineRunningSources atomic.Int32

// pipelineSnapshot /status 单源管线状态视图（running 态附带事件延迟与追平口径）。
func (s *sourceState) pipelineSnapshot(met *metrics.Metrics) api.PipelineSnapshot {
	p := s.stateRef.Load()
	if p == nil {
		return api.PipelineSnapshot{State: api.PipeIdle}
	}
	snap := api.PipelineSnapshot{State: p.State, Error: p.Error, Since: p.Since, PausedByOp: s.opPaused.Load()}
	if p.State == api.PipeRunning {
		if pl := s.curPipeline.Load(); pl != nil {
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
		if met != nil {
			if lagBytes, ok := met.MasterPositionLagBytesValue(); ok {
				snap.MasterLag, snap.HasCatchup = lagBytes, true
			}
		}
	}
	return snap
}

// ── sourceTable ─────────────────────────────────────────────────────

// sourceTable 源运行态集合（装配期构建，运行期只读）。
type sourceTable struct {
	order []*sourceState        // 配置序
	byID  map[string]*sourceState
}

// newSourceTable 构造源表：etcd 状态面（键空间 v2）逐源实例化——
// store/markers/pauseKey 的 key 下沉到 {prefix}/sources/{sid}/ 下，
// gate 锚共享 cluster leader key（PRD 04 §2/§3；SetGate 在源流程启动时注入）。
func newSourceTable(cfg *config.Config, prefix string, kv *etcdx.KV) *sourceTable {
	t := &sourceTable{byID: make(map[string]*sourceState, len(cfg.Sources))}
	for i := range cfg.Sources {
		spec := &cfg.Sources[i]
		srcPrefix := fmt.Sprintf("%s/sources/%s", prefix, spec.ID)
		store := position.NewStore(kv, srcPrefix+"/position", "", 0)
		src := &sourceState{
			sid:          spec.ID,
			spec:         spec,
			sourcePrefix: srcPrefix,
			store:        store,
			markers:      stockf.NewMarkerStore(kv, srcPrefix+"/stock"),
			pauseKey:     srcPrefix + "/pipeline-pause",
		}
		src.resumeFn = func() model.Position {
			rctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			p, ok, err := store.Load(rctx)
			if err != nil || !ok {
				return model.Position{}
			}
			return p
		}
		src.phase.Store(srcStarting)
		t.order = append(t.order, src)
		t.byID[spec.ID] = src
	}
	return t
}

// all 全部源（配置序快照；运行期不变）。
func (t *sourceTable) all() []*sourceState { return t.order }

// get 按 id 取源（API 路由用；未知 sid 返回 nil）。
func (t *sourceTable) get(sid string) *sourceState { return t.byID[sid] }

// ready 全部已就绪源（leader 启动口径：failed/starting 不启动，由监督循环补入）。
func (t *sourceTable) ready() []*sourceState {
	var out []*sourceState
	for _, s := range t.order {
		if s.phase.Load() == srcReady {
			out = append(out, s)
		}
	}
	return out
}

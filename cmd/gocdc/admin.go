// admin API（13 §3：API 优先；/api/v1 同进程）依赖装配与 HTTP server。
//
// 多源（PRD 06 §1.5）：Deps 持按源 SourceDeps 表（sid → 闭包集），路由中间件
// 解析 {sid} 查表；源集合装配期固定（配置不热更 ⇒ map 只读）。源数据面组件
// 与 Web 启动并发装配——闭包延迟取用 built，未就绪显式报错（standby 上
// POST /restock 的 stockSvc nil panic 边角随多源一并修复，PRD 06 §1.5）。

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"go.uber.org/zap"

	"gocdc/internal/api"
	"gocdc/internal/config"
	"gocdc/internal/etcdx"
	"gocdc/internal/logging"
	"gocdc/internal/model"
	"gocdc/internal/pipeline"

	"gocdc/internal/verify"
)

// buildSourceDeps 单源路由依赖：etcd 状态面恒可用（装配期构造）；数据面
// 闭包延迟取用 built（装配中/失败时显式报错，不 panic）。
func buildSourceDeps(rt *app, src *sourceState) *api.SourceDeps {
	notReady := func(what string) error {
		return fmt.Errorf("%s 未就绪（源装配中或装配失败，稍后重试或查看日志）", what)
	}
	builtOrErr := func(op string) (*sourceBuilt, error) {
		if b := src.built.Load(); b != nil {
			return b, nil
		}
		return nil, notReady(op)
	}
	sd := &api.SourceDeps{
		ID:      src.sid,
		Store:   src.store,
		Markers: src.markers,
		UseGTID: src.spec.Source.UseGTID,
	}
	// 数据面依赖惰性闭包：装配与 Web 启动并发，未就绪时 handler 显式 409
	sd.Rules = func() *pipeline.CompiledRules {
		if b := src.built.Load(); b != nil {
			return b.comp.rules
		}
		return nil
	}
	sd.Gate = func() pipeline.DDLGate {
		if b := src.built.Load(); b != nil {
			return b.comp.ddlGate
		}
		return nil
	}
	sd.DDLLister = func() pipeline.DDLLister {
		if b := src.built.Load(); b != nil {
			return b.comp.ddlGate
		}
		return nil
	}
	sd.MasterPos = func(mctx context.Context) (model.Position, error) {
		b, err := builtOrErr("源库连接")
		if err != nil {
			return model.Position{}, err
		}
		return showMasterStatus(mctx, b.srcDB)
	}
	sd.OldestBinlog = func(octx context.Context) (string, error) {
		b, err := builtOrErr("源库连接")
		if err != nil {
			return "", err
		}
		return oldestBinlog(octx, b.srcDB)
	}
	sd.Verify = func(vctx context.Context, database, table, pk string, sample int) (*verify.Result, error) {
		b, err := builtOrErr("源/目标连接")
		if err != nil {
			return nil, err
		}
		if b.comp.mysql == nil { // 纯 MQ 下游：无目标库可对账（409 语义）
			return nil, errors.New("targets.mysql 未启用：抽样对账仅支持 MySQL 直写下游")
		}
		vctx, cancel := context.WithTimeout(vctx, 2*time.Minute)
		defer cancel()
		return verify.CompareTable(vctx, &verify.Options{
			SrcDB: b.srcDB, TgtDB: b.comp.mysql.DB(), Schema: database,
			TargetDB: b.comp.targetDB(database, table),
			Table:    table, PK: pk, Sample: sample,
		})
	}
	sd.StockReset = func(sctx context.Context, sdb, stable string) error {
		svc := src.stockSvc.Load()
		if svc == nil { // standby/未承担管线：409 语义（PRD 06 §1.5，原为 panic→500）
			return errors.New("本节点当前不承担管线（热备/未当选）")
		}
		if err := svc.ResetTable(sctx, sdb, stable); err != nil {
			return err
		}
		// 清标记后立即触发该源流程重走（runStock 重扫无 done 标记表 → 增量
		// 从新快照位点续接）——不再要求运维单独重启进程；失位窗口由接管
		// 节点兜底（标记已清，当选必重扫）
		return rt.restockRestart(sctx, src)
	}
	sd.Pipeline = func() api.PipelineSnapshot { return src.pipelineSnapshot(rt.met) }
	sd.PausePipeline = func(pctx context.Context) error { return rt.pauseSourcePipeline(pctx, src) }
	sd.ResumePipeline = func(rctx context.Context) error { return rt.resumeSourcePipeline(rctx, src) }
	return sd
}

// buildAdminDeps 组装路由依赖（13 §3 + 多源 PRD 06 §1.5）：
//   - Elector/Resign 由 run() 在 elector 就绪后补挂（避免装配顺序耦合）；
//   - Sources 表：sid → 该源闭包集（装配期固定只读）；
//   - SourceStatuses/SourceList：/status 与 /sources 的数据源（cmd 原子读源表）；
//   - ConfigStore/Preflight/Shutdown：配置编辑与重载（13 §3；etcd 形态才有
//     ConfigStore，file 形态 nil → 端点 409）。
func buildAdminDeps(st *config.Startup, cfg *config.Config, cfgKey string, cfgKV *etcdx.KV,
	coord *coordination, rt *app, stop context.CancelFunc) api.Deps {

	// prefix=集群前缀（NewAuditStore 契约，内部自拼 /events/audit）——此前多传
	// 一段导致真实 key 双重拼接为 {prefix}/events/audit/events/audit（55 组
	// PP-09 抓到：etcd 侧运维观察路径与文档/etcdctl 直觉不符）
	auditStore := api.NewAuditStore(coord.kv, coord.prefix, coord.nodeID)

	deps := api.Deps{
		Version:    buildVersion,
		Cluster:    cfg.Cluster,
		NodeID:     coord.nodeID,
		ConfigSrc:  configSourceLabel(st),
		StartedAt:  time.Now(),
		ConfigJSON: maskConfigJSON(cfg),
		Audit:      auditStore,
		Sources:    make(map[string]*api.SourceDeps, len(cfg.Sources)),

		Preflight: rt.preflightRules,
		Shutdown:  stop,
	}
	if st.ConfigSource == "etcd" && cfgKV != nil {
		deps.ConfigStore = api.NewEtcdConfigStore(cfgKV, cfgKey)
		deps.ConfigKey = cfgKey
	}
	for _, src := range rt.sources.all() {
		sd := buildSourceDeps(rt, src)
		deps.Sources[src.sid] = sd
		deps.SourceOrder = append(deps.SourceOrder, src.sid)
	}
	deps.SourceStatuses = func() []api.SourceStatus { return rt.sourceStatuses() }
	deps.SourceList = func() []api.SourceSummary { return rt.sourceList() }
	return deps
}

// sourceStatuses 全部源运行摘要（/status sources[]）。
func (a *app) sourceStatuses() []api.SourceStatus {
	out := make([]api.SourceStatus, 0, len(a.sources.order))
	for _, src := range a.sources.all() {
		ss := api.SourceStatus{ID: src.sid, Phase: srcPhaseName(src.phase.Load()), UseGTID: src.spec.Source.UseGTID}
		if fi := src.failInfo.Load(); fi != nil {
			ss.PipelineError = fi.Err // 装配失败首因（failed(startup) 诊断）
		}
		ps := src.pipelineSnapshot(a.met)
		ss.PipelineState = ps.State
		if ss.Phase != "failed" { // failed(装配) 的首因错误优先于管线快照错误
			ss.PipelineError = ps.Error
		}
		ss.PipelineSince = ps.Since
		ss.PausedByOp = ps.PausedByOp
		if ps.HasLag {
			lag := ps.EventLag
			ss.EventLagSeconds = &lag
		}
		if ps.HasCatchup {
			lagBytes := ps.MasterLag
			ss.MasterLagBytes = &lagBytes
			up := lagBytes == 0
			ss.CaughtUp = &up
		}
		// 位点摘要（etcd 已持久化值；读取失败省略——/status 不因单源 etcd 抖动 500）
		pctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		if p, ok, err := src.store.Load(pctx); err == nil && ok {
			ss.Position = &api.PositionData{
				Name: p.Name, Pos: p.Pos, GTIDSet: p.GTIDSet,
				Seq: p.Seq, Term: p.Term, NodeID: p.NodeID, UpdatedAt: p.UpdatedAt,
			}
		}
		cancel()
		out = append(out, ss)
	}
	return out
}

// sourceList 源清单统计（GET /sources：地址/规则/表数/sink 集/状态摘要）。
func (a *app) sourceList() []api.SourceSummary {
	out := make([]api.SourceSummary, 0, len(a.sources.order))
	for _, src := range a.sources.all() {
		sum := api.SourceSummary{ID: src.sid, SourceAddr: src.spec.Source.Addr}
		t := &src.spec.Targets
		if t.MySQL.Enabled {
			sum.Sinks = append(sum.Sinks, "mysql")
			sum.TargetAddrs = append(sum.TargetAddrs, t.MySQL.Addr)
		}
		if t.Kafka.Enabled {
			sum.Sinks = append(sum.Sinks, "kafka")
			sum.TargetAddrs = append(sum.TargetAddrs, t.Kafka.Brokers...)
		}
		if t.RocketMQ.Enabled {
			sum.Sinks = append(sum.Sinks, "rocketmq")
			sum.TargetAddrs = append(sum.TargetAddrs, t.RocketMQ.NameServers...)
		}
		sum.Rules = len(src.spec.Rules)
		for i := range src.spec.Rules {
			sum.Tables += len(src.spec.Rules[i].Tables)
		}
		if p := src.phase.Load(); p == srcFailed {
			sum.State = "failed"
			if fi := src.failInfo.Load(); fi != nil {
				sum.Error = fi.Err
			}
		} else {
			sum.State = src.pipelineSnapshot(a.met).State
		}
		out = append(out, sum)
	}
	return out
}

// startAdminServer 启动 admin HTTP server（web.enabled=false 时不监听，
// 返回 nil 清理函数）。由调用方 defer 返回的清理（3s 优雅关闭）。
func startAdminServer(cfg *config.Config, deps api.Deps) func() {
	if !cfg.Observability.Web.Enabled {
		return nil
	}
	srv := &http.Server{
		Addr: cfg.Observability.Web.Addr,
		Handler: api.NewRouter(deps, api.AuthConfig{
			Username:     cfg.Observability.Web.Username,
			Password:     cfg.Observability.Web.Password,
			AuthDisabled: cfg.Observability.Web.AuthDisabled,
		}),
	}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logging.L().Error("Web 端口监听失败", zap.Error(err))
		}
	}()
	logging.L().Info("admin Web 就绪", zap.String("addr", cfg.Observability.Web.Addr))
	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	}
}

// maskConfigJSON 生效配置打码（password/secret_key → ***）——/config 端点
// 的泄密防线（key 名精确匹配；新增敏感字段需同步此处）。递归遍历天然覆盖
// sources[] 数组元素（多源，PRD 03 §6）。
func maskConfigJSON(cfg *config.Config) []byte {
	var walk func(m map[string]any)
	walk = func(m map[string]any) {
		for k, v := range m {
			if (k == "password" || k == "secret_key") && v != nil && v != "" {
				m[k] = "***"
			}
			if sub, ok := v.(map[string]any); ok {
				walk(sub)
			}
			if arr, ok := v.([]any); ok {
				for _, it := range arr {
					if sub, ok := it.(map[string]any); ok {
						walk(sub)
					}
				}
			}
		}
	}
	buf, err := json.Marshal(cfg)
	if err != nil {
		return []byte(`{"error":"marshal"}`)
	}
	var m map[string]any
	if err := json.Unmarshal(buf, &m); err != nil {
		return buf
	}
	walk(m)
	out, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return buf
	}
	return out
}

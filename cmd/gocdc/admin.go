// admin API（13 §3：API 优先；/api/v1 同进程）依赖装配与 HTTP server。

package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"go.uber.org/zap"
	"gocdc/internal/api"
	"gocdc/internal/config"
	"gocdc/internal/logging"
	"gocdc/internal/model"

	"gocdc/internal/verify"
)

// buildAdminDeps 组装路由依赖（13 §3）：
//   - Elector/Resign/StockReset 由 run() 在 elector 就绪后补挂（避免装配顺序耦合）；
//   - Verify 抽样对账：进程内已有源/目标连接——Web 形态为主（Docker 部署无需
//     进节点跑 CLI），目标库经规则 target_database 解析。
func buildAdminDeps(st *config.Startup, cfg *config.Config, cfgKey string,
	coord *coordination, comp *components, srcDB *sql.DB) api.Deps {

	// prefix=集群前缀（NewAuditStore 契约，内部自拼 /events/audit）——此前多传
	// 一段导致真实 key 双重拼接为 {prefix}/events/audit/events/audit（55 组
	// PP-09 抓到：etcd 侧运维观察路径与文档/etcdctl 直觉不符）
	auditStore := api.NewAuditStore(coord.kv, coord.prefix, coord.nodeID)
	return api.Deps{
		Version: buildVersion, Cluster: cfg.Cluster, NodeID: coord.nodeID,
		ConfigSrc:  configSourceLabel(st),
		StartedAt:  time.Now(),
		ConfigJSON: maskConfigJSON(cfg),
		Store:      coord.store, Markers: coord.markers,
		Rules: comp.rules, Gate: comp.ddlGate, Audit: auditStore,
		DDLLister: comp.ddlGate, // /ddl-events 挂起列表（RUNBOOK §5 排障入口）
		UseGTID:   cfg.Single().Source.UseGTID,
		MasterPos: func(mctx context.Context) (model.Position, error) {
			return showMasterStatus(mctx, srcDB)
		},
		OldestBinlog: func(octx context.Context) (string, error) {
			return oldestBinlog(octx, srcDB)
		},
		Verify: func(vctx context.Context, database, table, pk string, sample int) (*verify.Result, error) {
			vctx, cancel := context.WithTimeout(vctx, 2*time.Minute)
			defer cancel()
			return verify.CompareTable(vctx, &verify.Options{
				SrcDB: srcDB, TgtDB: comp.mysql.DB(), Schema: database,
				TargetDB: comp.targetDB(database, table),
				Table:    table, PK: pk, Sample: sample,
			})
		},
	}
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
// 的泄密防线（key 名精确匹配；新增敏感字段需同步此处）。
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

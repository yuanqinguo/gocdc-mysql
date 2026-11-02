package api

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"gocdc/internal/elector"
	"gocdc/internal/model"
	"gocdc/internal/pipeline"
	"gocdc/internal/position"
	"gocdc/internal/verify"
)

// SourceDeps 单源路由依赖（多源 PRD 06 §1.5：现行 Deps 的源私有字段按源
// 拆出；源集合在装配期固定（配置不热更 ⇒ 进程生命周期内不变，map 只读）。
//
// 数据面组件（规则/DDL 门/连接）与 Web 启动并发装配——相关字段为惰性闭包，
// 未就绪时返回 nil（handler 显式 409，绝不 nil deref）；etcd 状态面（Store/
// Markers）装配期即构造，恒可用。
type SourceDeps struct {
	ID     string
	Store  *position.Store
	Markers interface {
		ResetTable(ctx context.Context, db, table string) error
	}
	// Rules 规则编译产物（nil 返回 = 未装配）。
	Rules func() *pipeline.CompiledRules
	// Gate DDL 审批门（nil 返回 = 未装配）。
	Gate func() pipeline.DDLGate
	// DDLLister 挂起记录查询（nil 返回 = 未装配，列表口径出空数组）。
	DDLLister func() pipeline.DDLLister
	// StockReset 源级全量重跑（nil = 未装配——非 leader 节点走 409 语义错误）。
	StockReset func(ctx context.Context, db, table string) error
	// PausePipeline/ResumePipeline 该源人工暂停与恢复（13 §3 逐源版）。
	PausePipeline  func(ctx context.Context) error
	ResumePipeline func(ctx context.Context) error
	// Verify 该源抽样对账（nil = 未装配）。
	Verify func(ctx context.Context, database, table, pk string, sample int) (*verify.Result, error)
	// Pipeline 该源管线运行快照（nil = idle）。
	Pipeline func() PipelineSnapshot

	// UseGTID 该源 source.use_gtid（migrate 前置判断与 status 展示）。
	UseGTID bool
	// MasterPos 该源库当前尾部位点（GTID 迁移追平判定；nil = migrate-gtid 报未装配）。
	MasterPos func(ctx context.Context) (model.Position, error)
	// OldestBinlog 该源库最老保留 binlog（GTID→file:pos 反向迁移重放起点）。
	OldestBinlog func(ctx context.Context) (string, error)
}

// Deps cluster 级路由依赖 + 源依赖表（装配层注入；均为已构造组件的引用）。
type Deps struct {
	Version    string
	Cluster    string
	NodeID     string
	ConfigSrc  string // file:... | etcd:...
	StartedAt  time.Time
	ConfigJSON []byte // 打码后的生效配置（装配层负责打码）
	Elector    *elector.Elector
	Audit      AuditSink
	Resign     func() // 主动让位（elector.RequestResign 包装）

	// Sources 源依赖表（sid → deps）；SourceOrder 为展示序（配置序）。
	Sources     map[string]*SourceDeps
	SourceOrder []string

	// SourceStatuses 全部源运行摘要（/status sources[]；cmd 层原子读源表）。
	SourceStatuses func() []SourceStatus
	// SourceList 源清单统计（GET /sources：地址/规则/表数/sink 集/状态摘要）。
	SourceList func() []SourceSummary
}

// apiHandlers 各端点 handler 载体：持有 Deps 只读引用，按资源拆分到同包
// 各文件（status/rules/position/ddl/verify/leader/sources）。
type apiHandlers struct{ Deps }

// singleSource 唯一源便捷取用（v1 兼容层：单源部署旧路由等价别名）。
// 多源部署返回 nil——旧路由 400 提示改用 /sources/{sid}/...（PRD 06 §1.4）。
func (h apiHandlers) singleSource() *SourceDeps {
	if len(h.Sources) != 1 {
		return nil
	}
	return h.Sources[h.SourceOrder[0]]
}

// resolveSource 路由 {sid} 参数 → SourceDeps（未知 sid → 404 并写响应）。
func (h apiHandlers) resolveSource(c *gin.Context) *SourceDeps {
	sd, ok := h.Sources[c.Param("sid")]
	if !ok {
		abort(c, 404, "源不存在: "+c.Param("sid"))
		return nil
	}
	return sd
}

// requireSingle 旧路由兼容层守卫：单源部署解析唯一源；多源 400 指引新路由。
func (h apiHandlers) requireSingle(c *gin.Context) *SourceDeps {
	sd := h.singleSource()
	if sd == nil {
		abort(c, 400, "存在多个源，请使用 /api/v1/sources/{sid}/... 按源操作（源清单见 GET /api/v1/sources）")
		return nil
	}
	return sd
}

// NewRouter 构建 /api/v1 引擎（13 §3 + 多源 PRD 06 §1.2）。embed 静态页由
// 装配层挂载。旧路由保留为兼容层（单源部署等价别名；多源 400 指引），
// 前端一律走新路由——旧路由纯服务外部脚本/监控探针（弃用窗口 ≥ 2 个小版本）。
func NewRouter(deps Deps, auth AuthConfig) *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())

	h := apiHandlers{Deps: deps}
	api := r.Group("/api/v1")
	api.Use(authMiddleware(auth))

	// ── 只读（cluster 级）──
	api.GET("/status", h.status)
	api.GET("/sources", h.listSources)
	// /ddl-events = 全集群 pending 合并（记录带 source 字段）。单源部署下合并
	// 结果即唯一源数据——旧消费方零变化；多源部署为聚合视图（角标/聚合页用）。
	api.GET("/ddl-events", h.ddlEventsAll)
	api.GET("/audit", h.auditList)
	api.GET("/config", h.config)

	// ── 只读（源作用域）──
	api.GET("/sources/:sid/status", h.sourceStatus)
	api.GET("/sources/:sid/position", h.sourceGetPosition)
	api.GET("/sources/:sid/rules/effective", h.sourceEffectiveRules)
	api.GET("/sources/:sid/ddl-events", h.sourceDDLEvents)

	// ── 写操作（confirm 门禁 + 审计；源作用域）──
	api.POST("/sources/:sid/position", h.sourceSetPosition)
	api.POST("/sources/:sid/position/migrate-gtid", h.sourceMigrateGTID)
	api.POST("/sources/:sid/position/migrate-filepos", h.sourceMigrateFilePos)
	api.POST("/sources/:sid/ddl/:event_id/approve", h.sourceSettleDDL(pipeline.DecisionApprove))
	api.POST("/sources/:sid/ddl/:event_id/skip", h.sourceSettleDDL(pipeline.DecisionSkip))
	api.POST("/sources/:sid/rules/by-schema/:schema/:table/restock", h.sourceRestock)
	api.POST("/sources/:sid/pipeline/pause", h.sourcePausePipeline)
	api.POST("/sources/:sid/pipeline/resume", h.sourceResumePipeline)
	api.POST("/sources/:sid/verify", h.sourceVerify)

	// ── 写操作（cluster 级）──
	api.POST("/leader/resign", h.resign)

	// ── 兼容层（弃用窗口 ≥ 2 个小版本；单源部署 = 唯一源等价别名，
	//    多源部署 400 指引新路由）──
	api.GET("/rules/effective", h.legacySingle(h.legacyEffectiveRules))
	api.GET("/position", h.legacySingle(h.legacyGetPosition))
	api.POST("/ddl/:event_id/approve", h.legacySingle(h.legacySettleDDL(pipeline.DecisionApprove)))
	api.POST("/ddl/:event_id/skip", h.legacySingle(h.legacySettleDDL(pipeline.DecisionSkip)))
	api.POST("/rules/:key/restock", h.legacySingle(h.legacyRestock)) // compatibility route; key is split at the final dot.
	api.POST("/rules/by-schema/:schema/:table/restock", h.legacySingle(h.legacyRestockBySchemaTable))
	api.POST("/verify", h.legacySingle(h.legacyVerify))
	api.POST("/pipeline/pause", h.legacySingle(h.legacyPausePipeline))
	api.POST("/pipeline/resume", h.legacySingle(h.legacyResumePipeline))
	api.POST("/position", h.legacySingle(h.legacySetPosition))
	api.POST("/position/migrate-gtid", h.legacySingle(h.legacyMigrateGTID))
	api.POST("/position/migrate-filepos", h.legacySingle(h.legacyMigrateFilePos))

	// The static shell is public so it can collect credentials. Unknown API paths
	// still follow the Basic auth boundary and never fall through to index.html.
	r.NoRoute(func(c *gin.Context) {
		if path := c.Request.URL.Path; path == "/api/v1" || strings.HasPrefix(path, "/api/v1/") {
			authMiddleware(auth)(c)
			if c.IsAborted() {
				return
			}
			respondErr(c, 404, "接口不存在")
			return
		}
		serveSPA(c.Writer, c.Request)
	})
	return r
}

// legacySingle 兼容层包装：解析唯一源后进入实际 handler；多源 400。
func (h apiHandlers) legacySingle(next func(c *gin.Context, sd *SourceDeps)) gin.HandlerFunc {
	return func(c *gin.Context) {
		sd := h.requireSingle(c)
		if sd == nil {
			return
		}
		next(c, sd)
	}
}

// requireConfirm 写操作 confirm 门禁（13 §3）：缺 confirm=true 时输出 400
// 并终止。所有写端点第一步调用。
func requireConfirm(c *gin.Context) bool {
	if c.Query(queryConfirm) == confirmTrue {
		return true
	}
	abort(c, 400, "写操作需 confirm=true（13 §3）")
	return false
}

// errNotAssembly 兼容层/源路由共用的未装配错误。
func errNotAssembly(what string) error { return errors.New(what + " 未装配") }

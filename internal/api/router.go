package api

import (
	"context"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"gocdc/internal/elector"
	"gocdc/internal/model"
	"gocdc/internal/pipeline"
	"gocdc/internal/position"
	"gocdc/internal/verify"
)

// Deps 路由依赖（装配层注入；均为已构造组件的引用，无旁路构造）。
type Deps struct {
	Version    string
	Cluster    string
	NodeID     string
	ConfigSrc  string // file:... | etcd:...
	StartedAt  time.Time
	ConfigJSON []byte // 打码后的生效配置（装配层负责打码）
	Elector    *elector.Elector
	Store      *position.Store
	Markers    interface {
		ResetTable(ctx context.Context, db, table string) error
	}
	Rules      *pipeline.CompiledRules
	Gate       pipeline.DDLGate
	DDLLister  pipeline.DDLLister
	Audit      AuditSink
	Resign     func() // 主动让位（elector.RequestResign 包装）
	StockReset func(ctx context.Context, db, table string) error
	// PausePipeline/ResumePipeline 人工暂停与恢复管线（13 §3；cmd 装配闭包：
	// 暂停=写 etcd 持久标志 + 优雅停管线保持 Leader；恢复=删标志 + 从位点重启）。
	// nil = 未装配（端点报未装配错误）。
	PausePipeline  func(ctx context.Context) error
	ResumePipeline func(ctx context.Context) error
	// Verify 抽样对账（POST /verify；装配层闭包：进程内源/目标连接 +
	// 规则 target_database 解析）。nil = 未装配（端点报错）。
	Verify func(ctx context.Context, database, table, pk string, sample int) (*verify.Result, error)
	// Pipeline 管线运行状态快照（装配层闭包：cmd 层原子读取）。nil = /status
	// 的 pipeline_state 恒为 idle。
	Pipeline func() PipelineSnapshot

	// UseGTID 当前配置的 source.use_gtid（migrate-gtid/migrate-filepos 前置判断
	// 与 /status use_gtid 字段）。
	UseGTID bool
	// MasterPos 源库当前尾部位点（SHOW BINARY LOG STATUS 原子三元组；GTID 迁移
	// 追平判定）。nil = migrate-gtid 报未装配。
	MasterPos func(ctx context.Context) (model.Position, error)
	// OldestBinlog 源库最老保留 binlog 文件名（GTID→file:pos 反向迁移的重放起点）。
	OldestBinlog func(ctx context.Context) (string, error)
}

// apiHandlers 各端点 handler 载体：持有 Deps 只读引用，按资源拆分到同包
// 各文件（status/rules/position/ddl/verify/leader）。
type apiHandlers struct{ Deps }

// NewRouter 构建 /api/v1 引擎（13 §3）。embed 静态页由装配层挂载。
func NewRouter(deps Deps, auth AuthConfig) *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())

	h := apiHandlers{Deps: deps}
	api := r.Group("/api/v1")
	api.Use(authMiddleware(auth))

	// ── 只读（13 §3 只读清单）──
	api.GET("/status", h.status)
	api.GET("/rules/effective", h.effectiveRules)
	api.GET("/position", h.getPosition)
	api.GET("/ddl-events", h.ddlEvents)
	api.GET("/audit", h.auditList)
	api.GET("/config", h.config)

	// ── 写操作（confirm 门禁 + 审计；鉴权已由全局中间件承担——单凭据全权限）──
	w := api.Group("")
	w.POST("/ddl/:event_id/approve", h.settleDDL(pipeline.DecisionApprove))
	w.POST("/ddl/:event_id/skip", h.settleDDL(pipeline.DecisionSkip))
	w.POST("/rules/:key/restock", h.restock) // compatibility route; key is split at the final dot.
	w.POST("/rules/by-schema/:schema/:table/restock", h.restockBySchemaTable)
	w.POST("/verify", h.verify)
	w.POST("/leader/resign", h.resign)
	w.POST("/pipeline/pause", h.pausePipeline)
	w.POST("/pipeline/resume", h.resumePipeline)
	w.POST("/position", h.setPosition)
	w.POST("/position/migrate-gtid", h.migrateGTID)
	w.POST("/position/migrate-filepos", h.migrateFilePos)

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

// requireConfirm 写操作 confirm 门禁（13 §3）：缺 confirm=true 时输出 400
// 并终止。所有写端点第一步调用。
func requireConfirm(c *gin.Context) bool {
	if c.Query(queryConfirm) == confirmTrue {
		return true
	}
	abort(c, 400, "写操作需 confirm=true（13 §3）")
	return false
}

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

	// ── 配置编辑与重载（13 §3；etcd 形态注入，file 形态保持 nil → 409）──
	// ConfigStore 配置中心原始正文读写（PUT rules 的 read→splice→validate→
	// preflight→CAS 链服务端底座）。
	ConfigStore ConfigStore
	// Preflight 活体预检闭包（源/目标连接核对本次变更表；nil = 跳过并在
	// 报告注明）。
	Preflight PreflightFunc
	// Shutdown 优雅停机触发（SIGTERM 同链路：停拉取→排空→位点终值→resign，
	// 编排拉起应用新配置；nil = 未接入 → 409）。
	Shutdown func()
	// ConfigKey 配置中心 key（etcd 形态展示用）。
	ConfigKey string
}

// apiHandlers 各端点 handler 载体：持有 Deps 只读引用，按资源拆分到同包
// 各文件（status/rules/position/ddl/verify/leader/sources）。
type apiHandlers struct{ Deps }

// resolveSource 路由 {sid} 参数 → SourceDeps（未知 sid → 404 并写响应）。
func (h apiHandlers) resolveSource(c *gin.Context) *SourceDeps {
	sd, ok := h.Sources[c.Param("sid")]
	if !ok {
		abort(c, 404, "源不存在: "+c.Param("sid"))
		return nil
	}
	return sd
}

// NewRouter 构建 /api/v1 引擎（13 §3 + 多源 PRD 06 §1.2）。embed 静态页由
// 装配层挂载。源是一等路由段：源私有资源全部挂 /sources/{sid}/...。
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
	// /ddl-events = 全集群 pending 合并（记录带 source 字段，角标/聚合页用）。
	api.GET("/ddl-events", h.ddlEventsAll)
	api.GET("/audit", h.auditList)
	api.GET("/config", h.config)
	api.GET("/config/raw", h.configRaw)

	// ── 只读（源作用域）──
	api.GET("/sources/:sid/status", h.sourceStatus)
	api.GET("/sources/:sid/position", h.sourceGetPosition)
	api.GET("/sources/:sid/rules", h.sourceGetRules)
	api.GET("/sources/:sid/rules/effective", h.sourceEffectiveRules)
	api.GET("/sources/:sid/ddl-events", h.sourceDDLEvents)

	// ── 写操作（confirm 门禁 + 审计；源作用域）──
	api.POST("/sources/:sid/position", h.sourceSetPosition)
	api.POST("/sources/:sid/position/migrate-gtid", h.sourceMigrateGTID)
	api.POST("/sources/:sid/position/migrate-filepos", h.sourceMigrateFilePos)
	api.POST("/sources/:sid/ddl/:event_id/approve", h.sourceSettleDDL(pipeline.DecisionApprove))
	api.POST("/sources/:sid/ddl/:event_id/skip", h.sourceSettleDDL(pipeline.DecisionSkip))
	api.POST("/sources/:sid/rules/by-schema/:schema/:table/restock", h.sourceRestock)
	api.PUT("/sources/:sid/rules", h.sourcePutRules) // check_only 预检免 confirm
	api.POST("/sources/:sid/pipeline/pause", h.sourcePausePipeline)
	api.POST("/sources/:sid/pipeline/resume", h.sourceResumePipeline)
	api.POST("/sources/:sid/verify", h.sourceVerify)

	// ── 写操作（cluster 级）──
	api.POST("/leader/resign", h.resign)
	api.POST("/config/apply-restart", h.applyRestart)

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

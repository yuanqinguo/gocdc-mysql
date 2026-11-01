package api

import (
	"time"

	"gocdc/internal/pipeline"
)

// 本文件是 /api/v1 的线上契约（13 §3）：出入参结构体与常量。字段 json 名
// 即对外契约，改动须同步前端（web/）与 docs/13。

// ── 出参 data 结构 ──

// role 三态：Elector 缺席 = single（ha.enabled: false）；在选 = follower/leader。
const (
	RoleSingle   = "single"
	RoleLeader   = "leader"
	RoleFollower = "follower"
)

// StatusData GET /status 出参。
type StatusData struct {
	Role          string `json:"role"` // RoleSingle | RoleLeader | RoleFollower
	Leader        string `json:"leader"`
	Term          int64  `json:"term"`
	NodeID        string `json:"node_id"`
	Version       string `json:"version"`
	Cluster       string `json:"cluster"`
	ConfigSource  string `json:"config_source"`
	UptimeSeconds int64  `json:"uptime_seconds"`

	PipelineState   string    `json:"pipeline_state"`                  // Pipeline* 常量之一
	PipelineError   string    `json:"pipeline_error,omitempty"`        // paused 时的首因错误（人工介入依据）
	PipelineSince   time.Time `json:"pipeline_since,omitempty"`        // 进入当前状态的时刻
	PausedByOp      bool      `json:"pipeline_paused_by_op,omitempty"` // paused 源于人工暂停（/pipeline/pause；区别于致命错误）
	EventLagSeconds *int64    `json:"event_lag_seconds,omitempty"`     // 距最近业务写入秒（稀疏写入会自然老化，仅参考）
	CaughtUp        *bool     `json:"caught_up,omitempty"`             // GTID/位点追平口径：nil=无采样（stock 期/未 running）
	MasterLagBytes  *float64  `json:"master_lag_bytes,omitempty"`      // 与源库 SHOW MASTER STATUS 字节差（追赶中时展示）
	UseGTID         bool      `json:"use_gtid"`                        // source.use_gtid（位点模式迁移入口的显示判据）
}

// pipeline_state 取值（13 §3）。
const (
	PipeIdle    = "idle"    // 未参与同步（follower 热备/未当选）
	PipeStock   = "stock"   // 全量初始化进行中（增量未启动）
	PipeRunning = "running" // 增量管线运行中
	PipePaused  = "paused"  // 致命错误停止（需人工介入后重启进程）
	PipeStopped = "stopped" // 正常停止（失位/让位/停机）
)

// PipelineSnapshot 管线运行快照（装配层闭包提供，status 组装进 StatusData）。
type PipelineSnapshot struct {
	State      string
	Error      string
	Since      time.Time
	PausedByOp bool  // paused 源于人工暂停（/pipeline/pause），非致命错误
	EventLag   int64 // 最近业务写入距今秒数（HasLag=false 时忽略）
	HasLag     bool
	MasterLag  float64 // 与源库位点的字节差（HasCatchup=true 时有效）
	HasCatchup bool    // 运行中且已有 master lag 采样（stock 期/未 running 为 false）
}

// ApplyDDLPause DDL 决策等待覆盖：running 基态上叠加 paused（等待期进程仍在
// 运行、决策后自动恢复，与致命停机的 paused 语义区分靠 Error 文案）。
// 非 running 基态或 dp=nil 不动——等待覆盖只作用于运行中管线，其余状态
// 含义不变。
func (s *PipelineSnapshot) ApplyDDLPause(dp *pipeline.DDLPauseState) {
	if dp == nil || s.State != PipeRunning {
		return
	}
	s.State = PipePaused
	s.Error = "DDL 等待人工决策（管线暂停中，approve/skip 后自动恢复，无需重启）: " + dp.SQL
	s.Since = dp.Since
}

// EffectiveRule GET /rules/effective 单表生效视图（规则编译产物）。
type EffectiveRule struct {
	Key            string                       `json:"key"`
	Schema         string                       `json:"schema"`
	Table          string                       `json:"table"`
	TargetDatabase string                       `json:"target_database"`
	DoStock        bool                         `json:"do_stock"`
	DDLPolicy      string                       `json:"ddl_policy"`
	OrderByColumn  string                       `json:"order_by_column"`
	ExcludeColumns []string                     `json:"exclude_columns"`
	MaskColumns    map[string]EffectiveMaskSpec `json:"mask_columns"`
	Sinks          EffectiveRuleSinks           `json:"sinks"`
}

// EffectiveMaskSpec 是管理端可见的脱敏摘要，不包含 runtime salt 或固定替换值。
type EffectiveMaskSpec struct {
	Type string `json:"type"`
	Keep int    `json:"keep,omitempty"`
}

// EffectiveRuleSinks 生效规则启用的下游。
type EffectiveRuleSinks struct {
	MySQL         bool   `json:"mysql"`
	Kafka         bool   `json:"kafka"`
	RocketMQ      bool   `json:"rocketmq"`
	KafkaTopic    string `json:"kafka_topic"`
	RocketMQTopic string `json:"rocketmq_topic"`
	RocketMQTag   string `json:"rocketmq_tag"`
}

// PositionData GET /position 出参（model.Position 的 Web 视图；term 字段
// 对应存储模型的 leader_term，json 名以 Web 契约为准）。
type PositionData struct {
	Name      string    `json:"name"`
	Pos       uint32    `json:"pos"`
	GTIDSet   string    `json:"gtid_set"`
	Seq       uint64    `json:"seq"`
	Term      int64     `json:"term"`
	NodeID    string    `json:"node_id"`
	UpdatedAt time.Time `json:"updated_at"`
}

// ── 入参结构 ──

// VerifyRequest POST /verify body。
type VerifyRequest struct {
	Database string `json:"database"`
	Table    string `json:"table"`
	PK       string `json:"pk"`     // 空 = DefaultPK
	Sample   int    `json:"sample"` // 0 = verify 包默认抽样数
}

// DefaultPK verify 未指定主键列时的缺省值。
const DefaultPK = "id"

// SetPositionRequest POST /position body（危险操作：Confirm 须为
// ConfirmSetPosition 原样字符串——防止误触覆盖同步位点）。
type SetPositionRequest struct {
	Name    string `json:"name"`
	Pos     uint32 `json:"pos"`
	Confirm string `json:"confirm"`
}

// ── 写操作门禁与审计常量 ──

const (
	queryConfirm  = "confirm"
	confirmTrue   = "true"
	paramEventID  = "event_id"
	paramTableKey = "key"
	paramSchema   = "schema"
	paramTable    = "table"
)

// ConfirmSetPosition POST /position 的 body.confirm 专用口令（13 §3）。
const ConfirmSetPosition = "SET-POSITION"

// 审计动作（what）与 DDL 审批备注。
const (
	auditDDLApprove   = "ddl.approve"
	auditDDLSkip      = "ddl.skip"
	auditRuleRestock  = "rule.restock"
	auditVerifyRun    = "verify.run"
	auditLeaderResign = "leader.resign"
	auditPositionSet  = "position.set"

	// 位点模式迁移（use_gtid 切换的 Web 化路径，P3）。
	auditPositionMigrateGTID    = "position.migrate_gtid"
	auditPositionMigrateFilePos = "position.migrate_filepos"

	// 人工暂停/恢复管线（13 §3）：暂停=优雅停管线保持 Leader；持久标志在 etcd。
	auditPipelinePause  = "pipeline.pause"
	auditPipelineResume = "pipeline.resume"

	noteWebApprove = "web approve"
	noteWebSkip    = "web skip"
)

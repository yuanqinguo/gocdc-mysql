package logging

import "go.uber.org/zap"

// 生命周期事件名清单——11 §3 规定的必打事件，固定写入 zap 字段 event。
// 各模块打点时一律用本文件常量 + logging.Event()，禁止手写字符串。
const (
	EventLeaderElected = "leader_elected" // 当选 leader（附 term）
	EventLeaderLost    = "leader_lost"    // 失去 leader（lease 过期/Resign）
	EventPipelineStart = "pipeline_start" // 管线启动
	EventPipelineStop  = "pipeline_stop"  // 管线停止（含原因）
	EventPositionSaved = "position_saved" // 位点落盘（附 lag）
	EventSinkPaused    = "sink_paused"    // sink 熔断/规则暂停（附原因）
	EventSinkResumed   = "sink_resumed"   // 恢复投递
	EventDDLDetected   = "ddl_detected"   // 检测到 DDL（附表与策略）
	EventDDLApproved   = "ddl_approved"   // DDL 审批通过
	EventStockStart    = "stock_start"    // 全量开始（附表清单）
	EventStockDone     = "stock_done"     // 全量完成（附表与行数）
	EventConfigLoaded  = "config_loaded"  // 配置加载成功（附来源/版本）

	// 多源（PRD 07 §3）：源生命周期事件
	EventSourceComponentsFailed = "source_components_failed" // 源装配失败（附 error/退避）
	EventSourceReady            = "source_ready"             // 源装配完成就绪
	EventSourceRecovered        = "source_recovered"         // failed 源重试成功恢复
	EventEtcdKeysMigrated       = "etcd_keys_migrated"       // v1→v2 键迁移完成（附明细）
)

// Event 返回生命周期事件字段 zap.String("event", name)。
// name 取值必须是上方 Event* 常量（11 §3 清单），便于日志检索与告警。
func Event(name string) zap.Field { return zap.String("event", name) }

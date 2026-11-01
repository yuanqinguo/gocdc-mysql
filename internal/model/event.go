package model

import "encoding/json"

// EventType 事件类型（05 §2.1）。
type EventType string

const (
	EventInsert EventType = "INSERT"
	EventUpdate EventType = "UPDATE"
	EventDelete EventType = "DELETE"
	EventDDL    EventType = "DDL"
)

// Phase 事件来源阶段。
const (
	PhaseBinlog   = "binlog"   // 增量
	PhaseSnapshot = "snapshot" // 全量（06 文档）
)

// Event transform 之后的统一事件：消息构建与所有 sink 的输入（05 §2.1）。
//
// Before/After 中的值均为「已按 04 §7 转换、已脱敏」的终值，Go 类型封闭集合：
// nil / bool / int64 / uint64 / float64 / string / []byte / json.RawMessage。
// Pk 中的值是脱敏前的（主键永不脱敏/排除，10 §4）。
type Event struct {
	EventID         string // 全局唯一：binlog 为 file:pos:rowIdx；快照为 db.table.snap.P.chunk.idx
	Type            EventType
	Phase           string // PhaseBinlog | PhaseSnapshot
	Database, Table string

	Ts        uint64   // binlog header 秒级时间戳；快照 = 执行时刻
	SourcePos Position // 事件来源位点（快照事件为零值 + P）
	XID       uint64   // 事务号

	Pk      map[string]any // 主键列名→值（寻址用，禁脱敏）
	Before  map[string]any // UPDATE/DELETE 前像；INSERT 为 nil
	After   map[string]any // INSERT/UPDATE 后像；DELETE 为 nil
	Columns map[string]ColumnMeta

	SchemaVersion uint64 // 该表结构缓存版本（TableMeta.Version）
	SQL           string // 仅 DDL 事件
}

// ColumnMeta 列元信息（消息 columns 字段，自描述；05 §2.1）。
type ColumnMeta struct {
	Type      string `json:"type"`             // 规范化 MySQL 类型名（见 model/table_meta.go 常量）
	Masked    string `json:"masked,omitempty"` // 脱敏算法名（10 §2）
	Precision int    `json:"precision,omitempty"`
	Scale     int    `json:"scale,omitempty"`
	AsString  bool   `json:"as_string,omitempty"` // decimal 以字符串承载（04 §7）
}

// RawMessage JSON 列在 Event 中的承载类型：原样文本、不反序列化（04 §7）。
// 消息编码时直接嵌入 JSON 值；MySQL sink 按 []byte 绑参。
type RawMessage = json.RawMessage

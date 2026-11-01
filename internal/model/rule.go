package model

// Rule 配置规则的运行时形态（03 §3.2 rules[] 的编译产物）。
// 编译（通配展开 + 库级/表级合并）由管线装配层完成；transform
// 等消费方只读本结构。
type Rule struct {
	Key           string // "db.table" 小写；库级规则 Table 为 "*"
	Schema, Table string

	OrderByColumn string // 仅全量：快照分块排序键（无主键表兼作伪主键，05 §5.1）
	DoStock       bool   // 参与全量快照（06 文档）
	DDLPolicy     string // "auto" | "pause" | "ignore"（08 §2）

	TargetDatabase string // MySQL sink 目标库（05 §5.1.1）：空 = 与源库同名；表名不可改

	ExcludeColumns []string            // 整列不下发（任何 sink 都收不到，10 §2）
	MaskColumns    map[string]MaskSpec // 值脱敏：列名 → 规格（值替换后正常下发）
	Sinks          RuleSinks           // 该规则启用的下游

	Stock *StockOverride // chunk_rows / throttle_ms 覆盖；nil = 全局默认
}

// MaskSpec 脱敏规格（10 §1 算法集的运行时形态）。
// 与 config.MaskSpec 的差异：Salt 在规则编译期已从环境变量解析出实际值
// （10 §5：盐不进 etcd 明文），运行时不读 env。
const (
	MaskFixed      = "fixed"       // 替换固定值（Value，默认 ***）
	MaskHashMD5    = "hash_md5"    // hex 摘要（+Salt）
	MaskHashSHA256 = "hash_sha256" // hex 摘要（+Salt）
	MaskPhone      = "phone"       // 138****1234
	MaskEmail      = "email"       // a***@example.com
	MaskTruncate   = "truncate"    // 保留前 Keep 字符 + ...
	MaskNull       = "null"        // 置 null
)

type MaskSpec struct {
	Type  string // 上方 Mask* 常量之一
	Value string // fixed 的替换值
	Keep  int    // truncate 保留长度
	Salt  string // hash_* 的盐（编译期已解析）
}

// RuleSinks 规则启用的 sink 集合与 MQ 定向参数（05 §2.3）。
type RuleSinks struct {
	MySQL, Kafka, RocketMQ bool
	KafkaTopic             string // 空 = prefix + "{db}_{table}" 派生（Kafka/RocketMQ 统一规范，点不收）
	RocketMQTopic          string
	RocketMQTag            string
}

// StockOverride 全量快照的表级参数覆盖（06 文档）。
type StockOverride struct {
	ChunkRows  int // 单批扫描行数
	ThrottleMS int // 批间隔节流
}

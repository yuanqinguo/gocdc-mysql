// 本文件：全量配置结构体模型与严格解码（字段注释即字段文档；完整样例与
// 逐字段说明见本目录 README.md，包说明见 doc.go）。

package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
)

// Config 全量配置根（etcd 单 key，正文 YAML）。
//
// 加载链路（Load）：原始 YAML → ${ENV} 解析 → v1→v2 规范化（顶层
// source/targets/rules 自动包装为 sources[default]）→ 反序列化 →
// ApplyDefaults（源级覆盖合并 + 0 值填默认）→ Validate（严格校验，全部错误
// 一次报出）。因此字段注释中的「0 = 默认 X」均指省略/填 0 时由
// ApplyDefaults 填充。
type Config struct {
	Cluster string `json:"cluster"` // 必须等于 -cluster flag；flag 是 etcd key 前缀唯一来源（03 §3.1 裁决）

	Node Node `json:"node"`

	HA       HA       `json:"ha"`       // 多副本选主（cluster 级）
	Pipeline Pipeline `json:"pipeline"` // 管线参数：集群级默认模板，源可逐项覆盖（见 SourceSpec）
	Stock    Stock    `json:"stock"`    // 全量参数：集群级默认模板，源可逐项覆盖

	Sources []SourceSpec `json:"sources"` // ★ 同步源数组（多源 PRD 03）：1..64 条，每源独立连接/规则/下游/状态

	Observability Observability `json:"observability"` // 端口与日志（进程级端点，cluster 共享）
}

// SourceSpec 一条同步源（多源 PRD 03 §1）：现行单源集群全部数据面配置的
// 打包——源库连接 + 规则 + 下游 + 管线/全量参数。pipeline/stock 为覆盖补丁
//（nil = 全继承集群默认；非 nil 时未给字段逐项继承，见 ResolvePipeline）。
//
// Load/ApplyDefaults 之后 Pipeline/Stock 恒非 nil 且已是合并完整值——校验与
// 运行时看到的每个源都是完整值，与现行单源视角一致。
type SourceSpec struct {
	ID      string   `json:"id"`      // 源标识（source_id/sid）：^[a-z][a-z0-9_-]{1,31}$，cluster 内唯一；etcd key 段/指标 label/API 路径/日志/envelope 共用
	Source  Source   `json:"source"`  // 源库连接（现行 Source 块原样下沉）
	Targets Targets  `json:"targets"` // 该源独立下游（独立连接池/生产者，不跨源共享）
	Rules   []Rule   `json:"rules"`   // 规则组（作用域收敛到该源：schema 源内唯一即可）

	Pipeline *Pipeline `json:"pipeline,omitempty"` // 管线参数覆盖补丁（Load 后为合并完整值）
	Stock    *Stock    `json:"stock,omitempty"`    // 全量参数覆盖补丁（Load 后为合并完整值）
}

// SourceIDRe 源 id 字符集（多源 PRD 03 §1.2）：小写字母开头，总长 2~32，
// 与 cluster/topic 前缀字符集风格一致。
var SourceIDRe = regexp.MustCompile(`^[a-z][a-z0-9_-]{1,31}$`)

// DefaultSourceID v1 配置自动包装与 etcd 键迁移的落点源 id（保留值）。
const DefaultSourceID = "default"

// ResolvePipeline 合并解析：源覆盖 > 集群默认。字段级语义 = 非零覆盖
//（与 ApplyDefaults 的「0 值 = 未设」约定一致；结构上 0 值字段本就非法，
// 不存在「显式 0」的合法用例）。
func (s *SourceSpec) ResolvePipeline(base Pipeline) Pipeline {
	out := base
	if s.Pipeline != nil {
		o := *s.Pipeline
		if o.Workers != 0 {
			out.Workers = o.Workers
		}
		if o.WorkerQueueSize != 0 {
			out.WorkerQueueSize = o.WorkerQueueSize
		}
		if o.BatchSize != 0 {
			out.BatchSize = o.BatchSize
		}
		if o.FlushIntervalMS != 0 {
			out.FlushIntervalMS = o.FlushIntervalMS
		}
		if o.PositionSaveIntervalMS != 0 {
			out.PositionSaveIntervalMS = o.PositionSaveIntervalMS
		}
		if o.OnConvertError != "" {
			out.OnConvertError = o.OnConvertError
		}
		if o.DecimalAs != "" {
			out.DecimalAs = o.DecimalAs
		}
		if o.ZeroDatetimeAs != "" {
			out.ZeroDatetimeAs = o.ZeroDatetimeAs
		}
	}
	return out
}

// ResolveStock 合并解析：源覆盖 > 集群默认（字段级语义同 ResolvePipeline）。
func (s *SourceSpec) ResolveStock(base Stock) Stock {
	out := base
	if s.Stock != nil {
		o := *s.Stock
		if o.LockMode != "" {
			out.LockMode = o.LockMode
		}
		if o.ChunkRows != 0 {
			out.ChunkRows = o.ChunkRows
		}
		if o.ThrottleMS != 0 {
			out.ThrottleMS = o.ThrottleMS
		}
		if o.Workers != 0 {
			out.Workers = o.Workers
		}
	}
	return out
}

// FindSource 按 id 查源（装配层/API 路由用；不存在返回 nil）。
func (c *Config) FindSource(sid string) *SourceSpec {
	for i := range c.Sources {
		if c.Sources[i].ID == sid {
			return &c.Sources[i]
		}
	}
	return nil
}

// SourceIDs 全部源 id（有序即配置序）。
func (c *Config) SourceIDs() []string {
	out := make([]string, 0, len(c.Sources))
	for i := range c.Sources {
		out = append(out, c.Sources[i].ID)
	}
	return out
}

// Single 唯一源便捷取用（v1 包装/单源部署；调用方保证 len(Sources) ≥ 1，
// 校验 V1 已把关）。
func (c *Config) Single() *SourceSpec { return &c.Sources[0] }

// Node 节点身份。整节可省略：空值运行时自动派生，不参与必填校验。
// 主要用于多副本部署时在 Web 页面区分「这是哪台节点」。
type Node struct {
	ID            string `json:"id"`             // 空 = "{hostname}-{8位随机}"；k8s 建议注入 POD_NAME 保证唯一
	AdvertiseAddr string `json:"advertise_addr"` // 空 = "本机IP:web端口"；供集群页面定位节点
}

// Source 源库连接与 binlog 拉取参数：进程以「伪从库」身份（server_id）向源库
// 订阅 binlog 事件流。
type Source struct {
	Addr     string `json:"addr"`      // 源库 host:port，如 "10.0.0.1:3306"
	User     string `json:"user"`      // 账号需 REPLICATION SLAVE、REPLICATION CLIENT、SELECT 权限
	Password string `json:"password"`  // 支持 ${ENV_VAR} 占位，避免明文进 etcd
	Charset  string `json:"charset"`   // 连接字符集，一般 "utf8mb4"
	Flavor   string `json:"flavor"`    // mysql | mariadb（binlog 方言差异）
	ServerID uint32 `json:"server_id"` // 伪从库 ID：与其他从库/canal 等任何 binlog 订阅者
	// 冲突会互相踢下线。**0 = 缺省自动派生**（cmd 层按 node.id 做 fnv32——etcd
	// 配置中心共享一份配置时各副本 node.id 不同，派生值天然不同）；显式配置优先。
	// 约束仅「同一台源库上并存的 dump 连接间唯一」，与位点/binlog 内容无关。
	Timezone string `json:"timezone"` // DATETIME 值的解释时区（如 "Asia/Shanghai"）；必须显式声明，
	// （隐式取进程 Local 会跨机漂移）
	UseGTID             bool `json:"use_gtid"`              // true 时位点含 gtid_set（要求源库开启 GTID 模式）
	HeartbeatIntervalMS int  `json:"heartbeat_interval_ms"` // binlog 心跳周期 ms；0 = 默认 3000。
	// 心跳让低流量库也有事件推进位点/喂狗，防连接被判死
	ReadTimeoutMS  int      `json:"read_timeout_ms"` // binlog 网络读超时 ms；0 = 默认 60000
	ExcludeSchemas []string `json:"exclude_schemas"` // 额外排除的库名；四个系统库（mysql/sys/
	// information_schema/performance_schema）
	// 必须在列且不可移除，只能追加业务库
	TLS TLSOptions `json:"tls"` // 源库连接 TLS（可选）
}

// HA Leader 选举参数（03 §4）。多副本部署时经 etcd lease 选出唯一 leader 拉取
// binlog，其余副本 standby 热备；leader 宕机后 lease 过期自动切换。
type HA struct {
	// Enabled 集群模式开关：nil/true = etcd 选举多副本；
	// false = 单节点——跳过选举，进程直接以 leader 启动管线。位点/标记仍走
	// 协调面 etcd 租约门槛（session 死亡 → 门键消失 → 位点写被拒 → 管线停，
	// 与集群模式失位语义一致；恢复需重启进程）
	Enabled *bool `json:"enabled,omitempty"`
	// LeaseTTLSeconds 选举/session 租约秒数：必填 ≥2，建议 10。
	// 决定 leader 宕机后的切换时长（TTL 内旧 leader
	// 失联即开始等待过期）；
	// 单节点模式下是 leader 门键与节点注册的租约时长
	LeaseTTLSeconds       int  `json:"lease_ttl_seconds"`
	LeaderCheckIntervalMS int  `json:"leader_check_interval_ms"` // leader 在位复核周期 ms；0 = 默认 2000
	ResignOnShutdown      bool `json:"resign_on_shutdown"`       // true = 优雅停机时主动让位，standby 立即接管，
	// 不必等 lease 过期
}

// ClusterEnabled 集群模式是否开启（enabled 缺省 = 开启，nil 视作 true）。
func (h HA) ClusterEnabled() bool { return h.Enabled == nil || *h.Enabled }

// Pipeline 管线参数（04）。事件流经：binlog 解析 → 按表分片到 worker →
// 攒批 → 下游发送。攒批语义：攒满 batch_size 条 或 距批首条已过 flush_interval_ms，
// 二者先到先发（低流量靠时间兜底，高流量靠条数控延迟）。
type Pipeline struct {
	Workers int `json:"workers"` // 解析/发送并行度，1..64；按表分片——同表事件
	// 落同一 worker 串行保序，不同表并行
	WorkerQueueSize int `json:"worker_queue_size"` // 每 worker 有界队列长度（事件数），≥1。
	// 打满即反压阻塞 binlog 拉取，内存有上界（09 §1）
	BatchSize              int `json:"batch_size"`                // 攒批条数，≥1
	FlushIntervalMS        int `json:"flush_interval_ms"`         // 攒批超时 ms；0 = 默认 200
	PositionSaveIntervalMS int `json:"position_save_interval_ms"` // 位点保存节流 ms；0 = 默认 3000。
	// 越大重启重复越多、etcd 写越少（配合下游幂等消化）

	OnConvertError string `json:"on_convert_error"` // 值转换失败策略（04 §7）：null | zero | fail_event。
	// 空 = 默认 null（值置 NULL + 计数告警）；zero = 按类型零值；
	// fail_event = 整个事件失败（不可重试，规则 PAUSED + 告警）
	DecimalAs string `json:"decimal_as"` // DECIMAL/NUMERIC 承载形态（04 §7）：string | float。
	// 空 = 默认 string（无损，推荐）；float 有精度丢失风险，仅兼容旧消费方时用
	ZeroDatetimeAs string `json:"zero_datetime_as"` // 零日期（0000-00-00…）处理（04 §7）：null | string。
	// 空 = 默认 null（置 NULL + 计数）；string = 原样字符串透传
}

// Targets 下游目标集合（03 §3.2 设计要点）：同一规则可同时启用多个下游
// （如 MySQL 直写 + Kafka 分发），规则级 sinks 决定每条规则实际流向哪几个。
type Targets struct {
	MySQL    MySQLOptions    `json:"mysql"`
	Kafka    KafkaOptions    `json:"kafka"`
	RocketMQ RocketMQOptions `json:"rocketmq"`
}

// MySQLOptions MySQL 直写目标（05 §5）：目标表 = 源库同名同表、列一一对应，
// 没有任何表名/列名映射字段——同名同表约束在 schema 校验层强制，无错配空间。
type MySQLOptions struct {
	Enabled      bool   `json:"enabled"`
	Addr         string `json:"addr"`           // 目标库 host:port
	User         string `json:"user"`           // 需目标库/表的 DML（开 ddl_sync 时还需 DDL）权限
	Password     string `json:"password"`       // 支持 ${ENV_VAR} 占位
	Charset      string `json:"charset"`        // 一般 "utf8mb4"
	MaxOpenConns int    `json:"max_open_conns"` // 必须显式 ≥1：0=无上限，
	// 全量期间会打爆目标库连接
	MaxIdleConns       int `json:"max_idle_conns"`        // 空闲连接数；0 = 默认 5
	ConnMaxLifetimeMin int `json:"conn_max_lifetime_min"` // 连接最长存活分钟数；0 = 默认 30
	//（小于中间件/DB 的 wait_timeout，避免用陈旧连接）
	TxChunkRows int `json:"tx_chunk_rows"` // 单事务最大写入行数，≥1：大事务切批提交，
	// 避免目标端长事务/大回滚段
	TargetDatabase string `json:"target_database"` // 默认目标库（05 §5.1.1）：空 = 与源库同名。
	// 多源库聚合进一个库的用法在此配置；规则级 target_database 可按规则覆盖
	DDLSync bool `json:"ddl_sync"` // true = 允许 ddl_policy=auto 的规则把白名单 DDL
	//（如 ADD COLUMN）执行到目标库（08 §2）；false 则 auto 降级 pause
}

// KafkaOptions Kafka 目标（05 §3，客户端 franz-go）。
//
// 实际 topic 默认 = topic_prefix + "{db}_{table}"（如前缀 "cdc_" + edu.mall.admin
// 下的 t 表 → "cdc_edu_mall_admin_t"），规则级 sinks.kafka.topic 可按规则覆盖。
// 与 RocketMQ 统一命名（点号不收，理由见 sink.TopicName）。
// order_by 决定消息 key（从而决定分区内有序粒度）：
//
//	row   → key=主键：同一行的变更严格有序，不同行并行（推荐默认）
//	table → key=库名.表名：整表事件进同一分区全有序，顺序最强但吞吐最低
type KafkaOptions struct {
	Enabled            bool     `json:"enabled"`
	Brokers            []string `json:"brokers"`             // ["b1:9092", "b2:9092"]，至少一个
	TopicPrefix        string   `json:"topic_prefix"`        // topic 前缀，如 "cdc_"；字符限 [a-zA-Z0-9_-]（与 RocketMQ 统一）
	OrderBy            string   `json:"order_by"`            // row | table（见上）
	Compression        string   `json:"compression"`         // none|gzip|snappy|lz4|zstd，一般 snappy
	IdempotentProducer bool     `json:"idempotent_producer"` // true = 幂等 producer：重试不产生
	// 分区内重复/乱序（broker ≥0.11 时建议开）
	Ack             string `json:"ack"`               // all（全副本确认，不丢）| one（leader 确认，低延迟）
	SendTimeoutMS   int    `json:"send_timeout_ms"`   // 发送超时 ms；0 = 默认 5000
	MaxMessageBytes int    `json:"max_message_bytes"` // 客户端单条消息上限字节，必填 [1KB,100MB]，
	// 且必须 ≤ broker/topic 的 max.message.bytes，
	// 超宽行会导致发送失败
	SASL SASLOptions `json:"sasl"` // 接入带认证的集群时启用
	TLS  TLSOptions  `json:"tls"`  // 加密端口（如 9093）时启用
}

// RocketMQOptions RocketMQ 目标（05 §4，客户端 v5 gRPC）。
// 实际 topic 默认 = topic_prefix + "{db}_{table}"（如 "CDC_edu_mall_admin_t"），
// 与 Kafka 同一套命名规则（见 sink.TopicName）；规则级 sinks.rocketmq.topic 可按规则
// 覆盖；顺序消息按 order_by 派生的 key 做 selector。
type RocketMQOptions struct {
	Enabled         bool       `json:"enabled"`
	NameServers     []string   `json:"name_servers"`     // NameServer 地址列表，至少一个
	Group           string     `json:"group"`            // 生产者组名，如 "gocdc-producer"
	TopicPrefix     string     `json:"topic_prefix"`     // topic 前缀，如 "CDC_"
	OrderBy         string     `json:"order_by"`         // row | table（语义同 Kafka order_by）
	SendTimeoutMS   int        `json:"send_timeout_ms"`  // 发送超时 ms；0 = 默认 5000
	SendConcurrency int        `json:"send_concurrency"` // 组间并发发送上限（全局信号量）；0 = 默认 32，组内仍严格串行
	ACL             ACLOptions `json:"acl"`              // 集群开启 ACL 时填
}

// SASLOptions Kafka SASL 认证。密码经 password_env 引用环境变量，
// 不进 etcd 明文。
type SASLOptions struct {
	Enabled     bool   `json:"enabled"`
	Mechanism   string `json:"mechanism"` // plain | scram-sha256 | scram-sha512
	User        string `json:"user"`
	PasswordEnv string `json:"password_env"` // 存密码的环境变量名（如 KAFKA_SASL_PWD）
}

// ACLOptions RocketMQ ACL（v5 credentials）。SecretKey 支持 ${ENV_VAR} 占位。
type ACLOptions struct {
	AccessKey string `json:"access_key"`
	SecretKey string `json:"secret_key"`
}

// TLSOptions TLS 客户端参数（source/kafka 同构，03 §3.2）。
// 仅 CA 单向认证：填 ca 即可；mTLS：再填 cert+key（必须成对）。
type TLSOptions struct {
	Enabled    bool   `json:"enabled"`
	SkipVerify bool   `json:"skip_verify"` // 跳过服务端证书校验（自签测试用；无 ca 时必须显式 true）
	CA         string `json:"ca"`          // CA 证书文件路径
	Cert       string `json:"cert"`        // 客户端证书路径（mTLS）
	Key        string `json:"key"`         // 客户端私钥路径（mTLS）
}

// Rule 同步规则组（03 §3.2 rules[]；model.Rule 为其运行时编译产物）：
// 唯一形态 = 组形态白名单——组级声明「哪个库写到哪些下游、DDL 怎么处理」，
// tables 逐表列出该库要同步的表；**未列入的表（含未来新建的表）不同步**。
//
//	rules:
//	  - schema: edu.mall.admin
//	    sinks: { mysql: true }
//	    ddl_policy: auto
//	    tables:
//	      - { table: admin_user, order_by_column: id, do_stock: true }
//	      - { table: users, do_stock: false, mask_columns: { phone: phone } }
//
// 字段归属：组级 = schema/sinks/ddl_policy/target_database；
// 表级（TableRule）= table/order_by_column/do_stock(必填)/target_database/
// exclude_columns/mask_columns。
// 规则仅支持组形态（schema + tables 白名单）；表级字段（do_stock/order_by_column
// 等）必须写在 tables 条目内——写在组条目上会在解码期报错并指引（见 UnmarshalJSON）。
type Rule struct {
	Schema string `json:"schema"`

	// Sinks 组级下游开关（必填，至少启用一个；整组统一，表级不可配）。
	Sinks *RuleSinks `json:"sinks"`
	// DDLPolicy 组级 DDL 策略：auto | pause | ignore（08 §2）；省略 = pause（安全默认）。
	DDLPolicy string `json:"ddl_policy,omitempty"`
	// TargetDatabase 组级默认目标库（05 §5.1.1）：空 = targets.mysql.target_database
	// （再空 = 与源库同名）；表级可按表覆盖；表名不可改。
	TargetDatabase string `json:"target_database,omitempty"`
	// Tables 表白名单（必填非空）。
	Tables []TableRule `json:"tables"`
}

// TableRule 组内单表白名单条目（字段见 Rule 注释）。
type TableRule struct {
	Table string `json:"table"`
	// OrderByColumn do_stock=true 时必填：全量快照分块排序键（一般为主键），
	// 且不可被 exclude/mask（会破坏快照游标，06 §4）。
	OrderByColumn string `json:"order_by_column,omitempty"`
	// DoStock 必填（显式声明）：true=参与全量（先快照后增量，06）；
	// false=只增量不快照。
	DoStock *bool `json:"do_stock"`
	// TargetDatabase 表级目标库覆盖（05 §5.1.1）；空 = 组级（再空 = 全局默认）。
	TargetDatabase string `json:"target_database,omitempty"`
	// ExcludeColumns 不同步的列（下游表没有这些列）。
	ExcludeColumns []string `json:"exclude_columns,omitempty"`
	// MaskColumns 脱敏列 → 算法（见 MaskSpec）。
	MaskColumns map[string]MaskSpec `json:"mask_columns,omitempty"`
}

// UnmarshalJSON 组级严格解码：未知字段即报错——表级字段（table/order_by_column/
// do_stock/exclude_tables… 写在组条目上）或拼写错误在加载期精确定位，
// 而不是被静默丢弃。
func (r *Rule) UnmarshalJSON(data []byte) error {
	type alias Rule // 剥离方法，防递归
	var a alias
	if err := decodeStrict(data, &a); err != nil {
		return fmt.Errorf("不支持的规则组形态：组级仅允许 schema/sinks/ddl_policy/target_database，table/order_by_column/do_stock 等表级字段必须写在 tables 条目内（不支持平铺写法，见 internal/config/README.md §6）: %w", err)
	}
	*r = Rule(a)
	return nil
}

// UnmarshalJSON 表级条目严格解码：未知字段即报错——sinks/ddl_policy 是组级
// 字段、exclude_tables 非法（组形态无表级排除名单），拼写错误加载期定位。
func (t *TableRule) UnmarshalJSON(data []byte) error {
	type alias TableRule
	var a alias
	if err := decodeStrict(data, &a); err != nil {
		return fmt.Errorf("规则表级条目不识别（sinks/ddl_policy 在组级统一配置；不支持 exclude_tables）: %w", err)
	}
	*t = TableRule(a)
	return nil
}

// decodeStrict 严格 JSON 解码（未知字段拒绝）。
func decodeStrict(data []byte, dst any) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	return dec.Decode(dst)
}

// RuleSinks 规则级 sink 开关。常见写法直接用 bool 简写：
//
//	"sinks": { "mysql": true, "kafka": true }
//
// 需要覆盖 topic/tag 时才写对象（两种形态均合法，与 03 §3.2 对象形式兼容）：
//
//	"sinks": { "kafka": { "enabled": true, "topic": "cdc_custom" } }
//
// 约束：至少启用一个 sink；sink 应与 targets 中 enabled 的下游对应；
// MySQL sink 不接受 topic/tag（目标表=同名表，无映射）。
type RuleSinks struct {
	MySQL    RuleSink `json:"mysql"`
	Kafka    RuleSink `json:"kafka"`
	RocketMQ RuleSink `json:"rocketmq"`
}

// RuleSink 单个规则级 sink：bool 简写或对象。MySQL sink 没有目标表/列映射字段
// （同名同表约束从配置层面杜绝错配，03 §3.2），Topic/Tag 仅对 MQ sink 有意义。
type RuleSink struct {
	Enabled bool   `json:"enabled"`
	Topic   string `json:"topic,omitempty"` // kafka/rocketmq 覆盖；空 = prefix + "{db}_{table}" 派生（统一规范，覆盖值同样不收点）
	Tag     string `json:"tag,omitempty"`   // rocketmq 事件 tag 覆盖
}

// UnmarshalJSON 兼容 bool（{"mysql": true}）与对象（{"kafka": {...}}）两种形态。
func (s *RuleSink) UnmarshalJSON(data []byte) error {
	var b bool
	if err := json.Unmarshal(data, &b); err == nil {
		*s = RuleSink{Enabled: b}
		return nil
	}
	type alias RuleSink // 防递归
	var a alias
	if err := json.Unmarshal(data, &a); err != nil {
		return fmt.Errorf("sink 须为 bool 或 {enabled,topic,tag} 对象: %w", err)
	}
	*s = RuleSink(a)
	return nil
}

// Stock 全量初始化参数（06 §3/§4/§5）。
type Stock struct {
	LockMode string `json:"lock_mode"` // 快照锁模式（06 §3 降级矩阵）：auto | ftwrl | backup | none。
	// 空 = auto（按权限探测，日志声明）；none = 显式声明无锁快照（位点与快照
	// 非原子，可能窗口丢失——必须显式配置才允许，防无感降级）
	ChunkRows  int `json:"chunk_rows"`  // PK 游标单块行数；0 = 5000（06 §4）
	ThrottleMS int `json:"throttle_ms"` // 块间隔节流 ms；0 = 不节流
	Workers    int `json:"workers"`     // 表间并行度；0 = 4（与管线 workers 解耦，06 §7）
}

// Observability 可观测配置（11）：指标/健康探针端口、admin API、日志。
type Observability struct {
	Web         WebOptions `json:"web"`
	MetricsAddr string     `json:"metrics_addr"` // Prometheus /metrics 与 /healthz /readyz 监听地址，必填
	//（如 ":9595"；k8s 探针与抓取都指向这里）
	Log LogOptions `json:"log"`
}

// WebOptions admin API（13 §3/§5）：API 优先、默认安全。
// enabled=true 时 addr 与 username/password 均必填——admin API 不允许裸奔；
// 内网裸奔必须显式 auth_disabled=true（配置校验告警）。
type WebOptions struct {
	Enabled      bool   `json:"enabled"`
	Addr         string `json:"addr"`          // 监听地址，如 ":8060"
	Username     string `json:"username"`      // Basic 用户名（单凭据全权限）
	Password     string `json:"password"`      // Basic 密码；支持 ${ENV_VAR} 占位
	AuthDisabled bool   `json:"auth_disabled"` // 显式关闭鉴权（内网场景；显式声明才允许）
}

// LogOptions 日志（统一输出 stdout，由容器/collector 收集，
// 不做文件轮转）。
type LogOptions struct {
	Level string `json:"level"` // debug|info|warn|error；空 = info
	JSON  bool   `json:"json"`  // true = JSON 行格式（采集器友好）；false = 人类可读 console
}

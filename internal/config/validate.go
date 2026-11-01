package config

import (
	"fmt"
	"net"
	"regexp"
	"strings"

	"gocdc/internal/logging"
)

// 系统库四件套：exclude_schemas 不可移除（03 §3.2）。
var systemSchemas = []string{"mysql", "sys", "information_schema", "performance_schema"}

var (
	// identRe 表名/列名字符集：禁止空白、点、反引号、路径分隔符。
	identRe = regexp.MustCompile(`^[^.\x60\s/\\]+$`)
	// schemaRe 库名字符集：允许点（源库库名形如 edu.mall.admin——多源库按 .
	// 命名聚合，禁点等于无法迁移；SQL 侧全反引号引用、组合键仅作不透明字符串，
	// 无按点反解）。仍禁空白/反引号/路径分隔符。
	schemaRe = regexp.MustCompile(`^[^\x60\s/\\]+$`)
	// kafkaTopicRe Kafka topic 合法字符与长度（broker 侧规则）。不收点号：
	// 与 RocketMQ 统一命名规范（Kafka 官方也建议 topic 名不混用 "." 与 "_"，
	// metrics 聚合会把两者折叠成同名）。
	kafkaTopicRe = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,249}$`)
	// rocketTopicRe RocketMQ 用户 topic/group 字符集（保守子集）。
	rocketTopicRe = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,255}$`)
)

// Validate 严格校验（03 §3.1-3）：返回全部错误（不短路），调用方打印清单后退出。
// flagCluster 为 -cluster flag 值——key 前缀唯一来源，配置字段仅作冗余校验（03 §3.1）。
func Validate(c *Config, flagCluster string) []error {
	var errs []error
	errf := func(format string, args ...any) {
		errs = append(errs, fmt.Errorf(format, args...))
	}

	// ── 全局结构 ──────────────────────────────────────────────
	if c.Cluster == "" {
		errf("cluster 不能为空")
	} else if flagCluster != "" && c.Cluster != flagCluster {
		errf("cluster=%q 与 -cluster flag=%q 不一致：配置属于别的集群（03 §3.1 裁决）", c.Cluster, flagCluster)
	}
	if len(c.Rules) == 0 {
		errf("rules 不能为空")
	}
	if !c.Targets.MySQL.Enabled && !c.Targets.Kafka.Enabled && !c.Targets.RocketMQ.Enabled {
		errf("targets 至少启用一个下游（mysql/kafka/rocketmq）")
	}

	// ── source ────────────────────────────────────────────────
	if c.Source.Addr == "" {
		errf("source.addr 不能为空")
	} else if err := checkAddr(c.Source.Addr); err != nil {
		errf("source.addr: %v", err)
	}
	if c.Source.User == "" {
		errf("source.user 不能为空")
	}
	if c.Source.Charset == "" {
		errf("source.charset 不能为空")
	}
	if c.Source.Timezone == "" {
		errf("source.timezone 不能为空（DATETIME 解释时区必须显式化）")
	}
	// server_id=0 合法：cmd 层按 node.id 派生（etcd 配置中心共享一份配置时，
	// 各副本 node.id 不同 → 派生值天然不同；显式配置优先）
	switch c.Source.Flavor {
	case "mysql", "mariadb":
	default:
		errf("source.flavor=%q 须为 mysql|mariadb", c.Source.Flavor)
	}
	checkExcludeSchemas(c.Source.ExcludeSchemas, &errs)
	checkTLS("source.tls", c.Source.TLS, &errs)

	// ── ha ────────────────────────────────────────────────────
	if c.HA.LeaseTTLSeconds < 2 {
		errf("ha.lease_ttl_seconds=%d 须 ≥2（建议 10）", c.HA.LeaseTTLSeconds)
	}
	if c.HA.LeaderCheckIntervalMS <= 0 {
		errf("ha.leader_check_interval_ms=%d 须 >0", c.HA.LeaderCheckIntervalMS)
	}

	// ── pipeline ──────────────────────────────────────────────
	if c.Pipeline.Workers < 1 || c.Pipeline.Workers > 64 {
		errf("pipeline.workers=%d 须为 1..64", c.Pipeline.Workers)
	}
	if c.Pipeline.WorkerQueueSize < 1 {
		errf("pipeline.worker_queue_size=%d 须 ≥1（有界队列硬要求，09 §1）", c.Pipeline.WorkerQueueSize)
	}
	if c.Pipeline.BatchSize < 1 {
		errf("pipeline.batch_size=%d 须 ≥1", c.Pipeline.BatchSize)
	}
	if c.Pipeline.FlushIntervalMS <= 0 {
		errf("pipeline.flush_interval_ms=%d 须 >0", c.Pipeline.FlushIntervalMS)
	}
	if c.Pipeline.PositionSaveIntervalMS <= 0 {
		errf("pipeline.position_save_interval_ms=%d 须 >0", c.Pipeline.PositionSaveIntervalMS)
	}
	switch c.Stock.LockMode {
	case "", "auto", "ftwrl", "backup", "none":
	default:
		errf("stock.lock_mode=%q 须为 auto|ftwrl|backup|none（06 §3）", c.Stock.LockMode)
	}
	if c.Stock.ChunkRows < 0 || c.Stock.ChunkRows > 1000000 {
		errf("stock.chunk_rows=%d 须 0..1000000（0=默认 5000）", c.Stock.ChunkRows)
	}
	if c.Stock.Workers < 0 || c.Stock.Workers > 64 {
		errf("stock.workers=%d 须 0..64（0=默认 4）", c.Stock.Workers)
	}
	switch c.Pipeline.OnConvertError {
	case "null", "zero", "fail_event":
	default:
		errf("pipeline.on_convert_error=%q 须为 null|zero|fail_event（04 §7）", c.Pipeline.OnConvertError)
	}
	switch c.Pipeline.DecimalAs {
	case "string", "float":
	default:
		errf("pipeline.decimal_as=%q 须为 string|float（04 §7）", c.Pipeline.DecimalAs)
	}
	switch c.Pipeline.ZeroDatetimeAs {
	case "null", "string":
	default:
		errf("pipeline.zero_datetime_as=%q 须为 null|string（04 §7）", c.Pipeline.ZeroDatetimeAs)
	}

	// ── targets.mysql ─────────────────────────────────────────
	if m := c.Targets.MySQL; m.Enabled {
		if m.Addr == "" {
			errf("targets.mysql.addr 不能为空")
		} else if err := checkAddr(m.Addr); err != nil {
			errf("targets.mysql.addr: %v", err)
		}
		if m.User == "" {
			errf("targets.mysql.user 不能为空")
		}
		if m.Charset == "" {
			errf("targets.mysql.charset 不能为空")
		}
		if m.MaxOpenConns < 1 {
			errf("targets.mysql.max_open_conns=%d 必须显式 ≥1（0=无上限）", m.MaxOpenConns)
		}
		if m.MaxIdleConns > m.MaxOpenConns {
			errf("targets.mysql.max_idle_conns=%d 不可大于 max_open_conns=%d", m.MaxIdleConns, m.MaxOpenConns)
		}
		if m.TxChunkRows < 1 {
			errf("targets.mysql.tx_chunk_rows=%d 须 ≥1", m.TxChunkRows)
		}
		if m.TargetDatabase != "" && !schemaRe.MatchString(m.TargetDatabase) {
			errf("targets.mysql.target_database=%q 含非法字符（库名允许点，如 edu.mall.merge；禁止空白/反引号/路径符）", m.TargetDatabase)
		}
	}

	// ── targets.kafka ─────────────────────────────────────────
	if k := c.Targets.Kafka; k.Enabled {
		if len(k.Brokers) == 0 {
			errf("targets.kafka.brokers 不能为空")
		}
		for i, b := range k.Brokers {
			if err := checkAddr(b); err != nil {
				errf("targets.kafka.brokers[%d]: %v", i, err)
			}
		}
		if !kafkaTopicRe.MatchString(k.TopicPrefix) {
			errf("targets.kafka.topic_prefix=%q 不是合法 topic 前缀（[a-zA-Z0-9_-]，与 RocketMQ 统一不收点）", k.TopicPrefix)
		}
		checkOrderBy("targets.kafka.order_by", k.OrderBy, &errs)
		switch k.Compression {
		case "none", "gzip", "snappy", "lz4", "zstd":
		default:
			errf("targets.kafka.compression=%q 须为 none|gzip|snappy|lz4|zstd", k.Compression)
		}
		switch k.Ack {
		case "all", "one":
		default:
			errf("targets.kafka.ack=%q 须为 all|one", k.Ack)
		}
		if k.MaxMessageBytes < 1024 || k.MaxMessageBytes > 100<<20 {
			errf("targets.kafka.max_message_bytes=%d 须在 [1KB,100MB]（且 ≤ broker max.message.bytes）", k.MaxMessageBytes)
		}
		if k.SendTimeoutMS <= 0 {
			errf("targets.kafka.send_timeout_ms=%d 须 >0", k.SendTimeoutMS)
		}
		if k.SASL.Enabled {
			switch k.SASL.Mechanism {
			case "plain", "scram-sha256", "scram-sha512":
			default:
				errf("targets.kafka.sasl.mechanism=%q 须为 plain|scram-sha256|scram-sha512", k.SASL.Mechanism)
			}
			if k.SASL.User == "" {
				errf("targets.kafka.sasl.user 不能为空")
			}
			if !envNameRe.MatchString(k.SASL.PasswordEnv) {
				errf("targets.kafka.sasl.password_env=%q 不是合法环境变量名（密码不进 etcd 明文）", k.SASL.PasswordEnv)
			}
		}
		checkTLS("targets.kafka.tls", k.TLS, &errs)
	}

	// ── targets.rocketmq ──────────────────────────────────────
	if r := c.Targets.RocketMQ; r.Enabled {
		if len(r.NameServers) == 0 {
			errf("targets.rocketmq.name_servers 不能为空")
		}
		for i, a := range r.NameServers {
			if err := checkAddr(a); err != nil {
				errf("targets.rocketmq.name_servers[%d]: %v", i, err)
			}
		}
		if !rocketTopicRe.MatchString(r.Group) {
			errf("targets.rocketmq.group=%q 不是合法消费组名", r.Group)
		}
		if !rocketTopicRe.MatchString(r.TopicPrefix) {
			errf("targets.rocketmq.topic_prefix=%q 不是合法 topic 前缀", r.TopicPrefix)
		}
		checkOrderBy("targets.rocketmq.order_by", r.OrderBy, &errs)
		if r.SendTimeoutMS <= 0 {
			errf("targets.rocketmq.send_timeout_ms=%d 须 >0", r.SendTimeoutMS)
		}
		if r.SendConcurrency < 0 || r.SendConcurrency > 256 {
			errf("targets.rocketmq.send_concurrency=%d 须在 [0,256]（0 已被默认填充为 32）", r.SendConcurrency)
		}
	}

	// ── rules ─────────────────────────────────────────────────
	// 组形态白名单（唯一形态，见 Rule 注释）：一组一库；组级 sinks/ddl_policy，
	// tables 逐表白名单（未列入的表不同步，含未来新建的表）。
	schemaSeen := map[string]int{} // 小写 schema → 组下标（一组一库）
	for i := range c.Rules {
		r := &c.Rules[i]
		path := fmt.Sprintf("rules[%d]", i)
		if r.Schema == "" || !schemaRe.MatchString(r.Schema) {
			errf("%s.schema=%q 为空或含非法字符（库名允许点，如 edu.mall.admin；禁止空白/反引号/路径符）", path, r.Schema)
			continue
		}
		if prev, dup := schemaSeen[strings.ToLower(r.Schema)]; dup {
			errf("%s 与 rules[%d] 的 schema 重复（一组一库）", path, prev)
			continue
		}
		schemaSeen[strings.ToLower(r.Schema)] = i

		if r.Sinks == nil {
			errf("%s.sinks 未配置（组级必填，至少启用一个下游）", path)
		} else {
			validateSinks(r.Sinks, path, errf)
		}
		if r.DDLPolicy == "" {
			r.DDLPolicy = "pause" // 安全默认（08/16 修订）
		}
		switch r.DDLPolicy {
		case "auto", "pause", "ignore":
		default:
			errf("%s.ddl_policy=%q 须为 auto|pause|ignore", path, r.DDLPolicy)
		}
		if r.TargetDatabase != "" && !schemaRe.MatchString(r.TargetDatabase) {
			errf("%s.target_database=%q 含非法字符（库名允许点，如 edu.mall.merge；禁止空白/反引号/路径符）", path, r.TargetDatabase)
		}
		if len(r.Tables) == 0 {
			errf("%s.tables 不能为空（组形态即白名单：逐表列出要同步的表）", path)
			continue
		}
		seenT := map[string]bool{}
		for j := range r.Tables {
			t := &r.Tables[j]
			tpath := fmt.Sprintf("%s.tables[%d]", path, j)
			if t.Table == "" || t.Table == "*" || !identRe.MatchString(t.Table) {
				errf("%s.table=%q 须为具体表名（禁止空白/点/反引号/路径符/\"*\"；白名单逐表列出）", tpath, t.Table)
				continue
			}
			if seenT[strings.ToLower(t.Table)] {
				errf("%s 表级规则重复（%s）", tpath, t.Table)
			}
			seenT[strings.ToLower(t.Table)] = true
			if t.DoStock == nil {
				errf("%s.do_stock 必填（显式声明该表是否参与全量快照）", tpath)
				continue
			}
			if t.TargetDatabase != "" && !schemaRe.MatchString(t.TargetDatabase) {
				errf("%s.target_database=%q 含非法字符（库名允许点；禁止空白/反引号/路径符）", tpath, t.TargetDatabase)
			}
			if *t.DoStock {
				if t.OrderByColumn == "" {
					errf("%s.do_stock=true 但 order_by_column 为空（快照分块排序必需）", tpath)
				} else {
					for _, col := range t.ExcludeColumns {
						if col == t.OrderByColumn {
							errf("%s: order_by_column %q 不可同时出现在 exclude_columns（破坏快照游标）", tpath, col)
						}
					}
					if m, ok := t.MaskColumns[t.OrderByColumn]; ok {
						errf("%s: order_by_column %q 不可脱敏（type=%s 会破坏快照排序，10 §3）", tpath, t.OrderByColumn, m.Type)
					}
				}
			}
			checkColumns(tpath, t.ExcludeColumns, t.MaskColumns, errf)
		}
	}

	// ── observability ─────────────────────────────────────────
	if _, _, err := net.SplitHostPort(c.Observability.MetricsAddr); err != nil {
		errf("observability.metrics_addr=%q 不是合法 addr:port", c.Observability.MetricsAddr)
	}
	if w := c.Observability.Web; w.Enabled {
		if _, _, err := net.SplitHostPort(w.Addr); err != nil {
			errf("observability.web.addr=%q 不是合法 addr:port", w.Addr)
		}
		// auth_disabled=true = 显式裸奔豁免（内网场景必须显式声明才放开，
		// 03 §5.1）——此时凭据可空；未声明豁免则凭据必配。
		if !w.AuthDisabled && (w.Username == "" || w.Password == "") {
			errf("observability.web.username/password 不能为空（admin API 必须鉴权——HTTP Basic 单凭据，13 §3；密码建议 ${ENV} 占位；内网裸奔须显式 auth_disabled=true）")
		}
	}
	if _, err := logging.ParseLevel(c.Observability.Log.Level); err != nil {
		errf("observability.log.level: %v", err)
	}

	return errs
}

// checkExcludeSchemas 四系统库硬排除校验 + 去重（03 §3.2：可追加不可移除）。
func checkExcludeSchemas(list []string, errs *[]error) {
	lower := map[string]bool{}
	for _, s := range list {
		if lower[s] {
			*errs = append(*errs, fmt.Errorf("source.exclude_schemas 重复项 %q", s))
		}
		lower[s] = true
	}
	for _, sys := range systemSchemas {
		if !lower[sys] {
			*errs = append(*errs, fmt.Errorf("source.exclude_schemas 缺少系统库 %q（四系统库不可移除）", sys))
		}
	}
}

// checkTLS 证书/私钥成对与 CA 存在性。
func checkTLS(path string, t TLSOptions, errs *[]error) {
	if !t.Enabled {
		return
	}
	if (t.Cert == "") != (t.Key == "") {
		*errs = append(*errs, fmt.Errorf("%s: cert 与 key 必须成对提供", path))
	}
	if t.CA == "" && !t.SkipVerify {
		*errs = append(*errs, fmt.Errorf("%s: 未提供 ca 时必须显式 skip_verify=true（不推荐）", path))
	}
}

func checkOrderBy(path, v string, errs *[]error) {
	switch v {
	case "row", "table":
	default:
		*errs = append(*errs, fmt.Errorf("%s=%q 须为 row|table", path, v))
	}
}

// validateSinks 组级 sinks 校验（至少启用一个；MySQL sink 无 topic/tag）。
func validateSinks(s *RuleSinks, path string, errf func(string, ...any)) {
	if !s.MySQL.Enabled && !s.Kafka.Enabled && !s.RocketMQ.Enabled {
		errf("%s.sinks 至少启用一个（否则规则无输出）", path)
	}
	// MySQL sink 不存在目标表/列映射——配了 topic/tag 视为配置错误（05 §5.1）
	if s.MySQL.Topic != "" || s.MySQL.Tag != "" {
		errf("%s.sinks.mysql 不接受 topic/tag（目标表=同名表，无映射，05 §5.1）", path)
	}
	if s.Kafka.Enabled && s.Kafka.Topic != "" && !kafkaTopicRe.MatchString(s.Kafka.Topic) {
		errf("%s.sinks.kafka.topic=%q 不是合法 topic", path, s.Kafka.Topic)
	}
	if s.RocketMQ.Enabled && s.RocketMQ.Topic != "" && !rocketTopicRe.MatchString(s.RocketMQ.Topic) {
		errf("%s.sinks.rocketmq.topic=%q 不是合法 topic", path, s.RocketMQ.Topic)
	}
}

// checkColumns exclude/mask 列名与交叉冲突（10 §3：exclude 与 mask 语义互斥）。
func checkColumns(path string, excludeColumns []string, maskColumns map[string]MaskSpec, errf func(string, ...any)) {
	excluded := map[string]bool{}
	for _, c := range excludeColumns {
		if !identRe.MatchString(c) {
			errf("%s.exclude_columns 列名 %q 非法", path, c)
		}
		excluded[c] = true
	}
	for col, spec := range maskColumns {
		if !identRe.MatchString(col) {
			errf("%s.mask_columns 列名 %q 非法", path, col)
			continue
		}
		if excluded[col] {
			errf("%s: 列 %q 同时出现在 exclude_columns 与 mask_columns（语义互斥，10 §3）", path, col)
			continue
		}
		if err := validateMaskSpec(col, spec); err != nil {
			errf("%s.%v", path, err)
		}
	}
}

// checkAddr 校验 host:port 形态。
func checkAddr(s string) error {
	host, port, err := net.SplitHostPort(s)
	if err != nil {
		return fmt.Errorf("不是合法 addr:port: %w", err)
	}
	if host == "" {
		return fmt.Errorf("host 为空")
	}
	var p int
	if _, err := fmt.Sscanf(port, "%d", &p); err != nil || p < 1 || p > 65535 {
		return fmt.Errorf("端口 %q 非法", port)
	}
	return nil
}

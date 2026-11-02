package config

import (
	"fmt"
	"net"
	"regexp"
	"sort"
	"strings"

	"gocdc/internal/logging"
)

// 系统库四件套：exclude_schemas 不可移除（03 §3.2）。
var systemSchemas = []string{"mysql", "sys", "information_schema", "performance_schema"}

// maxSources 源数硬上限（多源 PRD 03 V1/05 §5.3：容量规划口径）。
const maxSources = 64

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

// Validate 严格校验（03 §3.1-3；多源 PRD 03 §2）：返回全部错误（不短路），
// 调用方打印清单后退出。flagCluster 为 -cluster flag 值——key 前缀唯一来源，
// 配置字段仅作冗余校验（03 §3.1）。
//
// 结构：cluster 级（cluster/ha/observability）+ 逐源 V3/V6（带 [sources/{sid}]
// 前缀）+ 跨源 V4（目标命名空间不相交）/V5（同实例 server_id 冲突）。
func Validate(c *Config, flagCluster string) []error {
	var errs []error
	errf := func(format string, args ...any) {
		errs = append(errs, fmt.Errorf(format, args...))
	}

	// ── cluster 级结构 ────────────────────────────────────────
	if c.Cluster == "" {
		errf("cluster 不能为空")
	} else if flagCluster != "" && c.Cluster != flagCluster {
		errf("cluster=%q 与 -cluster flag=%q 不一致：配置属于别的集群（03 §3.1 裁决）", c.Cluster, flagCluster)
	}
	if c.HA.LeaseTTLSeconds < 2 {
		errf("ha.lease_ttl_seconds=%d 须 ≥2（建议 10）", c.HA.LeaseTTLSeconds)
	}
	if c.HA.LeaderCheckIntervalMS <= 0 {
		errf("ha.leader_check_interval_ms=%d 须 >0", c.HA.LeaderCheckIntervalMS)
	}
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

	// ── sources：V1 数量 + V2 id + 逐源 V3/V6 ────────────────
	if len(c.Sources) == 0 {
		errf("sources 不能为空（至少 1 条；v1 顶层 source/targets/rules 自动包装为 default 源）")
	}
	if len(c.Sources) > maxSources {
		errf("sources 至多 %d 条（容量规划见 PRD 05 §5），当前 %d", maxSources, len(c.Sources))
	}
	idSeen := map[string]int{} // sid → 源下标
	for i := range c.Sources {
		spec := &c.Sources[i]
		if !SourceIDRe.MatchString(spec.ID) {
			errf("sources[%d].id=%q 非法（须匹配 ^[a-z][a-z0-9_-]{1,31}$：小写字母开头，总长 2~32）", i, spec.ID)
		} else if prev, dup := idSeen[spec.ID]; dup {
			errf("sources[%d].id=%q 与 sources[%d] 重复（cluster 内唯一）", i, spec.ID, prev)
		} else {
			idSeen[spec.ID] = i
		}
		validateSourceSpec(spec, sourcePath(i, spec.ID), errf)
	}

	// ── 跨源：V4 目标不相交 + V5 同实例 server_id ─────────────
	checkCrossSourceTargets(c, errf)
	checkCrossServerID(c, errf)

	return errs
}

// sourcePath 错误定位前缀：id 合法用 sources/{sid}（PRD 03 §2.1），
// 否则退回下标定位（id 本身已在 V2 报错）。
func sourcePath(i int, sid string) string {
	if SourceIDRe.MatchString(sid) {
		return fmt.Sprintf("[sources/%s] ", sid)
	}
	return fmt.Sprintf("[sources[%d]] ", i)
}

// validateSourceSpec 单源全套校验（V3：现行全部源内校验逐源执行；V6：合并后
// pipeline/stock 范围校验）。prefix 为 [sources/{sid}] 定位前缀。
func validateSourceSpec(spec *SourceSpec, prefix string, errf func(string, ...any)) {
	// ── source（源库连接）────────────────────────────────────
	if spec.Source.Addr == "" {
		errf("%ssource.addr 不能为空", prefix)
	} else if err := checkAddr(spec.Source.Addr); err != nil {
		errf("%ssource.addr: %v", prefix, err)
	}
	if spec.Source.User == "" {
		errf("%ssource.user 不能为空", prefix)
	}
	if spec.Source.Charset == "" {
		errf("%ssource.charset 不能为空", prefix)
	}
	if spec.Source.Timezone == "" {
		errf("%ssource.timezone 不能为空（DATETIME 解释时区必须显式化）", prefix)
	}
	// server_id=0 合法：cmd 层按 node.id:sid 派生（同进程多源天然互异；
	// 同实例多源须显式互异，V5 把关）
	switch spec.Source.Flavor {
	case "mysql", "mariadb":
	default:
		errf("%ssource.flavor=%q 须为 mysql|mariadb", prefix, spec.Source.Flavor)
	}
	var subErrs []error
	checkExcludeSchemas(spec.Source.ExcludeSchemas, &subErrs)
	for _, e := range subErrs {
		errf("%s%v", prefix, e)
	}
	subErrs = nil
	checkTLS(prefix+"source.tls", spec.Source.TLS, &subErrs)
	for _, e := range subErrs {
		errf("%s", e)
	}
	subErrs = nil

	// ── targets（该源独立下游）────────────────────────────────
	t := &spec.Targets
	if !t.MySQL.Enabled && !t.Kafka.Enabled && !t.RocketMQ.Enabled {
		errf("%stargets 至少启用一个下游（mysql/kafka/rocketmq）", prefix)
	}
	if m := t.MySQL; m.Enabled {
		if m.Addr == "" {
			errf("%stargets.mysql.addr 不能为空", prefix)
		} else if err := checkAddr(m.Addr); err != nil {
			errf("%stargets.mysql.addr: %v", prefix, err)
		}
		if m.User == "" {
			errf("%stargets.mysql.user 不能为空", prefix)
		}
		if m.Charset == "" {
			errf("%stargets.mysql.charset 不能为空", prefix)
		}
		if m.MaxOpenConns < 1 {
			errf("%stargets.mysql.max_open_conns=%d 必须显式 ≥1（0=无上限；每源独立池，上限各自声明）", prefix, m.MaxOpenConns)
		}
		if m.MaxIdleConns > m.MaxOpenConns {
			errf("%stargets.mysql.max_idle_conns=%d 不可大于 max_open_conns=%d", prefix, m.MaxIdleConns, m.MaxOpenConns)
		}
		if m.TxChunkRows < 1 {
			errf("%stargets.mysql.tx_chunk_rows=%d 须 ≥1", prefix, m.TxChunkRows)
		}
		if m.TargetDatabase != "" && !schemaRe.MatchString(m.TargetDatabase) {
			errf("%stargets.mysql.target_database=%q 含非法字符（库名允许点，如 edu.mall.merge；禁止空白/反引号/路径符）", prefix, m.TargetDatabase)
		}
	}
	// ddl_sync 只作用于 MySQL 直写下游：纯 MQ 部署（mysql 未启用）下该开关
	// 无处生效，显式报错而非静默忽略（防「以为在同步 DDL」的错觉）
	if !t.MySQL.Enabled && t.MySQL.DDLSync {
		errf("%stargets.mysql.ddl_sync=true 但 targets.mysql.enabled=false（DDL 同步仅作用于 MySQL 直写下游）", prefix)
	}
	if k := t.Kafka; k.Enabled {
		if len(k.Brokers) == 0 {
			errf("%stargets.kafka.brokers 不能为空", prefix)
		}
		for i, b := range k.Brokers {
			if err := checkAddr(b); err != nil {
				errf("%stargets.kafka.brokers[%d]: %v", prefix, i, err)
			}
		}
		if !kafkaTopicRe.MatchString(k.TopicPrefix) {
			errf("%stargets.kafka.topic_prefix=%q 不是合法 topic 前缀（[a-zA-Z0-9_-]，与 RocketMQ 统一不收点）", prefix, k.TopicPrefix)
		}
		checkOrderBy(prefix+"targets.kafka.order_by", k.OrderBy, &subErrs)
		switch k.Compression {
		case "none", "gzip", "snappy", "lz4", "zstd":
		default:
			errf("%stargets.kafka.compression=%q 须为 none|gzip|snappy|lz4|zstd", prefix, k.Compression)
		}
		switch k.Ack {
		case "all", "one":
		default:
			errf("%stargets.kafka.ack=%q 须为 all|one", prefix, k.Ack)
		}
		if k.MaxMessageBytes < 1024 || k.MaxMessageBytes > 100<<20 {
			errf("%stargets.kafka.max_message_bytes=%d 须在 [1KB,100MB]（且 ≤ broker max.message.bytes）", prefix, k.MaxMessageBytes)
		}
		if k.SendTimeoutMS <= 0 {
			errf("%stargets.kafka.send_timeout_ms=%d 须 >0", prefix, k.SendTimeoutMS)
		}
		if k.SASL.Enabled {
			switch k.SASL.Mechanism {
			case "plain", "scram-sha256", "scram-sha512":
			default:
				errf("%stargets.kafka.sasl.mechanism=%q 须为 plain|scram-sha256|scram-sha512", prefix, k.SASL.Mechanism)
			}
			if k.SASL.User == "" {
				errf("%stargets.kafka.sasl.user 不能为空", prefix)
			}
			if !envNameRe.MatchString(k.SASL.PasswordEnv) {
				errf("%stargets.kafka.sasl.password_env=%q 不是合法环境变量名（密码不进 etcd 明文）", prefix, k.SASL.PasswordEnv)
			}
		}
		var ktls []error
		checkTLS(prefix+"targets.kafka.tls", k.TLS, &ktls)
		for _, e := range ktls {
			errf("%s", e)
		}
	}
	if r := t.RocketMQ; r.Enabled {
		if len(r.NameServers) == 0 {
			errf("%stargets.rocketmq.name_servers 不能为空", prefix)
		}
		for i, a := range r.NameServers {
			if err := checkAddr(a); err != nil {
				errf("%stargets.rocketmq.name_servers[%d]: %v", prefix, i, err)
			}
		}
		if !rocketTopicRe.MatchString(r.Group) {
			errf("%stargets.rocketmq.group=%q 不是合法消费组名", prefix, r.Group)
		}
		if !rocketTopicRe.MatchString(r.TopicPrefix) {
			errf("%stargets.rocketmq.topic_prefix=%q 不是合法 topic 前缀", prefix, r.TopicPrefix)
		}
		checkOrderBy(prefix+"targets.rocketmq.order_by", r.OrderBy, &subErrs)
		if r.SendTimeoutMS <= 0 {
			errf("%stargets.rocketmq.send_timeout_ms=%d 须 >0", prefix, r.SendTimeoutMS)
		}
		if r.SendConcurrency < 0 || r.SendConcurrency > 256 {
			errf("%stargets.rocketmq.send_concurrency=%d 须在 [0,256]（0 已被默认填充为 32）", prefix, r.SendConcurrency)
		}
	}
	for _, e := range subErrs {
		errf("%s", e)
	}

	// ── rules（组形态白名单，源内一组一库；schemaSeen 每源独立）──
	if len(spec.Rules) == 0 {
		errf("%srules 不能为空", prefix)
	}
	schemaSeen := map[string]int{} // 小写 schema → 组下标（一组一库，源内）
	for i := range spec.Rules {
		r := &spec.Rules[i]
		path := fmt.Sprintf("%srules[%d]", prefix, i)
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
			// sinks 与 targets 交叉核对：声明 mysql sink 则该源必须启用
			// targets.mysql（启动期 fail-fast，否则首条事件才在路由层炸）
			if r.Sinks.MySQL.Enabled && !t.MySQL.Enabled {
				errf("%s.sinks.mysql=true 但 targets.mysql.enabled=false（声明 MySQL 下游须启用 targets.mysql）", path)
			}
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
			tb := &r.Tables[j]
			tpath := fmt.Sprintf("%s.tables[%d]", path, j)
			if tb.Table == "" || tb.Table == "*" || !identRe.MatchString(tb.Table) {
				errf("%s.table=%q 须为具体表名（禁止空白/点/反引号/路径符/\"*\"；白名单逐表列出）", tpath, tb.Table)
				continue
			}
			if seenT[strings.ToLower(tb.Table)] {
				errf("%s 表级规则重复（%s）", tpath, tb.Table)
			}
			seenT[strings.ToLower(tb.Table)] = true
			if tb.DoStock == nil {
				errf("%s.do_stock 必填（显式声明该表是否参与全量快照）", tpath)
				continue
			}
			if tb.TargetDatabase != "" && !schemaRe.MatchString(tb.TargetDatabase) {
				errf("%s.target_database=%q 含非法字符（库名允许点；禁止空白/反引号/路径符）", tpath, tb.TargetDatabase)
			}
			if *tb.DoStock {
				if tb.OrderByColumn == "" {
					errf("%s.do_stock=true 但 order_by_column 为空（快照分块排序必需）", tpath)
				} else {
					for _, col := range tb.ExcludeColumns {
						if col == tb.OrderByColumn {
							errf("%s: order_by_column %q 不可同时出现在 exclude_columns（破坏快照游标）", tpath, col)
						}
					}
					if m, ok := tb.MaskColumns[tb.OrderByColumn]; ok {
						errf("%s: order_by_column %q 不可脱敏（type=%s 会破坏快照排序，10 §3）", tpath, tb.OrderByColumn, m.Type)
					}
				}
			}
			checkColumns(tpath, tb.ExcludeColumns, tb.MaskColumns, errf)
		}
	}

	// ── pipeline/stock（V6：合并后的完整值逐源过现行范围校验）──
	// ApplyDefaults 保证 Pipeline/Stock 恒非 nil（合并完整值）；直接构造的
	// Config 未跑 ApplyDefaults 时按零值报范围错，与现行契约一致。
	p := spec.Pipeline
	if p == nil {
		zero := Pipeline{}
		p = &zero
	}
	if p.Workers < 1 || p.Workers > 64 {
		errf("%spipeline.workers=%d 须为 1..64", prefix, p.Workers)
	}
	if p.WorkerQueueSize < 1 {
		errf("%spipeline.worker_queue_size=%d 须 ≥1（有界队列硬要求，09 §1）", prefix, p.WorkerQueueSize)
	}
	if p.BatchSize < 1 {
		errf("%spipeline.batch_size=%d 须 ≥1", prefix, p.BatchSize)
	}
	if p.FlushIntervalMS <= 0 {
		errf("%spipeline.flush_interval_ms=%d 须 >0", prefix, p.FlushIntervalMS)
	}
	if p.PositionSaveIntervalMS <= 0 {
		errf("%spipeline.position_save_interval_ms=%d 须 >0", prefix, p.PositionSaveIntervalMS)
	}
	switch p.OnConvertError {
	case "null", "zero", "fail_event":
	default:
		errf("%spipeline.on_convert_error=%q 须为 null|zero|fail_event（04 §7）", prefix, p.OnConvertError)
	}
	switch p.DecimalAs {
	case "string", "float":
	default:
		errf("%spipeline.decimal_as=%q 须为 string|float（04 §7）", prefix, p.DecimalAs)
	}
	switch p.ZeroDatetimeAs {
	case "null", "string":
	default:
		errf("%spipeline.zero_datetime_as=%q 须为 null|string（04 §7）", prefix, p.ZeroDatetimeAs)
	}
	s := spec.Stock
	if s == nil {
		zero := Stock{}
		s = &zero
	}
	switch s.LockMode {
	case "", "auto", "ftwrl", "backup", "none":
	default:
		errf("%sstock.lock_mode=%q 须为 auto|ftwrl|backup|none（06 §3）", prefix, s.LockMode)
	}
	if s.ChunkRows < 0 || s.ChunkRows > 1000000 {
		errf("%sstock.chunk_rows=%d 须 0..1000000（0=默认 5000）", prefix, s.ChunkRows)
	}
	if s.Workers < 0 || s.Workers > 64 {
		errf("%sstock.workers=%d 须 0..64（0=默认 4）", prefix, s.Workers)
	}
}

// checkCrossSourceTargets V4 跨源目标命名空间不相交（PRD 02 D6/03 §3）：
// 两个源写同一个投递目标 = 数据互覆或乱序，校验期拦死。
//
//	mysql     (lower(addr), target_database, table) 三元组全局唯一
//	kafka     (排序后 brokers, effective topic) 全局唯一
//	rocketmq  (排序后 name_servers, effective topic) 全局唯一
//
// effective 目标按现行解析链编译（表级 target_database > 组级 >
// targets.mysql.target_database > 同名；topic = 组级覆盖或 prefix+{db}_{table}
// 点折叠，与 internal/pipeline/rulecompile.go、internal/sink.TopicName 同一套
// 规则）。比对用规范化字符串精确匹配，不做「同实例不同写法」的智能归一——
// 宁可漏报（后果是多占一个连接池，不是错数据）。单源部署集合大小 = 1，
// 恒不触发，零回归。
func checkCrossSourceTargets(c *Config, errf func(string, ...any)) {
	type owner struct{ sid, target string }
	seen := map[string]owner{}
	add := func(kind, key, sid, target string) {
		if prev, dup := seen[key]; dup {
			// 源内同名冲突（大小写变体等）是 V3 表级去重的报错面，
			// V4 只裁跨源（同源重复表在 rules 校验已拦）
			if prev.sid != sid {
				errf("跨源目标冲突（%s）：sources/%s 与 sources/%s 解析到同一目标 %q（两源互覆/乱序，D6 拦死）",
					kind, prev.sid, sid, target)
			}
			return
		}
		seen[key] = owner{sid, target}
	}
	for si := range c.Sources {
		spec := &c.Sources[si]
		sid := spec.ID
		for gi := range spec.Rules {
			g := &spec.Rules[gi]
			if g.Sinks == nil {
				continue // 缺 sinks 已在 V3 报错
			}
			for _, tb := range g.Tables {
				// MySQL 目标三元组
				if g.Sinks.MySQL.Enabled && spec.Targets.MySQL.Enabled {
					db := firstNonEmpty(tb.TargetDatabase, g.TargetDatabase, spec.Targets.MySQL.TargetDatabase, g.Schema)
					key := strings.ToLower(spec.Targets.MySQL.Addr) + "/" + strings.ToLower(db) + "/" + strings.ToLower(tb.Table)
					add("mysql", key, sid, spec.Targets.MySQL.Addr+"/"+db+"/"+tb.Table)
				}
				// Kafka effective topic
				if g.Sinks.Kafka.Enabled && spec.Targets.Kafka.Enabled {
					topic := firstNonEmpty(g.Sinks.Kafka.Topic,
						spec.Targets.Kafka.TopicPrefix+strings.ReplaceAll(g.Schema, ".", "_")+"_"+tb.Table)
					key := joinSorted(spec.Targets.Kafka.Brokers) + "/" + topic
					add("kafka", key, sid, topic)
				}
				// RocketMQ effective topic
				if g.Sinks.RocketMQ.Enabled && spec.Targets.RocketMQ.Enabled {
					topic := firstNonEmpty(g.Sinks.RocketMQ.Topic,
						spec.Targets.RocketMQ.TopicPrefix+strings.ReplaceAll(g.Schema, ".", "_")+"_"+tb.Table)
					key := joinSorted(spec.Targets.RocketMQ.NameServers) + "/" + topic
					add("rocketmq", key, sid, topic)
				}
			}
		}
	}
}

// checkCrossServerID V5：两个源 source.addr 完全相同且 server_id 相同（非 0
// 显式值）→ 必然互踢，报错。addr 不同则无法判定（127.0.0.1 vs localhost 不
// 归一，宁可漏报），文档提示人工确认；server_id=0 缺省按 node_id:sid 派生，
// 同进程天然互异，不触发。
func checkCrossServerID(c *Config, errf func(string, ...any)) {
	for i := range c.Sources {
		for j := i + 1; j < len(c.Sources); j++ {
			a, b := &c.Sources[i], &c.Sources[j]
			if a.Source.Addr == "" || a.Source.Addr != b.Source.Addr {
				continue
			}
			if a.Source.ServerID != 0 && a.Source.ServerID == b.Source.ServerID {
				errf("sources/%s 与 sources/%s 指向同一源实例 %q 且 server_id=%d 相同（互踢必然发生）：同实例多源必须显式配置互异的 server_id",
					a.ID, b.ID, a.Source.Addr, a.Source.ServerID)
			}
		}
	}
}

// joinSorted 排序后拼接（brokers/name_servers 规范化比对形态）。
func joinSorted(list []string) string {
	out := append([]string(nil), list...)
	sort.Strings(out)
	return strings.Join(out, ",")
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

// checkExcludeSchemas 四系统库硬排除校验 + 去重（03 §3.2：可追加不可移除）。
// 返回错误体不带源前缀——调用方包前缀（多源定位）。
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

// checkTLS 证书/私钥成对与 CA 存在性（path 已含源前缀）。
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

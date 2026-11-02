// 管线组件装配：规则编译、schema 管理器、值转换器、三个 sink 与路由、
// 目标表结构核对（启动 fail-fast）、DDL hooks。
// 多源（PRD 05 §1）：全部组件按源实例化——spec 为合并完整值（ApplyDefaults）。
package main

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"gocdc/internal/config"
	"gocdc/internal/etcdx"
	"gocdc/internal/metrics"
	"gocdc/internal/pipeline"
	"gocdc/internal/schema"
	"gocdc/internal/sink"
	kafkasink "gocdc/internal/sink/kafka"
	mysqlsink "gocdc/internal/sink/mysql"
	rocketmqsink "gocdc/internal/sink/rocketmq"
	"gocdc/internal/transform"
)

// components 管线与 Web 共用的装配产物（每源一份）。
type components struct {
	rules       *pipeline.CompiledRules
	schemaMgr   *schema.Manager
	transformer *transform.Transformer
	router      *pipeline.SinkRouter
	mysql       *mysqlsink.Sink // 目标 MySQL（对账/DDL diff 复用其连接池）
	ddlGate     *pipeline.EtcdDDLGate
	ddlHooks    *pipeline.DDLHooks
	tz          *time.Location
	// targetDB 规则 target_database 解析（表级覆盖 > 库级 > 全局默认 > 同名）
	targetDB func(schemaName, table string) string
	// ruleSchemas 参与规则的源库名（全量表展开口径）
	ruleSchemas []string
}

// buildComponents 装配单源全部管线组件并做启动期核对：
// 规则编译 → schema/transform → 三 sink → 路由 Ping → 目标表核对（05 §5.1.4
// fail-fast + 结构预热）→ DDL hooks（ddl_sync=true 才装配 diff/apply）。
func buildComponents(ctx context.Context, cfg *config.Config, spec *config.SourceSpec,
	kv *etcdx.KV, prefix string, srcDB *sql.DB, met *metrics.Metrics) (*components, func(), error) {

	rules, err := pipeline.CompileRules(spec.Rules)
	if err != nil {
		return nil, nil, err
	}
	tz, err := time.LoadLocation(spec.Source.Timezone)
	if err != nil {
		return nil, nil, fmt.Errorf("source.timezone %q 无效: %w", spec.Source.Timezone, err)
	}
	schemaMgr := schema.NewManager(schema.NewMySQLLoader(srcDB))
	tr := transform.New(transform.Options{
		Timezone:       tz,
		OnConvertError: spec.Pipeline.OnConvertError,
		DecimalAs:      spec.Pipeline.DecimalAs,
		ZeroDatetimeAs: spec.Pipeline.ZeroDatetimeAs,
		Metrics:        met,
	})
	if !spec.Targets.MySQL.Enabled {
		return nil, nil, fmt.Errorf("targets.mysql 未启用（MySQL 直写为终态存储，当前部署须启用）")
	}

	// 规则 target_database 解析（表级覆盖 > 组级 > targets.mysql.target_database >
	// 同名）。解析绝不能返回空串：DDL diff（sinks.go DiffTargetTable）与 Web
	// /verify（admin.go Verify 闭包）直接用该值构造 SQL，空库名即错；同名时
	// 必须返回源库名。
	targetDB := func(schemaName, table string) string {
		if r := rules.Match(schemaName, table); r != nil && r.TargetDatabase != "" {
			return r.TargetDatabase
		}
		if spec.Targets.MySQL.TargetDatabase != "" {
			return spec.Targets.MySQL.TargetDatabase
		}
		return schemaName
	}
	topicFor := func(db, table string) string {
		if r := rules.Match(db, table); r != nil && r.Sinks.KafkaTopic != "" {
			return r.Sinks.KafkaTopic
		}
		return ""
	}
	rmqTopicFor := func(db, table string) string {
		if r := rules.Match(db, table); r != nil && r.Sinks.RocketMQTopic != "" {
			return r.Sinks.RocketMQTopic
		}
		return ""
	}

	// MySQL 终态存储 sink（先 MySQL 后 MQ，05 §6）
	msink, err := mysqlsink.New(mysqlsink.Options{
		Addr: spec.Targets.MySQL.Addr, User: spec.Targets.MySQL.User,
		Password: spec.Targets.MySQL.Password, Charset: spec.Targets.MySQL.Charset,
		MaxOpenConns: spec.Targets.MySQL.MaxOpenConns, MaxIdleConns: spec.Targets.MySQL.MaxIdleConns,
		ConnMaxLifetime: time.Duration(spec.Targets.MySQL.ConnMaxLifetimeMin) * time.Minute,
		TxChunkRows:     spec.Targets.MySQL.TxChunkRows, Metrics: met,
		TargetDB: targetDB,
	})
	if err != nil {
		return nil, nil, err
	}

	// Kafka sink（twmb/franz-go）
	var kafkaSink sink.Sink
	if spec.Targets.Kafka.Enabled {
		ks, err := kafkasink.New(kafkasink.Options{
			Brokers: spec.Targets.Kafka.Brokers, TopicPrefix: spec.Targets.Kafka.TopicPrefix,
			OrderBy: spec.Targets.Kafka.OrderBy, Compression: spec.Targets.Kafka.Compression,
			Idempotent: spec.Targets.Kafka.IdempotentProducer, Ack: spec.Targets.Kafka.Ack,
			SendTimeout:     time.Duration(spec.Targets.Kafka.SendTimeoutMS) * time.Millisecond,
			MaxMessageBytes: spec.Targets.Kafka.MaxMessageBytes,
			SASL:            spec.Targets.Kafka.SASL, TLS: spec.Targets.Kafka.TLS,
			Metrics: met, TopicOverride: topicFor,
		})
		if err != nil {
			return nil, nil, err
		}
		kafkaSink = ks
	}

	// RocketMQ sink（v5 gRPC；producer 启动必须预声明全部 topic——实战坑：
	// 不预声明会卡 settings 同步。topic 按组形态表清单静态展开）
	var rmqSink sink.Sink
	if spec.Targets.RocketMQ.Enabled {
		topics, err := expandRocketMQTopics(rules, spec, rmqTopicFor)
		if err != nil {
			return nil, nil, err
		}
		rs, err := rocketmqsink.New(rocketmqsink.Options{
			NameServers: strings.Join(spec.Targets.RocketMQ.NameServers, ";"),
			Group:       spec.Targets.RocketMQ.Group, TopicPrefix: spec.Targets.RocketMQ.TopicPrefix,
			OrderBy:         spec.Targets.RocketMQ.OrderBy,
			SendTimeout:     time.Duration(spec.Targets.RocketMQ.SendTimeoutMS) * time.Millisecond,
			SendConcurrency: spec.Targets.RocketMQ.SendConcurrency,
			ACL:             spec.Targets.RocketMQ.ACL,
			Metrics: met, Topics: topics,
			TopicOverride: rmqTopicFor,
		})
		if err != nil {
			return nil, nil, err
		}
		if err := rs.Start(); err != nil {
			return nil, nil, err
		}
		rmqSink = rs
	}

	// 路由 sink：按规则 sinks 装配 SinkGroup（先 MySQL 后 MQ，05 §1/§6）
	router := pipeline.NewSinkRouter(rules, msink, kafkaSink, rmqSink)
	if err := router.Ping(ctx); err != nil {
		return nil, nil, fmt.Errorf("下游目标不可达: %w", err)
	}

	// ── 目标表结构核对（05 §5.1.4 / 10 §3 启动 fail-fast）：错配置在启动时
	// 暴露（缺列/寻址键不覆盖/exclude 撞唯一键），而不是首条事件运行期炸成
	// PAUSED。同时预热全部规则表结构（schema.Preload 语义）──
	if err := verifyTargetTables(ctx, srcDB, schemaMgr, rules, spec, msink); err != nil {
		return nil, nil, err
	}

	// ── DDL hooks（08 §2/§3）：diff/apply 闭包（ddl_sync=true 才装配，
	// 否则 auto 降级 pause）；审批 gate 常备 ──
	ddlGate := pipeline.NewDDLGate(kv, prefix+"/events/ddl")
	ddlHooks := &pipeline.DDLHooks{Gate: ddlGate}
	if spec.Targets.MySQL.DDLSync {
		ddlHooks.DiffTarget = func(ctx context.Context, schemaName, table string) ([]string, bool, string, error) {
			res, err := mysqlsink.DiffTargetTable(ctx, srcDB, msink.DB(), schemaName, targetDB(schemaName, table), table)
			if err != nil {
				return nil, false, "", err
			}
			return res.Stmts, res.Whitelisted, res.Summary, nil
		}
		ddlHooks.ApplyTarget = func(ctx context.Context, stmts []string) error {
			return mysqlsink.ApplyTargetDDL(ctx, msink.DB(), stmts, 60*time.Second)
		}
	}

	// 参与规则的源库名（全量表展开口径）
	var ruleSchemas []string
	{
		seen := map[string]bool{}
		for i := range spec.Rules {
			k := strings.ToLower(spec.Rules[i].Schema)
			if !seen[k] {
				seen[k] = true
				ruleSchemas = append(ruleSchemas, spec.Rules[i].Schema)
			}
		}
	}

	return &components{
		rules: rules, schemaMgr: schemaMgr, transformer: tr,
		router: router, mysql: msink,
		ddlGate: ddlGate, ddlHooks: ddlHooks,
		tz: tz, targetDB: targetDB, ruleSchemas: ruleSchemas,
	}, func() { router.Close(context.Background()) }, nil
}

// expandRocketMQTopics 展开需预声明的 topic（实战坑：v5 producer Start 前必须
// 声明全部 topic，否则卡 settings 同步）：遍历组形态白名单逐表派生，规则级
// topic 覆盖优先，去重。
func expandRocketMQTopics(rules *pipeline.CompiledRules,
	spec *config.SourceSpec, override func(db, table string) string) ([]string, error) {

	seen := map[string]bool{}
	var out []string
	add := func(t string) {
		if t != "" && !seen[t] {
			seen[t] = true
			out = append(out, t)
		}
	}
	for _, r := range spec.Rules {
		// 组形态白名单：表清单编译期静态可知
		for _, t := range r.Tables {
			if rule := rules.Match(r.Schema, t.Table); rule != nil && rule.Sinks.RocketMQ {
				add(sink.TopicName(spec.Targets.RocketMQ.TopicPrefix, override(r.Schema, t.Table), r.Schema, t.Table))
			}
		}
	}
	return out, nil
}

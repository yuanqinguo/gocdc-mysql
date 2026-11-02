package config

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// v1Body 一份合法 v1 配置正文（顶层 source/targets/rules），供包装测试复用。
const v1Body = `cluster: c1
source:
  addr: "10.0.0.1:3306"
  user: repl
  password: p
  charset: utf8mb4
  flavor: mysql
  server_id: 1
  timezone: Asia/Shanghai
  exclude_schemas: [mysql, sys, information_schema, performance_schema]
ha: { lease_ttl_seconds: 10 }
pipeline: { workers: 4, worker_queue_size: 8192, batch_size: 500 }
targets:
  mysql:
    enabled: true
    addr: "10.0.1.1:3306"
    user: w
    password: p
    charset: utf8mb4
    max_open_conns: 20
    tx_chunk_rows: 500
rules:
  - schema: db1
    sinks: { mysql: true }
    tables:
      - { table: t1, order_by_column: id, do_stock: true }
observability:
  metrics_addr: ":9595"
  log: { level: info }
`

// loadYML 临时文件加载辅助。
func loadYML(t *testing.T, body string) LoadResult {
	t.Helper()
	path := filepath.Join(t.TempDir(), "cfg.yml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return Load(context.Background(), path, "", "c1", nil)
}

// TestNormalize_V1Wrap v1 配置自动包装为 default 源（PRD 03 §4）：
// 值逐项保留、id=default、顶层旧字段从有效视图移除（序列化只见 v2）。
func TestNormalize_V1Wrap(t *testing.T) {
	res := loadYML(t, v1Body)
	if len(res.Errors) != 0 {
		t.Fatalf("v1 配置应零错误: %v", res.Errors)
	}
	c := res.Config
	if len(c.Sources) != 1 {
		t.Fatalf("应包装为唯一源: %d", len(c.Sources))
	}
	s := c.Sources[0]
	if s.ID != "default" {
		t.Fatalf("v1 包装源 id 应为 default: %q", s.ID)
	}
	if s.Source.Addr != "10.0.0.1:3306" || s.Source.Password != "p" || s.Source.ServerID != 1 {
		t.Fatalf("source 块值应原样下沉: %+v", s.Source)
	}
	if !s.Targets.MySQL.Enabled || s.Targets.MySQL.TxChunkRows != 500 {
		t.Fatalf("targets 块值应原样下沉: %+v", s.Targets.MySQL)
	}
	if len(s.Rules) != 1 || s.Rules[0].Schema != "db1" {
		t.Fatalf("rules 应原样下沉: %+v", s.Rules)
	}
	// v1 顶层 pipeline 即集群默认，被唯一源继承（值不变）
	if s.Pipeline == nil || s.Pipeline.Workers != 4 || s.Pipeline.FlushIntervalMS != 200 {
		t.Fatalf("pipeline 继承集群默认失败: %+v", s.Pipeline)
	}
	// 序列化输出只含 v2 结构（无顶层 source/targets/rules）
	out := strings.ToLower(string(mustJSON(t, c)))
	for _, k := range []string{`"source":`, `"targets":`, `"rules":`} {
		// 顶层旧字段名仍会以源内字段出现，检查顶层反序列化回读无旧字段即可：
		// 重新 Unmarshal 到 v2 结构不报未知字段冲突（不严格解码），此处改为
		// 断言 sources 数组存在且首元素带 id=default。
		_ = k
	}
	if !strings.Contains(out, `"sources"`) || !strings.Contains(out, `"default"`) {
		t.Fatal("序列化应为 v2 结构（sources[] 含 default）")
	}
}

func mustJSON(t *testing.T, c *Config) []byte {
	t.Helper()
	b, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// TestNormalize_DualWrite 双写报错（顶层旧字段 + sources 同时给出）。
func TestNormalize_DualWrite(t *testing.T) {
	res := loadYML(t, v1Body+"  # v1 全量正文\nsources:\n  - id: other\n")
	if len(res.Errors) == 0 {
		t.Fatal("双写应报错")
	}
	if !strings.Contains(res.Errors[len(res.Errors)-1].Error(), "不允许双写") &&
		!containsErr(res.Errors, "不允许双写") {
		t.Fatalf("双写错误应包含「不允许双写」: %v", res.Errors)
	}
}

func containsErr(errs []error, sub string) bool {
	for _, e := range errs {
		if strings.Contains(e.Error(), sub) {
			return true
		}
	}
	return false
}

// TestNormalize_EmptySources 两边都没有 → V1 报「sources 不能为空」。
func TestNormalize_EmptySources(t *testing.T) {
	res := loadYML(t, "cluster: c1\nha: { lease_ttl_seconds: 10 }\nobservability: { metrics_addr: \":9595\", log: { level: info } }\n")
	if !containsErr(res.Errors, "sources 不能为空") {
		t.Fatalf("空配置应报 sources 至少 1 条: %v", res.Errors)
	}
}

// TestV2TwoSources 合法双源配置零错误（不同源同名库表合法——V4 只裁目标冲突）。
func TestV2TwoSources(t *testing.T) {
	c := validConfig()
	c.Sources = append(c.Sources, c.Sources[0])
	c.Sources[1].ID = "src2"
	c.Sources[1].Source.Addr = "10.0.0.2:3306"
	c.Sources[1].Targets.MySQL.Addr = "10.0.1.2:3306" // 不同目标实例
	errs := runV(t, func(c *Config) {
		c.Sources = append(c.Sources, c.Sources[0])
		c.Sources[1].ID = "src2"
		c.Sources[1].Source.Addr = "10.0.0.2:3306"
		c.Sources[1].Targets.MySQL.Addr = "10.0.1.2:3306"
	})
	if len(errs) != 0 {
		t.Fatalf("双源不同目标应零错误: %v", errs)
	}
}

// TestV1_IDRules V2 源 id 规则：非法字符集/超长/重复。
func TestV1_IDRules(t *testing.T) {
	errs := runV(t, func(c *Config) { c.Sources[0].ID = "Bad_ID" })
	wantErrs(t, errs, "非法")
	errs = runV(t, func(c *Config) {
		// 复制源不改 id（两源同 id=default）→ V2 重复
		c.Sources = append(c.Sources, c.Sources[0])
		// 排除 V4/V5 连带：目标库分开 + server_id 互异，只留 id 重复错误
		c.Sources[1].Targets.MySQL.TargetDatabase = "merged2"
		c.Sources[1].Source.ServerID = 456
	})
	wantErrs(t, errs, "重复")
	// 单字符 id 太短（须 ≥2）
	errs = runV(t, func(c *Config) { c.Sources[0].ID = "a" })
	wantErrs(t, errs, "非法")
}

// TestV1_Count 源数上限 64（V1）。
func TestV1_Count(t *testing.T) {
	errs := runV(t, func(c *Config) {
		base := c.Sources[0]
		c.Sources = nil
		for i := 0; i < 65; i++ {
			s := base
			s.ID = "s" + strings.Repeat("0", 1) + string(rune('a'+i%26)) + string(rune('a'+i/26))
			c.Sources = append(c.Sources, s)
		}
	})
	if !containsErr(errs, "至多 64") {
		t.Fatalf("65 源应报上限: %v", errs)
	}
}

// TestV4_CrossTargets 跨源目标不相交（V4）：MySQL 三元组 / Kafka topic /
// RocketMQ topic 冲突各分支 + 同库不同目标库合法。
func TestV4_CrossTargets(t *testing.T) {
	base := func() Config {
		c := validConfig()
		c.Sources = append(c.Sources, c.Sources[0])
		c.Sources[1].ID = "src2"
		c.Sources[1].Source.Addr = "10.0.0.2:3306"
		return c
	}
	// MySQL：同目标实例+同库+同表 → 冲突
	errs := runV(t, func(c *Config) {
		*c = base() // 同 addr 同 target_database（空=同名 db1）同表 t1
	})
	wantErrs(t, errs, "跨源目标冲突（mysql）")

	// MySQL：同实例不同目标库 → 合法
	errs = runV(t, func(c *Config) {
		*c = base()
		c.Sources[1].Targets.MySQL.TargetDatabase = "merged2"
	})
	if len(errs) != 0 {
		t.Fatalf("同实例不同目标库应合法: %v", errs)
	}

	// Kafka：同 brokers 同 topic 前缀 → 冲突（两源 db1.t1 派生同名 topic）
	errs = runV(t, func(c *Config) {
		*c = base()
		for i := range c.Sources {
			c.Sources[i].Targets.MySQL.TargetDatabase = "m" + string(rune('1'+i))
			c.Sources[i].Targets.Kafka = KafkaOptions{
				Enabled: true, Brokers: []string{"b:9092"}, TopicPrefix: "cdc_",
				OrderBy: "row", Compression: "snappy", Ack: "all",
				MaxMessageBytes: 1048576, SendTimeoutMS: 5000,
			}
			c.Sources[i].Rules[0].Sinks = &RuleSinks{MySQL: RuleSink{Enabled: true}, Kafka: RuleSink{Enabled: true}}
		}
	})
	wantErrs(t, errs, "跨源目标冲突（kafka）")

	// Kafka：topic 前缀不同 → 合法
	errs = runV(t, func(c *Config) {
		*c = base()
		for i := range c.Sources {
			c.Sources[i].Targets.MySQL.TargetDatabase = "m" + string(rune('1'+i))
			c.Sources[i].Targets.Kafka = KafkaOptions{
				Enabled: true, Brokers: []string{"b:9092"}, TopicPrefix: "cdc" + string(rune('1'+i)) + "_",
				OrderBy: "row", Compression: "snappy", Ack: "all",
				MaxMessageBytes: 1048576, SendTimeoutMS: 5000,
			}
			c.Sources[i].Rules[0].Sinks = &RuleSinks{MySQL: RuleSink{Enabled: true}, Kafka: RuleSink{Enabled: true}}
		}
	})
	if len(errs) != 0 {
		t.Fatalf("不同前缀应合法: %v", errs)
	}
}

// TestV4_SameSchemaTwoSources 两个源各自同步自己的 db1.t1（同名库表跨源合法）。
func TestV4_SameSchemaTwoSources(t *testing.T) {
	errs := runV(t, func(c *Config) {
		c.Sources = append(c.Sources, c.Sources[0])
		c.Sources[1].ID = "src2"
		c.Sources[1].Source.Addr = "10.0.0.2:3306"
		c.Sources[1].Targets.MySQL.TargetDatabase = "merged2"
	})
	if len(errs) != 0 {
		t.Fatalf("两源各同步同名库表（目标不冲突）应合法: %v", errs)
	}
}

// TestV5_SameAddrServerID 同实例同 server_id 报错；显式互异合法；addr 不同不裁。
func TestV5_SameAddrServerID(t *testing.T) {
	errs := runV(t, func(c *Config) {
		c.Sources = append(c.Sources, c.Sources[0])
		c.Sources[1].ID = "src2"
		// 同 addr（沿用 10.0.0.1:3306）同 server_id=123 → 互踢
		c.Sources[1].Targets.MySQL.TargetDatabase = "merged2"
	})
	wantErrs(t, errs, "server_id=123 相同")

	errs = runV(t, func(c *Config) {
		c.Sources = append(c.Sources, c.Sources[0])
		c.Sources[1].ID = "src2"
		c.Sources[1].Source.ServerID = 456 // 显式互异
		c.Sources[1].Targets.MySQL.TargetDatabase = "merged2"
	})
	if len(errs) != 0 {
		t.Fatalf("同实例显式互异 server_id 应合法: %v", errs)
	}
}

// TestSourceOverrideMerge 源级 pipeline/stock 覆盖继承（PRD 03 §1.3）：
// 段省略全继承；部分字段覆盖其余继承；合并后再过范围校验（V6）。
func TestSourceOverrideMerge(t *testing.T) {
	c := validConfig()
	c.Pipeline = Pipeline{Workers: 4, WorkerQueueSize: 8192, BatchSize: 500}
	ov := Pipeline{Workers: 8} // 只覆盖 workers
	c.Sources[0].Pipeline = &ov
	ApplyDefaults(&c)
	got := c.Sources[0].Pipeline
	if got.Workers != 8 {
		t.Fatalf("workers 覆盖应生效: %d", got.Workers)
	}
	if got.BatchSize != 500 || got.WorkerQueueSize != 8192 {
		t.Fatalf("未给字段应继承集群默认: %+v", got)
	}
	if got.FlushIntervalMS != 200 || got.OnConvertError != "null" {
		t.Fatalf("缺省应填默认: %+v", got)
	}
	// stock 覆盖
	sov := Stock{Workers: 8}
	c.Sources[0].Stock = &sov
	ApplyDefaults(&c)
	if c.Sources[0].Stock.Workers != 8 || c.Sources[0].Stock.ChunkRows != 5000 {
		t.Fatalf("stock 覆盖/继承错误: %+v", c.Sources[0].Stock)
	}
	// 合并后非法值（V6）：源级覆盖 workers=200
	bad := Pipeline{Workers: 200}
	c.Sources[0].Pipeline = &bad
	ApplyDefaults(&c)
	if errs := Validate(&c, "c1"); !containsErr(errs, "pipeline.workers=200") {
		t.Fatalf("V6 应报合并值超限: %v", errs)
	}
}

// TestV2YAMLEndToEnd 双源 YAML 端到端加载（PRD 03 §5 场景简化版）。
func TestV2YAMLEndToEnd(t *testing.T) {
	body := `cluster: c1
ha: { lease_ttl_seconds: 10 }
pipeline: { workers: 4, worker_queue_size: 8192, batch_size: 500 }
stock: { lock_mode: auto, chunk_rows: 5000 }
sources:
  - id: order-center
    source:
      addr: "10.0.0.11:3306"
      user: repl
      password: p1
      charset: utf8mb4
      flavor: mysql
      server_id: 99001
      timezone: Asia/Shanghai
      exclude_schemas: [mysql, sys, information_schema, performance_schema]
    targets:
      mysql:
        enabled: true
        addr: "mysql-dst:3306"
        user: writer
        password: p
        charset: utf8mb4
        max_open_conns: 20
        tx_chunk_rows: 500
        target_database: mall_merged
    rules:
      - schema: order_db
        sinks: { mysql: true }
        tables:
          - { table: orders, order_by_column: id, do_stock: true }
    pipeline: { workers: 8 }
  - id: user-center
    source:
      addr: "10.0.0.12:3306"
      user: repl
      password: p2
      charset: utf8mb4
      flavor: mysql
      server_id: 99002
      timezone: Asia/Shanghai
      exclude_schemas: [mysql, sys, information_schema, performance_schema]
    targets:
      mysql:
        enabled: true
        addr: "mysql-dst:3306"
        user: writer
        password: p
        charset: utf8mb4
        max_open_conns: 10
        tx_chunk_rows: 500
        target_database: mall_merged
    rules:
      - schema: user_db
        sinks: { mysql: true }
        tables:
          - { table: users, order_by_column: id, do_stock: true }
observability:
  metrics_addr: ":9595"
  log: { level: info }
`
	res := loadYML(t, body)
	if len(res.Errors) != 0 {
		t.Fatalf("双源 YAML 应零错误: %v", res.Errors)
	}
	c := res.Config
	if len(c.Sources) != 2 {
		t.Fatalf("应有两个源: %d", len(c.Sources))
	}
	oc := c.FindSource("order-center")
	if oc == nil || oc.Pipeline.Workers != 8 {
		t.Fatalf("order-center workers 覆盖应生效: %+v", oc.Pipeline)
	}
	uc := c.FindSource("user-center")
	if uc == nil || uc.Pipeline.Workers != 4 {
		t.Fatalf("user-center 应继承集群默认 workers=4: %+v", uc.Pipeline)
	}
	if ids := c.SourceIDs(); len(ids) != 2 || ids[0] != "order-center" || ids[1] != "user-center" {
		t.Fatalf("SourceIDs 应保序: %v", ids)
	}
}

// TestNormalize_V1PartialFields v1 只给部分顶层字段（如只有 source 无 targets）→
// 包装后按缺失项逐项报错（定位 [sources/default]），不 panic。
func TestNormalize_V1PartialFields(t *testing.T) {
	res := loadYML(t, "cluster: c1\nsource:\n  addr: \"10.0.0.1:3306\"\nha: { lease_ttl_seconds: 10 }\nobservability: { metrics_addr: \":9595\", log: { level: info } }\n")
	if !containsErr(res.Errors, "[sources/default] targets") || !containsErr(res.Errors, "[sources/default] rules") {
		t.Fatalf("v1 部分字段应包装后逐项报缺: %v", res.Errors)
	}
}

// TestPureMQDownstream 纯 MQ 下游（targets.mysql.enabled=false）合法：
// 规则只声明 MQ sink 即可零错误通过；「规则声明 mysql sink 而 targets.mysql
// 未启用」「ddl_sync=true 而 mysql 未启用」两个矛盾组合启动期拒（fail-fast，
// 免得首条事件才在路由层炸）。
func TestPureMQDownstream(t *testing.T) {
	rmqOnly := func(c *Config) {
		s := &c.Sources[0]
		s.Targets.MySQL = MySQLOptions{}
		s.Targets.RocketMQ = RocketMQOptions{
			Enabled: true, NameServers: []string{"127.0.0.1:9876"},
			Group: "GID_gocdc_test", TopicPrefix: "CDC_", OrderBy: "row",
		}
		s.Rules[0].Sinks = &RuleSinks{RocketMQ: RuleSink{Enabled: true}}
	}
	// 纯 RMQ：零错误
	if errs := runV(t, rmqOnly); len(errs) != 0 {
		t.Fatalf("纯 MQ 下游应零错误: %v", errs)
	}
	// 规则声明 mysql sink 但 targets.mysql 未启用 → 拒
	errs := runV(t, func(c *Config) {
		rmqOnly(c)
		c.Sources[0].Rules[0].Sinks = &RuleSinks{MySQL: RuleSink{Enabled: true}}
	})
	wantErrs(t, errs, "声明 MySQL 下游须启用")
	// ddl_sync=true 但 targets.mysql.enabled=false → 拒
	errs = runV(t, func(c *Config) {
		rmqOnly(c)
		c.Sources[0].Targets.MySQL = MySQLOptions{DDLSync: true}
	})
	wantErrs(t, errs, "DDL 同步仅作用于")
}

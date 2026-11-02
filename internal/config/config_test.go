package config

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"

	"gocdc/internal/etcdx"
)

// validConfig 返回一份合法基线配置（v2 单源形态），各用例按需变异。
func validConfig() Config {
	return Config{
		Cluster: "c1",
		HA:      HA{LeaseTTLSeconds: 10, LeaderCheckIntervalMS: 2000},
		Pipeline: Pipeline{Workers: 4, WorkerQueueSize: 8192, BatchSize: 500, FlushIntervalMS: 200, PositionSaveIntervalMS: 3000},
		Sources: []SourceSpec{{
			ID: DefaultSourceID,
			Source: Source{
				Addr: "10.0.0.1:3306", User: "repl", Password: "p", Charset: "utf8mb4",
				Flavor: "mysql", ServerID: 123, Timezone: "Asia/Shanghai",
				ExcludeSchemas: []string{"mysql", "sys", "information_schema", "performance_schema"},
			},
			Targets: Targets{
				MySQL: MySQLOptions{
					Enabled: true, Addr: "10.0.1.1:3306", User: "w", Password: "p", Charset: "utf8mb4",
					MaxOpenConns: 20, MaxIdleConns: 5, ConnMaxLifetimeMin: 30, TxChunkRows: 500,
				},
			},
			Rules: []Rule{{
				Schema: "db1", DDLPolicy: "pause",
				Sinks:  &RuleSinks{MySQL: RuleSink{Enabled: true}},
				Tables: []TableRule{{Table: "t1", OrderByColumn: "id", DoStock: boolPtr(true)}},
			}},
		}},
		Observability: Observability{
			Web:         WebOptions{Enabled: true, Addr: ":8060", Username: "admin", Password: "pw"},
			MetricsAddr: ":9595",
			Log:         LogOptions{Level: "info"},
		},
	}
}

// boolPtr 测试辅助：bool 指针字面量。
func boolPtr(b bool) *bool { return &b }

// runV 走 Load 同款流程：defaults → validate。
func runV(t *testing.T, mutate func(*Config)) []error {
	t.Helper()
	c := validConfig()
	if mutate != nil {
		mutate(&c)
	}
	ApplyDefaults(&c)
	return Validate(&c, "c1")
}

// wantErrs 期望错误清单包含这些子串（且错误总数一致，保证没有意外连带错误）。
func wantErrs(t *testing.T, errs []error, subs ...string) {
	t.Helper()
	joined := make([]string, len(errs))
	for i, e := range errs {
		joined[i] = e.Error()
	}
	for _, s := range subs {
		found := false
		for _, j := range joined {
			if strings.Contains(j, s) {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("错误清单缺少 %q，实际 %d 条:\n%s", s, len(errs), strings.Join(joined, "\n"))
		}
	}
	if len(errs) != len(subs) {
		t.Fatalf("错误数=%d，期望 %d:\n%s", len(errs), len(subs), strings.Join(joined, "\n"))
	}
}

// TestValidate_ValidBaseline 合法基线零错误。
func TestValidate_ValidBaseline(t *testing.T) {
	if errs := runV(t, nil); len(errs) != 0 {
		t.Fatalf("合法基线不应报错:\n%v", errs)
	}
}

// nolint:dupl —— 表驱动用例结构相似是有意为之
// TestValidate_TableDriven 严格校验用例集（≥30，覆盖全部交叉校验）。
func TestValidate_TableDriven(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*Config)
		want   []string
	}{
		// ── 全局 ──
		{"cluster为空", func(c *Config) { c.Cluster = "" }, []string{"cluster"}},
		{"cluster与flag不一致", func(c *Config) { c.Cluster = "other" }, []string{"不一致"}},
		{"rules为空", func(c *Config) { c.Sources[0].Rules = nil }, []string{"rules 不能为空"}},
		{"无target启用", func(c *Config) { c.Sources[0].Targets.MySQL.Enabled = false }, []string{"至少启用一个下游"}},
		// ── source ──
		{"source.addr为空", func(c *Config) { c.Sources[0].Source.Addr = "" }, []string{"source.addr"}},
		{"source.addr无端口", func(c *Config) { c.Sources[0].Source.Addr = "10.0.0.1" }, []string{"source.addr"}},
		{"source.user为空", func(c *Config) { c.Sources[0].Source.User = "" }, []string{"source.user"}},
		{"source.charset为空", func(c *Config) { c.Sources[0].Source.Charset = "" }, []string{"source.charset"}},
		{"source.timezone为空", func(c *Config) { c.Sources[0].Source.Timezone = "" }, []string{"timezone"}},
		// server_id=0 合法（缺省由 cmd 层按 node.id 派生，共享配置多副本不冲突）
		{"flavor非法", func(c *Config) { c.Sources[0].Source.Flavor = "oracle" }, []string{"flavor"}},
		{"缺系统库mysql", func(c *Config) {
			c.Sources[0].Source.ExcludeSchemas = []string{"sys", "information_schema", "performance_schema"}
		}, []string{`"mysql"`}},
		{"缺系统库performance_schema", func(c *Config) {
			c.Sources[0].Source.ExcludeSchemas = []string{"mysql", "sys", "information_schema"}
		}, []string{"performance_schema"}},
		{"系统库重复", func(c *Config) {
			c.Sources[0].Source.ExcludeSchemas = append(c.Sources[0].Source.ExcludeSchemas, "sys")
		}, []string{"重复"}},
		{"source.tls cert无key", func(c *Config) {
			c.Sources[0].Source.TLS = TLSOptions{Enabled: true, Cert: "a.pem"}
		}, []string{"成对", "skip_verify"}},
		{"source.tls 无CA", func(c *Config) {
			c.Sources[0].Source.TLS = TLSOptions{Enabled: true}
		}, []string{"skip_verify"}},
		// ── ha/pipeline ──
		{"lease_ttl过短", func(c *Config) { c.HA.LeaseTTLSeconds = 1 }, []string{"lease_ttl_seconds"}},
		{"leader_check负数", func(c *Config) { c.HA.LeaderCheckIntervalMS = -1 }, []string{"leader_check_interval_ms"}},
		{"workers为0", func(c *Config) { c.Pipeline.Workers = 0 }, []string{"workers"}},
		{"workers超上限", func(c *Config) { c.Pipeline.Workers = 65 }, []string{"workers"}},
		{"queue_size为0", func(c *Config) { c.Pipeline.WorkerQueueSize = 0 }, []string{"worker_queue_size"}},
		{"batch_size为0", func(c *Config) { c.Pipeline.BatchSize = 0 }, []string{"batch_size"}},
		// ── targets.mysql ──
		{"mysql.addr为空", func(c *Config) { c.Sources[0].Targets.MySQL.Addr = "" }, []string{"mysql.addr"}},
		{"mysql.max_open_conns为0", func(c *Config) { c.Sources[0].Targets.MySQL.MaxOpenConns = 0 }, []string{"max_open_conns", "max_idle_conns"}},
		{"mysql.idle大于open", func(c *Config) {
			c.Sources[0].Targets.MySQL.MaxIdleConns = 30
		}, []string{"max_idle_conns"}},
		{"mysql.tx_chunk_rows为0", func(c *Config) { c.Sources[0].Targets.MySQL.TxChunkRows = 0 }, []string{"tx_chunk_rows"}},
		// ── targets.kafka ──
		{"kafka.brokers为空", func(c *Config) {
			c.Sources[0].Targets.MySQL.Enabled = false
			c.Sources[0].Targets.Kafka.Enabled = true
			c.Sources[0].Targets.Kafka.Brokers = nil
			c.Sources[0].Targets.Kafka.TopicPrefix = "cdc_"
			c.Sources[0].Targets.Kafka.OrderBy = "row"
			c.Sources[0].Targets.Kafka.Compression = "snappy"
			c.Sources[0].Targets.Kafka.Ack = "all"
			c.Sources[0].Targets.Kafka.MaxMessageBytes = 1048576
			c.Sources[0].Rules[0].Sinks = &RuleSinks{Kafka: RuleSink{Enabled: true}}
		}, []string{"brokers"}},
		{"kafka.broker非法", func(c *Config) {
			c.Sources[0].Targets.Kafka.Enabled = true
			c.Sources[0].Targets.Kafka.Brokers = []string{"no-port"}
			c.Sources[0].Targets.Kafka.TopicPrefix = "cdc_"
			c.Sources[0].Targets.Kafka.OrderBy = "row"
			c.Sources[0].Targets.Kafka.Compression = "snappy"
			c.Sources[0].Targets.Kafka.Ack = "all"
			c.Sources[0].Targets.Kafka.MaxMessageBytes = 1048576
		}, []string{"brokers[0]"}},
		{"kafka.topic_prefix非法", func(c *Config) {
			c.Sources[0].Targets.Kafka.Enabled = true
			c.Sources[0].Targets.Kafka.Brokers = []string{"b:9092"}
			c.Sources[0].Targets.Kafka.TopicPrefix = "cdc !"
			c.Sources[0].Targets.Kafka.OrderBy = "row"
			c.Sources[0].Targets.Kafka.Compression = "snappy"
			c.Sources[0].Targets.Kafka.Ack = "all"
			c.Sources[0].Targets.Kafka.MaxMessageBytes = 1048576
		}, []string{"topic_prefix"}},
		{"kafka.topic_prefix含点（统一命名不收点，旧 cdc. 前缀须迁移）", func(c *Config) {
			c.Sources[0].Targets.Kafka.Enabled = true
			c.Sources[0].Targets.Kafka.Brokers = []string{"b:9092"}
			c.Sources[0].Targets.Kafka.TopicPrefix = "cdc."
			c.Sources[0].Targets.Kafka.OrderBy = "row"
			c.Sources[0].Targets.Kafka.Compression = "snappy"
			c.Sources[0].Targets.Kafka.Ack = "all"
			c.Sources[0].Targets.Kafka.MaxMessageBytes = 1048576
		}, []string{"topic_prefix"}},
		{"kafka.order_by非法", func(c *Config) {
			c.Sources[0].Targets.Kafka.Enabled = true
			c.Sources[0].Targets.Kafka.Brokers = []string{"b:9092"}
			c.Sources[0].Targets.Kafka.TopicPrefix = "cdc_"
			c.Sources[0].Targets.Kafka.Compression = "snappy"
			c.Sources[0].Targets.Kafka.Ack = "all"
			c.Sources[0].Targets.Kafka.MaxMessageBytes = 1048576
		}, []string{"order_by"}},
		{"kafka.compression非法", func(c *Config) {
			c.Sources[0].Targets.Kafka.Enabled = true
			c.Sources[0].Targets.Kafka.Brokers = []string{"b:9092"}
			c.Sources[0].Targets.Kafka.TopicPrefix = "cdc_"
			c.Sources[0].Targets.Kafka.OrderBy = "row"
			c.Sources[0].Targets.Kafka.Ack = "all"
			c.Sources[0].Targets.Kafka.MaxMessageBytes = 1048576
		}, []string{"compression"}},
		{"kafka.max_message_bytes过小", func(c *Config) {
			c.Sources[0].Targets.Kafka.Enabled = true
			c.Sources[0].Targets.Kafka.Brokers = []string{"b:9092"}
			c.Sources[0].Targets.Kafka.TopicPrefix = "cdc_"
			c.Sources[0].Targets.Kafka.OrderBy = "row"
			c.Sources[0].Targets.Kafka.Compression = "snappy"
			c.Sources[0].Targets.Kafka.Ack = "all"
			c.Sources[0].Targets.Kafka.MaxMessageBytes = 10
		}, []string{"max_message_bytes"}},
		{"kafka.sasl机制非法", func(c *Config) {
			c.Sources[0].Targets.Kafka.Enabled = true
			c.Sources[0].Targets.Kafka.Brokers = []string{"b:9092"}
			c.Sources[0].Targets.Kafka.TopicPrefix = "cdc_"
			c.Sources[0].Targets.Kafka.OrderBy = "row"
			c.Sources[0].Targets.Kafka.Compression = "snappy"
			c.Sources[0].Targets.Kafka.Ack = "all"
			c.Sources[0].Targets.Kafka.MaxMessageBytes = 1048576
			c.Sources[0].Targets.Kafka.SASL = SASLOptions{Enabled: true, Mechanism: "ntlm", User: "u", PasswordEnv: "P"}
		}, []string{"sasl.mechanism"}},
		{"kafka.sasl密码env非法名", func(c *Config) {
			c.Sources[0].Targets.Kafka.Enabled = true
			c.Sources[0].Targets.Kafka.Brokers = []string{"b:9092"}
			c.Sources[0].Targets.Kafka.TopicPrefix = "cdc_"
			c.Sources[0].Targets.Kafka.OrderBy = "row"
			c.Sources[0].Targets.Kafka.Compression = "snappy"
			c.Sources[0].Targets.Kafka.Ack = "all"
			c.Sources[0].Targets.Kafka.MaxMessageBytes = 1048576
			c.Sources[0].Targets.Kafka.SASL = SASLOptions{Enabled: true, Mechanism: "plain", User: "u", PasswordEnv: "9BAD"}
		}, []string{"password_env"}},
		// ── targets.rocketmq ──
		{"rocketmq.name_servers为空", func(c *Config) {
			c.Sources[0].Targets.MySQL.Enabled = false
			c.Sources[0].Targets.RocketMQ.Enabled = true
			c.Sources[0].Targets.RocketMQ.Group = "g1"
			c.Sources[0].Targets.RocketMQ.TopicPrefix = "CDC_"
			c.Sources[0].Targets.RocketMQ.OrderBy = "row"
			c.Sources[0].Rules[0].Sinks = &RuleSinks{RocketMQ: RuleSink{Enabled: true}}
		}, []string{"name_servers"}},
		{"rocketmq.group非法", func(c *Config) {
			c.Sources[0].Targets.RocketMQ.Enabled = true
			c.Sources[0].Targets.RocketMQ.NameServers = []string{"n:9876"}
			c.Sources[0].Targets.RocketMQ.TopicPrefix = "CDC_"
			c.Sources[0].Targets.RocketMQ.OrderBy = "row"
		}, []string{"group"}},
		{"rocketmq.send_concurrency超限", func(c *Config) {
			c.Sources[0].Targets.RocketMQ.Enabled = true
			c.Sources[0].Targets.RocketMQ.NameServers = []string{"n:9876"}
			c.Sources[0].Targets.RocketMQ.Group = "g1"
			c.Sources[0].Targets.RocketMQ.TopicPrefix = "CDC_"
			c.Sources[0].Targets.RocketMQ.OrderBy = "row"
			c.Sources[0].Targets.RocketMQ.SendTimeoutMS = 5000
			c.Sources[0].Targets.RocketMQ.SendConcurrency = 257
		}, []string{"send_concurrency"}},
		// ── rules ──
		{"rule.schema非法字符", func(c *Config) { c.Sources[0].Rules[0].Schema = "a b" }, []string{"schema"}},
		{"表table非法字符", func(c *Config) { c.Sources[0].Rules[0].Tables[0].Table = "a.b" }, []string{"table"}},
		{"组缺sinks", func(c *Config) { c.Sources[0].Rules[0].Sinks = nil }, []string{"sinks"}},
		{"sinks全关", func(c *Config) {
			c.Sources[0].Rules[0].Sinks = &RuleSinks{}
		}, []string{"至少启用一个"}},
		{"mysql sink配topic", func(c *Config) {
			c.Sources[0].Rules[0].Sinks = &RuleSinks{MySQL: RuleSink{Enabled: true, Topic: "x"}}
		}, []string{"不接受 topic/tag"}},
		{"kafka topic非法", func(c *Config) {
			c.Sources[0].Rules[0].Sinks = &RuleSinks{Kafka: RuleSink{Enabled: true, Topic: "非 法"}}
		}, []string{"sinks.kafka.topic"}},
		{"kafka topic含点（统一命名不收点，旧覆盖值须迁移）", func(c *Config) {
			c.Sources[0].Rules[0].Sinks = &RuleSinks{Kafka: RuleSink{Enabled: true, Topic: "cdc.custom"}}
		}, []string{"sinks.kafka.topic"}},
		{"ddl_policy非法", func(c *Config) { c.Sources[0].Rules[0].DDLPolicy = "drop" }, []string{"ddl_policy"}},
		{"do_stock缺排序列", func(c *Config) { c.Sources[0].Rules[0].Tables[0].OrderByColumn = "" }, []string{"order_by_column"}},
		{"排序列被exclude", func(c *Config) {
			c.Sources[0].Rules[0].Tables[0].ExcludeColumns = []string{"id"}
		}, []string{"不可同时出现在 exclude_columns"}},
		{"排序列被mask", func(c *Config) {
			c.Sources[0].Rules[0].Tables[0].MaskColumns = map[string]MaskSpec{"id": {Type: "phone"}}
		}, []string{"不可脱敏"}},
		{"exclude与mask冲突", func(c *Config) {
			c.Sources[0].Rules[0].Tables[0].MaskColumns = map[string]MaskSpec{"phone": {Type: "phone"}}
			c.Sources[0].Rules[0].Tables[0].ExcludeColumns = []string{"phone"}
		}, []string{"语义互斥"}},
		{"mask算法非法", func(c *Config) {
			c.Sources[0].Rules[0].Tables[0].MaskColumns = map[string]MaskSpec{"phone": {Type: "rot13"}}
		}, []string{"不在算法集"}},
		{"truncate缺keep", func(c *Config) {
			c.Sources[0].Rules[0].Tables[0].MaskColumns = map[string]MaskSpec{"remark": {Type: "truncate"}}
		}, []string{"keep"}},
		{"组schema重复", func(c *Config) {
			c.Sources[0].Rules = append(c.Sources[0].Rules, Rule{Schema: "DB1", Sinks: &RuleSinks{MySQL: RuleSink{Enabled: true}},
				Tables: []TableRule{{Table: "t9", DoStock: boolPtr(false)}}})
		}, []string{"schema 重复"}},
		{"组内表重复", func(c *Config) {
			c.Sources[0].Rules[0].Tables = append(c.Sources[0].Rules[0].Tables, TableRule{Table: "T1", DoStock: boolPtr(false)})
		}, []string{"重复"}},
		{"tables为空", func(c *Config) { c.Sources[0].Rules[0].Tables = nil }, []string{"tables 不能为空"}},
		{"do_stock缺失", func(c *Config) { c.Sources[0].Rules[0].Tables[0].DoStock = nil }, []string{"do_stock 必填"}},
		{"表通配符", func(c *Config) { c.Sources[0].Rules[0].Tables[0].Table = "*" }, []string{"具体表名"}},
		// ── observability ──
		{"metrics_addr非法", func(c *Config) { c.Observability.MetricsAddr = "9595" }, []string{"metrics_addr"}},
		{"web.addr非法", func(c *Config) { c.Observability.Web.Addr = "8060" }, []string{"web.addr"}},
		{"web缺凭据", func(c *Config) { c.Observability.Web.Username = ""; c.Observability.Web.Password = "" }, []string{"username/password"}},
		{"log级别非法", func(c *Config) { c.Observability.Log.Level = "verbose" }, []string{"log.level"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			wantErrs(t, runV(t, tc.mutate), tc.want...)
		})
	}
}

// TestValidate_GroupDefaults 组形态默认值：组级 ddl_policy 省略 → pause 安全默认。
func TestValidate_GroupDefaults(t *testing.T) {
	c := validConfig()
	c.Sources[0].Rules[0].DDLPolicy = ""
	ApplyDefaults(&c)
	if errs := Validate(&c, "c1"); len(errs) != 0 {
		t.Fatalf("组级 ddl_policy 省略应默认 pause:\n%v", errs)
	}
	if c.Sources[0].Rules[0].DDLPolicy != "pause" {
		t.Fatalf("ddl_policy 应被填充为 pause: %q", c.Sources[0].Rules[0].DDLPolicy)
	}

	// 表级显式 do_stock=false：只增量不快照（不要求排序列）
	c = validConfig()
	c.Sources[0].Rules[0].Tables[0] = TableRule{Table: "t1", DoStock: boolPtr(false)}
	ApplyDefaults(&c)
	if errs := Validate(&c, "c1"); len(errs) != 0 {
		t.Fatalf("表级 do_stock=false 合法:\n%v", errs)
	}
}

// TestRuleSinkUnmarshal bool/对象双形态。
func TestRuleSinkUnmarshal(t *testing.T) {
	var rs RuleSinks
	if err := json.Unmarshal([]byte(`{"mysql":true,"kafka":{"enabled":true,"topic":"cdc_t"}}`), &rs); err != nil {
		t.Fatal(err)
	}
	if !rs.MySQL.Enabled || rs.MySQL.Topic != "" {
		t.Fatalf("mysql bool 简写解析错误: %+v", rs.MySQL)
	}
	if !rs.Kafka.Enabled || rs.Kafka.Topic != "cdc_t" {
		t.Fatalf("kafka 对象形态解析错误: %+v", rs.Kafka)
	}
	if err := json.Unmarshal([]byte(`{"mysql":"yes"}`), &rs); err == nil {
		t.Fatal("非法形态应报错")
	}
}

// TestMaskSpecUnmarshal 字符串/对象双形态。
func TestMaskSpecUnmarshal(t *testing.T) {
	var m MaskSpec
	if err := json.Unmarshal([]byte(`"phone"`), &m); err != nil || m.Type != "phone" {
		t.Fatalf("字符串形态解析错误: %+v err=%v", m, err)
	}
	if err := json.Unmarshal([]byte(`{"type":"truncate","keep":3}`), &m); err != nil || m.Keep != 3 {
		t.Fatalf("对象形态解析错误: %+v err=%v", m, err)
	}
	if err := json.Unmarshal([]byte(`42`), &m); err == nil {
		t.Fatal("非法形态应报错")
	}
}

// TestResolveEnv ${VAR} 解析与缺失报错。
func TestResolveEnv(t *testing.T) {
	t.Setenv("GOCDC_TEST_PW", "s3cret")
	tree := map[string]any{
		"a": "${GOCDC_TEST_PW}",
		"b": []any{map[string]any{"c": "x-${GOCDC_TEST_PW}-y"}},
		"d": 123,
	}
	out, errs := ResolveEnv(tree)
	if len(errs) != 0 {
		t.Fatalf("不应报错: %v", errs)
	}
	m := out.(map[string]any)
	if m["a"] != "s3cret" {
		t.Fatalf("顶层替换错误: %v", m["a"])
	}
	inner := m["b"].([]any)[0].(map[string]any)["c"]
	if inner != "x-s3cret-y" {
		t.Fatalf("嵌套替换错误: %v", inner)
	}
	if m["d"] != 123 {
		t.Fatalf("非字符串应原样: %v", m["d"])
	}

	_, errs = ResolveEnv(map[string]any{"x": "${GOCDC_TEST_MISSING_VAR}"})
	if len(errs) != 1 || !strings.Contains(errs[0].Error(), "GOCDC_TEST_MISSING_VAR") {
		t.Fatalf("缺失变量应报错并带名字: %v", errs)
	}
}

// TestApplyDefaults 零值填默认、非零保留（集群默认 + 逐源填充）。
func TestApplyDefaults(t *testing.T) {
	c := &Config{Sources: []SourceSpec{{ID: "s1"}}}
	ApplyDefaults(c)
	if c.Sources[0].Source.HeartbeatIntervalMS != 3000 || c.Pipeline.PositionSaveIntervalMS != 3000 ||
		c.Sources[0].Targets.MySQL.MaxIdleConns != 5 || c.Sources[0].Targets.Kafka.SendTimeoutMS != 5000 ||
		c.Sources[0].Targets.RocketMQ.SendConcurrency != 32 {
		t.Fatalf("默认值填充错误: %+v", c)
	}
	c.Pipeline.FlushIntervalMS = 50
	ApplyDefaults(c)
	if c.Pipeline.FlushIntervalMS != 50 {
		t.Fatal("非零值不应被覆盖")
	}
}

// TestSchemaTagsAllSnakeCase schema-tag 同步断言：全树导出字段必须带合法
// snake_case json tag（03 §3.2 字段一一对应的机器防线）。
func TestSchemaTagsAllSnakeCase(t *testing.T) {
	snake := regexp.MustCompile(`^[a-z][a-z0-9]*(_[a-z0-9]+)*$`)
	visited := map[reflect.Type]bool{}

	var walk func(typ reflect.Type, path string)
	walk = func(typ reflect.Type, path string) {
		for typ.Kind() == reflect.Ptr || typ.Kind() == reflect.Slice || typ.Kind() == reflect.Map || typ.Kind() == reflect.Array {
			typ = typ.Elem()
		}
		if typ.Kind() != reflect.Struct || visited[typ] {
			return
		}
		visited[typ] = true
		for i := 0; i < typ.NumField(); i++ {
			f := typ.Field(i)
			if f.PkgPath != "" { // 未导出
				continue
			}
			tag := f.Tag.Get("json")
			name := strings.Split(tag, ",")[0]
			if name == "" || name == "-" {
				t.Errorf("%s.%s 缺少 json tag", path, f.Name)
				continue
			}
			if !snake.MatchString(name) {
				t.Errorf("%s.%s json tag %q 不是 snake_case", path, f.Name, name)
			}
			walk(f.Type, path+"."+f.Name)
		}
	}
	walk(reflect.TypeOf(Config{}), "Config")
}

// fakeLoadKV 实现 clientv3.KV 的最小桩（仅 Get）。
type fakeLoadKV struct{ vals map[string]string }

func (f *fakeLoadKV) Get(_ context.Context, key string, _ ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	resp := &clientv3.GetResponse{}
	if v, ok := f.vals[key]; ok {
		resp.Kvs = []*mvccpb.KeyValue{{Key: []byte(key), Value: []byte(v)}}
	}
	return resp, nil
}
func (f *fakeLoadKV) Put(context.Context, string, string, ...clientv3.OpOption) (*clientv3.PutResponse, error) {
	return &clientv3.PutResponse{}, nil
}
func (f *fakeLoadKV) Delete(context.Context, string, ...clientv3.OpOption) (*clientv3.DeleteResponse, error) {
	return &clientv3.DeleteResponse{}, nil
}
func (f *fakeLoadKV) Compact(context.Context, int64, ...clientv3.CompactOption) (*clientv3.CompactResponse, error) {
	return &clientv3.CompactResponse{}, nil
}
func (f *fakeLoadKV) Do(context.Context, clientv3.Op) (clientv3.OpResponse, error) {
	return clientv3.OpResponse{}, nil
}
func (f *fakeLoadKV) Txn(context.Context) clientv3.Txn { return nil }

// TestLoad_FromFile 文件加载 + ENV 解析 + cluster 裁决。
func TestLoad_FromFile(t *testing.T) {
	t.Setenv("GOCDC_TEST_SRC_PW", "srcpw")
	raw := `cluster: c1
source:
  addr: "10.0.0.1:3306"
  user: repl
  password: "${GOCDC_TEST_SRC_PW}"
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
    password: x
    charset: utf8mb4
    max_open_conns: 20
    tx_chunk_rows: 500
rules:
  - schema: db1
    sinks: { mysql: true }
    tables:
      - { table: t1, order_by_column: id, do_stock: true }
observability:
  web: { enabled: true, addr: ":8060", username: admin, password: t }
  metrics_addr: ":9595"
  log: { level: info }
`
	dir := t.TempDir()
	path := filepath.Join(dir, "gocdc.yaml")
	if err := os.WriteFile(path, []byte(raw), 0o600); err != nil {
		t.Fatal(err)
	}

	res := Load(context.Background(), path, "", "c1", nil)
	if len(res.Errors) != 0 {
		t.Fatalf("合法配置不应报错:\n%v", res.Errors)
	}
	if res.Config.Sources[0].Source.Password != "srcpw" {
		t.Fatalf("ENV 解析失败: %q", res.Config.Sources[0].Source.Password)
	}
	if res.Config.Sources[0].Rules[0].DDLPolicy != "pause" {
		t.Fatalf("ddl_policy 库级省略应默认 pause，实际 %q", res.Config.Sources[0].Rules[0].DDLPolicy)
	}

	// cluster 与 flag 不一致 → fail-fast
	res = Load(context.Background(), path, "", "other", nil)
	wantErrs(t, res.Errors, "不一致")

	// 坏 YAML 语法
	bad := filepath.Join(dir, "bad.yaml")
	_ = os.WriteFile(bad, []byte("{nope"), 0o600)
	res = Load(context.Background(), bad, "", "c1", nil)
	if len(res.Errors) != 1 {
		t.Fatalf("坏 YAML 应报 1 错: %v", res.Errors)
	}
}

// TestLoad_YAMLFormat YAML 唯一格式：
// 合法 YAML 通过全链路（ENV 解析/默认值/校验）；坏语法报单个 yaml 错误。
func TestLoad_YAMLFormat(t *testing.T) {
	t.Setenv("GOCDC_TEST_SRC_PW", "srcpw")
	raw := `cluster: c1
source:
  addr: "10.0.0.1:3306"
  user: repl
  password: "${GOCDC_TEST_SRC_PW}"
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
    password: x
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
	dir := t.TempDir()
	path := filepath.Join(dir, "gocdc.yaml")
	if err := os.WriteFile(path, []byte(raw), 0o600); err != nil {
		t.Fatal(err)
	}

	res := Load(context.Background(), path, "", "c1", nil)
	if len(res.Errors) != 0 {
		t.Fatalf("合法 YAML 配置不应报错:\n%v", res.Errors)
	}
	if res.Config.Sources[0].Source.Password != "srcpw" {
		t.Fatalf("YAML 下 ENV 解析失败: %q", res.Config.Sources[0].Source.Password)
	}
	if res.Config.Sources[0].Targets.MySQL.MaxIdleConns != 5 || res.Config.Pipeline.FlushIntervalMS != 200 {
		t.Fatalf("YAML 下默认值未填充: idle=%d flush=%d",
			res.Config.Sources[0].Targets.MySQL.MaxIdleConns, res.Config.Pipeline.FlushIntervalMS)
	}
	t0 := res.Config.Sources[0].Rules[0].Tables[0]
	if !res.Config.Sources[0].Rules[0].Sinks.MySQL.Enabled || t0.DoStock == nil || !*t0.DoStock {
		t.Fatalf("YAML 规则解析错误: %+v", res.Config.Sources[0].Rules[0])
	}

	// 坏 YAML → 单错误且含 yaml 定位信息
	bad := filepath.Join(dir, "bad.yaml")
	_ = os.WriteFile(bad, []byte("source: [unclosed"), 0o600)
	res = Load(context.Background(), bad, "", "c1", nil)
	if len(res.Errors) != 1 || !strings.Contains(res.Errors[0].Error(), "yaml") {
		t.Fatalf("坏 YAML 应报 1 错并含 yaml 信息: %v", res.Errors)
	}
}

// TestLoad_FromEtcd etcd 路径：key 存在/缺失两态。
func TestLoad_FromEtcd(t *testing.T) {
	fk := &fakeLoadKV{vals: map[string]string{
		"/gocdc/c1/config": "cluster: c1\nrules: []\nsource:\n  exclude_schemas: []\ntargets: {}\n",
	}}
	kv := etcdx.NewKV(&clientv3.Client{KV: fk})

	res := Load(context.Background(), "", "/gocdc/c1/config", "c1", kv)
	found := false
	for _, e := range res.Errors {
		if strings.Contains(e.Error(), "rules") {
			found = true
			break
		}
	}
	if len(res.Errors) == 0 || !found {
		t.Fatalf("etcd 配置应进入校验并报 rules 错误: %v", res.Errors)
	}

	// key 不存在 → fail-fast
	res = Load(context.Background(), "", "/gocdc/c9/config", "c9", kv)
	if len(res.Errors) != 1 || !strings.Contains(res.Errors[0].Error(), "不存在") {
		t.Fatalf("缺失 key 应报「不存在」: %v", res.Errors)
	}
}

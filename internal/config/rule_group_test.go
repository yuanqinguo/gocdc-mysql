package config

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// 组形态合法基线：组级 sinks/ddl_policy/target_database，表级逐表 order_by/do_stock。
func TestValidate_GroupRules_Valid(t *testing.T) {
	errs := runV(t, func(c *Config) {
		c.Sources[0].Rules = []Rule{{
			Schema: "edu.mall.admin", DDLPolicy: "auto",
			Sinks:          &RuleSinks{MySQL: RuleSink{Enabled: true}},
			TargetDatabase: "edu.mall.merge",
			Tables: []TableRule{
				{Table: "admin_user", OrderByColumn: "id", DoStock: boolPtr(true)},
				{Table: "users", DoStock: boolPtr(false),
					TargetDatabase: "other.merge",
					MaskColumns:    map[string]MaskSpec{"phone": {Type: "phone"}}},
			},
		}}
	})
	if len(errs) != 0 {
		t.Fatalf("合法组形态应零错误: %v", errs)
	}
}

// 组形态 ddl_policy 缺省 → pause 原地填充（安全默认）。
func TestValidate_GroupRules_DefaultDDLPolicy(t *testing.T) {
	c := validConfig()
	c.Sources[0].Rules[0].DDLPolicy = ""
	ApplyDefaults(&c)
	if errs := Validate(&c, "c1"); len(errs) != 0 {
		t.Fatalf("应零错误: %v", errs)
	}
	if c.Sources[0].Rules[0].DDLPolicy != "pause" {
		t.Fatalf("ddl_policy 缺省应填 pause: %q", c.Sources[0].Rules[0].DDLPolicy)
	}
}

// 组形态结构性错误清单（不短路，全部一次报出）。
func TestValidate_GroupRules_Errors(t *testing.T) {
	group := func(mutate func(*Rule)) []Rule {
		g := Rule{
			Schema: "db1", DDLPolicy: "auto",
			Sinks:  &RuleSinks{MySQL: RuleSink{Enabled: true}},
			Tables: []TableRule{{Table: "t1", OrderByColumn: "id", DoStock: boolPtr(true)}},
		}
		if mutate != nil {
			mutate(&g)
		}
		return []Rule{g}
	}
	cases := []struct {
		name    string
		rules   []Rule
		contain string
	}{
		{"组级缺 sinks", group(func(g *Rule) { g.Sinks = nil }), "sinks"},
		{"do_stock 缺失", group(func(g *Rule) { g.Tables[0].DoStock = nil }), "do_stock 必填"},
		{"do_stock=true 无排序键", group(func(g *Rule) { g.Tables[0].OrderByColumn = "" }), "order_by_column 为空"},
		{"组内重复表", group(func(g *Rule) { g.Tables = append(g.Tables, TableRule{Table: "t1", DoStock: boolPtr(false)}) }), "重复"},
		{"通配表名", group(func(g *Rule) { g.Tables[0].Table = "*" }), "须为具体表名"},
		{"表级 target_database 非法", group(func(g *Rule) { g.Tables[0].TargetDatabase = "a b" }), "非法字符"},
	}
	for _, tc := range cases {
		errs := runV(t, func(c *Config) { c.Sources[0].Rules = tc.rules })
		if len(errs) == 0 {
			t.Fatalf("%s: 应报错", tc.name)
		}
		found := false
		for _, e := range errs {
			if strings.Contains(e.Error(), tc.contain) {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("%s: 错误清单缺 %q: %v", tc.name, tc.contain, errs)
		}
	}
}

// 平铺形态拒绝（表级字段写在组条目上）：解码期未知字段即报错（不静默丢弃），
// 错误信息指引组形态。
func TestRule_LegacyFlatFormRejected(t *testing.T) {
	dir := t.TempDir()
	write := func(name, body string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	head := `cluster: c1
source:
  addr: "10.0.0.1:3306"
  user: repl
  password: pwd
  charset: utf8mb4
  flavor: mysql
  server_id: 1
  timezone: Asia/Shanghai
  exclude_schemas: [mysql, sys, information_schema, performance_schema]
ha: { lease_ttl_seconds: 10 }
pipeline: { workers: 4, worker_queue_size: 8192, batch_size: 500, flush_interval_ms: 200, position_save_interval_ms: 3000 }
targets:
  mysql:
    enabled: true
    addr: "10.0.1.1:3306"
    user: w
    password: pwd
    charset: utf8mb4
    max_open_conns: 20
    tx_chunk_rows: 500
`
	cases := []struct {
		name    string
		rules   string
		contain string
	}{
		{"库级平铺", `
rules:
  - schema: db1
    do_stock: true
    order_by_column: id
    sinks: { mysql: true }
`, "不支持的规则组形态"},
		{"表级 sinks", `
rules:
  - schema: db1
    sinks: { mysql: true }
    tables:
      - { table: t1, do_stock: true, sinks: { mysql: true } }
`, "表级条目不识别"},
	}
	for _, tc := range cases {
		res := Load(context.Background(), write("cfg.yml", head+tc.rules), "", "c1", nil)
		if len(res.Errors) == 0 {
			t.Fatalf("%s: 应报错", tc.name)
		}
		found := false
		for _, e := range res.Errors {
			if strings.Contains(e.Error(), tc.contain) {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("%s: 错误缺 %q: %v", tc.name, tc.contain, res.Errors)
		}
	}
}

// 组形态 YAML 端到端：块状嵌套表清单经真实 Load 链路解析。
func TestLoad_GroupRulesYAML(t *testing.T) {
	yml := `
cluster: c1
source:
  addr: "10.0.0.1:3306"
  user: repl
  password: pwd
  charset: utf8mb4
  flavor: mysql
  server_id: 123
  timezone: Asia/Shanghai
  exclude_schemas: [mysql, sys, information_schema, performance_schema]
ha: { lease_ttl_seconds: 10 }
pipeline: { workers: 4, worker_queue_size: 8192, batch_size: 500, flush_interval_ms: 200, position_save_interval_ms: 3000 }
targets:
  mysql:
    enabled: true
    addr: "10.0.1.1:3306"
    user: writer
    password: pwd
    charset: utf8mb4
    max_open_conns: 20
    tx_chunk_rows: 500
rules:
  - schema: dev_edu.mall.admin
    sinks: { mysql: true }
    ddl_policy: auto
    tables:
      - { table: admin_user, order_by_column: id, do_stock: true }
      - table: users
        order_by_column: id
        do_stock: false
        mask_columns:
          phone: phone
observability:
  metrics_addr: ":9595"
  web: { enabled: false }
  log: { level: info }
`
	path := filepath.Join(t.TempDir(), "cfg.yml")
	if err := os.WriteFile(path, []byte(yml), 0o600); err != nil {
		t.Fatal(err)
	}
	res := Load(context.Background(), path, "", "c1", nil)
	if len(res.Errors) != 0 {
		t.Fatalf("组形态 YAML 应加载成功: %v", res.Errors)
	}
	g := res.Config.Sources[0].Rules[0]
	if len(g.Tables) != 2 || g.Tables[0].Table != "admin_user" || g.Tables[1].Table != "users" {
		t.Fatalf("组表清单解析: %+v", g.Tables)
	}
	if g.Tables[1].MaskColumns["phone"].Type != "phone" {
		t.Fatalf("表级脱敏解析: %+v", g.Tables[1].MaskColumns)
	}
}

package config

import (
	"strings"
	"testing"
)

func doStockPtr(v bool) *bool { return &v }

// validV2Body 双源测试正文：注释 / ${ENV} 占位 / 危险枚举引号 / v2 sources 形态。
const validV2Body = `# 集群配置（顶部注释必须保留）
cluster: test-edit

ha:
  lease_ttl_seconds: 10
  leader_check_interval_ms: 2000

observability:
  metrics_addr: "127.0.0.1:9595"

pipeline:
  workers: 4
  worker_queue_size: 8192
  batch_size: 500

sources:
  - id: src-a
    source:
      addr: "src-a:3306"
      user: root
      password: ${SRC_A_PASSWORD}   # 占位不可被解析成明文
      charset: utf8mb4
      flavor: mysql
      timezone: UTC
      exclude_schemas: [mysql, sys, information_schema, performance_schema]
    targets:
      mysql:
        enabled: true
        addr: "dst:3306"
        user: root
        password: ${DST_PASSWORD}
        charset: utf8mb4
        max_open_conns: 10
        tx_chunk_rows: 500
    rules:
      - schema: db1
        sinks: {mysql: true}
        ddl_policy: auto
        tables:
          - {table: t1, order_by_column: id, do_stock: true}
          # 表级注释（随所在组重写，允许丢失）
          - {table: t2, do_stock: false, mask_columns: {phone: "null"}}
  - id: src-b
    source:
      addr: "src-b:3306"
      user: root
      password: "pw-b"
      charset: utf8mb4
      flavor: mysql
      timezone: UTC
      exclude_schemas: [mysql, sys, information_schema, performance_schema]
    targets:
      mysql:
        enabled: true
        addr: "dst:3306"
        user: root
        password: "pw-b"
        charset: utf8mb4
        max_open_conns: 10
        tx_chunk_rows: 500
    rules:
      - schema: db2
        sinks: {mysql: true}
        tables:
          - {table: t9, order_by_column: id, do_stock: true}
`

func minimalTables(t1, t2 string) []TableRule {
	return []TableRule{
		{Table: t1, OrderByColumn: "id", DoStock: doStockPtr(true)},
		{Table: t2, DoStock: doStockPtr(false), MaskColumns: map[string]MaskSpec{"phone": {Type: MaskNull}}},
	}
}

// TestSpliceSourceRules_V2_Preserves 手术三铁律：段外注释、${ENV} 占位、
// 危险枚举引号、另一源内容全部保留；新 rules 生效且可经 LoadFromRaw 通过校验。
func TestSpliceSourceRules_V2_Preserves(t *testing.T) {
	t.Setenv("SRC_A_PASSWORD", "real-secret")
	t.Setenv("DST_PASSWORD", "real-dst")
	newRules := []Rule{{
		Schema: "db1",
		Sinks:  &RuleSinks{MySQL: RuleSink{Enabled: true}},
		Tables: append(minimalTables("t1", "t2"), TableRule{Table: "t3", OrderByColumn: "id", DoStock: doStockPtr(true)}),
	}}
	out, err := SpliceSourceRules([]byte(validV2Body), "src-a", newRules)
	if err != nil {
		t.Fatalf("splice: %v", err)
	}
	outStr := string(out)

	// ① 段外注释保留（顶部 + 未触碰的 src-b 段内）
	for _, comment := range []string{"# 集群配置（顶部注释必须保留）", "# 占位不可被解析成明文"} {
		if !strings.Contains(outStr, comment) {
			t.Fatalf("段外注释丢失: %q\n---\n%s", comment, outStr)
		}
	}
	// ② ${ENV} 占位原样保留（绝不能出现解析后的明文）
	if !strings.Contains(outStr, "${SRC_A_PASSWORD}") || !strings.Contains(outStr, "${DST_PASSWORD}") {
		t.Fatalf("${ENV} 占位丢失:\n%s", outStr)
	}
	if strings.Contains(outStr, "real-secret") {
		t.Fatalf("占位被解析成明文写回（泄密）:\n%s", outStr)
	}
	// ③ 未触碰源（src-b/db2.t9）保留
	if !strings.Contains(outStr, "db2") || !strings.Contains(outStr, "t9") {
		t.Fatalf("未触碰源内容丢失:\n%s", outStr)
	}
	// ④ 新表已写入
	if !strings.Contains(outStr, "t3") {
		t.Fatalf("新表未写入:\n%s", outStr)
	}
	// ⑤ 危险枚举（mask type null）不可裸写（折叠为字符串简写后仍须带引号）
	if !strings.Contains(outStr, `phone: "null"`) || strings.Contains(outStr, "phone: null\n") {
		t.Fatalf("mask type=null 必须带引号防 YAML 字面量:\n%s", outStr)
	}
	// ⑥ 全链校验通过 + 语义正确（LoadFromRaw 用 ${ENV} 已 set 的环境）
	res := LoadFromRaw(out, "test-edit")
	if len(res.Errors) > 0 {
		t.Fatalf("替换后正文未通过加载链: %v\n---\n%s", res.Errors, outStr)
	}
	var spec *SourceSpec
	for i := range res.Config.Sources {
		if res.Config.Sources[i].ID == "src-a" {
			spec = &res.Config.Sources[i]
		}
	}
	if spec == nil || len(spec.Rules[0].Tables) != 3 {
		t.Fatalf("src-a 新规则未生效: %+v", spec)
	}
	if spec.Rules[0].Tables[1].MaskColumns["phone"].Type != MaskNull {
		t.Fatalf("mask type 应为字符串 %q: %+v", MaskNull, spec.Rules[0].Tables[1].MaskColumns)
	}
	// ⑦ 未触碰源语义不变
	var specB *SourceSpec
	for i := range res.Config.Sources {
		if res.Config.Sources[i].ID == "src-b" {
			specB = &res.Config.Sources[i]
		}
	}
	if specB == nil || len(specB.Rules[0].Tables) != 1 || specB.Rules[0].Tables[0].Table != "t9" {
		t.Fatalf("src-b 规则被误改: %+v", specB)
	}
}

// TestSpliceSourceRules_V1_TopLevel v1 顶层形态（真实生产正文形态）：
// default 源替换顶层 rules；其他 sid 报错。
func TestSpliceSourceRules_V1_TopLevel(t *testing.T) {
	t.Setenv("PW", "x")
	v1 := `# v1 注释
cluster: c1
ha:
  lease_ttl_seconds: 10
  leader_check_interval_ms: 2000
observability:
  metrics_addr: "127.0.0.1:9595"
pipeline:
  workers: 4
  worker_queue_size: 8192
  batch_size: 500
source:
  addr: "s:3306"
  user: root
  password: ${PW}
  charset: utf8mb4
  flavor: mysql
  timezone: UTC
  exclude_schemas: [mysql, sys, information_schema, performance_schema]
targets:
  mysql:
    enabled: true
    addr: "d:3306"
    user: root
    password: ${PW}
    charset: utf8mb4
    max_open_conns: 5
    tx_chunk_rows: 100
rules:
  - schema: db1
    sinks: {mysql: true}
    tables:
      - {table: t1, order_by_column: id, do_stock: true}
`
	newRules := []Rule{{
		Schema: "db1",
		Sinks:  &RuleSinks{MySQL: RuleSink{Enabled: true}},
		Tables: []TableRule{{Table: "t1", OrderByColumn: "id", DoStock: doStockPtr(true)},
			{Table: "t2", DoStock: doStockPtr(false)}},
	}}
	out, err := SpliceSourceRules([]byte(v1), DefaultSourceID, newRules)
	if err != nil {
		t.Fatalf("v1 splice: %v", err)
	}
	if !strings.Contains(string(out), "t2") || !strings.Contains(string(out), "# v1 注释") {
		t.Fatalf("v1 替换结果不符:\n%s", out)
	}
	if res := LoadFromRaw(out, "c1"); len(res.Errors) > 0 {
		t.Fatalf("v1 替换后校验失败: %v\n%s", res.Errors, out)
	}
	if _, err := SpliceSourceRules([]byte(v1), "other", newRules); err == nil {
		t.Fatalf("v1 非 default 源应报错")
	}
}

// TestSpliceSourceRules_Errors 未知源 / 非 mapping 正文。
func TestSpliceSourceRules_Errors(t *testing.T) {
	newRules := []Rule{{
		Schema: "db1", Sinks: &RuleSinks{MySQL: RuleSink{Enabled: true}},
		Tables: []TableRule{{Table: "t1", DoStock: doStockPtr(true)}},
	}}
	if _, err := SpliceSourceRules([]byte(validV2Body), "nope", newRules); err == nil {
		t.Fatalf("未知源应报错")
	}
	if _, err := SpliceSourceRules([]byte("- a\n- b"), DefaultSourceID, newRules); err == nil {
		t.Fatalf("非 mapping 正文应报错")
	}
}

// TestSpliceSourceRules_AppendRulesKey 源条目缺 rules 键时追加（而非报错）。
func TestSpliceSourceRules_AppendRulesKey(t *testing.T) {
	t.Setenv("PW", "x")
	body := `cluster: c1
ha:
  lease_ttl_seconds: 10
  leader_check_interval_ms: 2000
observability:
  metrics_addr: "127.0.0.1:9595"
pipeline:
  workers: 4
  worker_queue_size: 8192
  batch_size: 500
sources:
  - id: s1
    source:
      addr: "s:3306"
      user: root
      password: ${PW}
      charset: utf8mb4
      flavor: mysql
      timezone: UTC
      exclude_schemas: [mysql, sys, information_schema, performance_schema]
    targets:
      mysql:
        enabled: true
        addr: "d:3306"
        user: root
        password: ${PW}
        charset: utf8mb4
        max_open_conns: 5
        tx_chunk_rows: 100
`
	newRules := []Rule{{
		Schema: "db1", Sinks: &RuleSinks{MySQL: RuleSink{Enabled: true}},
		Tables: []TableRule{{Table: "t1", OrderByColumn: "id", DoStock: doStockPtr(true)}},
	}}
	out, err := SpliceSourceRules([]byte(body), "s1", newRules)
	if err != nil {
		t.Fatalf("追加 rules 键: %v", err)
	}
	if res := LoadFromRaw(out, "c1"); len(res.Errors) > 0 {
		t.Fatalf("追加后校验失败: %v\n%s", res.Errors, out)
	}
}

// TestLoadFromRaw 基本链路：合法通过；cluster 不一致报错；非法正文报错。
func TestLoadFromRaw(t *testing.T) {
	t.Setenv("SRC_A_PASSWORD", "p")
	t.Setenv("DST_PASSWORD", "p")
	if res := LoadFromRaw([]byte(validV2Body), "test-edit"); len(res.Errors) != 0 {
		t.Fatalf("合法正文应零错误: %v", res.Errors)
	}
	res := LoadFromRaw([]byte(validV2Body), "other-cluster")
	if len(res.Errors) == 0 || !strings.Contains(res.Errors[0].Error(), "不一致") {
		t.Fatalf("cluster 不一致应报错: %v", res.Errors)
	}
	if res := LoadFromRaw([]byte("!!!not-yaml["), "c"); len(res.Errors) == 0 {
		t.Fatalf("非法 YAML 应报错")
	}
}

// TestDiffRules diff 矩阵：增/删/表级改/组级改/重排不算改。
func TestDiffRules(t *testing.T) {
	mk := func(tables ...TableRule) []Rule {
		return []Rule{{
			Schema: "db1", Sinks: &RuleSinks{MySQL: RuleSink{Enabled: true}}, Tables: tables,
		}}
	}
	oldR := mk(
		TableRule{Table: "keep", OrderByColumn: "id", DoStock: doStockPtr(true)},
		TableRule{Table: "drop", DoStock: doStockPtr(false)},
		TableRule{Table: "chg", DoStock: doStockPtr(false)},
	)
	newR := mk(
		TableRule{Table: "keep", OrderByColumn: "id", DoStock: doStockPtr(true)},
		TableRule{Table: "chg", DoStock: doStockPtr(true)}, // do_stock 变化
		TableRule{Table: "add", OrderByColumn: "id", DoStock: doStockPtr(true)},
	)
	added, removed, modified := DiffRules(oldR, newR)
	if len(added) != 1 || added[0].Table != "add" {
		t.Fatalf("added: %+v", added)
	}
	if len(removed) != 1 || removed[0].Table != "drop" {
		t.Fatalf("removed: %+v", removed)
	}
	if len(modified) != 1 || modified[0].Table != "chg" {
		t.Fatalf("modified: %+v", modified)
	}

	// 重排不算变化
	added, removed, modified = DiffRules(oldR, mk(
		TableRule{Table: "chg", DoStock: doStockPtr(false)},
		TableRule{Table: "keep", OrderByColumn: "id", DoStock: doStockPtr(true)},
		TableRule{Table: "drop", DoStock: doStockPtr(false)},
	))
	if len(added)+len(removed)+len(modified) != 0 {
		t.Fatalf("重排不应产生 diff: +%v -%v ~%v", added, removed, modified)
	}

	// 组级变化（sinks 增加 kafka）→ 整组 modified
	groupChanged := []Rule{{
		Schema: "db1",
		Sinks:  &RuleSinks{MySQL: RuleSink{Enabled: true}, Kafka: RuleSink{Enabled: true}},
		Tables: []TableRule{{Table: "keep", OrderByColumn: "id", DoStock: doStockPtr(true)},
			TableRule{Table: "drop", DoStock: doStockPtr(false)},
			TableRule{Table: "chg", DoStock: doStockPtr(false)},
		}},
	}
	added, removed, modified = DiffRules(oldR, groupChanged)
	if len(added)+len(removed) != 0 || len(modified) != 3 {
		t.Fatalf("组级变化应整组 modified: +%v -%v ~%v", added, removed, modified)
	}

	// 组消失 → 全部 removed；exclude_columns 顺序变化不算改
	_, removed, _ = DiffRules(oldR, nil)
	if len(removed) != 3 {
		t.Fatalf("组消失应全部 removed: %v", removed)
	}
	a1 := TableRule{Table: "x", DoStock: doStockPtr(false), ExcludeColumns: []string{"a", "b"}}
	a2 := TableRule{Table: "x", DoStock: doStockPtr(false), ExcludeColumns: []string{"b", "a"}}
	oldR2 := mk(a1)
	newR2 := mk(a2)
	added, removed, modified = DiffRules(oldR2, newR2)
	if len(added)+len(removed)+len(modified) != 0 {
		t.Fatalf("exclude 顺序变化不应算改: +%v -%v ~%v", added, removed, modified)
	}

	// 大小写变体表按同一张表处理（ci 口径）
	oldR3 := mk(TableRule{Table: "Users", DoStock: doStockPtr(false)})
	newR3 := mk(TableRule{Table: "users", DoStock: doStockPtr(false)})
	added, removed, modified = DiffRules(oldR3, newR3)
	if len(added)+len(removed)+len(modified) != 0 {
		t.Fatalf("大小写变体应视为同一表: +%v -%v ~%v", added, removed, modified)
	}
}

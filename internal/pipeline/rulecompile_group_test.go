package pipeline

import (
	"testing"

	"gocdc/internal/config"
)

// 组形态编译：组级 sinks/ddl_policy/target_database 拷贝到每表；
// 白名单外表（含未来新表）不产生规则。
func TestCompileRules_GroupExpansion(t *testing.T) {
	doTrue, doFalse := true, false
	cr, err := CompileRules([]config.Rule{{
		Schema: "db1", DDLPolicy: "auto",
		Sinks:          &config.RuleSinks{MySQL: config.RuleSink{Enabled: true}},
		TargetDatabase: "tgt.merge",
		Tables: []config.TableRule{
			{Table: "t1", OrderByColumn: "id", DoStock: &doTrue},
			{Table: "t2", DoStock: &doFalse, TargetDatabase: "t2db"},
		},
	}})
	if err != nil {
		t.Fatal(err)
	}

	m1 := cr.Match("DB1", "T1") // 库表名大小写不敏感
	if m1 == nil || !m1.Sinks.MySQL || m1.DDLPolicy != "auto" ||
		m1.TargetDatabase != "tgt.merge" || !m1.DoStock || m1.OrderByColumn != "id" {
		t.Fatalf("t1 生效规则（组级拷贝）: %+v", m1)
	}
	m2 := cr.Match("db1", "t2")
	if m2 == nil || m2.DoStock || m2.TargetDatabase != "t2db" {
		t.Fatalf("t2 生效规则（表级 target_database 覆盖）: %+v", m2)
	}
	if cr.Match("db1", "t3") != nil {
		t.Fatal("未列出的表不得产生规则")
	}
	if cr.Match("db2", "t1") != nil {
		t.Fatal("其他库不得命中组规则")
	}
	gs := cr.Groups()
	if len(gs) != 1 || len(gs[0].Tables) != 2 || gs[0].Schema != "db1" {
		t.Fatalf("Groups 视图: %+v", gs)
	}
	// SchemaHasTables 判据（DDL 放行）在白名单下照常工作
	if !cr.SchemaHasTables("db1") || cr.SchemaHasTables("db2") {
		t.Fatalf("SchemaHasTables: db1=%v db2=%v", cr.SchemaHasTables("db1"), cr.SchemaHasTables("db2"))
	}
	// 生效视图 = 全部白名单表（含尚未见过事件的表），按 Key 排序
	views := cr.EffectiveRules()
	if len(views) != 2 || views[0].Key != "db1.t1" || views[1].Key != "db1.t2" {
		t.Fatalf("EffectiveRules: %+v", views)
	}
	if cr.KnownTables() != 2 || cr.MatchByKey("db1.t2") != m2 {
		t.Fatalf("KnownTables/MatchByKey: %d", cr.KnownTables())
	}
}

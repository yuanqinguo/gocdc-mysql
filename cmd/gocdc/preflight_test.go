// 预检纯函数单测：定位/解析链。触库的核对路径由集成测试覆盖
// （Windows 单测不触库不算验证过——VERIFY 教训）。
package main

import (
	"testing"

	"gocdc/internal/config"
)

func stockPtr(v bool) *bool { return &v }

func testRules() []config.Rule {
	return []config.Rule{
		{
			Schema: "edu.boat.admin",
			Sinks:  &config.RuleSinks{MySQL: config.RuleSink{Enabled: true}},
			Tables: []config.TableRule{
				{Table: "users", OrderByColumn: "id", DoStock: stockPtr(true)},
				{Table: "orders", DoStock: stockPtr(false), TargetDatabase: "merge.orders"},
			},
		},
		{
			Schema:         "edu.boat.content",
			Sinks:          &config.RuleSinks{Kafka: config.RuleSink{Enabled: true}},
			TargetDatabase: "merge.content",
			Tables:         []config.TableRule{{Table: "posts", OrderByColumn: "id", DoStock: stockPtr(true)}},
		},
	}
}

// TestFindTableRule 大小写不敏感定位（与白名单 ci 口径一致）。
func TestFindTableRule(t *testing.T) {
	rules := testRules()
	group, tb, ok := findTableRule(rules, "EDU.BOAT.ADMIN", "Users")
	if !ok || group.Schema != "edu.boat.admin" || tb.Table != "users" {
		t.Fatalf("定位失败: %v %+v %v", ok, tb, group)
	}
	if _, _, ok := findTableRule(rules, "edu.boat.admin", "nope"); ok {
		t.Fatalf("不存在表不应命中")
	}
	if _, _, ok := findTableRule(rules, "edu.boat.other", "users"); ok {
		t.Fatalf("不存在库不应命中")
	}
}

// TestTargetDBOf 解析优先级：表级 > 组级 > targets 默认 > 同名。
func TestTargetDBOf(t *testing.T) {
	rules := testRules()
	spec := &config.SourceSpec{}
	spec.Targets.MySQL.TargetDatabase = "global.merge"

	// 表级覆盖
	group, _ := findGroup(rules, "edu.boat.admin")
	_, tb, _ := findTableRule(rules, "edu.boat.admin", "orders")
	if got := targetDBOf(spec, group, &tb, "edu.boat.admin"); got != "merge.orders" {
		t.Fatalf("表级覆盖: %s", got)
	}
	// 组级
	_, tb2, _ := findTableRule(rules, "edu.boat.admin", "users")
	if got := targetDBOf(spec, group, &tb2, "edu.boat.admin"); got != "global.merge" {
		t.Fatalf("组级空应落到全局: %s", got)
	}
	// 组级覆盖
	group2, _ := findGroup(rules, "edu.boat.content")
	_, tb3, _ := findTableRule(rules, "edu.boat.content", "posts")
	if got := targetDBOf(spec, group2, &tb3, "edu.boat.content"); got != "merge.content" {
		t.Fatalf("组级覆盖: %s", got)
	}
	// 同名回退（组级也空时才轮到同名）
	spec.Targets.MySQL.TargetDatabase = ""
	if got := targetDBOf(spec, group, &tb2, "edu.boat.admin"); got != "edu.boat.admin" {
		t.Fatalf("同名回退: %s", got)
	}
}

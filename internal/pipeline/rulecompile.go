package pipeline

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"

	"gocdc/internal/config"
	"gocdc/internal/model"
)

// CompiledRules 组形态规则（tables 白名单，唯一形态）的运行时编译产物：
//   - 组级 sinks/ddl_policy/target_database 拷贝到每表 → 每表独立生效规则；
//   - tables 是白名单：未列出的表（含未来新建的表）Match 返回 nil；
//   - EffectiveRules 输出全量生效视图（含启动后尚未见过事件的表）；
//     Groups 输出组原文（Web 库级摘要）。
type CompiledRules struct {
	mu      sync.RWMutex
	groups  []config.Rule          // 组原文（配置顺序；Web 摘要用）
	tables  map[string]*model.Rule // "db.table" 小写 → 生效规则（编译期全量构建）
	schemas map[string]bool        // 出现过表规则的小写 schema（DDL 放行判据）
}

// CompileRules 编译组规则；结构性错误（缺 sinks/tables 为空/缺 do_stock/重复表）
// 返回错误——生产路径 Validate 已前置把关，此处为直接调用方的最后防线。
func CompileRules(rules []config.Rule) (*CompiledRules, error) {
	cr := &CompiledRules{
		tables:  map[string]*model.Rule{},
		schemas: map[string]bool{},
	}
	for gi := range rules {
		g := &rules[gi]
		if g.Sinks == nil {
			return nil, fmt.Errorf("规则编译: 组 %s 缺 sinks", g.Schema)
		}
		if len(g.Tables) == 0 {
			return nil, fmt.Errorf("规则编译: 组 %s tables 为空（白名单须逐表列出）", g.Schema)
		}
		cr.groups = append(cr.groups, *g)
		ddl := g.DDLPolicy
		if ddl == "" {
			ddl = "pause" // 安全默认（08/16 修订）
		}
		for ti := range g.Tables {
			t := &g.Tables[ti]
			if t.Table == "" {
				return nil, fmt.Errorf("规则编译: 组 %s 含空表名", g.Schema)
			}
			if t.DoStock == nil {
				return nil, fmt.Errorf("规则编译: %s.%s 缺 do_stock（须显式声明是否参与全量）", g.Schema, t.Table)
			}
			key := strings.ToLower(g.Schema) + "." + strings.ToLower(t.Table)
			if _, dup := cr.tables[key]; dup {
				return nil, fmt.Errorf("规则编译: 表级规则重复（%s）", key)
			}
			m := &model.Rule{
				Schema:         g.Schema,
				Table:          t.Table,
				OrderByColumn:  t.OrderByColumn,
				DoStock:        *t.DoStock,
				DDLPolicy:      ddl,
				TargetDatabase: firstNonEmpty(t.TargetDatabase, g.TargetDatabase),
				ExcludeColumns: append([]string(nil), t.ExcludeColumns...),
				MaskColumns:    maskFrom(t.MaskColumns),
			}
			m.Key = key
			m.Sinks = sinksFrom(g.Sinks)
			cr.tables[key] = m
			cr.schemas[strings.ToLower(g.Schema)] = true
		}
	}
	return cr, nil
}

// Match 返回某表的生效规则；tables 白名单未命中（未列入即不同步）返回 nil。
// 编译期已全量构建，热路径是一次 map 查找（binlog 每行事件调用）。
func (cr *CompiledRules) Match(db, table string) *model.Rule {
	cr.mu.RLock()
	defer cr.mu.RUnlock()
	return cr.tables[strings.ToLower(db)+"."+strings.ToLower(table)]
}

// MatchByKey 取已编译表的生效规则（"db.table" 小写键；未命中返回 nil）。
// 供路由/视图层按 Key 复用。
func (cr *CompiledRules) MatchByKey(key string) *model.Rule {
	cr.mu.RLock()
	defer cr.mu.RUnlock()
	return cr.tables[key]
}

// EffectiveRules 全量生效规则快照（按 Key 排序；含尚未见过事件的表——
// 白名单在编译期即完整可知，不依赖运行期是否见过事件）。
func (cr *CompiledRules) EffectiveRules() []model.Rule {
	cr.mu.RLock()
	defer cr.mu.RUnlock()
	keys := make([]string, 0, len(cr.tables))
	for k := range cr.tables {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]model.Rule, 0, len(keys))
	for _, k := range keys {
		out = append(out, *cr.tables[k])
	}
	return out
}

// KnownTables 白名单表数。
func (cr *CompiledRules) KnownTables() int {
	cr.mu.RLock()
	defer cr.mu.RUnlock()
	return len(cr.tables)
}

// SchemaHasTables 该库是否存在任一表规则（Dispatcher 对无法定位目标表的
// DDL 的放行判据：仅表级白名单的库，其 DDL 也须进入 pause/审批流程，
// 不得静默丢弃）。
func (cr *CompiledRules) SchemaHasTables(schema string) bool {
	cr.mu.RLock()
	defer cr.mu.RUnlock()
	return cr.schemas[strings.ToLower(schema)]
}

// Groups 组原文快照（Web 库级摘要；配置顺序，值拷贝）。
func (cr *CompiledRules) Groups() []config.Rule {
	cr.mu.RLock()
	defer cr.mu.RUnlock()
	out := make([]config.Rule, len(cr.groups))
	copy(out, cr.groups)
	return out
}

// ── 组级 → 每表生效规则的字段映射 ───────────────────────────────────────

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

func maskFrom(in map[string]config.MaskSpec) map[string]model.MaskSpec {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]model.MaskSpec, len(in))
	for col, spec := range in {
		out[col] = model.MaskSpec{
			Type: spec.Type, Value: spec.Value, Keep: spec.Keep,
			Salt: resolveSalt(spec),
		}
	}
	return out
}

// resolveSalt 编译期解析盐环境变量（10 §5：盐不进 etcd 明文）。
// 变量缺失 → 空盐（哈希无盐退化为可反查；config 校验已查名字合法性，
// 运行期缺失在此降级，启动检查器（internal/check）负责告警）。
func resolveSalt(spec config.MaskSpec) string {
	if spec.SaltEnv == "" {
		return ""
	}
	return os.Getenv(spec.SaltEnv)
}

func sinksFrom(s *config.RuleSinks) model.RuleSinks {
	m := model.RuleSinks{}
	if s == nil {
		return m
	}
	m.MySQL = s.MySQL.Enabled
	m.Kafka = s.Kafka.Enabled
	m.RocketMQ = s.RocketMQ.Enabled
	m.KafkaTopic = s.Kafka.Topic
	m.RocketMQTopic = s.RocketMQ.Topic
	m.RocketMQTag = s.RocketMQ.Tag
	return m
}

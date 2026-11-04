// 原始正文手术与重载（Web 规则编辑 PUT 链路的服务端底座，13 §3）：
//
//	LoadFromRaw      原始 YAML 字节走完整加载链（parse → ${ENV} →
//	                 defaults → Validate），Load 的纯内存变体（可测/可复用）；
//	SpliceSourceRules yaml.Node 层手术——只替换指定源的 rules 段，段外内容
//	                 的注释文本/键序/${ENV} 占位/引号风格保留（缩进/折行/空行
//	                 由重编码统一为 2 空格风格，非逐字节）；
//	DiffRules        规则组新旧 diff（预检范围的纯函数决策）。
//
// 铁律：合并绝不做 struct 往返——Load 链的 ResolveEnv 会把 ${ENV} 解析成真值，
// 以解析后的 Config 重新序列化会把明文密码写回配置中心并毁掉全部注释；
// 手术在未解析的原始字节上进行，校验用替换后正文的新副本，读与写彻底分离。
package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"gopkg.in/yaml.v3"
)

// LoadFromRaw 从原始配置正文加载（与 Load 同一条解析/校验链，仅省去读取来源）。
// 返回 Config 与全部错误（调用方决定失败语义——启动退出 / Web 400）。
func LoadFromRaw(raw []byte, flagCluster string) LoadResult {
	tree, perr := parseYAML(raw)
	if perr != nil {
		return LoadResult{Errors: []error{perr}}
	}
	resolved, envErrs := ResolveEnv(tree)
	if verrs := rejectV1Form(resolved); len(verrs) > 0 {
		return LoadResult{Errors: append(envErrs, verrs...)}
	}
	buf, err := json.Marshal(resolved)
	if err != nil {
		return LoadResult{Errors: append(envErrs, fmt.Errorf("配置重新编码失败: %w", err))}
	}
	var cfg Config
	if err := json.Unmarshal(buf, &cfg); err != nil {
		return LoadResult{Errors: append(envErrs, fmt.Errorf("配置结构解析失败: %w", err))}
	}
	errs := append([]error{}, envErrs...)
	ApplyDefaults(&cfg)
	errs = append(errs, Validate(&cfg, flagCluster)...)
	return LoadResult{Config: &cfg, Errors: errs}
}

// SpliceSourceRules 在 raw 的 yaml.Node 层替换 sourceID 源的 rules 段，返回
// 新正文字节：定位 sources[] 中 id==sourceID 的条目，替换/追加其 rules 键
// （正文无 sources 键 = 已移除的 v1 顶层形态，报错给改写指引）。
//
// rules 值节点由 JSON 编码桥接（json tags 即权威字段名），再经 plainSafe
// 归一去掉不必要的双引号——安全值（如 mask 算法名 null/true）保留引号，
// 防裸字面量被 YAML 解析成 null/bool（内存教训：枚举值须引号）。
// 被替换段内部的注释随重写丢失；段外注释文本/键序/占位/引号风格保留
// （encodeNode 重编码统一 2 空格缩进与折行，正文若有其他缩进风格会被归一）。
func SpliceSourceRules(raw []byte, sourceID string, rules []Rule) ([]byte, error) {
	var doc yaml.Node
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		return nil, fmt.Errorf("配置 YAML 解析失败: %w", err)
	}
	if len(doc.Content) == 0 || doc.Content[0].Kind != yaml.MappingNode {
		return nil, fmt.Errorf("配置正文须为 YAML mapping（顶层键值形态）")
	}
	root := doc.Content[0]

	valNode, err := rulesValueNode(rules)
	if err != nil {
		return nil, err
	}

	if srcs := mappingValue(root, "sources"); srcs != nil {
		if srcs.Kind != yaml.SequenceNode {
			return nil, fmt.Errorf("sources 须为列表")
		}
		for _, item := range srcs.Content {
			if item.Kind != yaml.MappingNode {
				continue
			}
			if idv := mappingValue(item, "id"); idv != nil && idv.Kind == yaml.ScalarNode && idv.Value == sourceID {
				setMappingValue(item, "rules", valNode)
				return encodeNode(&doc)
			}
		}
		return nil, fmt.Errorf("sources 中不存在 id=%q 的源（PUT 只改既有源的规则；加源须走配置正文新增条目）", sourceID)
	}
	return nil, fmt.Errorf("配置正文无 sources 键（顶层 source/targets/rules 形态已移除，请改写为 sources[] 后再编辑）")
}

// rulesValueNode rules 值节点：struct → JSON（权威字段名）→ yaml.Node →
// 风格归一（block 展开 + 布尔/字符串简写 + 引号安全化）。空数组同样走全链
// （rules: []——校验层报「rules 不能为空」）。
func rulesValueNode(rules []Rule) (*yaml.Node, error) {
	buf, err := json.Marshal(rules)
	if err != nil {
		return nil, fmt.Errorf("规则序列化失败: %w", err)
	}
	var doc yaml.Node
	if err := yaml.Unmarshal(buf, &doc); err != nil {
		return nil, fmt.Errorf("规则 YAML 编码失败: %w", err)
	}
	if len(doc.Content) == 0 {
		return nil, fmt.Errorf("规则节点为空")
	}
	root := doc.Content[0]
	collapseSingletonMaps(root) // JSON 对象 → bool/字符串简写（sinks/mask 惯用形态）
	normalizeQuotes(root)
	clearFlowStyle(root)
	return root, nil
}

// collapseSingletonMaps 规则子树的简写归一：只作用于 sinks / mask_columns
// 两个容器——条目值为单键对象 {enabled: x} → x（RuleSink bool 简写）、
// {type: "s"} → "s"（MaskSpec 字符串简写），其余结构原样深入。
//
// 不得盲折叠整棵子树：mask_columns 的键是用户数据（列名）——单列名为
// enabled/type 时，容器自身会被误当成简写对象整体替换为其值（MaskSpec 对象
// 顶掉全列映射），解码必炸或语义全错（2026-11-04 review 抓到）。
func collapseSingletonMaps(n *yaml.Node) {
	if n == nil {
		return
	}
	switch n.Kind {
	case yaml.MappingNode:
		for i := 0; i+1 < len(n.Content); i += 2 {
			key, val := n.Content[i], n.Content[i+1]
			switch key.Value {
			case "sinks":
				collapseShorthandEntries(val, "enabled")
			case "mask_columns":
				collapseShorthandEntries(val, "type")
			default:
				collapseSingletonMaps(val) // 组/表层级继续深入
			}
		}
	case yaml.SequenceNode:
		for _, c := range n.Content {
			collapseSingletonMaps(c)
		}
	}
}

// collapseShorthandEntries 简写容器（sinks/mask_columns）内逐条目折叠：值为
// 单键 {shorthandKey: x} 的对象 → 标量 x。容器本身不动。
func collapseShorthandEntries(container *yaml.Node, shorthandKey string) {
	if container == nil || container.Kind != yaml.MappingNode {
		return
	}
	for i := 0; i+1 < len(container.Content); i += 2 {
		val := container.Content[i+1]
		if val.Kind == yaml.MappingNode && len(val.Content) == 2 &&
			val.Content[0].Kind == yaml.ScalarNode && val.Content[0].Value == shorthandKey {
			*val = *val.Content[1]
		}
	}
}

// normalizeQuotes 递归去掉值可安全裸写的双引号（plainSafe：以无 style 解析回
// 同一字符串才去）；null/true/数字形态等危险值保留引号。
func normalizeQuotes(n *yaml.Node) {
	if n == nil {
		return
	}
	if n.Kind == yaml.ScalarNode && n.Tag == "!!str" && n.Style&yaml.DoubleQuotedStyle != 0 && plainSafeScalar(n.Value) {
		n.Style &^= yaml.DoubleQuotedStyle
	}
	for _, c := range n.Content {
		normalizeQuotes(c)
	}
}

// clearFlowStyle 展开 flow 风格（JSON 桥接产物为 flow）为 block——与既有
// 配置正文的块级形态一致，可读性不降级。
func clearFlowStyle(n *yaml.Node) {
	if n == nil {
		return
	}
	n.Style &^= yaml.FlowStyle
	for _, c := range n.Content {
		clearFlowStyle(c)
	}
}

// plainSafeScalar 该字符串以 plain 风格解析是否还原为同一字符串（防裸写
// null/true/数字/日期等被 YAML 1.2 核心解析成非 string）。
func plainSafeScalar(v string) bool {
	var probe any
	if err := yaml.Unmarshal([]byte(v), &probe); err != nil {
		return false
	}
	s, ok := probe.(string)
	return ok && s == v
}

// mappingValue mapping 节点按 key 取 value 节点（无该键返回 nil）。
func mappingValue(m *yaml.Node, key string) *yaml.Node {
	if m == nil || m.Kind != yaml.MappingNode {
		return nil
	}
	for i := 0; i+1 < len(m.Content); i += 2 {
		if k := m.Content[i]; k.Kind == yaml.ScalarNode && k.Value == key {
			return m.Content[i+1]
		}
	}
	return nil
}

// setMappingValue 替换或追加 mapping 的键值对。
func setMappingValue(m *yaml.Node, key string, val *yaml.Node) {
	for i := 0; i+1 < len(m.Content); i += 2 {
		if k := m.Content[i]; k.Kind == yaml.ScalarNode && k.Value == key {
			m.Content[i+1] = val
			return
		}
	}
	keyNode := &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: key}
	m.Content = append(m.Content, keyNode, val)
}

// encodeNode 文档节点 → YAML 字节（2 空格缩进，与 deploy 配置形态一致）。
// 注意：这是整文档重编码——缩进/折行/空行按编码器风格归一，非逐字节拷贝；
// 节点内容（注释文本/键序/标量原值/引号风格）来自解析树，语义保留。
func encodeNode(doc *yaml.Node) ([]byte, error) {
	var buf bytes.Buffer
	enc := yaml.NewEncoder(&buf)
	enc.SetIndent(2)
	if err := enc.Encode(doc); err != nil {
		return nil, fmt.Errorf("配置 YAML 重编码失败: %w", err)
	}
	if err := enc.Close(); err != nil {
		return nil, fmt.Errorf("配置 YAML 重编码收尾失败: %w", err)
	}
	return buf.Bytes(), nil
}

// ── 规则 diff（预检范围决策，纯函数）──────────────────────────────

// TableRef 一张表的定位（diff 结果项）。
type TableRef struct {
	Schema string `json:"schema"`
	Table  string `json:"table"`
}

// DiffRules 新旧规则组 diff（大小写不敏感定位——白名单匹配与 information_schema
// 同为 ci 口径）。added/removed 逐表；modified 含两类：表级字段变化（逐表）、
// 组级字段变化（sinks/ddl_policy/target_database——影响组内全部表，整组标记）。
// 组/表的重排不算变化。输出按新配置顺序（removed 按旧配置顺序）。
func DiffRules(oldRules, newRules []Rule) (added, removed, modified []TableRef) {
	oldGroups := groupIndex(oldRules)
	newGroups := groupIndex(newRules)

	// 旧有新无 = removed（组消失则组内全部表 removed）
	for _, og := range oldRules {
		ng, ok := newGroups[strings.ToLower(og.Schema)]
		if !ok {
			for _, t := range og.Tables {
				removed = append(removed, TableRef{Schema: og.Schema, Table: t.Table})
			}
			continue
		}
		ntables := tableIndex(newRules[ng])
		for _, t := range og.Tables {
			if _, ok := ntables[strings.ToLower(t.Table)]; !ok {
				removed = append(removed, TableRef{Schema: og.Schema, Table: t.Table})
			}
		}
	}

	// 新有旧无 = added；两边都有且字段变化 = modified
	for _, ng := range newRules {
		oi, ok := oldGroups[strings.ToLower(ng.Schema)]
		groupChanged := false
		if ok {
			og := oldRules[oi]
			groupChanged = !reflect.DeepEqual(og.Sinks, ng.Sinks) ||
				og.DDLPolicy != ng.DDLPolicy || og.TargetDatabase != ng.TargetDatabase
		}
		otables := map[string]bool{}
		if ok {
			for _, t := range oldRules[oi].Tables {
				otables[strings.ToLower(t.Table)] = true
			}
		}
		for _, t := range ng.Tables {
			ref := TableRef{Schema: ng.Schema, Table: t.Table}
			if !ok || !otables[strings.ToLower(t.Table)] {
				added = append(added, ref)
				continue
			}
			if groupChanged || tableChanged(oldRules[oi], t) {
				modified = append(modified, ref)
			}
		}
	}
	return added, removed, modified
}

// groupIndex 小写 schema → 组下标（重复组由校验层报错，此处取首现）。
func groupIndex(rules []Rule) map[string]int {
	out := make(map[string]int, len(rules))
	for i := range rules {
		k := strings.ToLower(rules[i].Schema)
		if _, dup := out[k]; !dup {
			out[k] = i
		}
	}
	return out
}

// tableIndex 小写表名 → 组内下标。
func tableIndex(g Rule) map[string]int {
	out := make(map[string]int, len(g.Tables))
	for i := range g.Tables {
		k := strings.ToLower(g.Tables[i].Table)
		if _, dup := out[k]; !dup {
			out[k] = i
		}
	}
	return out
}

// tableChanged 同名表的新条目相对旧组内条目是否变化。
func tableChanged(oldGroup Rule, newTable TableRule) bool {
	idx := tableIndex(oldGroup)
	i, ok := idx[strings.ToLower(newTable.Table)]
	if !ok {
		return false
	}
	old := oldGroup.Tables[i]
	if (old.DoStock == nil) != (newTable.DoStock == nil) {
		return true
	}
	if old.DoStock != nil && newTable.DoStock != nil && *old.DoStock != *newTable.DoStock {
		return true
	}
	return old.OrderByColumn != newTable.OrderByColumn ||
		old.TargetDatabase != newTable.TargetDatabase ||
		!stringSetEqual(old.ExcludeColumns, newTable.ExcludeColumns) ||
		!reflect.DeepEqual(normalizeMasks(old.MaskColumns), normalizeMasks(newTable.MaskColumns))
}

// stringSetEqual 列表按集合比较（exclude_columns 语义为集合，顺序无关）。
func stringSetEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	seen := make(map[string]int, len(a))
	for _, s := range a {
		seen[s]++
	}
	for _, s := range b {
		seen[s]--
		if seen[s] < 0 {
			return false
		}
	}
	return true
}

// normalizeMasks 拷贝（防 DeepEqual 前调用方改写；map 比较本身无副作用，
// 统一入口便于后续加归一规则）。
func normalizeMasks(m map[string]MaskSpec) map[string]MaskSpec {
	out := make(map[string]MaskSpec, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

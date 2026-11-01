package pipeline

import (
	"regexp"
	"strings"
)

// ddlRefRe 抓取单表 DDL 的表引用段（08 §3 barrier 的表定位）：
// TABLE/EXISTS 关键字后的首个 [\w$.`"] 连续段（到空白为止）。
// 覆盖 ALTER/CREATE/DROP/TRUNCATE/RENAME TABLE [IF [NOT] EXISTS] 形态；
// 多表 RENAME 等复杂形态在引用段解析失败 → 空表名 → pause（安全兜底，不猜）。
var ddlRefRe = regexp.MustCompile(
	"(?is)^\\s*(?:ALTER|CREATE|DROP|TRUNCATE|RENAME)\\s+TABLE\\s+(?:IF\\s+(?:NOT\\s+)?EXISTS\\s+)?([\\w$.`\"]+)")

// ddlTarget 从 DDL 文本解析目标表（schema 可空 = 语句未限定，用会话默认库）。
// 库名允许点（`edu.mall.admin`.users——多源库按 . 聚合命名，schemaRe 同语义）：
// 带点库名的首个点不得当库名分隔符，否则解析出 schema=edu/table=mall 被
// want 静默丢弃，auto/pause 双策略全部失效。
// 解析按 MySQL 语义（双引号归一为反引号）：
//   - 引号开头：`db(可含点)` 后跟 .tbl —— 引号内整体为库名
//   - 引号在段中：裸 db 后跟 `tbl` —— 引号包的是表
//   - 无引号：db.tbl 两段或 tbl 一段（无引号限定名 MySQL 最多两段，
//     三段以上属非法输入 → 解析失败 pause 兜底）
func ddlTarget(query, defaultSchema string) (schema, table string) {
	m := ddlRefRe.FindStringSubmatch(query)
	if m == nil {
		return "", ""
	}
	ref := strings.ReplaceAll(m[1], `"`, "`")

	if i := strings.Index(ref, "`"); i >= 0 {
		var db, tbl string
		if i == 0 { // 引号开头：`db(可含点)`.tbl 或 `tbl`（引号包表）
			j := strings.Index(ref[1:], "`")
			if j < 0 {
				return "", "" // 引号不闭合：复杂形态，pause 兜底
			}
			db = ref[1 : 1+j]
			rest := ""
			if 2+j < len(ref) {
				rest = ref[2+j:]
			}
			if rest == "" { // `tbl`：引号内就是表名
				return defaultSchema, db
			}
			if !strings.HasPrefix(rest, ".") {
				return "", "" // 残余非法（如多表 RENAME 的 TO 子句尾巴）
			}
			tbl = strings.Trim(strings.TrimPrefix(rest, "."), "`")
		} else { // db.`tbl`：ref[:i] 含分隔点，必须去掉尾点——否则 schema="shop."
			// 被 rules.Match 判非同步表静默丢弃（保留字表名如 shop.`order`
			// 是典型场景，DDL 将不挂起不同步且无任何日志）
			db = strings.TrimSuffix(ref[:i], ".")
			tbl = strings.Trim(ref[i+1:], "`")
		}
		if db == "" || tbl == "" || strings.ContainsAny(tbl, ".` ") {
			return "", "" // 表段残余非法
		}
		return db, tbl
	}

	parts := strings.Split(ref, ".")
	switch len(parts) {
	case 1:
		return defaultSchema, parts[0]
	case 2:
		return parts[0], parts[1]
	default:
		return "", "" // 无引号三段以上：MySQL 非法输入，pause 兜底
	}
}

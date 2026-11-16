package schema

import (
	"strings"
)

// parsed COLUMN_TYPE 的解析产物（如 "decimal(10,2) unsigned"、"enum('a','b')"）。
type parsed struct {
	base     string   // 基类型名（decimal/int/varchar/enum...，小写归一）
	unsigned bool     // 属性词 unsigned/zerofill（zerofill 隐含 unsigned）
	hasLen   bool     // 带括号参数
	m1, m2   int      // 括号参数：decimal→(精度,标度)；其余→(长度/fsp)
	vals     []string // enum/set 取值（已去转义，保留原始大小写）
}

// parseColumnType 解析 information_schema.COLUMNS.COLUMN_TYPE 文本。
// MySQL 生成语法：base[(M[,D])] [unsigned [zerofill]] 与 enum/set('v1','v2',...)。
//
// 2026-11-16 review P1-1/P1-2 修复：
//   - 括号内取值（enum/set）保留原始大小写——取值字面量是数据而非语法，
//     整体 ToLower 会不可逆破坏（enum('New') → 'new'，MQ 下游直接收到错值）；
//     只有小写的部分是括号前的基类型名与括号后的属性词。
//   - 属性词按词判定而非后缀匹配——"unsigned zerofill" 不以 " unsigned"
//     结尾导致 unsigned 彻底丢失（>2^63 的无符号键被按有符号解释为负数）。
func parseColumnType(s string) parsed {
	var p parsed
	s = strings.TrimSpace(s)

	// 拆三段：head（括号前）/ inner（括号内，原样保留）/ tail（括号后属性串）。
	head, inner, tail := s, "", ""
	if open := strings.IndexByte(s, '('); open >= 0 {
		if closed := strings.LastIndexByte(s, ')'); closed > open {
			head, inner, tail = s[:open], s[open+1:closed], s[closed+1:]
		} else {
			head = s[:open] // 括号不闭合的异常文本：退化为无参
		}
	}

	// 基类型名 = head 的首段标识符（类型名均为纯字母单词）；head 剩余部分
	// 视为属性词（覆盖无括号形态 "int unsigned" 的属性长在 head 内的场景）。
	ident, attrs := splitIdent(head)
	p.base = strings.ToLower(ident)
	applyAttrs(&p, attrs)
	applyAttrs(&p, tail)

	if p.base == "enum" || p.base == "set" {
		p.vals = parseQuotedList(inner)
		return p
	}
	if inner == "" {
		return p
	}
	parts := strings.SplitN(inner, ",", 2)
	n, ok := atoiAll(parts[0])
	if !ok {
		return p
	}
	p.hasLen, p.m1 = true, n
	if len(parts) == 2 {
		if m2, ok := atoiAll(parts[1]); ok {
			p.m2 = m2
		}
	}
	return p
}

// splitIdent 拆出 head 的首段类型标识符与其余部分。
func splitIdent(head string) (ident, rest string) {
	head = strings.TrimSpace(head)
	i := 0
	for i < len(head) && isTypeChar(head[i]) {
		i++
	}
	return head[:i], head[i:]
}

// isTypeChar 类型名字符集（纯字母；MySQL 类型名不含数字/下划线）。
func isTypeChar(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z'
}

// applyAttrs 属性词判定：unsigned/zerofill 置无符号位（zerofill 在 MySQL
// 隐含 unsigned）。未知属性词原样忽略（前向兼容新语法）。
func applyAttrs(p *parsed, tail string) {
	for _, w := range strings.Fields(strings.ToLower(tail)) {
		switch w {
		case "unsigned", "zerofill":
			p.unsigned = true
		}
	}
}

// parseQuotedList 解析 'a','b”q','c\\d' 形态的取值列表（” 转义单引号、
// 反斜杠转义——与 MySQL enum/set 字面量规则一致）。
func parseQuotedList(s string) []string {
	var (
		out   []string
		cur   strings.Builder
		inStr bool
	)
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case !inStr:
			if c == '\'' {
				inStr = true
			}
		case c == '\\' && i+1 < len(s): // 反斜杠转义
			i++
			cur.WriteByte(s[i])
		case c == '\'':
			if i+1 < len(s) && s[i+1] == '\'' { // '' → 字面单引号
				i++
				cur.WriteByte('\'')
			} else {
				out = append(out, cur.String())
				cur.Reset()
				inStr = false
			}
		default:
			cur.WriteByte(c)
		}
	}
	return out
}

func atoiAll(s string) (int, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, false
	}
	n := 0
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return 0, false
		}
		n = n*10 + int(s[i]-'0')
	}
	return n, true
}

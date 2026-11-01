package schema

import (
	"strings"
)

// parsed COLUMN_TYPE 的解析产物（如 "decimal(10,2) unsigned"、"enum('a','b')"）。
type parsed struct {
	base     string   // 基类型名（decimal/int/varchar/enum...）
	unsigned bool     // 尾缀 " unsigned"
	hasLen   bool     // 带括号参数
	m1, m2   int      // 括号参数：decimal→(精度,标度)；其余→(长度/fsp)
	vals     []string // enum/set 取值（已去转义）
}

// parseColumnType 解析 information_schema.COLUMNS.COLUMN_TYPE 文本。
// MySQL 生成语法：base[(M[,D])] [" unsigned"] 与 enum/set('v1','v2',...)。
func parseColumnType(s string) parsed {
	var p parsed
	p.base = strings.ToLower(strings.TrimSpace(s))
	if strings.HasSuffix(p.base, " unsigned") {
		p.unsigned = true
		p.base = strings.TrimSuffix(p.base, " unsigned")
	}
	open := strings.IndexByte(p.base, '(')
	if open < 0 {
		return p
	}
	closed := strings.LastIndexByte(p.base, ')')
	if closed < open { // 异常文本：退化为无参
		p.base = p.base[:open]
		return p
	}
	inner := p.base[open+1 : closed]
	p.base = strings.TrimSpace(p.base[:open])

	if p.base == "enum" || p.base == "set" {
		p.vals = parseQuotedList(inner)
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

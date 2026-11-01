// SQL 生成（05 §5）：语句形态细节见 buildUpsert/buildDelete；类型绑定按
// 04 §7 产出（decimal 字符串/JSON []byte/BIT 整型），从 Event 取值即可。

package mysql

import (
	"sort"
	"strings"
)

// quoteIdent 标识符加反引号并转义内嵌反引号（库/表/列名一律经此，杜绝注入）。
func quoteIdent(s string) string {
	return "`" + strings.ReplaceAll(s, "`", "``") + "`"
}

// buildUpsert 生成 n 行的 INSERT ... ON DUPLICATE KEY UPDATE：
//
//	INSERT INTO `db`.`tbl` (`c1`,`c2`) VALUES (?,?),(?,?) ON DUPLICATE KEY UPDATE `c1`=VALUES(`c1`),...
//
// 返回 SQL 与参数个数（n × len(cols)）。
func buildUpsert(db, table string, cols []string, n int) string {
	var b strings.Builder
	b.WriteString("INSERT INTO ")
	b.WriteString(quoteIdent(db))
	b.WriteByte('.')
	b.WriteString(quoteIdent(table))
	b.WriteString(" (")
	for i, c := range cols {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(quoteIdent(c))
	}
	b.WriteString(") VALUES")
	ph := placeholders(len(cols))
	for i := 0; i < n; i++ {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteByte('(')
		b.WriteString(ph)
		b.WriteByte(')')
	}
	b.WriteString(" ON DUPLICATE KEY UPDATE ")
	for i, c := range cols {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(quoteIdent(c))
		b.WriteString("=VALUES(")
		b.WriteString(quoteIdent(c))
		b.WriteByte(')')
	}
	return b.String()
}

// buildDelete 生成 n 组主键谓词的批量 DELETE（联合主键多列谓词，05 §5.2）：
//
//	DELETE FROM `db`.`tbl` WHERE (`pk1`,`pk2`) IN ((?,?),(?,?))
func buildDelete(db, table string, pkCols []string, n int) string {
	var b strings.Builder
	b.WriteString("DELETE FROM ")
	b.WriteString(quoteIdent(db))
	b.WriteByte('.')
	b.WriteString(quoteIdent(table))
	b.WriteString(" WHERE (")
	for i, c := range pkCols {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(quoteIdent(c))
	}
	b.WriteString(") IN (")
	ph := placeholders(len(pkCols))
	for i := 0; i < n; i++ {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteByte('(')
		b.WriteString(ph)
		b.WriteByte(')')
	}
	b.WriteByte(')')
	return b.String()
}

func placeholders(n int) string {
	if n <= 0 {
		return ""
	}
	if n == 1 {
		return "?"
	}
	return strings.Repeat("?,", n-1) + "?"
}

// sortedKeys 返回 map 键的字典序清单（SQL 生成用稳定列序；列序不影响语义）。
func sortedKeys[T any](m map[string]T) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

package schema

import "testing"

// BenchmarkParseColumnType COLUMN_TYPE 解析（每次表结构加载一次，非每行热路径；
// 含 enum/set 取值列表与 zerofill 属性形态）。
func BenchmarkParseColumnType(b *testing.B) {
	cases := []string{
		"bigint(20) unsigned zerofill",
		"decimal(10,2) unsigned",
		`enum('New','Done','IN_PROGRESS','CANCELLED')`,
		"varchar(191)",
		"datetime(3)",
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = parseColumnType(cases[i%len(cases)])
	}
}

// BenchmarkQuoteIdent 标识符反引号（SQL 生成热路径，每列一次）。
func BenchmarkSchemaKey(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = key("Edu.Mall.Shop", "Orders")
	}
}

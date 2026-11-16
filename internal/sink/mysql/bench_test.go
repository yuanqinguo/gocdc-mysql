package mysql

import "testing"

// benchCols 宽表列清单（10 / 50 列两种规格）。
func benchCols(n int) []string {
	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, "col_"+itoa(i))
	}
	return out
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b [8]byte
	p := len(b)
	for i > 0 {
		p--
		b[p] = byte('0' + i%10)
		i /= 10
	}
	return string(b[p:])
}

// BenchmarkBuildUpsert_500x10 默认批大小 × 10 列（单条多行 INSERT）。
func BenchmarkBuildUpsert_500x10(b *testing.B) {
	cols := benchCols(10)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = buildUpsert("shop", "orders", cols, 500)
	}
}

// BenchmarkBuildUpsert_500x50 宽表（SQL 文本膨胀，关联 max_allowed_packet 风险）。
func BenchmarkBuildUpsert_500x50(b *testing.B) {
	cols := benchCols(50)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = buildUpsert("shop", "orders", cols, 500)
	}
}

// BenchmarkBuildDelete_500x2 联合主键批量 DELETE。
func BenchmarkBuildDelete_500x2(b *testing.B) {
	pk := []string{"a", "b"}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = buildDelete("shop", "orders", pk, 500)
	}
}

// BenchmarkFlatten_500x10 参数展平（每 batch 一次，分配量随列宽增长）。
func BenchmarkFlatten_500x10(b *testing.B) {
	rows := make([][]any, 500)
	for i := range rows {
		rows[i] = make([]any, 10)
		for j := range rows[i] {
			rows[i][j] = int64(i*10 + j)
		}
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = flatten(rows)
	}
}

package transform

import (
	"fmt"
	"testing"
	"time"

	"gocdc/internal/model"
)

// benchMeta 10 列典型表（int/bigint/decimal/varchar/datetime/enum/json/bit）。
func benchMeta() *model.TableMeta {
	return &model.TableMeta{
		Schema: "shop", Table: "orders",
		Columns: []model.Column{
			{Name: "id", Type: model.TypeBigint, IsPK: true},
			{Name: "user_id", Type: model.TypeBigint},
			{Name: "amount", Type: model.TypeDecimal, Precision: 12, Scale: 4},
			{Name: "qty", Type: model.TypeInt},
			{Name: "remark", Type: model.TypeVarchar, Precision: 128},
			{Name: "status", Type: model.TypeEnum, EnumValues: []string{"new", "paid", "void"}},
			{Name: "ext", Type: model.TypeJSON},
			{Name: "flags", Type: model.TypeBit},
			{Name: "created_at", Type: model.TypeDatetime},
			{Name: "phone", Type: model.TypeVarchar, Precision: 20},
		},
		PKColumns: []string{"id"},
	}
}

func benchRow(seed int) []any {
	return []any{
		int64(seed), int64(seed * 7), []byte("1234.5678"), int64(3),
		[]byte("hello world"), int64(2), []byte(`{"k":[1,2,3]}`), []byte{10},
		time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC), []byte("13800001234"),
	}
}

// BenchmarkRow_Insert 单行转换（无脱敏）：worker 最热路径。
func BenchmarkRow_Insert(b *testing.B) {
	tr := New(Options{})
	meta := benchMeta()
	in := RowInput{Meta: meta, Type: model.EventInsert, EventID: "e", After: benchRow(1)}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := tr.Row(in); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkRow_UpdateWithMask UPDATE + 两列脱敏（phone=phone, remark=hash_sha256）。
func BenchmarkRow_UpdateWithMask(b *testing.B) {
	tr := New(Options{})
	meta := benchMeta()
	rule := &model.Rule{
		Schema: "shop", Table: "orders", Key: "shop.orders",
		MaskColumns: map[string]model.MaskSpec{
			"phone":  {Type: model.MaskPhone},
			"remark": {Type: model.MaskHashSHA256, Salt: "s3cret"},
		},
	}
	in := RowInput{
		Meta: meta, Rule: rule, Type: model.EventUpdate, EventID: "e",
		Before: benchRow(1), After: benchRow(2),
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := tr.Row(in); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkRow_Parallel 并发转换（4 worker 形态）。
func BenchmarkRow_Parallel(b *testing.B) {
	tr := New(Options{})
	meta := benchMeta()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		n := 0
		in := RowInput{Meta: meta, Type: model.EventInsert, EventID: "e"}
		for pb.Next() {
			n++
			in.After = benchRow(n)
			in.EventID = fmt.Sprintf("e%d", n)
			if _, err := tr.Row(in); err != nil {
				b.Fatal(err)
			}
		}
	})
}

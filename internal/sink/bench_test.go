package sink

import (
	"testing"
	"time"

	"gocdc/internal/model"
)

// benchEvent 10 列事件（含 before/after/columns，模拟 UPDATE）。
func benchEvent() *model.Event {
	cols := map[string]model.ColumnMeta{}
	after := map[string]any{}
	before := map[string]any{}
	names := []string{"id", "user_id", "amount", "qty", "remark", "status", "ext", "flags", "created_at", "phone"}
	for i, n := range names {
		cols[n] = model.ColumnMeta{Type: "varchar"}
		after[n] = "value-" + n
		before[n] = "old-" + n
		_ = i
	}
	after["id"] = int64(1)
	before["id"] = int64(1)
	return &model.Event{
		EventID: "mysql-bin.000001:1234:0", Type: model.EventUpdate, Phase: model.PhaseBinlog,
		Database: "shop", Table: "orders", Ts: uint64(time.Now().Unix()),
		SourcePos: model.Position{Name: "mysql-bin.000001", Pos: 1234}, XID: 99,
		Pk: map[string]any{"id": int64(1)}, Before: before, After: after,
		Columns: cols, SchemaVersion: 3,
	}
}

// BenchmarkBuildMessage 事件 → MQ 消息（每事件一次，JSON 编码是主要成本）。
func BenchmarkBuildMessage(b *testing.B) {
	ev := benchEvent()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := BuildMessage(ev, OrderByRow, "default"); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkBuildMessage_InsertOnly 仅后像（INSERT 形态，消息体更小）。
func BenchmarkBuildMessage_InsertOnly(b *testing.B) {
	ev := benchEvent()
	ev.Type = model.EventInsert
	ev.Before = nil
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := BuildMessage(ev, OrderByRow, "default"); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkTopicName topic 派生（每事件一次）。
func BenchmarkTopicName(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = TopicName("CDC_", "", "edu.mall.shop", "orders")
	}
}

package pipeline

import (
	"sync/atomic"
	"testing"

	"gocdc/internal/model"
)

// BenchmarkWatermarkAck 单 goroutine ack（顺序推进）。
func BenchmarkWatermarkAck(b *testing.B) {
	wm := NewWatermark(model.Position{Name: "mysql-bin.000001", Pos: 4})
	b.ReportAllocs()
	var seq uint64
	for i := 0; i < b.N; i++ {
		seq++
		wm.SetPos(seq, model.Position{Name: "mysql-bin.000001", Pos: uint32(seq)})
		wm.Ack(seq)
	}
}

// BenchmarkWatermarkAckInterleaved 乱序 ack（模拟 N worker 分片：ack 顺序散乱，
// 水位洞收敛是热路径的一部分）。
func BenchmarkWatermarkAckInterleaved(b *testing.B) {
	wm := NewWatermark(model.Position{})
	const window = 4096
	b.ReportAllocs()
	for i := 0; i < b.N; i += window {
		for j := 0; j < window && i+j < b.N; j++ {
			wm.Ack(uint64(i + window - j)) // 逆序：制造 pending 重排
		}
		for j := 0; j < window && i+j < b.N; j++ {
			wm.Ack(uint64(i + j + 1))
		}
	}
}

// BenchmarkShard 表分片哈希（每个行事件一次）。
func BenchmarkShard(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = Shard("edu.mall.shop", "orders", 8)
	}
}

// BenchmarkWatermarkAckParallel 并发 ack（水位锁竞争度）。
func BenchmarkWatermarkAckParallel(b *testing.B) {
	wm := NewWatermark(model.Position{})
	var seq atomic.Uint64
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			n := seq.Add(1)
			wm.SetPos(n, model.Position{Name: "f", Pos: uint32(n)})
			wm.Ack(n)
		}
	})
}

package pipeline

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	sqldriver "github.com/go-sql-driver/mysql"
	"go.uber.org/zap"

	"gocdc/internal/model"
	"gocdc/internal/sink"
)

// flakySink 前 fail 次返回可重试错误（1213 死锁），之后成功。
type flakySink struct {
	mu     sync.Mutex
	fail   int
	calls  int
	permAt int // 第 N 次起返回不可重试错误（0=不启用）
}

var errDeadlock = &sqldriver.MySQLError{Number: 1213, Message: "Deadlock found"}

func (f *flakySink) Name() string                { return "flaky" }
func (f *flakySink) Ping(context.Context) error  { return nil }
func (f *flakySink) Close(context.Context) error { return nil }
func (f *flakySink) Send(_ context.Context, _ []*model.Event) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.permAt > 0 && f.calls >= f.permAt {
		return sink.Permanent(errors.New("column mismatch"))
	}
	if f.calls <= f.fail {
		return errDeadlock
	}
	return nil
}

func newFlushWorker(sn sink.Sink) *Worker {
	return &Worker{
		sn: sn, wm: NewWatermark(model.Position{}), log: zap.NewNop(),
		batch: []*model.Event{{Database: "db", Table: "t", Type: model.EventInsert}},
		seqs:  []uint64{1},
	}
}

// TestWorkerFlush_RetryableRetries 可重试错误（死锁）原地退避重试到成功，
// 批不清空、seq 在成功后 ack（一次失败不得停摆整管线）。
func TestWorkerFlush_RetryableRetries(t *testing.T) {
	fs := &flakySink{fail: 2}
	w := newFlushWorker(fs)
	if err := w.flush(context.Background()); err != nil {
		t.Fatalf("重试后应成功: %v", err)
	}
	if fs.calls != 3 {
		t.Fatalf("应尝试 3 次，实际 %d", fs.calls)
	}
	if base, _ := w.wm.Current(); base != 1 {
		t.Fatalf("成功后应 ack seq1，base=%d", base)
	}
	if len(w.batch) != 0 {
		t.Fatal("成功后批应清空")
	}
}

// TestWorkerFlush_PermanentStops 不可重试错误立即上抛且批保留（现场
// 不破坏，人工修复后重发同一批）。
func TestWorkerFlush_PermanentStops(t *testing.T) {
	fs := &flakySink{permAt: 1}
	w := newFlushWorker(fs)
	err := w.flush(context.Background())
	if err == nil {
		t.Fatal("permanent 错误应上抛")
	}
	if !sink.IsPermanent(err) {
		t.Fatalf("应携带 permanent 标记: %v", err)
	}
	if len(w.batch) != 1 || len(w.seqs) != 1 {
		t.Fatal("失败后批必须保留（不清空、不 ack）")
	}
	if base, _ := w.wm.Current(); base != 0 {
		t.Fatal("失败不得 ack（位点不变式）")
	}
}

// TestWorkerFlush_ShutdownFinalAttempt 停机路径（ctx 已取消）仍给最后一次
// 发送机会（独立短超时 ctx），成功则 ack——用已取消 ctx 调 Send 必然失败。
func TestWorkerFlush_ShutdownFinalAttempt(t *testing.T) {
	fs := &flakySink{} // sink 健康
	w := newFlushWorker(fs)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := w.flush(ctx); err != nil {
		t.Fatalf("停机冲刷应成功: %v", err)
	}
	if base, _ := w.wm.Current(); base != 1 {
		t.Fatal("停机冲刷成功后应 ack")
	}
}

// TestWorkerFlush_BackoffProgress 退避窗口内 ctx 取消 → 返回（不无限阻塞）。
func TestWorkerFlush_BackoffProgress(t *testing.T) {
	fs := &flakySink{fail: 1000} // 永远失败（可重试）
	w := newFlushWorker(fs)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := w.flush(ctx)
	if err == nil {
		t.Fatal("ctx 取消后应返回错误")
	}
	if d := time.Since(start); d > 3*time.Second {
		t.Fatalf("应在 ctx 取消后及时返回，耗时 %v", d)
	}
}

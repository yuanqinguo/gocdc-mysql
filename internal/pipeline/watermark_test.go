package pipeline

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"gocdc/internal/config"
	"gocdc/internal/model"
	"gocdc/internal/position"
)

// TestWatermark_OrderAcks 乱序 ack 下位点精确（N=1 也可能乱序
// 由 batch 粒度 ack 造成）。
func TestWatermark_OrderAcks(t *testing.T) {
	wm := NewWatermark(model.Position{Name: "bin.000001", Pos: 4})
	// 锚点：seq5=XID(pos 500)、seq9=rotate(新文件)、seq12=XID(pos 900)
	wm.SetPos(5, model.Position{Name: "bin.000001", Pos: 500})
	wm.SetPos(9, model.Position{Name: "bin.000002", Pos: 4})
	wm.SetPos(12, model.Position{Name: "bin.000002", Pos: 900})

	// 乱序确认 8,7,6（1..5 未确认）→ base 停在 0、无推进、位点保持起点
	for _, s := range []uint64{8, 7, 6} {
		wm.Ack(s)
	}
	if safe, pos, ok := wm.Advance(); ok || safe != 0 || pos.Pos != 4 {
		t.Fatalf("乱序确认下 base 应停在 0: safe=%d ok=%v pos=%+v", safe, ok, pos)
	}
	// 补上 1..5 → base 跨过锚点 5，位点=500
	for _, s := range []uint64{1, 2, 3, 4, 5} {
		wm.Ack(s)
	}
	safe, pos, ok := wm.Advance()
	if !ok || safe != 8 || pos.Name != "bin.000001" || pos.Pos != 500 {
		t.Fatalf("跨过 XID 锚点: safe=%d pos=%+v ok=%v", safe, pos, ok)
	}
	// 11,10 → base=8（9 是 rotate 锚点未确认）
	wm.Ack(11)
	wm.Ack(10)
	if safe, _, _ := wm.Advance(); safe != 8 {
		t.Fatalf("rotate 未确认时 base=8: %d", safe)
	}
	// 9 → 跨文件，位点=bin.000002:4（04 §3：rotate 屏障后才可跨文件记位点）
	wm.Ack(9)
	safe, pos, ok = wm.Advance()
	if !ok || safe != 11 || pos.Name != "bin.000002" || pos.Pos != 4 {
		t.Fatalf("跨 rotate: safe=%d pos=%+v", safe, pos)
	}
	// 无推进时 Advance 返回 false
	if _, _, ok := wm.Advance(); ok {
		t.Fatal("无推进应 ok=false")
	}
	// 12 → 最终位点 900
	wm.Ack(12)
	safe, pos, _ = wm.Advance()
	if safe != 12 || pos.Pos != 900 {
		t.Fatalf("终值: safe=%d pos=%+v", safe, pos)
	}
	if wm.Pending() != 0 {
		t.Fatalf("pending 应清空: %d", wm.Pending())
	}
}

// TestWatermark_ConcurrentAcks 并发乱序 ack 1 万事件位点精确。
func TestWatermark_ConcurrentAcks(t *testing.T) {
	wm := NewWatermark(model.Position{})
	wm.SetPos(10000, model.Position{Name: "f", Pos: 999999})
	var wg sync.WaitGroup
	for i := 1; i <= 100; i++ { // 100 goroutine 倒序+乱序混合 ack
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			for j := 10000; j >= 1; j-- {
				if (j+base)%7 == 0 {
					wm.Ack(uint64(j))
				}
			}
		}(i)
	}
	// 保证全部 seq 被 ack
	wg.Wait()
	for j := 1; j <= 10000; j++ {
		wm.Ack(uint64(j))
	}
	safe, pos, ok := wm.Advance()
	if !ok || safe != 10000 || pos.Pos != 999999 {
		t.Fatalf("万级乱序 ack 后位点: safe=%d pos=%+v", safe, pos)
	}
}

// TestWatermark_NeverAhead 位点不变式：未确认的锚点绝不成为 lastPos。
func TestWatermark_NeverAhead(t *testing.T) {
	wm := NewWatermark(model.Position{Name: "f", Pos: 4})
	wm.SetPos(100, model.Position{Name: "f", Pos: 1000})
	wm.Ack(100) // 只确认锚点自身，前面 1..99 未确认
	wm.Ack(1)
	_, pos, ok := wm.Advance()
	if !ok || pos.Pos != 4 {
		t.Fatalf("位点不得超前于未确认事件: %+v", pos)
	}
}

// TestCompileRules_Errors 组形态结构性错误（直接调用 CompileRules 的最后防线）。
func TestCompileRules_Errors(t *testing.T) {
	doTrue := true
	if _, err := CompileRules([]config.Rule{{Schema: "db"}}); err == nil {
		t.Fatal("缺 sinks 应报错")
	}
	if _, err := CompileRules([]config.Rule{{Schema: "db", Sinks: &config.RuleSinks{}}}); err == nil {
		t.Fatal("tables 为空应报错")
	}
	if _, err := CompileRules([]config.Rule{{Schema: "db",
		Sinks:  &config.RuleSinks{MySQL: config.RuleSink{Enabled: true}},
		Tables: []config.TableRule{{Table: "t"}}}}); err == nil {
		t.Fatal("缺 do_stock 应报错")
	}
	if _, err := CompileRules([]config.Rule{{Schema: "db",
		Sinks: &config.RuleSinks{MySQL: config.RuleSink{Enabled: true}},
		Tables: []config.TableRule{
			{Table: "t", DoStock: &doTrue},
			{Table: "t", DoStock: &doTrue},
		}}}); err == nil {
		t.Fatal("重复表应报错")
	}
}

// ── Persister ───────────────────────────────────────────────────────────

type memStore struct {
	mu     sync.Mutex
	saved  []model.Position
	gateOK bool
}

func (m *memStore) Load(_ context.Context) (model.Position, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.saved) == 0 {
		return model.Position{}, false, nil
	}
	return m.saved[len(m.saved)-1], true, nil
}

func (m *memStore) Save(_ context.Context, p model.Position) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.gateOK {
		return position.ErrGateRejected
	}
	m.saved = append(m.saved, p)
	return nil
}

func TestPersister_ForceAndCircuit(t *testing.T) {
	wm := NewWatermark(model.Position{Name: "f", Pos: 4})
	wm.SetPos(1, model.Position{Name: "f", Pos: 100})
	wm.Ack(1)
	store := &memStore{gateOK: true}

	circ := make(chan error, 1)
	p := NewPersister(wm, store, time.Hour, 3, "n1", 7, nil, nil, func(e error) { circ <- e }, nil)
	pctx, stopP := context.WithCancel(context.Background())
	defer stopP()
	go func() { _ = p.Run(pctx) }()
	t.Cleanup(stopP)

	// 强制保存：位点带 term/node/seq
	p.Force()
	deadline := time.After(2 * time.Second)
	for {
		store.mu.Lock()
		n := len(store.saved)
		store.mu.Unlock()
		if n > 0 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("强制保存未执行")
		case <-time.After(10 * time.Millisecond):
		}
	}
	store.mu.Lock()
	got := store.saved[0]
	store.mu.Unlock()
	if got.Term != 7 || got.NodeID != "n1" || got.Seq != 1 || got.Pos != 100 {
		t.Fatalf("落盘位点: %+v", got)
	}

	// gate 拒绝 → 立即熔断
	store.mu.Lock()
	store.gateOK = false
	store.mu.Unlock()
	p.Force()
	select {
	case err := <-circ:
		if !errors.Is(err, position.ErrGateRejected) {
			t.Fatalf("熔断原因: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("gate 拒绝应立即熔断")
	}
}

func TestPersister_MaxFailures(t *testing.T) {
	wm := NewWatermark(model.Position{})
	wm.SetPos(1, model.Position{Name: "f", Pos: 9})
	wm.Ack(1)
	store := &memStore{gateOK: false} // 全部失败
	circ := make(chan error, 1)
	p := NewPersister(wm, store, time.Hour, 3, "n", 0, nil, nil, func(e error) { circ <- e }, nil)
	pctx, stopP := context.WithCancel(context.Background())
	defer stopP()
	go func() { _ = p.Run(pctx) }()
	p.Force()
	select {
	case <-circ:
	case <-time.After(2 * time.Second):
		t.Fatal("连续失败达上限应熔断")
	}
}

var _ = fmt.Sprintf

// TestWatermark_DuplicateAckIdempotent 重复 ack 幂等：
// Worker 对同一 RowsEvent 每行 ack 同一 seq——推进后重复确认不得在 pending
// 残留（否则 K 行事件泄漏 K-1 条，Pending 指标虚高腐蚀洞检测告警）。
func TestWatermark_DuplicateAckIdempotent(t *testing.T) {
	wm := NewWatermark(model.Position{})
	wm.SetPos(1, model.Position{Name: "f", Pos: 100})
	wm.SetPos(2, model.Position{Name: "f", Pos: 200})
	wm.Ack(1)
	wm.Ack(2)
	// 已越过的 seq 重复 ack 一万次（模拟 100 行事件 × 多事件的重复确认量级）
	for i := 0; i < 10000; i++ {
		wm.Ack(1)
		wm.Ack(2)
	}
	if got := wm.Pending(); got != 0 {
		t.Fatalf("重复 ack 后 pending 应为 0，实际 %d（泄漏）", got)
	}
	base, pos := wm.Current()
	if base != 2 || pos.Pos != 200 {
		t.Fatalf("base/pos=%d/%d，期望 2/200", base, pos.Pos)
	}
}

// TestWatermark_DuplicateAckPendingHole 乱序窗口内重复 ack 不产生虚假 pending：
// seq2 先确认（洞），重复确认 seq2 不叠加；seq1 补齐后洞清零。
func TestWatermark_DuplicateAckPendingHole(t *testing.T) {
	wm := NewWatermark(model.Position{})
	wm.SetPos(2, model.Position{Name: "f", Pos: 200})
	for i := 0; i < 100; i++ {
		wm.Ack(2) // 未连续：pending 只应有 1 条
	}
	if got := wm.Pending(); got != 1 {
		t.Fatalf("重复的洞 ack 应只记 1 条 pending，实际 %d", got)
	}
	wm.Ack(1)
	if got := wm.Pending(); got != 0 {
		t.Fatalf("补齐后 pending 应为 0，实际 %d", got)
	}
}

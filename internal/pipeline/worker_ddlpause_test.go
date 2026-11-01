package pipeline

import (
	"context"
	"testing"
	"time"

	"gocdc/internal/model"
)

// fakeDDLGate 单测审批 gate：WaitDecision 阻塞至 decide 喂入决策。
type fakeDDLGate struct {
	decided  chan string
	recorded chan DDLRecord
}

func newFakeDDLGate() *fakeDDLGate {
	return &fakeDDLGate{decided: make(chan string, 1), recorded: make(chan DDLRecord, 8)}
}

func (g *fakeDDLGate) Record(_ context.Context, rec DDLRecord) error { g.recorded <- rec; return nil }
func (g *fakeDDLGate) Settled(_ context.Context, _ string) (string, bool, error) {
	return "", false, nil
}
func (g *fakeDDLGate) WaitDecision(ctx context.Context, _ string) (string, error) {
	select {
	case d := <-g.decided:
		return d, nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}
func (g *fakeDDLGate) Settle(_ context.Context, _, _, _ string) error { return nil }

// pauseOnDDL 测试用构造：仅装配 gate 依赖（flush 空批早退不触 sink）。
func newPauseTestWorker(hooks *DDLHooks) *Worker {
	return NewWorker(nil, 0, nil, nil, nil, nil, NewWatermark(model.Position{}),
		nil, hooks, nil, nil, 0, 0, nil)
}

// TestHandleDDL_UnlocatablePauseVisibility 无法定位目标表的 DDL：
// 记录 ID 按 binlog 坐标生成（重放稳定，防空 ID 互覆）+ 等待期 ddlPause
// 可见、决策后清零。
func TestHandleDDL_UnlocatablePauseVisibility(t *testing.T) {
	gate := newFakeDDLGate()
	hooks := &DDLHooks{Gate: gate}
	w := newPauseTestWorker(hooks)

	u := &EventUnit{Seq: 7, Kind: UnitDDL, Schema: "db1",
		Query: "SAVEPOINT `sp0xa1cb20`", Pos: model.Position{Name: "f", Pos: 100}}

	errCh := make(chan error, 1)
	go func() { errCh <- w.handleDDL(context.Background(), u) }()

	// 等待期：挂起记录带稳定 ID + ddlPause 叠加可见
	var rec DDLRecord
	select {
	case rec = <-gate.recorded:
	case <-time.After(2 * time.Second):
		t.Fatal("挂起记录未写入")
	}
	if rec.ID != "db1..f.100" {
		t.Fatalf("记录 ID 应按 binlog 坐标生成: %q", rec.ID)
	}
	if rec.Position != "f:100" {
		t.Fatalf("记录坐标: %+v", rec)
	}
	// Status 默认 pending 由 Gate.Record 实现补（fake 不模拟默认值）
	if rec.Status != "" && rec.Status != DecisionPending {
		t.Fatalf("记录状态: %+v", rec)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		if p := hooks.Pause(); p != nil {
			if p.SQL != u.Query {
				t.Fatalf("等待态 SQL: %+v", p)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("等待期 ddlPause 未置位")
		}
		time.Sleep(5 * time.Millisecond)
	}

	// skip 决策 → 返回 nil + 等待态清零
	gate.decided <- DecisionSkip
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("skip 后应恢复: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("决策后未返回")
	}
	if p := hooks.Pause(); p != nil {
		t.Fatalf("决策后 ddlPause 应清零: %+v", p)
	}
}

// TestPauseOnDDL_NoGateNoOverlay 无 gate 路径不停留等待态（fatal 停管线，走生命周期 paused）。
func TestPauseOnDDL_NoGateNoOverlay(t *testing.T) {
	hooks := &DDLHooks{} // Gate=nil
	w := newPauseTestWorker(hooks)
	err := w.pauseOnDDL(context.Background(),
		&EventUnit{Kind: UnitDDL, Schema: "db", Table: "t", Query: "ALTER TABLE t DROP COLUMN c"},
		DDLRecord{}, "db", "t", "ddl_policy=pause")
	if err == nil {
		t.Fatal("无 gate 应返回 ErrPaused 停管线")
	}
	if p := hooks.Pause(); p != nil {
		t.Fatalf("无 gate 不应置等待态: %+v", p)
	}
}

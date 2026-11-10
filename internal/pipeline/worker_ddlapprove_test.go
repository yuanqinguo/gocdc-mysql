package pipeline

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"gocdc/internal/config"
	"gocdc/internal/model"
	"gocdc/internal/schema"
	mysqlsink "gocdc/internal/sink/mysql"
)

// stubSchemaLoader 不触库的空结构加载器（handleDDL 的 Refresh 失败仅 Warn）。
type stubSchemaLoader struct{}

func (stubSchemaLoader) Load(_ context.Context, schemaName, table string) (*model.TableMeta, error) {
	return &model.TableMeta{Schema: schemaName, Table: table}, nil
}

// 2026-11-09 approve 必炸陷阱修复的 worker 层回归：
//   1. approve = 自动执行全部对账语句（含白名单外删列/改名/类型变更）；
//   2. 断点续跑命中已决 approve = 只放行不重执行（重启死循环根治）；
//   3. auto 路径 ErrSourceTableGone = 过期放行（此前无出口 crash 循环）。

// settledGate 预置已决决策的 fake gate（断点续跑场景）。
type settledGate struct {
	fakeDDLGate
	mu    sync.Mutex
	dec   string
	ready bool
}

func (g *settledGate) Settled(_ context.Context, _ string) (string, bool, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.dec, g.ready, nil
}

type applyRecorder struct {
	mu    sync.Mutex
	calls [][]string
}

func (a *applyRecorder) record(stmts []string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.calls = append(a.calls, stmts)
}

func (a *applyRecorder) joined() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	var all []string
	for _, c := range a.calls {
		all = append(all, c...)
	}
	return strings.Join(all, "\n")
}

func approveTestWorker(t *testing.T, policy string, gate DDLGate,
	diff func(ctx context.Context, schema, table string) (*TargetDiff, error),
	apply *applyRecorder) *Worker {
	t.Helper()
	doTrue := true
	rules, err := CompileRules([]config.Rule{{
		Schema: "db1", TargetDatabase: "dstdb", DDLPolicy: policy,
		Sinks:  &config.RuleSinks{MySQL: config.RuleSink{Enabled: true}},
		Tables: []config.TableRule{{Table: "t1", OrderByColumn: "id", DoStock: &doTrue}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	hooks := &DDLHooks{Gate: gate, DiffTarget: diff}
	if apply != nil {
		hooks.ApplyTarget = func(_ context.Context, stmts []string) error {
			apply.record(stmts)
			return nil
		}
	}
	return NewWorker(nil, 0, "", rules, schema.NewManager(stubSchemaLoader{}), nil, nil,
		NewWatermark(model.Position{}), nil, hooks, nil, nil, 0, 0, nil)
}

// TestHandleDDL_ApproveExecutesFullReconciliation：非白名单差异（删列+改名+类型
// 变更混合——用户场景）→ auto 转 pause 挂起 → approve → 全量语句执行 + 复验 → 恢复。
// 修复前：approve 复验必失败 fatal（白名单外差异无语句可执行）。
func TestHandleDDL_ApproveExecutesFullReconciliation(t *testing.T) {
	gate := newFakeDDLGate()
	diffCalls := 0
	diff := func(_ context.Context, _, _ string) (*TargetDiff, error) {
		diffCalls++
		if diffCalls <= 2 { // auto 判定 + approve 执行：混合差异（非白名单）
			return &TargetDiff{
				AutoStmts:   []string{"ALTER TABLE dstdb.t1 ADD COLUMN c2 int"},
				AllStmts:    []string{"ALTER TABLE dstdb.t1 ADD COLUMN c2 int", "ALTER TABLE dstdb.t1 DROP COLUMN legacy", "ALTER TABLE dstdb.t1 CHANGE COLUMN old new varchar(64)"},
				Whitelisted: false,
				Summary:     "对账语句 3 条（白名单内 1）",
			}, nil
		}
		// 复验：已一致
		return &TargetDiff{Whitelisted: true, Summary: "已一致（无差异）"}, nil
	}
	apply := &applyRecorder{}
	w := approveTestWorker(t, "auto", gate, diff, apply)

	u := &EventUnit{Seq: 9, Kind: UnitDDL, Schema: "db1", Table: "t1",
		Query: "ALTER TABLE db1.t1 ADD COLUMN c2 int", Pos: model.Position{Name: "f", Pos: 200}}

	errCh := make(chan error, 1)
	go func() { errCh <- w.handleDDL(context.Background(), u) }()

	select {
	case <-gate.recorded:
	case <-time.After(2 * time.Second):
		t.Fatal("非白名单差异应挂起等待审批")
	}
	gate.decided <- DecisionApprove

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("approve 应执行全量对账并恢复，而非 fatal: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("决策后未返回")
	}
	got := apply.joined()
	for _, want := range []string{"ADD COLUMN c2", "DROP COLUMN legacy", "CHANGE COLUMN old new"} {
		if !strings.Contains(got, want) {
			t.Errorf("approve 须执行全部对账语句，缺: %s；实际执行:\n%s", want, got)
		}
	}
}

// TestHandleDDL_SettledApproveSkipsExecution：断点续跑命中已决 approve →
// 不重执行目标端变更（重复 ALTER 必报错 = 重启死循环根源），直接放行。
func TestHandleDDL_SettledApproveSkipsExecution(t *testing.T) {
	gate := &settledGate{fakeDDLGate: *newFakeDDLGate(), dec: DecisionApprove, ready: true}
	diffCalls := 0
	diff := func(_ context.Context, _, _ string) (*TargetDiff, error) {
		diffCalls++
		return &TargetDiff{AllStmts: []string{"ALTER TABLE dstdb.t1 DROP COLUMN legacy"},
			Whitelisted: false, Summary: "残余差异"}, nil
	}
	apply := &applyRecorder{}
	w := approveTestWorker(t, "pause", gate, diff, apply)

	u := &EventUnit{Seq: 5, Kind: UnitDDL, Schema: "db1", Table: "t1",
		Query: "ALTER TABLE db1.t1 DROP COLUMN legacy", Pos: model.Position{Name: "f", Pos: 300}}
	if err := w.handleDDL(context.Background(), u); err != nil {
		t.Fatalf("settled approve 应直接放行: %v", err)
	}
	if got := apply.joined(); got != "" {
		t.Fatalf("断点续跑不得重执行目标端变更: %s", got)
	}
	if diffCalls != 0 {
		t.Fatalf("断点续跑不得重算 diff: %d", diffCalls)
	}
}

// TestHandleDDL_AutoSourceTableGone：auto 策略下源表已删/改名走（DROP TABLE /
// RENAME 表走）→ 过期放行。修复前：fatal「差异计算失败」且 auto 无审批记录，
// 重启重放同一事件 = 无出口 crash 循环。
func TestHandleDDL_AutoSourceTableGone(t *testing.T) {
	gate := newFakeDDLGate()
	diff := func(_ context.Context, _, _ string) (*TargetDiff, error) {
		return nil, errors.Join(mysqlsink.ErrSourceTableGone, errors.New("db1.t1"))
	}
	apply := &applyRecorder{}
	w := approveTestWorker(t, "auto", gate, diff, apply)

	u := &EventUnit{Seq: 3, Kind: UnitDDL, Schema: "db1", Table: "t1",
		Query: "DROP TABLE db1.t1", Pos: model.Position{Name: "f", Pos: 400}}
	done := make(chan error, 1)
	go func() { done <- w.handleDDL(context.Background(), u) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("源表已不存在应过期放行，而非 fatal: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("未返回")
	}
	if got := apply.joined(); got != "" {
		t.Fatalf("过期 DDL 不应执行任何目标端变更: %s", got)
	}
	// 过期放行须留审计记录（skip）
	select {
	case rec := <-gate.recorded:
		if rec.Status != DecisionSkip || !strings.Contains(rec.Note, "过期") {
			t.Fatalf("过期放行审计记录: %+v", rec)
		}
	default:
		t.Fatal("过期放行未写审计记录")
	}
}

// TestHandleDDL_AutoWhitelistedExecutesOnlyWhitelist：auto 全白名单 → 只执行
// 白名单子集（AutoStmts），执行后复验须收敛。
func TestHandleDDL_AutoWhitelistedExecutesOnlyWhitelist(t *testing.T) {
	gate := newFakeDDLGate()
	diffCalls := 0
	diff := func(_ context.Context, _, _ string) (*TargetDiff, error) {
		diffCalls++
		if diffCalls == 1 {
			return &TargetDiff{
				AutoStmts:   []string{"ALTER TABLE dstdb.t1 ADD COLUMN c2 int"},
				AllStmts:    []string{"ALTER TABLE dstdb.t1 ADD COLUMN c2 int"},
				Whitelisted: true, Summary: "加列 c2",
			}, nil
		}
		return &TargetDiff{Whitelisted: true, Summary: "已一致（无差异）"}, nil
	}
	apply := &applyRecorder{}
	w := approveTestWorker(t, "auto", gate, diff, apply)

	u := &EventUnit{Seq: 2, Kind: UnitDDL, Schema: "db1", Table: "t1",
		Query: "ALTER TABLE db1.t1 ADD COLUMN c2 int", Pos: model.Position{Name: "f", Pos: 500}}
	done := make(chan error, 1)
	go func() { done <- w.handleDDL(context.Background(), u) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("白名单内应自动同步: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("未返回")
	}
	if !strings.Contains(apply.joined(), "ADD COLUMN c2") {
		t.Fatal("白名单语句未执行")
	}
}

// TestHandleDDL_TruncateMirror：TRUNCATE approve = 目标端同步清空（数据镜像，
// 2026-11-10 用户纠偏）；skip = 目标数据不动。
func TestHandleDDL_TruncateMirror(t *testing.T) {
	run := func(t *testing.T, decision string) (called int, err error) {
		gate := newFakeDDLGate()
		var mu sync.Mutex
		n := 0
		hooks := &DDLHooks{Gate: gate,
			TruncateTarget: func(_ context.Context, _, _ string) error {
				mu.Lock()
				n++
				mu.Unlock()
				return nil
			}}
		// TRUNCATE 分支不触 DiffTarget（结构 diff 对数据无感知）；只装 ApplyTarget
		// 防御误走（若走到即 panic 揭示分支错误）
		hooks.ApplyTarget = func(_ context.Context, _ []string) error {
			t.Error("TRUNCATE 不应走 diff/apply 收敛循环")
			return nil
		}
		w := approveTestWorker(t, "auto", hooks.Gate, nil, nil)
		w.ddlHooks = hooks
		u := &EventUnit{Seq: 11, Kind: UnitDDL, Schema: "db1", Table: "t1",
			Query: "TRUNCATE TABLE db1.t1", Pos: model.Position{Name: "f", Pos: 600}}
		done := make(chan error, 1)
		go func() { done <- w.handleDDL(context.Background(), u) }()
		select {
		case <-gate.recorded:
		case <-time.After(2 * time.Second):
			t.Fatal("TRUNCATE 应挂起")
		}
		gate.decided <- decision
		select {
		case err = <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("未返回")
		}
		mu.Lock()
		called = n
		mu.Unlock()
		return called, err
	}

	if called, err := run(t, DecisionApprove); err != nil || called != 1 {
		t.Fatalf("approve 应执行目标端 TRUNCATE 恰一次: called=%d err=%v", called, err)
	}
	if called, err := run(t, DecisionSkip); err != nil || called != 0 {
		t.Fatalf("skip 不应清目标数据: called=%d err=%v", called, err)
	}
}

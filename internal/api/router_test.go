package api

import (
	"context"
	"encoding/json"
	"io"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"gocdc/internal/config"
	"gocdc/internal/pipeline"
	"gocdc/internal/position"
	"gocdc/internal/verify"
)

func stringsReader(s string) io.Reader { return strings.NewReader(s) }

type ginEngine = gin.Engine

func modelRuleHelper() config.Rule {
	doStock := true
	return config.Rule{
		Schema: "db1", Sinks: &config.RuleSinks{MySQL: config.RuleSink{Enabled: true}},
		Tables: []config.TableRule{{Table: "t1", DoStock: &doStock}},
	}
}

// fakeGate 实现 pipeline.DDLGate（测试注入）。
type fakeGate struct {
	mu   sync.Mutex
	recs map[string]pipeline.DDLRecord
}

func (f *fakeGate) Record(_ context.Context, rec pipeline.DDLRecord) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.recs == nil {
		f.recs = map[string]pipeline.DDLRecord{}
	}
	f.recs[rec.ID] = rec
	return nil
}

func (f *fakeGate) Settled(_ context.Context, id string) (string, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	rec, ok := f.recs[id]
	if !ok {
		return "", false, nil
	}
	switch rec.Status {
	case pipeline.DecisionApprove, pipeline.DecisionSkip, pipeline.DecisionSynced:
		return rec.Status, true, nil
	}
	return "", false, nil
}

func (f *fakeGate) Settle(_ context.Context, id, decision, note string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	rec, ok := f.recs[id]
	if !ok {
		rec.ID = id
	}
	rec.Status = decision
	rec.Note = note
	f.recs[id] = rec
	return nil
}

func (f *fakeGate) WaitDecision(ctx context.Context, id string) (string, error) {
	for {
		d, ok, err := f.Settled(ctx, id)
		if err != nil || ok {
			return d, err
		}
		time.Sleep(10 * time.Millisecond)
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
	}
}

func testDeps() (Deps, *fakeGate) {
	store := position.NewStore(nil, "/p", "", 0)
	gate := &fakeGate{}
	sd := &SourceDeps{
		ID: config.DefaultSourceID, Store: store,
		Rules: func() *pipeline.CompiledRules { return mustRules() },
		Gate:  func() pipeline.DDLGate { return gate },
	}
	deps := Deps{
		Version: "test", Cluster: "c1", NodeID: "n1",
		ConfigSrc: "file:test", StartedAt: time.Now(),
		ConfigJSON: []byte(`{"masked":true}`),
		Sources:     map[string]*SourceDeps{config.DefaultSourceID: sd},
		SourceOrder: []string{config.DefaultSourceID},
	}
	// /status 与 /sources 的装配层闭包（生产由 cmd 提供；测试从源快照闭包派生）
	deps.SourceStatuses = func() []SourceStatus {
		ss := SourceStatus{ID: sd.ID, Phase: "ready", PipelineState: PipeIdle, UseGTID: sd.UseGTID}
		if sd.Pipeline != nil {
			ps := sd.Pipeline()
			ss.PipelineState, ss.PipelineError, ss.PipelineSince, ss.PausedByOp = ps.State, ps.Error, ps.Since, ps.PausedByOp
			if ps.HasLag {
				lag := ps.EventLag
				ss.EventLagSeconds = &lag
			}
		}
		return []SourceStatus{ss}
	}
	deps.SourceList = func() []SourceSummary {
		return []SourceSummary{{ID: sd.ID, State: PipeIdle, Rules: 1, Tables: 1, Sinks: []string{"mysql"}}}
	}
	return deps, gate
}

// defaultSD 取 testDeps 的唯一源依赖（多字段变异用）。
func defaultSD(deps Deps) *SourceDeps { return deps.Sources[config.DefaultSourceID] }

func mustRules() *pipeline.CompiledRules {
	doStock := true
	cr, err := pipeline.CompileRules([]config.Rule{modelRuleHelper()})
	if err != nil {
		panic(err)
	}
	_ = doStock
	return cr
}

// router 测试引擎（无 etcd 依赖；Basic 单凭据 admin/pw）。
func testRouter(t *testing.T) (*ginEngine, *fakeGate) {
	t.Helper()
	deps, gate := testDeps()
	return NewRouter(deps, AuthConfig{Username: "admin", Password: "pw"}), gate
}

type nilDDLLister struct{}

func (nilDDLLister) PendingByTable(context.Context, string, string) ([]pipeline.DDLRecord, error) {
	return nil, nil
}

type nilAuditSink struct{}

func (nilAuditSink) appendAudit(string, string, string, time.Time) error { return nil }
func (nilAuditSink) appendAuditSource(string, string, string, string, time.Time) error {
	return nil
}
func (nilAuditSink) List(context.Context, int) ([]auditEntry, error) { return nil, nil }

func TestRouter_Auth(t *testing.T) {
	r, _ := testRouter(t)

	// 无凭据 → 401
	req := httptest.NewRequest("GET", "/api/v1/status", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != 401 {
		t.Fatalf("无凭据应 401: %d", w.Code)
	}
	// 正确用户名 + 错误密码 → 401
	req = httptest.NewRequest("GET", "/api/v1/status", nil)
	req.SetBasicAuth("admin", "wrong")
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != 401 {
		t.Fatalf("错误密码应 401: %d", w.Code)
	}
	// 正确凭据 → 200
	req = httptest.NewRequest("GET", "/api/v1/status", nil)
	req.SetBasicAuth("admin", "pw")
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("正确凭据应 200: %d", w.Code)
	}
	// 单凭据全权限：写操作同样通过（confirm 门禁另有拦截）
	req = httptest.NewRequest("POST", "/api/v1/leader/resign?confirm=true", nil)
	req.SetBasicAuth("admin", "pw")
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("单凭据写应 200: %d %s", w.Code, w.Body.String())
	}
}

// TestRouter_StatusPipelineFields /status 源摘要契约（管线状态以 sources[]
// 为准）：未装配 Pipeline 闭包 = idle；装配后透传 state/error/event_lag_seconds。
func TestRouter_StatusPipelineFields(t *testing.T) {
	fetchSource := func(deps Deps) map[string]any {
		t.Helper()
		r := NewRouter(deps, AuthConfig{Username: "admin", Password: "pw"})
		req := httptest.NewRequest("GET", "/api/v1/status", nil)
		req.SetBasicAuth("admin", "pw")
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		if w.Code != 200 {
			t.Fatalf("status 应 200: %d %s", w.Code, w.Body.String())
		}
		var envelope struct {
			Code int `json:"code"`
			Data struct {
				Sources []map[string]any `json:"sources"`
			} `json:"data"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &envelope); err != nil {
			t.Fatalf("解析 status 响应: %v", err)
		}
		if len(envelope.Data.Sources) != 1 {
			t.Fatalf("单源应 1 项摘要: %+v", envelope.Data.Sources)
		}
		return envelope.Data.Sources[0]
	}

	// 未装配 Pipeline → idle（未参与同步的诚实口径）
	deps, _ := testDeps()
	src := fetchSource(deps)
	if src["pipeline_state"] != PipeIdle {
		t.Fatalf("未装配应 idle: %v", src["pipeline_state"])
	}

	// 装配 paused → 透传错误与事件延迟
	deps2, _ := testDeps()
	defaultSD(deps2).Pipeline = func() PipelineSnapshot {
		return PipelineSnapshot{State: PipePaused, Error: "位点熔断", Since: time.Now(), EventLag: 42, HasLag: true}
	}
	src = fetchSource(deps2)
	if src["pipeline_state"] != PipePaused || src["pipeline_error"] != "位点熔断" {
		t.Fatalf("paused 字段透传: %v/%v", src["pipeline_state"], src["pipeline_error"])
	}
	if lag, ok := src["event_lag_seconds"].(float64); !ok || lag != 42 {
		t.Fatalf("event_lag_seconds 应为 42: %v", src["event_lag_seconds"])
	}

	// 人工暂停快照 → paused_by_op=true（前端切换「恢复管线」入口的依据）
	deps3, _ := testDeps()
	defaultSD(deps3).Pipeline = func() PipelineSnapshot {
		return PipelineSnapshot{State: PipePaused, Error: "管线已被人工暂停", Since: time.Now(), PausedByOp: true}
	}
	src = fetchSource(deps3)
	if src["pipeline_paused_by_op"] != true {
		t.Fatalf("人工暂停应透传 paused_by_op=true: %v", src["pipeline_paused_by_op"])
	}

	// 非人工暂停（致命错误/停机）→ 字段省略（omitempty，前端不出现恢复入口）
	deps4, _ := testDeps()
	defaultSD(deps4).Pipeline = func() PipelineSnapshot {
		return PipelineSnapshot{State: PipePaused, Error: "sink 不可重试", Since: time.Now()}
	}
	src = fetchSource(deps4)
	if _, exists := src["pipeline_paused_by_op"]; exists {
		t.Fatalf("非人工暂停不应出现 paused_by_op: %v", src["pipeline_paused_by_op"])
	}
}

func TestRouter_WriteConfirm(t *testing.T) {
	r, gate := testRouter(t)
	// 造一条 pending DDL
	_ = gate.Record(context.Background(), pipeline.DDLRecord{
		ID: "db1.t1.f.100", Schema: "db1", Table: "t1", Status: pipeline.DecisionPending,
	})

	// 无 confirm → 400
	req := httptest.NewRequest("POST", "/api/v1/sources/default/ddl/db1.t1.f.100/skip", nil)
	req.SetBasicAuth("admin", "pw")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != 400 {
		t.Fatalf("缺 confirm 应 400: %d", w.Code)
	}
	// confirm=true → skip 生效
	req = httptest.NewRequest("POST", "/api/v1/sources/default/ddl/db1.t1.f.100/skip?confirm=true", nil)
	req.SetBasicAuth("admin", "pw")
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("skip 应 200: %d %s", w.Code, w.Body.String())
	}
	dec, ok, _ := gate.Settled(context.Background(), "db1.t1.f.100")
	if !ok || dec != pipeline.DecisionSkip {
		t.Fatalf("决策未生效: %q %v", dec, ok)
	}
}

func TestRouter_EffectiveRules(t *testing.T) {
	deps, _ := testDeps()
	_ = defaultSD(deps).Rules().Match("db1", "t1") // 预热编译视图缓存
	r := NewRouter(deps, AuthConfig{Username: "admin", Password: "pw"})
	req := httptest.NewRequest("GET", "/api/v1/sources/default/rules/effective", nil)
	req.SetBasicAuth("admin", "pw")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("effective 应 200: %d", w.Code)
	}
	if !contains(w.Body.String(), "db1") {
		t.Fatalf("effective 应含规则: %s", w.Body.String())
	}
}

func TestRouter_EffectiveRulesRedactMaskSecrets(t *testing.T) {
	t.Setenv("GOCDC_TEST_MASK_SALT", "never-return-this-salt")
	doStock := true
	rule := config.Rule{
		Schema: "db1",
		Sinks: &config.RuleSinks{
			MySQL:    config.RuleSink{Enabled: true},
			RocketMQ: config.RuleSink{Enabled: true, Topic: "events.user", Tag: "UPDATE"},
		},
		Tables: []config.TableRule{{
			Table: "admin_user", DoStock: &doStock,
			MaskColumns: map[string]config.MaskSpec{
				"phone": {Type: config.MaskHashSHA256, SaltEnv: "GOCDC_TEST_MASK_SALT"},
				"email": {Type: config.MaskFixed, Value: "never-return-this-value"},
				"name":  {Type: config.MaskTruncate, Keep: 4},
			},
		}},
	}
	compiled, err := pipeline.CompileRules([]config.Rule{rule})
	if err != nil {
		t.Fatal(err)
	}
	_ = compiled.Match("db1", "admin_user")
	deps, _ := testDeps()
	defaultSD(deps).Rules = func() *pipeline.CompiledRules { return compiled }
	r := NewRouter(deps, AuthConfig{Username: "admin", Password: "pw"})
	req := httptest.NewRequest("GET", "/api/v1/sources/default/rules/effective", nil)
	req.SetBasicAuth("admin", "pw")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("effective 应 200: %d %s", w.Code, w.Body.String())
	}
	for _, want := range []string{`"phone":{"type":"hash_sha256"}`, `"name":{"type":"truncate","keep":4}`, `"rocketmq_tag":"UPDATE"`, `"rocketmq_topic":"events.user"`} {
		if !contains(w.Body.String(), want) {
			t.Fatalf("脱敏摘要缺少 %s: %s", want, w.Body.String())
		}
	}
	for _, secret := range []string{"never-return-this-salt", "never-return-this-value", `"salt"`, `"value"`} {
		if contains(w.Body.String(), secret) {
			t.Fatalf("effective 暴露了脱敏秘密 %q: %s", secret, w.Body.String())
		}
	}
}

func TestRouter_ListEndpointsReturnEmptyArrays(t *testing.T) {
	deps, _ := testDeps()
	defaultSD(deps).DDLLister = func() pipeline.DDLLister { return nilDDLLister{} }
	deps.Audit = nilAuditSink{}
	r := NewRouter(deps, AuthConfig{Username: "admin", Password: "pw"})

	for _, path := range []string{"/api/v1/ddl-events", "/api/v1/audit"} {
		req := httptest.NewRequest("GET", path, nil)
		req.SetBasicAuth("admin", "pw")
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		if w.Code != 200 || !contains(w.Body.String(), `"data":[]`) {
			t.Fatalf("%s empty payload = %d %s, want data array", path, w.Code, w.Body.String())
		}
	}
}

func TestRouter_RestockSupportsDottedSchemas(t *testing.T) {
	deps, _ := testDeps()
	var gotSchema, gotTable string
	defaultSD(deps).StockReset = func(_ context.Context, schema, table string) error {
		gotSchema, gotTable = schema, table
		return nil
	}
	r := NewRouter(deps, AuthConfig{Username: "admin", Password: "pw"})

	for _, path := range []string{
		"/api/v1/sources/default/rules/by-schema/edu.mall.admin/users/restock?confirm=true",
	} {
		req := httptest.NewRequest("POST", path, nil)
		req.SetBasicAuth("admin", "pw")
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		if w.Code != 200 || gotSchema != "edu.mall.admin" || gotTable != "users" {
			t.Fatalf("restock %s: status=%d schema=%q table=%q body=%s", path, w.Code, gotSchema, gotTable, w.Body.String())
		}
	}
}

func TestRouter_PositionSet(t *testing.T) {
	t.Skip("position 写依赖真实 etcd Store（集成覆盖：TestIntegrationStoreGate/Elector_Takeover）")
	r, _ := testRouter(t)
	body := `{"name":"bin.1","pos":100,"confirm":"SET-POSITION"}`
	req := httptest.NewRequest("POST", "/api/v1/sources/default/position", stringsReader(body))
	req.SetBasicAuth("admin", "pw")
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("position 应 200: %d %s", w.Code, w.Body.String())
	}
	// 错误 confirm → 400
	req = httptest.NewRequest("POST", "/api/v1/sources/default/position", stringsReader(`{"confirm":"no"}`))
	req.SetBasicAuth("admin", "pw")
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != 400 {
		t.Fatalf("错误 confirm 应 400: %d", w.Code)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// TestRouter_Verify 抽样对账端点契约：confirm 门禁、参数透传、缺参 400、
// 数据差异走 200（Mismatch 在 data 里而非 500——数据差异不是工具故障）。
func TestRouter_Verify(t *testing.T) {
	deps, _ := testDeps()
	defaultSD(deps).Verify = func(_ context.Context, database, table, pk string, sample int) (*verify.Result, error) {
		if database != "db1" || table != "t1" || pk != "id" || sample != 50 {
			t.Errorf("参数透传不符: %s %s %s %d", database, table, pk, sample)
		}
		return &verify.Result{SrcCount: 10, TgtCount: 9, Checked: 5, Mismatch: 1,
			Diffs: []string{"MISS pk=3"}}, nil
	}
	r := NewRouter(deps, AuthConfig{Username: "admin", Password: "pw"})
	body := `{"database":"db1","table":"t1","pk":"id","sample":50}`

	// 无 confirm → 400
	req := httptest.NewRequest("POST", "/api/v1/sources/default/verify", stringsReader(body))
	req.SetBasicAuth("admin", "pw")
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != 400 {
		t.Fatalf("缺 confirm 应 400: %d", w.Code)
	}

	// confirm=true → 200 + 结果数据（Mismatch=1 仍是 200）
	req = httptest.NewRequest("POST", "/api/v1/sources/default/verify?confirm=true", stringsReader(body))
	req.SetBasicAuth("admin", "pw")
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("verify 应 200: %d %s", w.Code, w.Body.String())
	}
	for _, want := range []string{`"mismatch":1`, `"src_count":10`, `"tgt_count":9`, "MISS pk=3"} {
		if !contains(w.Body.String(), want) {
			t.Fatalf("响应缺 %s: %s", want, w.Body.String())
		}
	}

	// 缺 database/table → 400
	req = httptest.NewRequest("POST", "/api/v1/sources/default/verify?confirm=true", stringsReader(`{"table":"t1"}`))
	req.SetBasicAuth("admin", "pw")
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != 400 {
		t.Fatalf("缺 database 应 400: %d", w.Code)
	}
}

package api

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"gocdc/internal/pipeline"
	"gocdc/internal/position"
)

// multiSourceDeps 双源测试依赖（sid-a / sid-b 各带独立 store/gate/lister）。
func multiSourceDeps(t *testing.T) (Deps, *fakeGate, *fakeGate) {
	t.Helper()
	mk := func(sid string, gate *fakeGate) *SourceDeps {
		return &SourceDeps{
			ID: sid, Store: position.NewStore(nil, "/"+sid, "", 0),
			Rules:     func() *pipeline.CompiledRules { return mustRules() },
			Gate:      func() pipeline.DDLGate { return gate },
			DDLLister: func() pipeline.DDLLister { return gateLister{gate} },
		}
	}
	ga, gb := &fakeGate{}, &fakeGate{}
	deps := Deps{
		Version: "test", Cluster: "c1", NodeID: "n1",
		ConfigSrc: "file:test", StartedAt: time.Now(),
		Sources: map[string]*SourceDeps{
			"sid-a": mk("sid-a", ga),
			"sid-b": mk("sid-b", gb),
		},
		SourceOrder: []string{"sid-a", "sid-b"},
	}
	deps.SourceStatuses = func() []SourceStatus {
		return []SourceStatus{
			{ID: "sid-a", Phase: "ready", PipelineState: PipeRunning},
			{ID: "sid-b", Phase: "failed", PipelineState: PipeIdle, PipelineError: "源库不可达"},
		}
	}
	return deps, ga, gb
}

// gateLister fakeGate 适配 DDLLister（pending 过滤）。
type gateLister struct{ g *fakeGate }

func (l gateLister) PendingByTable(context.Context, string, string) ([]pipeline.DDLRecord, error) {
	l.g.mu.Lock()
	defer l.g.mu.Unlock()
	var out []pipeline.DDLRecord
	for _, r := range l.g.recs {
		if r.Status == pipeline.DecisionPending {
			out = append(out, r)
		}
	}
	return out, nil
}

func doReq(r *ginEngine, method, path string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, nil)
	req.SetBasicAuth("admin", "pw")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

// TestMultiSource_LegacyRoutes400 多源部署旧路由 400 指引（PRD 06 §1.4）。
func TestMultiSource_LegacyRoutes400(t *testing.T) {
	deps, _, _ := multiSourceDeps(t)
	r := NewRouter(deps, AuthConfig{Username: "admin", Password: "pw"})

	for _, path := range []string{
		"/api/v1/position",
		"/api/v1/rules/effective",
	} {
		w := doReq(r, "GET", path)
		if w.Code != 400 || !strings.Contains(w.Body.String(), "存在多个源") {
			t.Fatalf("%s 多源应 400 指引: %d %s", path, w.Code, w.Body.String())
		}
	}
	for _, path := range []string{
		"/api/v1/position?confirm=true",
		"/api/v1/pipeline/pause?confirm=true",
		"/api/v1/pipeline/resume?confirm=true",
		"/api/v1/verify?confirm=true",
		"/api/v1/ddl/ev1/skip?confirm=true",
		"/api/v1/rules/by-schema/db1/t1/restock?confirm=true",
		"/api/v1/position/migrate-gtid?confirm=true",
	} {
		w := doReq(r, "POST", path)
		if w.Code != 400 || !strings.Contains(w.Body.String(), "存在多个源") {
			t.Fatalf("%s 多源应 400 指引: %d %s", path, w.Code, w.Body.String())
		}
	}
}

// TestMultiSource_SingleSourceLegacyAlias 单源部署旧路由 = 唯一源等价别名
//（不出现「存在多个源」400；nil etcd 客户端的 Store 读失败 500 可接受，
// 与 TestRouter_MigrateGuards 同口径——守卫路径由真实 etcd 集成覆盖）。
func TestMultiSource_SingleSourceLegacyAlias(t *testing.T) {
	deps, _ := testDeps() // 单源 default
	r := NewRouter(deps, AuthConfig{Username: "admin", Password: "pw"})
	// /rules/effective 不触 etcd：等价别名直接返回数据
	w := doReq(r, "GET", "/api/v1/rules/effective")
	if w.Code != 200 || !strings.Contains(w.Body.String(), "db1") {
		t.Fatalf("单源 /rules/effective 应 200 等价别名: %d %s", w.Code, w.Body.String())
	}
	// /position 不出现多源 400（nil store 500 为 etcd 依赖，语义正确）
	w = doReq(r, "GET", "/api/v1/position")
	if w.Code == 400 && strings.Contains(w.Body.String(), "存在多个源") {
		t.Fatalf("单源 /position 不应 400 多源指引: %s", w.Body.String())
	}
}

// TestMultiSource_SourceRoutes 源作用域路由：已知 sid 200、未知 sid 404。
func TestMultiSource_SourceRoutes(t *testing.T) {
	deps, _, _ := multiSourceDeps(t)
	r := NewRouter(deps, AuthConfig{Username: "admin", Password: "pw"})

	w := doReq(r, "GET", "/api/v1/sources/sid-a/status")
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"id":"sid-a"`) {
		t.Fatalf("sid-a status 应 200: %d %s", w.Code, w.Body.String())
	}
	w = doReq(r, "GET", "/api/v1/sources/nope/position")
	if w.Code != 404 || !strings.Contains(w.Body.String(), "源不存在") {
		t.Fatalf("未知 sid 应 404: %d %s", w.Code, w.Body.String())
	}
}

// TestMultiSource_DDLEventsMerged /ddl-events 全集群合并（记录带 source 字段；
// 单源部署 = 唯一源数据，旧消费方零变化）。
func TestMultiSource_DDLEventsMerged(t *testing.T) {
	deps, ga, gb := multiSourceDeps(t)
	_ = ga.Record(context.Background(), pipeline.DDLRecord{
		ID: "db1.t1.f.100", Schema: "db1", Table: "t1", Status: pipeline.DecisionPending})
	_ = gb.Record(context.Background(), pipeline.DDLRecord{
		ID: "db2.t2.f.200", Schema: "db2", Table: "t2", Status: pipeline.DecisionPending})
	r := NewRouter(deps, AuthConfig{Username: "admin", Password: "pw"})

	w := doReq(r, "GET", "/api/v1/ddl-events")
	if w.Code != 200 {
		t.Fatalf("ddl-events 应 200: %d", w.Code)
	}
	body := w.Body.String()
	for _, want := range []string{`"source":"sid-a"`, `"source":"sid-b"`, "db1.t1.f.100", "db2.t2.f.200"} {
		if !strings.Contains(body, want) {
			t.Fatalf("合并列表缺 %s: %s", want, body)
		}
	}
	// 源作用域列表只含该源记录
	w = doReq(r, "GET", "/api/v1/sources/sid-a/ddl-events")
	if w.Code != 200 || strings.Contains(w.Body.String(), "db2.t2") || !strings.Contains(w.Body.String(), "db1.t1") {
		t.Fatalf("sid-a 列表应只含本源记录: %s", w.Body.String())
	}
}

// TestMultiSource_StatusSources /status sources[] 数组 + 多源顶层镜像置空。
func TestMultiSource_StatusSources(t *testing.T) {
	deps, _, _ := multiSourceDeps(t)
	r := NewRouter(deps, AuthConfig{Username: "admin", Password: "pw"})
	w := doReq(r, "GET", "/api/v1/status")
	if w.Code != 200 {
		t.Fatalf("status 应 200: %d", w.Code)
	}
	var env struct {
		Data StatusData `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &env); err != nil {
		t.Fatal(err)
	}
	if len(env.Data.Sources) != 2 {
		t.Fatalf("sources 应 2 项: %+v", env.Data.Sources)
	}
	if env.Data.Sources[0].ID != "sid-a" || env.Data.Sources[0].PipelineState != PipeRunning {
		t.Fatalf("sid-a 摘要错误: %+v", env.Data.Sources[0])
	}
	if env.Data.Sources[1].ID != "sid-b" || env.Data.Sources[1].Phase != "failed" ||
		env.Data.Sources[1].PipelineError != "源库不可达" {
		t.Fatalf("sid-b 摘要错误: %+v", env.Data.Sources[1])
	}
	// 多源：顶层管线字段置空（idle），以 sources[] 为准
	if env.Data.PipelineState != PipeIdle {
		t.Fatalf("多源顶层镜像应 idle: %q", env.Data.PipelineState)
	}
}

// TestMultiSource_SourcesList GET /sources 清单。
func TestMultiSource_SourcesList(t *testing.T) {
	deps, _, _ := multiSourceDeps(t)
	deps.SourceList = func() []SourceSummary {
		return []SourceSummary{
			{ID: "sid-a", SourceAddr: "10.0.0.1:3306", Sinks: []string{"mysql"}, Rules: 1, Tables: 2, State: "running"},
			{ID: "sid-b", SourceAddr: "10.0.0.2:3306", Sinks: []string{"mysql", "kafka"}, Rules: 2, Tables: 5, State: "failed", Error: "源库不可达"},
		}
	}
	r := NewRouter(deps, AuthConfig{Username: "admin", Password: "pw"})
	w := doReq(r, "GET", "/api/v1/sources")
	if w.Code != 200 {
		t.Fatalf("sources 应 200: %d", w.Code)
	}
	body := w.Body.String()
	for _, want := range []string{`"id":"sid-a"`, `"source_addr":"10.0.0.1:3306"`, `"tables":5`, `"源库不可达"`} {
		if !strings.Contains(body, want) {
			t.Fatalf("源清单缺 %s: %s", want, body)
		}
	}
}

// TestMultiSource_AuditSourceField 源级操作审计带 source 字段（E14）。
func TestMultiSource_AuditSourceField(t *testing.T) {
	var got []auditEntry
	sink := &captureAuditSink{append: func(e auditEntry) { got = append(got, e) }}
	deps, _, _ := multiSourceDeps(t)
	deps.Audit = sink
	defaultlike := deps.Sources["sid-a"]
	defaultlike.PausePipeline = func(context.Context) error { return nil }
	r := NewRouter(deps, AuthConfig{Username: "admin", Password: "pw"})

	w := doReq(r, "POST", "/api/v1/sources/sid-a/pipeline/pause?confirm=true")
	if w.Code != 200 {
		t.Fatalf("pause 应 200: %d %s", w.Code, w.Body.String())
	}
	if len(got) == 0 || got[0].Source != "sid-a" {
		t.Fatalf("审计条目应带 source=sid-a: %+v", got)
	}
}

type captureAuditSink struct{ append func(auditEntry) }

func (c *captureAuditSink) appendAudit(who, what, result string, at time.Time) error {
	c.append(auditEntry{Who: who, What: what, Result: result, At: at})
	return nil
}
func (c *captureAuditSink) appendAuditSource(who, sid, what, result string, at time.Time) error {
	c.append(auditEntry{Who: who, What: what, Result: result, Source: sid, At: at})
	return nil
}
func (c *captureAuditSink) List(context.Context, int) ([]auditEntry, error) { return nil, nil }

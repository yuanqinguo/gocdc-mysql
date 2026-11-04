package api

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"gocdc/internal/config"
)

// ── 测试替身 ──────────────────────────────────────────────────────

// fakeConfigStore 内存配置中心：CAS base 校验 + revision 递增。
type fakeConfigStore struct {
	mu     sync.Mutex
	raw    string
	rev    int64
	writes int
}

func newFakeStore(raw string) *fakeConfigStore { return &fakeConfigStore{raw: raw, rev: 100} }

func (f *fakeConfigStore) ReadRaw(context.Context) (int64, []byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.rev, []byte(f.raw), nil
}

func (f *fakeConfigStore) CASWrite(_ context.Context, base int64, raw []byte) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if base != f.rev {
		return 0, ErrConfigCASConflict
	}
	f.raw = string(raw)
	f.rev++
	f.writes++
	return f.rev, nil
}

// recordingAudit 记录审计条目（断言 what/source/result；what 为
// what+path+detail 拼接串，与 AuditStore 写入形态一致）。
type recordingAudit struct {
	mu   sync.Mutex
	what []string
	src  []string
	res  []string
}

func (r *recordingAudit) appendAudit(string, string, string, time.Time) error { return nil }

func (r *recordingAudit) appendAuditSource(_ string, sid, what, result string, _ time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.what = append(r.what, what)
	r.src = append(r.src, sid)
	r.res = append(r.res, result)
	return nil
}

func (r *recordingAudit) List(context.Context, int) ([]auditEntry, error) { return nil, nil }

// fakePreflight 可编程预检替身（记录入参、返回配置的报告）。
type fakePreflight struct {
	mu       sync.Mutex
	sid      string
	oldRules []config.Rule
	newRules []config.Rule
	report   PreflightReport
}

func (f *fakePreflight) run(_ context.Context, sid string, oldRules, newRules []config.Rule) PreflightReport {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sid, f.oldRules, f.newRules = sid, oldRules, newRules
	return f.report
}

// editDeps 带配置编辑依赖的测试路由。
func editDeps(t *testing.T) (Deps, *fakeConfigStore, *fakePreflight, *recordingAudit) {
	t.Helper()
	deps, _ := testDeps()
	store := newFakeStore(editRawBody)
	pre := &fakePreflight{}
	aud := &recordingAudit{}
	deps.ConfigStore = store
	deps.Preflight = pre.run
	deps.Shutdown = func() {}
	deps.ConfigKey = "/gocdc/c1/config"
	deps.Audit = aud
	return deps, store, pre, aud
}

// editRawBody 合法 v2 配置正文（default 源 + db1.t1 规则）。
const editRawBody = `cluster: c1
ha:
  lease_ttl_seconds: 10
  leader_check_interval_ms: 2000
observability:
  metrics_addr: "127.0.0.1:9595"
pipeline:
  workers: 4
  worker_queue_size: 8192
  batch_size: 500
sources:
  - id: default
    source:
      addr: "s:3306"
      user: root
      password: ${PW}
      charset: utf8mb4
      flavor: mysql
      timezone: UTC
      exclude_schemas: [mysql, sys, information_schema, performance_schema]
    targets:
      mysql:
        enabled: true
        addr: "d:3306"
        user: root
        password: ${PW}
        charset: utf8mb4
        max_open_conns: 5
        tx_chunk_rows: 100
    rules:
      - schema: db1
        sinks: {mysql: true}
        tables:
          - {table: t1, order_by_column: id, do_stock: true}
`

// rulesPayload 规则 JSON（t1 + 新增 t2）。
const rulesPayload = `{"rules":[{"schema":"db1","sinks":{"mysql":true},"ddl_policy":"pause",
  "tables":[{"table":"t1","order_by_column":"id","do_stock":true},
            {"table":"t2","do_stock":false}]}]}`

func putRulesReq(r Deps, path, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest("PUT", path, stringsReader(body))
	req.SetBasicAuth("admin", "pw")
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	NewRouter(r, AuthConfig{Username: "admin", Password: "pw"}).ServeHTTP(w, req)
	return w
}

// TestRouter_ConfigRaw raw 端点：etcd 形态回传原文+revision；file 形态 409。
func TestRouter_ConfigRaw(t *testing.T) {
	t.Setenv("PW", "x")
	deps, store, _, _ := editDeps(t)
	r := NewRouter(deps, AuthConfig{Username: "admin", Password: "pw"})

	req := httptest.NewRequest("GET", "/api/v1/config/raw", nil)
	req.SetBasicAuth("admin", "pw")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("raw 应 200: %d %s", w.Code, w.Body.String())
	}
	for _, want := range []string{`"revision":100`, `"config_key":"/gocdc/c1/config"`, "db1", "${PW}"} {
		if !contains(w.Body.String(), want) {
			t.Fatalf("raw 缺 %s: %s", want, w.Body.String())
		}
	}

	// file 形态（ConfigStore nil）→ 409
	deps2, _ := testDeps()
	r2 := NewRouter(deps2, AuthConfig{Username: "admin", Password: "pw"})
	req = httptest.NewRequest("GET", "/api/v1/config/raw", nil)
	req.SetBasicAuth("admin", "pw")
	w = httptest.NewRecorder()
	r2.ServeHTTP(w, req)
	if w.Code != 409 {
		t.Fatalf("file 形态应 409: %d %s", w.Code, w.Body.String())
	}
	_ = store
}

// TestRouter_GetRules GET rules 端点：组形态原样 + revision。
func TestRouter_GetRules(t *testing.T) {
	t.Setenv("PW", "x")
	deps, _, _, _ := editDeps(t)
	r := NewRouter(deps, AuthConfig{Username: "admin", Password: "pw"})

	w := cfgRawDo(r, "GET", "/api/v1/sources/default/rules", "")
	if w.Code != 200 || !contains(w.Body.String(), `"rules":[{`) || !contains(w.Body.String(), `"revision":100`) {
		t.Fatalf("get rules 应 200 带组形态: %d %s", w.Code, w.Body.String())
	}
	// 未知源 404；file 形态 409
	w = cfgRawDo(r, "GET", "/api/v1/sources/nope/rules", "")
	if w.Code != 404 {
		t.Fatalf("未知源应 404: %d", w.Code)
	}
	deps2, _ := testDeps()
	r2 := NewRouter(deps2, AuthConfig{Username: "admin", Password: "pw"})
	w = cfgRawDo(r2, "GET", "/api/v1/sources/default/rules", "")
	if w.Code != 409 {
		t.Fatalf("file 形态应 409: %d", w.Code)
	}
}

// cfgRawDo 测试请求助手。
func cfgRawDo(r *ginEngine, method, path, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, stringsReader(body))
	req.SetBasicAuth("admin", "pw")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

// TestRouter_PutRules_Save 保存主链路：splice+CAS 写入、diff 报告、审计。
func TestRouter_PutRules_Save(t *testing.T) {
	t.Setenv("PW", "x")
	deps, store, pre, aud := editDeps(t)

	// 缺 confirm → 400
	w := putRulesReq(deps, "/api/v1/sources/default/rules?base=100", rulesPayload)
	if w.Code != 400 {
		t.Fatalf("缺 confirm 应 400: %d %s", w.Code, w.Body.String())
	}
	// 缺 base → 400
	w = putRulesReq(deps, "/api/v1/sources/default/rules?confirm=true", rulesPayload)
	if w.Code != 400 || !contains(w.Body.String(), "base") {
		t.Fatalf("缺 base 应 400: %d %s", w.Code, w.Body.String())
	}
	// base 不一致 → 409 CAS
	w = putRulesReq(deps, "/api/v1/sources/default/rules?confirm=true&base=99", rulesPayload)
	if w.Code != 409 {
		t.Fatalf("CAS 冲突应 409: %d %s", w.Code, w.Body.String())
	}
	if store.writes != 0 {
		t.Fatalf("冲突不应写: %d", store.writes)
	}
	// 正确保存 → 200 + 新 revision + diff 报告
	w = putRulesReq(deps, "/api/v1/sources/default/rules?confirm=true&base=100", rulesPayload)
	if w.Code != 200 {
		t.Fatalf("保存应 200: %d %s", w.Code, w.Body.String())
	}
	for _, want := range []string{`"revision":101`, `"added":[{"schema":"db1","table":"t2"}]`, `"checked":0`} {
		if !contains(w.Body.String(), want) {
			t.Fatalf("响应缺 %s: %s", want, w.Body.String())
		}
	}
	// etcd 侧新表已写入（${ENV} 占位不被破坏）
	if !strings.Contains(store.raw, "table: t2") || !strings.Contains(store.raw, "table: t1") {
		t.Fatalf("etcd 正文缺新表:\n%s", store.raw)
	}
	if !strings.Contains(store.raw, "${PW}") {
		t.Fatalf("etcd 正文占位被破坏:\n%s", store.raw)
	}
	// 预检入参（旧=1 表 / 新=2 表）
	pre.mu.Lock()
	if pre.sid != "default" || len(pre.oldRules) != 1 || len(pre.newRules) != 1 || len(pre.newRules[0].Tables) != 2 {
		t.Fatalf("预检入参不符: %+v", pre)
	}
	pre.mu.Unlock()
	// 审计（what 含 what+path+detail 拼接）：CAS 冲突失败 + 成功保存两条
	aud.mu.Lock()
	defer aud.mu.Unlock()
	if len(aud.what) != 2 || !contains(aud.what[0], auditConfigRulesUpdate) || aud.src[0] != "default" {
		t.Fatalf("审计不符: %+v", aud)
	}
	if !contains(aud.res[0], "CAS 冲突") || aud.res[1] != "ok" {
		t.Fatalf("审计结果不符: %v", aud.res)
	}
	if !contains(aud.what[1], "+1") || !contains(aud.what[1], "db1.t2") {
		t.Fatalf("审计缺 diff 摘要: %q", aud.what[1])
	}
}

// TestRouter_PutRules_Rejections 校验/预检拦截：未知字段 400、空规则 400、
// 预检错误 422 且不写。
func TestRouter_PutRules_Rejections(t *testing.T) {
	t.Setenv("PW", "x")
	deps, store, pre, _ := editDeps(t)

	// 未知字段（表级平铺写法）→ 400 带指引
	w := putRulesReq(deps, "/api/v1/sources/default/rules?confirm=true&base=100",
		`{"rules":[{"schema":"db1","sinks":{"mysql":true},"do_stock":true,"tables":[]}]}`)
	if w.Code != 400 || !contains(w.Body.String(), "不支持的规则组形态") {
		t.Fatalf("平铺写法应 400 带指引: %d %s", w.Code, w.Body.String())
	}
	// 空规则 → 校验 400（rules 不能为空）
	w = putRulesReq(deps, "/api/v1/sources/default/rules?confirm=true&base=100", `{"rules":[]}`)
	if w.Code != 400 || !contains(w.Body.String(), "rules 不能为空") {
		t.Fatalf("空规则应 400: %d %s", w.Code, w.Body.String())
	}
	// 预检错误 → 422 不写
	pre.report = PreflightReport{Checked: 1, Errors: []string{"目标表 db1.t2 不存在"}}
	w = putRulesReq(deps, "/api/v1/sources/default/rules?confirm=true&base=100", rulesPayload)
	if w.Code != 422 || !contains(w.Body.String(), "目标表 db1.t2 不存在") {
		t.Fatalf("预检错误应 422: %d %s", w.Code, w.Body.String())
	}
	if store.writes != 0 {
		t.Fatalf("预检失败不应写: %d", store.writes)
	}
	// 未知源 → 404
	w = putRulesReq(deps, "/api/v1/sources/nope/rules?confirm=true&base=100", rulesPayload)
	if w.Code != 404 {
		t.Fatalf("未知源应 404: %d", w.Code)
	}
}

// TestRouter_PutRules_StaleBaseFailFast CAS base 快速失败：与正文当前
// revision 不一致时在预检前即 409——不白跑活体预检（预检要开源/目标真实
// 连接，冲突场景纯浪费），不写存储。2026-11-04 review 优化。
func TestRouter_PutRules_StaleBaseFailFast(t *testing.T) {
	t.Setenv("PW", "x")
	deps, store, pre, _ := editDeps(t)

	w := putRulesReq(deps, "/api/v1/sources/default/rules?confirm=true&base=99", rulesPayload)
	if w.Code != 409 || !contains(w.Body.String(), "CAS 冲突") {
		t.Fatalf("陈旧 base 应快速 409: %d %s", w.Code, w.Body.String())
	}
	pre.mu.Lock()
	uncalled := pre.sid == ""
	pre.mu.Unlock()
	if !uncalled {
		t.Fatal("冲突路径不得调用活体预检（ReadRaw 后即拒）")
	}
	if store.writes != 0 {
		t.Fatalf("冲突路径不得写: %d", store.writes)
	}
}

// TestRouter_ConfigEdit_V1Body 配置中心正文为 v1 顶层形态（已移除的形态——
// 外部 etcdctl 写回旧正文 / 升级未改写正文）时：raw 照常透传，GET/PUT rules
// 500 且错误详情直达 UI（含 v1 形态指引），绝不写入。2026-11 移除自动包装后
// 编辑链路对新形态的唯一合法输入是 sources[] 正文。
func TestRouter_ConfigEdit_V1Body(t *testing.T) {
	t.Setenv("PW", "x")
	deps, store, _, _ := editDeps(t)
	store.raw = `cluster: c1
ha: { lease_ttl_seconds: 10 }
observability: { metrics_addr: ":9595", log: { level: info } }
source:
  addr: "s:3306"
  user: root
  password: ${PW}
  charset: utf8mb4
  flavor: mysql
  timezone: UTC
  exclude_schemas: [mysql, sys, information_schema, performance_schema]
targets:
  mysql:
    enabled: true
    addr: "d:3306"
    user: root
    password: ${PW}
    charset: utf8mb4
    max_open_conns: 5
    tx_chunk_rows: 100
rules:
  - schema: db1
    sinks: {mysql: true}
    tables:
      - {table: t1, order_by_column: id, do_stock: true}
`
	r := NewRouter(deps, AuthConfig{Username: "admin", Password: "pw"})

	// raw 端点不解析正文 → 照常 200（运维可用它看正文内容诊断）
	w := cfgRawDo(r, "GET", "/api/v1/config/raw", "")
	if w.Code != 200 {
		t.Fatalf("v1 正文 raw 应照常 200: %d %s", w.Code, w.Body.String())
	}
	// GET rules → 500 带形态指引（不是笼统的「无法解析」）
	w = cfgRawDo(r, "GET", "/api/v1/sources/default/rules", "")
	if w.Code != 500 || !contains(w.Body.String(), "v1 单源形态已移除") || !contains(w.Body.String(), "sources") {
		t.Fatalf("v1 正文 GET rules 应 500 带指引: %d %s", w.Code, w.Body.String())
	}
	// PUT rules（confirm+base 齐备）→ 500 带指引，绝不写
	w = putRulesReq(deps, "/api/v1/sources/default/rules?confirm=true&base=100", rulesPayload)
	if w.Code != 500 || !contains(w.Body.String(), "v1 单源形态已移除") {
		t.Fatalf("v1 正文 PUT rules 应 500 带指引: %d %s", w.Code, w.Body.String())
	}
	if store.writes != 0 {
		t.Fatalf("v1 正文 PUT 绝不应写: %d", store.writes)
	}
	if contains(store.raw, "t2") {
		t.Fatalf("v1 正文被意外改写:\n%s", store.raw)
	}
}

// TestRouter_PutRules_CheckOnly 预演：免 confirm、不写、报告照常返回。
func TestRouter_PutRules_CheckOnly(t *testing.T) {
	t.Setenv("PW", "x")
	deps, store, _, aud := editDeps(t)
	pre := deps.Preflight
	_ = pre

	w := putRulesReq(deps, "/api/v1/sources/default/rules?check_only=true", rulesPayload)
	if w.Code != 200 {
		t.Fatalf("check_only 应 200: %d %s", w.Code, w.Body.String())
	}
	if !contains(w.Body.String(), `"added"`) || contains(w.Body.String(), `"revision":101`) {
		t.Fatalf("check_only 响应不符: %s", w.Body.String())
	}
	if store.writes != 0 {
		t.Fatalf("check_only 不应写: %d", store.writes)
	}
	aud.mu.Lock()
	defer aud.mu.Unlock()
	if len(aud.what) != 0 {
		t.Fatalf("check_only 不应审计: %+v", aud)
	}
}

// TestRouter_ApplyRestart 重载端点：门禁、触发、409。
func TestRouter_ApplyRestart(t *testing.T) {
	oldDelay := applyRestartDelay
	applyRestartDelay = 5 * time.Millisecond
	defer func() { applyRestartDelay = oldDelay }()

	deps, _ := testDeps()
	fired := make(chan struct{}, 1)
	deps.Shutdown = func() { fired <- struct{}{} }
	aud := &recordingAudit{}
	deps.Audit = aud
	r := NewRouter(deps, AuthConfig{Username: "admin", Password: "pw"})

	// 无 confirm → 400
	req := httptest.NewRequest("POST", "/api/v1/config/apply-restart", nil)
	req.SetBasicAuth("admin", "pw")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != 400 {
		t.Fatalf("缺 confirm 应 400: %d", w.Code)
	}
	// confirm=true → 200 + 异步触发
	req = httptest.NewRequest("POST", "/api/v1/config/apply-restart?confirm=true", nil)
	req.SetBasicAuth("admin", "pw")
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("apply-restart 应 200: %d %s", w.Code, w.Body.String())
	}
	select {
	case <-fired:
	case <-time.After(2 * time.Second):
		t.Fatalf("Shutdown 未被触发")
	}
	aud.mu.Lock()
	if len(aud.what) != 1 || !contains(aud.what[0], auditConfigApplyRestart) {
		aud.mu.Unlock()
		t.Fatalf("审计不符: %+v", aud)
	}
	aud.mu.Unlock()

	// 未接入 → 409
	deps2, _ := testDeps()
	deps2.Shutdown = nil
	r2 := NewRouter(deps2, AuthConfig{Username: "admin", Password: "pw"})
	req = httptest.NewRequest("POST", "/api/v1/config/apply-restart?confirm=true", nil)
	req.SetBasicAuth("admin", "pw")
	w = httptest.NewRecorder()
	r2.ServeHTTP(w, req)
	if w.Code != 409 {
		t.Fatalf("未接入应 409: %d", w.Code)
	}
	_ = errors.New
}

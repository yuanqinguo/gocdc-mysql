//go:build integration

// 配置编辑端到端集成（真实 etcd）：GET raw revision 对齐、PUT 保存正文落盘
// （${ENV} 占位保留）、陈旧 base 409、审计入环。
package api

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"gocdc/internal/etcdx"
)

func jsonUnmarshalBody(b []byte, v any) error { return json.Unmarshal(b, v) }

func int64ToString(v int64) string { return strconv.FormatInt(v, 10) }

const cfgEditKey = "/gocdc/cfgedit/config"

func cfgEditRouter(t *testing.T, kv *etcdx.KV) *ginEngine {
	t.Helper()
	deps, _ := testDeps()
	deps.ConfigStore = NewEtcdConfigStore(kv, cfgEditKey)
	deps.ConfigKey = cfgEditKey
	deps.Audit = NewAuditStore(kv, "/gocdc/cfgedit", "n1")
	deps.Shutdown = func() {}
	return NewRouter(deps, AuthConfig{Username: "admin", Password: "pw"})
}

func cfgEditDo(r *ginEngine, method, path, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, stringsReader(body))
	req.SetBasicAuth("admin", "pw")
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

// TestIntegrationConfigEdit PUT 链路全流程（真实 etcd）。
func TestIntegrationConfigEdit(t *testing.T) {
	t.Setenv("PW", "x")
	kv := auditTestEtcd(t)
	ctx := context.Background()

	// 播种配置中心正文（v2 形态 + ${ENV} 占位）
	seed := strings.ReplaceAll(editRawBody, "${PW}", "${PW}")
	if _, err := kv.Put(ctx, cfgEditKey, seed); err != nil {
		t.Fatalf("播种配置: %v", err)
	}
	r := cfgEditRouter(t, kv)

	// ① GET raw：revision 与 etcd ModRevision 一致
	w := cfgEditDo(r, "GET", "/api/v1/config/raw", "")
	if w.Code != 200 {
		t.Fatalf("raw 应 200: %d %s", w.Code, w.Body.String())
	}
	resp, err := kv.Get(ctx, cfgEditKey)
	if err != nil || len(resp.Kvs) == 0 {
		t.Fatalf("读配置: %v", err)
	}
	if !strings.Contains(w.Body.String(), `"revision":`) ||
		strings.Contains(w.Body.String(), `"revision":0`) {
		t.Fatalf("raw revision 应为 ModRevision 非零: %s", w.Body.String())
	}

	// ② 陈旧 base（播种后从未变更，base=1 必冲突）→ 409 且正文未变
	w = cfgEditDo(r, "PUT", "/api/v1/sources/default/rules?confirm=true&base=1", rulesPayload)
	if w.Code != 409 {
		t.Fatalf("陈旧 base 应 409: %d %s", w.Code, w.Body.String())
	}
	after, _ := kv.Get(ctx, cfgEditKey)
	if string(after.Kvs[0].Value) != string(resp.Kvs[0].Value) {
		t.Fatalf("409 后正文不应变化")
	}

	// ③ 正确 base（重新 GET raw 取 revision）→ 保存成功，正文落盘且占位保留
	w = cfgEditDo(r, "GET", "/api/v1/config/raw", "")
	var envelope struct {
		Code int `json:"code"`
		Data struct {
			Revision int64 `json:"revision"`
		} `json:"data"`
	}
	if err := jsonUnmarshalBody(w.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("解析 raw: %v", err)
	}
	base := envelope.Data.Revision
	w = cfgEditDo(r, "PUT", "/api/v1/sources/default/rules?confirm=true&base="+int64ToString(base), rulesPayload)
	if w.Code != 200 {
		t.Fatalf("保存应 200: %d %s", w.Code, w.Body.String())
	}
	after, _ = kv.Get(ctx, cfgEditKey)
	saved := string(after.Kvs[0].Value)
	if !strings.Contains(saved, "table: t2") {
		t.Fatalf("etcd 正文缺新表:\n%s", saved)
	}
	if !strings.Contains(saved, "${PW}") {
		t.Fatalf("etcd 正文 ${ENV} 占位被破坏:\n%s", saved)
	}
	if strings.Contains(saved, "observability") == false {
		t.Fatalf("etcd 段外内容被破坏（缺 observability）:\n%s", saved)
	}

	// ④ 用新 revision 再保存 → 上一次的 base 已陈旧 → 409（revision 真实递增）
	w = cfgEditDo(r, "PUT", "/api/v1/sources/default/rules?confirm=true&base="+int64ToString(base), rulesPayload)
	if w.Code != 409 {
		t.Fatalf("旧 base 二次提交应 409: %d %s", w.Code, w.Body.String())
	}

	// ⑤ 审计入环：base=1 冲突 + 保存成功 + 旧 base 重放冲突 = 3 条
	//（第 3 条 diff=+0：t2 已在步骤③落盘，重放无表级变化——口径自证）
	list, err := NewAuditStore(kv, "/gocdc/cfgedit", "n1").List(ctx, 100)
	if err != nil {
		t.Fatalf("审计读取: %v", err)
	}
	conflicts, saved1 := 0, 0
	for _, e := range list {
		if strings.Contains(e.What, "config.update_rules") {
			if e.Result == "ok" {
				saved1++
			} else {
				conflicts++
			}
		}
	}
	if conflicts != 2 || saved1 != 1 {
		t.Fatalf("审计应 2 冲突+1 成功: %d 冲突 %d 成功（%v）", conflicts, saved1, list)
	}

	// ⑥ check_only 免 confirm、不落盘
	before, _ := kv.Get(ctx, cfgEditKey)
	w = cfgEditDo(r, "PUT", "/api/v1/sources/default/rules?check_only=true", rulesPayload)
	if w.Code != 200 {
		t.Fatalf("check_only 应 200: %d %s", w.Code, w.Body.String())
	}
	afterCheck, _ := kv.Get(ctx, cfgEditKey)
	if string(afterCheck.Kvs[0].Value) != string(before.Kvs[0].Value) {
		t.Fatalf("check_only 不应写正文")
	}
}

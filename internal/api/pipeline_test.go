package api

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

// pipelineTestRouter 带暂停/恢复计数闭包的路由（装配计数器断言调用次数）。
func pipelineTestRouter(t *testing.T) (*ginEngine, *atomic.Int32, *atomic.Int32) {
	t.Helper()
	deps, _ := testDeps()
	var pauses, resumes atomic.Int32
	defaultSD(deps).PausePipeline = func(context.Context) error {
		pauses.Add(1)
		return nil
	}
	defaultSD(deps).ResumePipeline = func(context.Context) error {
		resumes.Add(1)
		return nil
	}
	return NewRouter(deps, AuthConfig{Username: "admin", Password: "pw"}), &pauses, &resumes
}

func TestRouter_PipelinePauseResume(t *testing.T) {
	r, pauses, resumes := pipelineTestRouter(t)

	// 缺 confirm → 400，闭包不被调用
	req := httptest.NewRequest("POST", "/api/v1/sources/default/pipeline/pause", nil)
	req.SetBasicAuth("admin", "pw")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != 400 {
		t.Fatalf("pause 缺 confirm 应 400: %d", w.Code)
	}
	if pauses.Load() != 0 {
		t.Fatalf("confirm 门禁前不应调用闭包: %d", pauses.Load())
	}

	// confirm=true → 200，pause 恰好一次
	req = httptest.NewRequest("POST", "/api/v1/sources/default/pipeline/pause?confirm=true", nil)
	req.SetBasicAuth("admin", "pw")
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("pause 应 200: %d %s", w.Code, w.Body.String())
	}
	if pauses.Load() != 1 {
		t.Fatalf("pause 闭包应恰好调用一次: %d", pauses.Load())
	}

	// resume 同构
	req = httptest.NewRequest("POST", "/api/v1/sources/default/pipeline/resume?confirm=true", nil)
	req.SetBasicAuth("admin", "pw")
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("resume 应 200: %d %s", w.Code, w.Body.String())
	}
	if resumes.Load() != 1 {
		t.Fatalf("resume 闭包应恰好调用一次: %d", resumes.Load())
	}
}

func TestRouter_PipelineNotWired(t *testing.T) {
	r, _ := testRouter(t) // Deps 未装配 Pause/Resume

	for _, path := range []string{"/api/v1/sources/default/pipeline/pause", "/api/v1/sources/default/pipeline/resume"} {
		req := httptest.NewRequest("POST", path+"?confirm=true", nil)
		req.SetBasicAuth("admin", "pw")
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		if w.Code != 500 {
			t.Fatalf("%s 未装配应 500: %d %s", path, w.Code, w.Body.String())
		}
	}
}

func TestRouter_PipelinePauseError(t *testing.T) {
	deps, _ := testDeps()
	defaultSD(deps).PausePipeline = func(context.Context) error { return errors.New("本节点当前不承担管线") }
	r := NewRouter(deps, AuthConfig{Username: "admin", Password: "pw"})

	req := httptest.NewRequest("POST", "/api/v1/sources/default/pipeline/pause?confirm=true", nil)
	req.SetBasicAuth("admin", "pw")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != 500 || !strings.Contains(w.Body.String(), "不承担管线") {
		t.Fatalf("闭包错误应 500 带消息: %d %s", w.Code, w.Body.String())
	}
}

package api

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRouter_EmbeddedSPA(t *testing.T) {
	r, _ := testRouter(t)

	for _, path := range []string{"/", "/rules", "/config"} {
		t.Run(path, func(t *testing.T) {
			req := httptest.NewRequest("GET", path, nil)
			w := httptest.NewRecorder()
			r.ServeHTTP(w, req)
			if w.Code != 200 {
				t.Fatalf("GET %s: got %d: %s", path, w.Code, w.Body.String())
			}
			if got := w.Header().Get("Content-Type"); !strings.HasPrefix(got, "text/html") {
				t.Fatalf("GET %s content type = %q", path, got)
			}
			if !strings.Contains(strings.ToLower(w.Body.String()), "gocdc") {
				t.Fatalf("GET %s did not serve embedded index: %s", path, w.Body.String())
			}
		})
	}
}

func TestRouter_StaticShellIsPublicButAPIIsProtected(t *testing.T) {
	r, _ := testRouter(t)

	page := httptest.NewRecorder()
	r.ServeHTTP(page, httptest.NewRequest("GET", "/rules", nil))
	if page.Code != 200 || !strings.Contains(page.Header().Get("Content-Type"), "text/html") {
		t.Fatalf("unauthenticated SPA shell got %d (%s), want public HTML", page.Code, page.Header().Get("Content-Type"))
	}

	api := httptest.NewRecorder()
	r.ServeHTTP(api, httptest.NewRequest("GET", "/api/v1/status", nil))
	if api.Code != 401 {
		t.Fatalf("unauthenticated API got %d, want 401", api.Code)
	}
}

func TestRouter_UnknownAPIPathRequiresAuthAndDoesNotServeSPA(t *testing.T) {
	r, _ := testRouter(t)
	unauthenticated := httptest.NewRecorder()
	r.ServeHTTP(unauthenticated, httptest.NewRequest("GET", "/api/v1/not-a-route", nil))
	if unauthenticated.Code != 401 {
		t.Fatalf("unauthenticated unknown API path got %d, want 401", unauthenticated.Code)
	}

	req := httptest.NewRequest("GET", "/api/v1/not-a-route", nil)
	req.SetBasicAuth("admin", "pw")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != 404 {
		t.Fatalf("unknown API path got %d, want 404", w.Code)
	}
	if strings.Contains(w.Header().Get("Content-Type"), "text/html") {
		t.Fatal("unknown API path unexpectedly served SPA")
	}
}

func TestServeSPA_MissingAssetIsNotHTML(t *testing.T) {
	req := httptest.NewRequest("GET", "/assets/missing.js", nil)
	w := httptest.NewRecorder()
	serveSPA(w, req)
	if w.Code != 404 {
		t.Fatalf("missing asset got %d, want 404", w.Code)
	}
	if strings.Contains(w.Header().Get("Content-Type"), "text/html") {
		t.Fatal("missing asset unexpectedly served index.html")
	}
}

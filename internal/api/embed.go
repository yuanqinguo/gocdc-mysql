package api

import (
	"embed"
	"io/fs"
	"net/http"
	"path"
	"strings"
)

// staticFiles contains the production Vite output. Rebuild it with
// `cd web && npm ci && npm run build` before compiling the Go binary.
//
//go:embed dist
var staticFiles embed.FS

var (
	webFiles  = mustSubFS(staticFiles, "dist")
	webServer = http.FileServer(http.FS(webFiles))
)

func mustSubFS(fsys fs.FS, dir string) fs.FS {
	sub, err := fs.Sub(fsys, dir)
	if err != nil {
		panic(err)
	}
	return sub
}

// serveSPA serves embedded assets and falls back to index.html for client-side
// routes. API paths never fall back to the app shell.
func serveSPA(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	requestPath := path.Clean("/" + strings.TrimPrefix(r.URL.Path, "/"))
	if requestPath == "/" {
		serveIndex(w)
		return
	}
	if requestPath == "/api" || strings.HasPrefix(requestPath, "/api/") {
		http.NotFound(w, r)
		return
	}

	filePath := strings.TrimPrefix(requestPath, "/")
	info, err := fs.Stat(webFiles, filePath)
	if err == nil && !info.IsDir() {
		clone := r.Clone(r.Context())
		urlCopy := *r.URL
		urlCopy.Path = "/" + filePath
		clone.URL = &urlCopy
		webServer.ServeHTTP(w, clone)
		return
	}

	// A missing asset must remain a 404 rather than returning HTML to the
	// browser's JavaScript/CSS loader.
	if path.Ext(requestPath) != "" || strings.HasPrefix(requestPath, "/assets/") {
		http.NotFound(w, r)
		return
	}
	serveIndex(w)
}

func serveIndex(w http.ResponseWriter) {
	index, err := fs.ReadFile(webFiles, "index.html")
	if err != nil {
		http.Error(w, "管理端静态资源缺失，请先构建 web/", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(index)
}

package metrics

import (
	"net/http"
	"sort"
	"strings"
	"sync"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Server 承载 /metrics、/healthz、/readyz（11 §2）：
//   - /healthz：liveness，只表示进程存活，不检查外部依赖（防级联重启）
//   - /readyz：就绪 = 全部已注册的就绪探针通过（配置已加载、etcd 可达、角色确定；
//     leader 附加管线运行中——各组件按需注册自己的探针）
type Server struct {
	mu     sync.RWMutex
	probes map[string]func() bool
}

// NewServer 构建健康检查服务。
func NewServer() *Server {
	return &Server{probes: make(map[string]func() bool)}
}

// RegisterProbe 注册/覆盖一个就绪探针（名称用于 /readyz 失败时的定位输出）。
func (s *Server) RegisterProbe(name string, fn func() bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.probes[name] = fn
}

// Handler 返回挂载了三个端点的 http.ServeMux。
func (s *Server) Handler(m *Metrics) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/readyz", s.readyz)
	return mux
}

func (s *Server) readyz(w http.ResponseWriter, _ *http.Request) {
	s.mu.RLock()
	names := make([]string, 0, len(s.probes))
	fns := make(map[string]func() bool, len(s.probes))
	for n, f := range s.probes {
		names = append(names, n)
		fns[n] = f
	}
	s.mu.RUnlock()
	sort.Strings(names)

	var failed []string
	for _, n := range names {
		if !fns[n]() {
			failed = append(failed, n)
		}
	}
	if len(failed) > 0 {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("not ready: " + strings.Join(failed, ",")))
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ready"))
}

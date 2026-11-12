// 服务级重启编排单测：纯函数（地址归一/排序）+ httptest 假 peer 的 fan-out
// （触发成功/触发失败/健康等待超时）。触 etcd 的 listPeers 由集成测试覆盖。
package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"gocdc/internal/config"
	"gocdc/internal/etcdx"
)

func TestNormalizePeerAddr(t *testing.T) {
	cases := []struct{ in, want string }{
		{"10.0.0.5:8060", "http://10.0.0.5:8060"},
		{"http://10.0.0.5:8060", "http://10.0.0.5:8060"},
		{"https://node1.cluster:8443/", "https://node1.cluster:8443"},
		{"  ", ""}, {"", ""},
	}
	for _, tc := range cases {
		if got := normalizePeerAddr(tc.in); got != tc.want {
			t.Fatalf("normalizePeerAddr(%q)=%q want %q", tc.in, got, tc.want)
		}
	}
}

func TestSortPeers_LeaderLast(t *testing.T) {
	peers := []registryPeer{
		{NodeInfo: etcdx.NodeInfo{NodeID: "n-leader", Role: "leader"}, Addr: "http://l"},
		{NodeInfo: etcdx.NodeInfo{NodeID: "n-fb2", Role: "follower"}, Addr: "http://f2"},
		{NodeInfo: etcdx.NodeInfo{NodeID: "n-fb1", Role: "follower"}, Addr: "http://f1"},
	}
	sorted := sortPeers(peers)
	if sorted[0].NodeInfo.NodeID != "n-fb1" || sorted[1].NodeInfo.NodeID != "n-fb2" {
		t.Fatalf("follower 应排前: %v %v", sorted[0], sorted[1])
	}
	if sorted[2].NodeInfo.NodeID != "n-leader" {
		t.Fatalf("leader 应排最后: %v", sorted[2])
	}
}

// TestClusterRestartFanout httptest 假 peer：① 正常重启（退出→拉起→健康）；
// ② 触发即失败（500）；③ 健康永不恢复（超时路径，等待上限调短）。
func TestClusterRestartFanout(t *testing.T) {
	var healthy atomic.Bool
	okSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/config/apply-restart":
			if r.URL.Query().Get("confirm") != "true" {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			healthy.Store(false) // 模拟退出
			go func() {
				time.Sleep(2 * time.Second)
				healthy.Store(true) // 模拟编排拉起就绪
			}()
			_, _ = w.Write([]byte(`{"code":0}`))
		case "/healthz":
			if healthy.Load() {
				_, _ = w.Write([]byte("ok"))
			} else {
				w.WriteHeader(http.StatusServiceUnavailable)
			}
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer okSrv.Close()
	badSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer badSrv.Close()
	stuckSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("not-ok"))
	}))
	defer stuckSrv.Close()

	cr := &clusterRestarter{
		cfg:         &config.Config{Observability: config.Observability{Web: config.WebOptions{Username: "admin", Password: "pw"}}},
		nodeID:      "self",
		client:      &http.Client{Timeout: 5 * time.Second},
		healthLimit: 4 * time.Second,
	}
	peers := []registryPeer{
		{NodeInfo: etcdx.NodeInfo{NodeID: "p-ok", Role: "follower"}, Addr: okSrv.URL},
		{NodeInfo: etcdx.NodeInfo{NodeID: "p-bad", Role: "follower"}, Addr: badSrv.URL},
		{NodeInfo: etcdx.NodeInfo{NodeID: "p-stuck", Role: "leader"}, Addr: stuckSrv.URL},
	}
	res := cr.fanoutPeers(context.Background(), peers)
	if res.Scope != "cluster" || len(res.Peers) != 3 {
		t.Fatalf("结果形态异常: %+v", res)
	}
	if !res.Peers[0].Ok {
		t.Fatalf("正常 peer 应 ok: %+v", res.Peers[0])
	}
	if res.Peers[1].Ok || res.Peers[1].Detail == "" {
		t.Fatalf("触发失败 peer 应带原因: %+v", res.Peers[1])
	}
	if res.Peers[2].Ok || res.Peers[2].Detail == "" {
		t.Fatalf("健康超时 peer 应带原因: %+v", res.Peers[2])
	}
}

// 单节点语义：无 peers → peers=[] + self 说明（行为与旧「重启本节点」一致）。
func TestClusterRestartFanout_SingleNode(t *testing.T) {
	cr := &clusterRestarter{
		cfg:         &config.Config{},
		nodeID:      "self",
		client:      &http.Client{Timeout: 5 * time.Second},
		healthLimit: time.Second,
	}
	res := cr.fanoutPeers(context.Background(), nil)
	if len(res.Peers) != 0 || res.Self == "" {
		t.Fatalf("单节点语义: %+v", res)
	}
}

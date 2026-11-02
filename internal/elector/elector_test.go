// 无 Docker 单测：选举参数纯函数 + verifyLeadership 复核状态机（注入内存 KV）。
// 需要 etcd 的多节点场景见 elector_integration_test.go（integration 标签）。
package elector

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestNodeValueNodeIDOfRoundTrip election 值 JSON 编解码往返；nodeIDOf 解析
// 失败回退原文（全等比较防节点 ID 互为前缀误判归属）。
func TestNodeValueNodeIDOfRoundTrip(t *testing.T) {
	v := nodeValue("node-1", "10.0.0.1:9900")
	if got := nodeIDOf(v); got != "node-1" {
		t.Fatalf("nodeIDOf(合法 JSON) = %q", got)
	}
	// 前缀节点：必须全等命中而非 Contains 子串
	if got := nodeIDOf(nodeValue("node-10", "x")); got == "node-1" {
		t.Fatal("node-10 被误判为 node-1（前缀互含）")
	}
	// 非 JSON（手工写入的旧值）：回退原文
	if got := nodeIDOf("raw-node"); got != "raw-node" {
		t.Fatalf("nodeIDOf(非 JSON) = %q，应回退原文", got)
	}
}

// TestNextBackoff 指数退避翻倍 + 10s 封顶。
func TestNextBackoff(t *testing.T) {
	cases := []struct {
		in, want time.Duration
	}{
		{100 * time.Millisecond, 200 * time.Millisecond},
		{2 * time.Second, 4 * time.Second},
		{8 * time.Second, 10 * time.Second}, // 翻倍越界 → 封顶
		{10 * time.Second, 10 * time.Second},
	}
	for _, c := range cases {
		if got := nextBackoff(c.in); got != c.want {
			t.Errorf("nextBackoff(%v) = %v, want %v", c.in, got, c.want)
		}
	}
}

// TestCheckTimeout 复核超时 = 2×复核周期，≤ lease TTL 一半，下限 1s。
func TestCheckTimeout(t *testing.T) {
	cases := []struct {
		interval time.Duration
		ttl      int64
		want     time.Duration
		note     string
	}{
		{2 * time.Second, 10, 4 * time.Second, "常规：2×周期 < TTL/2"},
		{5 * time.Second, 10, 5 * time.Second, "TTL/2 封顶（保证分区内 TTL 停管线）"},
		{200 * time.Millisecond, 10, time.Second, "下限 1s 防误判抖动"},
		{5 * time.Second, 0, 10 * time.Second, "TTL 未配置：仅 2×周期"},
	}
	for _, c := range cases {
		e := New(nil, Options{LeaderCheckInterval: c.interval, LeaseTTL: c.ttl})
		if got := e.checkTimeout(); got != c.want {
			t.Errorf("%s: checkTimeout(interval=%v,ttl=%d) = %v, want %v",
				c.note, c.interval, c.ttl, got, c.want)
		}
	}
}

// fakeKV 内存 KV（仅 verifyLeadership 用到的 Get/Put 路径；Txn/Compact 空实现）。
type fakeKV struct {
	mu     sync.Mutex
	vals   map[string]string
	rev    int64
	getErr map[string]error // 按 key 注入读取失败（etcd 瞬态）
	puts   [][2]string      // Put 记录（key, val），断言自愈重写用
}

func newFakeKV() *fakeKV {
	return &fakeKV{vals: map[string]string{}, getErr: map[string]error{}}
}

func (f *fakeKV) Get(_ context.Context, key string, _ ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.getErr[key]; err != nil {
		return nil, err
	}
	resp := &clientv3.GetResponse{Header: &etcdserverpb.ResponseHeader{Revision: f.rev}}
	if v, ok := f.vals[key]; ok {
		resp.Kvs = append(resp.Kvs, &mvccpb.KeyValue{Key: []byte(key), Value: []byte(v), ModRevision: f.rev})
		resp.Count = 1
	}
	return resp, nil
}

func (f *fakeKV) Put(_ context.Context, key, val string, _ ...clientv3.OpOption) (*clientv3.PutResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rev++
	f.vals[key] = val
	f.puts = append(f.puts, [2]string{key, val})
	return &clientv3.PutResponse{Header: &etcdserverpb.ResponseHeader{Revision: f.rev}}, nil
}

func (f *fakeKV) Delete(_ context.Context, _ string, _ ...clientv3.OpOption) (*clientv3.DeleteResponse, error) {
	return &clientv3.DeleteResponse{}, nil
}

func (f *fakeKV) Compact(context.Context, int64, ...clientv3.CompactOption) (*clientv3.CompactResponse, error) {
	return &clientv3.CompactResponse{}, nil
}

func (f *fakeKV) Do(context.Context, clientv3.Op) (clientv3.OpResponse, error) {
	return clientv3.OpResponse{}, nil
}

func (f *fakeKV) Txn(context.Context) clientv3.Txn { return nopTxn{} }

// nopTxn 空事务（复核路径不经过事务，仅满足 KV 接口）。
type nopTxn struct{}

func (nopTxn) If(...clientv3.Cmp) clientv3.Txn  { return nopTxn{} }
func (nopTxn) Then(...clientv3.Op) clientv3.Txn { return nopTxn{} }
func (nopTxn) Else(...clientv3.Op) clientv3.Txn { return nopTxn{} }
func (nopTxn) Commit() (*clientv3.TxnResponse, error) {
	return &clientv3.TxnResponse{Succeeded: true}, nil
}

// newTestElector 注入内存 KV 的选举器（不连网）。
func newTestElector(kv *fakeKV, nodeID string) *Elector {
	return New(&clientv3.Client{KV: kv}, Options{
		Prefix: "/gocdc/test", NodeID: nodeID, LeaseTTL: 10,
		LeaderCheckInterval: 200 * time.Millisecond,
	})
}

// TestVerifyLeadership 在位复核判定：key 消失/易主/etcd 错误 → 失位；
// 归属成立时 leader 指针缺失或易主 → 自愈重写（不因指针问题失位）。
// leaseID 以任意非零值注入（本路径只把它原样挂到自愈 Put 上）。
func TestVerifyLeadership(t *testing.T) {
	const me, other = "node-a", "node-b"
	const leaseID = clientv3.LeaseID(123)
	electionKey := "/gocdc/test/election/lease-1"
	myVal := nodeValue(me, "10.0.0.1")

	t.Run("选举 key 消失（lease 失效被 etcd 删除）→ 失位", func(t *testing.T) {
		kv := newFakeKV()
		e := newTestElector(kv, me)
		if reason := e.verifyLeadership(context.Background(), leaseID, electionKey); reason != "election_key_gone" {
			t.Fatalf("reason = %q, want election_key_gone", reason)
		}
	})

	t.Run("选举 key 易主 → 失位", func(t *testing.T) {
		kv := newFakeKV()
		kv.vals[electionKey] = nodeValue(other, "10.0.0.2")
		e := newTestElector(kv, me)
		if reason := e.verifyLeadership(context.Background(), leaseID, electionKey); reason != "election_key_taken" {
			t.Fatalf("reason = %q, want election_key_taken", reason)
		}
	})

	t.Run("etcd 读取失败 → 失位（复核有界，错误即判定）", func(t *testing.T) {
		kv := newFakeKV()
		kv.getErr[electionKey] = errors.New("etcd down")
		e := newTestElector(kv, me)
		if reason := e.verifyLeadership(context.Background(), leaseID, electionKey); reason == "" {
			t.Fatal("etcd 错误应返回失位原因")
		}
	})

	t.Run("归属成立 + 指针正常 → 不失位不重写", func(t *testing.T) {
		kv := newFakeKV()
		kv.vals[electionKey] = myVal
		kv.vals["/gocdc/test/leader"] = me
		e := newTestElector(kv, me)
		if reason := e.verifyLeadership(context.Background(), leaseID, electionKey); reason != "" {
			t.Fatalf("reason = %q, want 空（仍在位）", reason)
		}
		if n := len(kv.puts); n != 0 {
			t.Fatalf("指针正常不应触发重写，实际 Put %d 次", n)
		}
	})

	t.Run("指针缺失 → 自愈重写为本节点", func(t *testing.T) {
		kv := newFakeKV()
		kv.vals[electionKey] = myVal
		e := newTestElector(kv, me)
		if reason := e.verifyLeadership(context.Background(), leaseID, electionKey); reason != "" {
			t.Fatalf("指针缺失不应失位（election 归属仍成立）: %q", reason)
		}
		if len(kv.puts) != 1 || kv.puts[0][0] != "/gocdc/test/leader" || kv.puts[0][1] != me {
			t.Fatalf("应重写 leader 指针一次，实际 %v", kv.puts)
		}
	})

	t.Run("指针易主 → 夺回重写", func(t *testing.T) {
		kv := newFakeKV()
		kv.vals[electionKey] = myVal
		kv.vals["/gocdc/test/leader"] = other
		e := newTestElector(kv, me)
		if reason := e.verifyLeadership(context.Background(), leaseID, electionKey); reason != "" {
			t.Fatalf("reason = %q, want 空", reason)
		}
		if len(kv.puts) != 1 || kv.puts[0][1] != me {
			t.Fatalf("应重写 leader 指针为本节点，实际 %v", kv.puts)
		}
	})

	t.Run("指针读取瞬态失败 → 不失位不重写（下周期重试）", func(t *testing.T) {
		kv := newFakeKV()
		kv.vals[electionKey] = myVal
		kv.getErr["/gocdc/test/leader"] = errors.New("timeout")
		e := newTestElector(kv, me)
		if reason := e.verifyLeadership(context.Background(), leaseID, electionKey); reason != "" {
			t.Fatalf("leader 指针瞬态错误不应失位: %q", reason)
		}
		if n := len(kv.puts); n != 0 {
			t.Fatalf("读取失败不应触发重写，实际 Put %d 次", n)
		}
	})
}

// TestRoleTransitions 角色/term 原子读初始态。
func TestRoleTransitions(t *testing.T) {
	e := newTestElector(newFakeKV(), "node-a")
	if e.Role() != RoleFollower {
		t.Fatal("初始应为 follower")
	}
	if e.Term() != 0 {
		t.Fatalf("初始 term 应为 0，实际 %d", e.Term())
	}
	if e.LeaderPtr() != "" {
		t.Fatal("初始 leader 指针应为空（无主）")
	}
	// beFollower 幂等：follower → follower 不打日志不错乱
	e.beFollower("test")
	if e.Role() != RoleFollower {
		t.Fatal("beFollower 应保持 follower")
	}
}

// TestRequestResign 让位请求非阻塞（无接收方时不挂起，channel 容量 1 吸收重复）。
func TestRequestResign(t *testing.T) {
	e := newTestElector(newFakeKV(), "node-a")
	done := make(chan struct{})
	go func() {
		e.RequestResign()
		e.RequestResign()
		e.RequestResign()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("RequestResign 阻塞（应非阻塞丢重）")
	}
}

package etcdx

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// fakeKV 实现 clientv3.KV：按脚本注入错误序列，用于无容器单测。
// 约定：errs[i-1] 为第 i 次调用的错误；超出脚本后一律成功。
type fakeKV struct {
	mu       sync.Mutex
	getErrs  []error
	putErrs  []error
	gets     int
	puts     int
	txnFails int // 前 N 次 Commit 失败，之后成功
	txnCalls int
}

func (f *fakeKV) script(errs []error, n int) error {
	if n <= len(errs) {
		return errs[n-1]
	}
	return nil
}

func (f *fakeKV) Get(_ context.Context, _ string, _ ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	f.mu.Lock()
	f.gets++
	n := f.gets
	f.mu.Unlock()
	return &clientv3.GetResponse{}, f.script(f.getErrs, n)
}

func (f *fakeKV) Put(_ context.Context, _, _ string, _ ...clientv3.OpOption) (*clientv3.PutResponse, error) {
	f.mu.Lock()
	f.puts++
	n := f.puts
	f.mu.Unlock()
	return &clientv3.PutResponse{}, f.script(f.putErrs, n)
}

func (f *fakeKV) Delete(_ context.Context, _ string, _ ...clientv3.OpOption) (*clientv3.DeleteResponse, error) {
	return &clientv3.DeleteResponse{}, nil
}

func (f *fakeKV) Compact(_ context.Context, _ int64, _ ...clientv3.CompactOption) (*clientv3.CompactResponse, error) {
	return &clientv3.CompactResponse{}, nil
}

func (f *fakeKV) Do(_ context.Context, _ clientv3.Op) (clientv3.OpResponse, error) {
	return clientv3.OpResponse{}, nil
}

func (f *fakeKV) Txn(_ context.Context) clientv3.Txn { return &fakeTxn{kv: f} }

// fakeTxn 实现 clientv3.Txn（仅 Commit 有行为）。
type fakeTxn struct{ kv *fakeKV }

func (t *fakeTxn) If(_ ...clientv3.Cmp) clientv3.Txn  { return t }
func (t *fakeTxn) Then(_ ...clientv3.Op) clientv3.Txn { return t }
func (t *fakeTxn) Else(_ ...clientv3.Op) clientv3.Txn { return t }
func (t *fakeTxn) Do(_ clientv3.Op) clientv3.Txn      { return t }

func (t *fakeTxn) Commit() (*clientv3.TxnResponse, error) {
	t.kv.mu.Lock()
	t.kv.txnCalls++
	n := t.kv.txnCalls
	fail := t.kv.txnFails
	t.kv.mu.Unlock()
	if n <= fail {
		return nil, fmt.Errorf("etcdserver: down (第 %d 次)", n)
	}
	return &clientv3.TxnResponse{}, nil
}

// newTestKV 构建注入 fake 的 KV 包装：重试上限 3、退避 1ms（测试提速）。
func newTestKV(fk *fakeKV, backoff func(int) time.Duration) *KV {
	k := NewKV(&clientv3.Client{KV: fk})
	k.backoff = backoff
	return k
}

// TestKVPutRetriesTransient 瞬时错误按退避重试直至成功。
func TestKVPutRetriesTransient(t *testing.T) {
	fk := &fakeKV{putErrs: []error{
		errors.New("connection refused"),
		errors.New("connection reset by peer"),
	}}
	var attempts []int
	k := newTestKV(fk, func(a int) time.Duration {
		attempts = append(attempts, a)
		return time.Millisecond
	})

	if _, err := k.Put(context.Background(), "k", "v"); err != nil {
		t.Fatalf("重试后应成功，实际: %v", err)
	}
	if n := fk.puts; n != 3 {
		t.Fatalf("应尝试 3 次（1 初次+2 重试），实际 %d", n)
	}
	if len(attempts) != 2 || attempts[0] != 1 || attempts[1] != 2 {
		t.Fatalf("退避应按 attempt=1,2 调用，实际 %v", attempts)
	}
}

// TestKVPutNonRetryable 认证/授权类错误不重试，立即上抛。
func TestKVPutNonRetryable(t *testing.T) {
	fk := &fakeKV{putErrs: []error{rpctypes.ErrPermissionDenied}}
	k := newTestKV(fk, func(int) time.Duration { return time.Millisecond })

	if _, err := k.Put(context.Background(), "k", "v"); err == nil {
		t.Fatal("权限拒绝应报错")
	}
	if n := fk.puts; n != 1 {
		t.Fatalf("不可重试错误应只尝试 1 次，实际 %d", n)
	}
}

// TestKVGetRetryExhausted 重试耗尽后报错，且总尝试 = 1+retries。
func TestKVGetRetryExhausted(t *testing.T) {
	fk := &fakeKV{getErrs: []error{
		errors.New("e1"), errors.New("e2"), errors.New("e3"), errors.New("e4"),
	}}
	k := newTestKV(fk, func(int) time.Duration { return time.Millisecond })

	_, err := k.Get(context.Background(), "k")
	if err == nil {
		t.Fatal("重试耗尽应报错")
	}
	if n := fk.gets; n != 4 {
		t.Fatalf("应尝试 4 次（1 初次+3 重试），实际 %d", n)
	}
	if !strings.Contains(err.Error(), "e4") {
		t.Fatalf("错误应包含最后一次原因 e4: %v", err)
	}
}

// TestKVTxnRetry 事务瞬时失败同样按退避重试，build 每次尝试重新构造。
func TestKVTxnRetry(t *testing.T) {
	fk := &fakeKV{txnFails: 2}
	k := newTestKV(fk, func(int) time.Duration { return time.Millisecond })

	buildCalls := 0
	resp, err := k.Txn(context.Background(), func(txn clientv3.Txn) clientv3.Txn {
		buildCalls++
		return txn.If(clientv3.Compare(clientv3.Version("k"), "=", 0)).Then(clientv3.OpPut("k", "v"))
	})
	if err != nil {
		t.Fatalf("事务重试后应成功: %v", err)
	}
	if resp == nil {
		t.Fatal("应返回事务结果")
	}
	if buildCalls != 3 {
		t.Fatalf("build 应被调用 3 次（每次尝试一次），实际 %d", buildCalls)
	}
}

// TestKVRetryCtxCancelDuringBackoff 父 ctx 在退避等待期间取消，立即返回不继续重试。
func TestKVRetryCtxCancelDuringBackoff(t *testing.T) {
	fk := &fakeKV{putErrs: []error{errors.New("transient"), errors.New("transient")}}
	k := newTestKV(fk, func(int) time.Duration { return 100 * time.Millisecond })

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	_, err := k.Put(ctx, "k", "v")
	if err == nil {
		t.Fatal("ctx 取消后应报错")
	}
	if time.Since(start) > 90*time.Millisecond {
		t.Fatalf("取消后应尽快返回，实际耗时 %v", time.Since(start))
	}
	if n := fk.puts; n > 2 {
		t.Fatalf("取消后不应继续尝试，实际 %d 次", n)
	}
}

// TestIsRetryable 错误分类表。
func TestIsRetryable(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"主动取消", context.Canceled, false},
		{"超时(瞬态)", context.DeadlineExceeded, true},
		{"权限拒绝", rpctypes.ErrPermissionDenied, false},
		{"认证失败", rpctypes.ErrAuthFailed, false},
		{"认证版本冲突", rpctypes.ErrAuthOldRevision, false},
		{"网络瞬态", errors.New("connection refused"), true},
	} {
		if got := IsRetryable(tc.err); got != tc.want {
			t.Fatalf("%s: IsRetryable=%v，期望 %v", tc.name, got, tc.want)
		}
	}
}

// TestNewRequiresEndpoints 空 endpoints fail-fast。
func TestNewRequiresEndpoints(t *testing.T) {
	if _, err := New(Options{}); !errors.Is(err, ErrNoEndpoints) {
		t.Fatalf("应返回 ErrNoEndpoints，实际: %v", err)
	}
}

// TestNodeInfoJSON 注册值字段名与 03 §2 schema 对应。
func TestNodeInfoJSON(t *testing.T) {
	b, err := json.Marshal(NodeInfo{NodeID: "n1", AdvertiseAddr: "ip:8060", Version: "dev", Role: "follower"})
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"node_id", "advertise_addr", "version", "started_at", "role"} {
		if _, ok := m[key]; !ok {
			t.Fatalf("注册值缺少字段 %q: %s", key, b)
		}
	}
}

// TestRegistrarKey key 从 prefix 派生，无硬编码。
func TestRegistrarKey(t *testing.T) {
	r := NewRegistrar(nil, "/gocdc/c1", NodeInfo{NodeID: "node-a"})
	if got, want := r.Key(), "/gocdc/c1/nodes/node-a"; got != want {
		t.Fatalf("Key()=%s，期望 %s", got, want)
	}
}

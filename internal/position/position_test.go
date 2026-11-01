package position

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"

	"gocdc/internal/etcdx"
	"gocdc/internal/model"
)

// gateKV 模拟 etcd：gateKey 是否存活 + 附着 lease，事务按此裁决 Succeeded；
// 捕获位点 Put 的 value 供断言。足够覆盖 Store 的无容器单测。
type gateKV struct {
	mu        sync.Mutex
	gateAlive bool
	gateLease int64
	myLease   int64
	putKey    string
	putVal    string
	getVal    string // 预置位点（Load 用）；空 = 无位点
	getErr    error
}

func (f *gateKV) Get(_ context.Context, _ string, _ ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.getErr != nil {
		return nil, f.getErr
	}
	resp := &clientv3.GetResponse{}
	if f.getVal != "" {
		resp.Kvs = []*mvccpb.KeyValue{{Value: []byte(f.getVal)}}
	}
	return resp, nil
}

func (f *gateKV) Put(_ context.Context, key, val string, _ ...clientv3.OpOption) (*clientv3.PutResponse, error) {
	f.mu.Lock()
	f.putKey, f.putVal = key, val
	f.mu.Unlock()
	return &clientv3.PutResponse{}, nil
}

func (f *gateKV) Delete(_ context.Context, _ string, _ ...clientv3.OpOption) (*clientv3.DeleteResponse, error) {
	return &clientv3.DeleteResponse{}, nil
}

func (f *gateKV) Compact(_ context.Context, _ int64, _ ...clientv3.CompactOption) (*clientv3.CompactResponse, error) {
	return &clientv3.CompactResponse{}, nil
}

func (f *gateKV) Do(_ context.Context, _ clientv3.Op) (clientv3.OpResponse, error) {
	return clientv3.OpResponse{}, errors.New("not used")
}

func (f *gateKV) Txn(_ context.Context) clientv3.Txn { return &gateTxn{kv: f} }

type gateTxn struct{ kv *gateKV }

func (t *gateTxn) If(cmps ...clientv3.Cmp) clientv3.Txn {
	// 校验门槛比较式形态：LeaseID(gateKey) == myLease（形态固定，Value 直接校验语义标记）
	t.kv.mu.Lock()
	defer t.kv.mu.Unlock()
	if len(cmps) != 1 {
		t.kv.gateAlive = false
	}
	return t
}
func (t *gateTxn) Then(ops ...clientv3.Op) clientv3.Txn {
	t.kv.mu.Lock()
	defer t.kv.mu.Unlock()
	for _, op := range ops {
		if op.IsPut() {
			t.kv.putKey = string(op.KeyBytes())
			t.kv.putVal = string(op.ValueBytes())
		}
	}
	return t
}
func (t *gateTxn) Else(...clientv3.Op) clientv3.Txn { return t }
func (t *gateTxn) Do(clientv3.Op) clientv3.Txn      { return t }
func (t *gateTxn) Commit() (*clientv3.TxnResponse, error) {
	t.kv.mu.Lock()
	defer t.kv.mu.Unlock()
	ok := t.kv.gateAlive && t.kv.gateLease == t.kv.myLease
	return &clientv3.TxnResponse{Succeeded: ok}, nil
}

func newTestStore(fk *gateKV) *Store {
	cli := &clientv3.Client{KV: fk}
	kv := etcdx.NewKV(cli)
	return NewStore(kv, "/gocdc/c1/position", "/gocdc/c1/election", 42)
}

func TestStore_SaveGate(t *testing.T) {
	fk := &gateKV{gateAlive: true, gateLease: 42, myLease: 42}
	s := newTestStore(fk)

	p := model.Position{Name: "bin.000123", Pos: 45678, Seq: 9, Term: 7, NodeID: "n1"}
	if err := s.Save(context.Background(), p); err != nil {
		t.Fatalf("门槛通过时应写入: %v", err)
	}
	var saved model.Position
	if err := json.Unmarshal([]byte(fk.putVal), &saved); err != nil {
		t.Fatal(err)
	}
	if saved.V != positionVersion || saved.Name != "bin.000123" || saved.Pos != 45678 {
		t.Fatalf("落盘内容: %+v", saved)
	}
	if saved.UpdatedAt.IsZero() {
		t.Fatal("updated_at 应被填充")
	}
	if fk.putKey != "/gocdc/c1/position" {
		t.Fatalf("写入 key: %s", fk.putKey)
	}

	// lease 失效（key 消失）→ 写被拒
	fk.mu.Lock()
	fk.gateAlive = false
	fk.mu.Unlock()
	err := s.Save(context.Background(), p)
	if !errors.Is(err, ErrGateRejected) {
		t.Fatalf("lease 失效应返回 ErrGateRejected: %v", err)
	}

	// lease 易主（新 leader 的 lease 附着在 gateKey）→ 同样被拒
	fk.mu.Lock()
	fk.gateAlive = true
	fk.gateLease = 99
	fk.mu.Unlock()
	if err := s.Save(context.Background(), p); !errors.Is(err, ErrGateRejected) {
		t.Fatalf("lease 易主应被拒: %v", err)
	}
}

func TestStore_Load(t *testing.T) {
	fk := &gateKV{}
	s := newTestStore(fk)

	// 无位点
	if _, ok, err := s.Load(context.Background()); err != nil || ok {
		t.Fatalf("无位点应 (zero,false,nil): %v %v", ok, err)
	}

	// 有点位
	raw, _ := json.Marshal(model.Position{V: 1, Name: "bin.000001", Pos: 4, Seq: 3})
	fk.mu.Lock()
	fk.getVal = string(raw)
	fk.mu.Unlock()
	p, ok, err := s.Load(context.Background())
	if err != nil || !ok || p.Name != "bin.000001" || p.Pos != 4 {
		t.Fatalf("load: %v %v %+v", ok, err, p)
	}

	// 坏 JSON 显式报错（绝不带空/旧位点启动，03 §3.1 精神）
	fk.mu.Lock()
	fk.getVal = "{not-json"
	fk.mu.Unlock()
	if _, _, err := s.Load(context.Background()); err == nil || !strings.Contains(err.Error(), "解析失败") {
		t.Fatalf("坏位点应报错: %v", err)
	}
}

func TestThrottler(t *testing.T) {
	th := NewThrottler(3 * time.Second)
	t0 := time.Unix(1000, 0)
	if !th.Allow(t0) {
		t.Fatal("首次应允许")
	}
	if th.Allow(t0.Add(1 * time.Second)) {
		t.Fatal("窗口内应拒绝")
	}
	if !th.Allow(t0.Add(3 * time.Second)) {
		t.Fatal("窗口过后应允许")
	}
	// Reset 后立即拒绝（强制保存重置窗口）
	th.Reset(t0.Add(4 * time.Second))
	if th.Allow(t0.Add(5 * time.Second)) {
		t.Fatal("Reset 后窗口内应拒绝")
	}
	// interval=0 每次允许
	if !NewThrottler(0).Allow(t0) || !NewThrottler(0).Allow(t0) {
		t.Fatal("interval=0 应每次允许")
	}
}

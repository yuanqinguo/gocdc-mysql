package sink

import (
	"context"
	"errors"
	"testing"

	"gocdc/internal/model"
)

// fakeSink 记录调用并可注入失败。
type fakeSink struct {
	name   string
	calls  int
	events int
	failAt int // 第 N 次 Send 失败（0 = 永不）
	err    error
}

func (f *fakeSink) Name() string { return f.name }
func (f *fakeSink) Ping(context.Context) error {
	if f.err != nil {
		return f.err
	}
	return nil
}
func (f *fakeSink) Send(_ context.Context, batch []*model.Event) error {
	f.calls++
	f.events += len(batch)
	if f.failAt > 0 && f.calls == f.failAt {
		return f.err
	}
	return nil
}
func (f *fakeSink) Close(context.Context) error { return nil }

func TestSinkGroup_Order(t *testing.T) {
	my := &fakeSink{name: "mysql"}
	mq := &fakeSink{name: "kafka"}
	g, err := NewSinkGroup(my, mq)
	if err != nil {
		t.Fatal(err)
	}
	batch := []*model.Event{{EventID: "f:1:0", Type: model.EventInsert, Pk: map[string]any{"id": 1}}}
	if err := g.Send(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	if my.calls != 1 || mq.calls != 1 {
		t.Fatalf("两边都应投递: mysql=%d kafka=%d", my.calls, mq.calls)
	}
	if g.Name() != "group(mysql+kafka)" {
		t.Fatalf("name: %s", g.Name())
	}
}

func TestSinkGroup_SingleSideFailureStopsAll(t *testing.T) {
	my := &fakeSink{name: "mysql"}
	mq := &fakeSink{name: "kafka", failAt: 1, err: errors.New("broker down")}
	g, _ := NewSinkGroup(my, mq)
	batch := []*model.Event{{EventID: "f:1:0", Type: model.EventInsert, Pk: map[string]any{"id": 1}}}
	err := g.Send(context.Background(), batch)
	if err == nil || !stringsContains(err.Error(), "kafka") {
		t.Fatalf("应上抛 kafka 失败: %v", err)
	}
	// 单边失败：位点不推进（此处等价=返回错误）；下一轮 MySQL 仍会重复投递
	// （at-least-once 独立重试，MySQL 幂等消化，05 §6）
	if mq.events != 1 {
		t.Fatalf("kafka 应收到但未确认: %d", mq.events)
	}
	mq.failAt = 0
	if err := g.Send(context.Background(), batch); err != nil {
		t.Fatalf("恢复后应继续: %v", err)
	}
}

func TestSinkGroup_Empty(t *testing.T) {
	if _, err := NewSinkGroup(nil); err == nil {
		t.Fatal("空组应报错")
	}
}

func TestSinkGroup_PingAll(t *testing.T) {
	ok := &fakeSink{name: "a"}
	bad := &fakeSink{name: "b", err: errors.New("down")}
	g, _ := NewSinkGroup(ok, bad)
	if err := g.Ping(context.Background()); err == nil {
		t.Fatal("单边 ping 失败应上抛")
	}
}

func stringsContains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

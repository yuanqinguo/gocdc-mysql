package pipeline

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"gocdc/internal/config"
	"gocdc/internal/model"
)

type recSink struct {
	name   string
	mu     sync.Mutex // 并发测试共享单例（多 worker 并发 Send）
	calls  int
	batch  []*model.Event
	failAt int
}

func (r *recSink) Name() string { return r.name }
func (r *recSink) Ping(context.Context) error {
	return nil
}
func (r *recSink) Send(_ context.Context, batch []*model.Event) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	if r.failAt > 0 && r.calls >= r.failAt {
		return errors.New("sink down")
	}
	r.batch = append(r.batch, batch...)
	return nil
}
func (r *recSink) Close(context.Context) error { return nil }

func routerRules() []config.Rule {
	doTrue := true
	return []config.Rule{
		{Schema: "db1", Sinks: &config.RuleSinks{MySQL: config.RuleSink{Enabled: true}, Kafka: config.RuleSink{Enabled: true}},
			Tables: []config.TableRule{{Table: "a", DoStock: &doTrue}, {Table: "t", DoStock: &doTrue}}},
		{Schema: "db2", Sinks: &config.RuleSinks{MySQL: config.RuleSink{Enabled: true}},
			Tables: []config.TableRule{{Table: "b", DoStock: &doTrue}, {Table: "t", DoStock: &doTrue}}},
	}
}

func TestSinkRouter_Routing(t *testing.T) {
	cr, err := CompileRules(routerRules())
	if err != nil {
		t.Fatal(err)
	}
	my, kaf := &recSink{name: "mysql"}, &recSink{name: "kafka"}
	r := NewSinkRouter(cr, my, kaf, nil)

	batch := []*model.Event{
		{Database: "db1", Table: "a", Type: model.EventInsert, Pk: map[string]any{"id": 1}},
		{Database: "db2", Table: "b", Type: model.EventInsert, Pk: map[string]any{"id": 2}},
		{Database: "db1", Table: "a", Type: model.EventUpdate, Pk: map[string]any{"id": 1}},
	}
	if err := r.Send(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	// db1 → mysql+kafka；db2 → mysql
	if len(my.batch) != 3 {
		t.Fatalf("mysql 应收 3 事件: %d", len(my.batch))
	}
	if len(kaf.batch) != 2 {
		t.Fatalf("kafka 应收 db1 的 2 事件: %d", len(kaf.batch))
	}
	// 组内保序：kafka 只收 db1 且按批序
	if kaf.batch[0].Type != model.EventInsert || kaf.batch[1].Type != model.EventUpdate {
		t.Fatalf("kafka 组内保序: %+v", kaf.batch)
	}
}

func TestSinkRouter_UnconfiguredSink(t *testing.T) {
	cr, _ := CompileRules(routerRules())
	r := NewSinkRouter(cr, &recSink{name: "mysql"}, nil /*kafka 未配*/, nil)
	batch := []*model.Event{{Database: "db1", Table: "a", Type: model.EventInsert, Pk: map[string]any{"id": 1}}}
	err := r.Send(context.Background(), batch)
	if err == nil || !strings.Contains(err.Error(), "kafka") {
		t.Fatalf("启用未配置 sink 应报错: %v", err)
	}
}

func TestSinkRouter_SingleSideFailureStopsAll(t *testing.T) {
	cr, _ := CompileRules(routerRules())
	my := &recSink{name: "mysql"}
	kaf := &recSink{name: "kafka", failAt: 1}
	r := NewSinkRouter(cr, my, kaf, nil)
	batch := []*model.Event{{Database: "db1", Table: "a", Type: model.EventInsert, Pk: map[string]any{"id": 1}}}
	if err := r.Send(context.Background(), batch); err == nil {
		t.Fatal("单边失败应上抛（位点停推）")
	}
	// 恢复后续投
	kaf.failAt = 0
	if err := r.Send(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
}

// TestSinkRouter_ConcurrentSend 多 worker 并发首投不同规则组：
// groups 懒加载若不持锁，并发写 map 触发 runtime fatal（-race 下必现）。
func TestSinkRouter_ConcurrentSend(t *testing.T) {
	cr, err := CompileRules(routerRules())
	if err != nil {
		t.Fatal(err)
	}
	r := NewSinkRouter(cr, &recSink{name: "mysql"}, &recSink{name: "kafka"}, nil)
	const workers = 16
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			db := "db1"
			if i%2 == 1 {
				db = "db2"
			}
			for j := 0; j < 50; j++ {
				ev := &model.Event{Database: db, Table: "t", Type: model.EventInsert, Pk: map[string]any{"id": j}}
				if err := r.Send(context.Background(), []*model.Event{ev}); err != nil {
					t.Errorf("并发 Send: %v", err)
					return
				}
			}
		}(i)
	}
	wg.Wait()
}

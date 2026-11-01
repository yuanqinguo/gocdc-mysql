package pipeline

import (
	"context"
	"testing"
	"time"

	"gocdc/internal/config"
	"gocdc/internal/model"
	"gocdc/internal/source"
)

// TestShard_Affinity 同表粘连：同一张表的分片结果稳定（04 §2 单队列保序前提）。
func TestShard_Affinity(t *testing.T) {
	for _, n := range []int{1, 2, 4, 8, 64} {
		seen := map[string]int{}
		for i := 0; i < 100; i++ {
			s := Shard("db1", "users", n)
			if s < 0 || s >= n {
				t.Fatalf("shard 越界: %d/%d", s, n)
			}
			if old, ok := seen["db1.users"]; ok && old != s {
				t.Fatalf("同表分片漂移: %d vs %d (n=%d)", old, s, n)
			}
			seen["db1.users"] = s
		}
	}
	// 不同表大概率分散（4 worker 下两表同片概率 1/4，10 组至多允许少量重合）
	same := 0
	for i := 0; i < 20; i++ {
		if Shard("db", "t_a", 4) == Shard("db", "t_b", 4) {
			same++
		}
	}
	if same == 20 {
		t.Fatal("分片函数疑似常量")
	}
	// 大小写归一
	if Shard("DB1", "Users", 8) != Shard("db1", "users", 8) {
		t.Fatal("分片应大小写不敏感")
	}
}

// TestDispatcher_RotateBroadcast rotate 广播到全部队列（04 §3 屏障语义）。
func TestDispatcher_RotateBroadcast(t *testing.T) {
	const n = 3
	queues := make([]chan *EventUnit, n)
	for i := range queues {
		queues[i] = make(chan *EventUnit, 8)
	}
	cr, err := CompileRules(testRules())
	if err != nil {
		t.Fatal(err)
	}
	d := NewDispatcher(cr, nil, queues, nil)

	wm := NewWatermark(model.Position{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	src := make(chan *source.Event, 8)
	done := make(chan error, 1)
	go func() { done <- d.Run(ctx, src, wm, "n") }()

	src <- &source.Event{Kind: source.KindRotate, File: "bin.000002", LogPos: 4}
	close(src)

	// 每个队列都应收到一个 rotate 单元
	for i := range queues {
		select {
		case u := <-queues[i]:
			if u.Kind != UnitRotate || u.Pos.Name != "bin.000002" {
				t.Fatalf("queue %d 收到非 rotate: %+v", i, u)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("queue %d 未收到 rotate 广播", i)
		}
	}
	// Dispatcher 退出后队列关闭
	<-done
	for i := range queues {
		if _, ok := <-queues[i]; ok {
			t.Fatalf("queue %d 未关闭", i)
		}
	}
}

// TestDispatcher_Backpressure 队列满阻塞 Dispatcher（04 §4 反压，绝不丢弃）。
func TestDispatcher_Backpressure(t *testing.T) {
	queues := []chan *EventUnit{make(chan *EventUnit, 1)}
	cr, err := CompileRules(testRules())
	if err != nil {
		t.Fatal(err)
	}
	d := NewDispatcher(cr, nil, queues, nil)
	wm := NewWatermark(model.Position{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	src := make(chan *source.Event, 16)
	done := make(chan error, 1)
	go func() { done <- d.Run(ctx, src, wm, "n") }()

	// 灌 3 个行事件（队列容量 1 + 阻塞中的 1 = 最多吞 2 个后阻塞）
	for i := 0; i < 3; i++ {
		src <- &source.Event{Kind: source.KindRow, Schema: "db1", Table: "users", File: "f", LogPos: uint32(i + 10)}
	}
	time.Sleep(200 * time.Millisecond)
	// 队列满：队列里只有 1 个单元，dispatcher 阻塞在第二个 send —— src 未被消费即反压
	if len(queues[0]) != 1 {
		t.Fatalf("队列深度应为 1: %d", len(queues[0]))
	}
	// 取出一个后 dispatcher 继续推进
	<-queues[0]
	deadline := time.After(2 * time.Second)
	for len(queues[0]) == 0 {
		select {
		case <-deadline:
			t.Fatal("反压解除后未继续投递")
		case <-time.After(10 * time.Millisecond):
		}
	}
	cancel()
	<-done
}

// TestWorkerPoolUnit 行单元池回收不破坏数据（Put 后 Get 得到干净外壳）。
func TestWorkerPoolUnit(t *testing.T) {
	u := GetUnit()
	u.Seq = 7
	u.Schema = "db1"
	PutUnit(u)
	u2 := GetUnit()
	if u2.Seq != 0 || u2.Schema != "" {
		t.Fatalf("池化外壳未清零: %+v", u2)
	}
	PutUnit(u2)
}

// testRules 测试用规则（组形态：db1 白名单 users/orders 两表）。
func testRules() []config.Rule {
	doTrue := true
	return []config.Rule{{
		Schema: "db1", Sinks: &config.RuleSinks{MySQL: config.RuleSink{Enabled: true}},
		Tables: []config.TableRule{
			{Table: "users", DoStock: &doTrue},
			{Table: "orders", DoStock: &doTrue},
		},
	}}
}

// TestDispatcher_DDLRouting DDL 路由三态：
//   - 可解析 DDL → 路由到目标表所属分片（与该表行事件同队列，barrier 语义）
//   - 白名单外表的 DDL（未列入 tables）→ 忽略
//   - 完全未配置库的 DDL → 忽略（不占 seq）
func TestDispatcher_DDLRouting(t *testing.T) {
	const n = 4
	queues := make([]chan *EventUnit, n)
	for i := range queues {
		queues[i] = make(chan *EventUnit, 8)
	}
	doTrue := true
	rules := []config.Rule{
		{Schema: "db1", Sinks: &config.RuleSinks{MySQL: config.RuleSink{Enabled: true}},
			Tables: []config.TableRule{{Table: "users", DoStock: &doTrue}}}, // db1 白名单仅 users
		{Schema: "db2", Sinks: &config.RuleSinks{MySQL: config.RuleSink{Enabled: true}},
			Tables: []config.TableRule{{Table: "t_only", OrderByColumn: "id", DoStock: &doTrue}}},
	}
	cr, err := CompileRules(rules)
	if err != nil {
		t.Fatal(err)
	}
	d := NewDispatcher(cr, nil, queues, nil)
	wm := NewWatermark(model.Position{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	src := make(chan *source.Event, 8)
	go func() {
		src <- &source.Event{Kind: source.KindQuery, Schema: "db1", Query: "ALTER TABLE db1.users ADD COLUMN c INT", File: "f", LogPos: 100}
		src <- &source.Event{Kind: source.KindQuery, Schema: "db2", Query: "DROP TABLE t_only", File: "f", LogPos: 200}
		src <- &source.Event{Kind: source.KindQuery, Schema: "dbX", Query: "ALTER TABLE no_rule ADD COLUMN c INT", File: "f", LogPos: 300}
		src <- &source.Event{Kind: source.KindQuery, Schema: "db1", Query: "TRUNCATE TABLE tmp_not_listed", File: "f", LogPos: 400}
		close(src)
	}()
	if err := d.Run(ctx, src, wm, "node"); err != nil {
		t.Fatal(err)
	}
	var got []*EventUnit
	for i := range queues {
		for {
			select {
			case u, ok := <-queues[i]:
				if !ok {
					queues[i] = nil
					break
				}
				got = append(got, u)
				continue
			default:
			}
			break
		}
	}
	if len(got) != 2 {
		t.Fatalf("应仅 2 个 DDL 单元进队（db1.users / db2.t_only），实际 %d: %+v", len(got), got)
	}
	byTable := map[string]*EventUnit{}
	for _, u := range got {
		byTable[u.Table] = u
	}
	u1 := byTable["users"]
	if u1 == nil {
		t.Fatal("db1.users 的 DDL 丢失")
	}
	if u1.Schema != "db1" || u1.Table != "users" {
		t.Fatalf("DDL 单元应携带解析出的库表: %+v", u1)
	}
	u2 := byTable["t_only"]
	if u2 == nil {
		t.Fatal("仅表级规则的 db2.t_only DDL 被丢弃（P0）")
	}
	if u2.Table != "t_only" || u2.Schema != "db2" {
		t.Fatalf("DDL 单元应携带解析出的库表: %+v", u2)
	}
	if u1.Seq == u2.Seq {
		t.Fatal("seq 应单调")
	}
}

// TestDispatcher_DDLRoutesToTableShard DDL 与该表行事件进同一分片队列
// （跨 worker 顺序正确性的结构保证：DDL 前的行事件必然先于 DDL 被转换）。
func TestDispatcher_DDLRoutesToTableShard(t *testing.T) {
	const n = 4
	queues := make([]chan *EventUnit, n)
	for i := range queues {
		queues[i] = make(chan *EventUnit, 8)
	}
	cr, _ := CompileRules(testRules())
	d := NewDispatcher(cr, nil, queues, nil)
	wm := NewWatermark(model.Position{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	src := make(chan *source.Event, 8)
	go func() {
		src <- &source.Event{Kind: source.KindQuery, Schema: "db1", Query: "ALTER TABLE `orders` ADD COLUMN c INT", File: "f", LogPos: 50}
		close(src)
	}()
	if err := d.Run(ctx, src, wm, "node"); err != nil {
		t.Fatal(err)
	}
	want := Shard("db1", "orders", n)
	for i := range queues {
		n := len(queues[i])
		if i == want && n != 1 {
			t.Fatalf("DDL 应进表分片 %d，该队列空", want)
		}
		if i != want && n != 0 {
			t.Fatalf("非表分片 %d 不应收到 DDL（收到 %d）", i, n)
		}
	}
}

package rocketmq

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	rmq "github.com/apache/rocketmq-clients/golang/v5"

	"gocdc/internal/model"
	"gocdc/internal/sink"
)

func mustCtx() context.Context { return context.Background() }

func TestNew_Validation(t *testing.T) {
	if _, err := New(Options{}); err == nil || !strings.Contains(err.Error(), "name_servers") {
		t.Fatalf("缺 name_servers 应报错: %v", err)
	}
	// 合法构造（无网络副作用）
	if _, err := New(Options{NameServers: "127.0.0.1:8081"}); err != nil {
		t.Fatalf("构造: %v", err)
	}
	// SendConcurrency 缺省填充
	s, err := New(Options{NameServers: "127.0.0.1:8081"})
	if err != nil {
		t.Fatal(err)
	}
	if s.opts.SendConcurrency != defaultSendConcurrency || cap(s.sem) != defaultSendConcurrency {
		t.Fatalf("SendConcurrency 默认填充错误: %d", s.opts.SendConcurrency)
	}
}

// TestSend_NotStarted Ping/Send 的启动门槛（Ping 无副作用语义，P1-11）。
func TestSend_NotStarted(t *testing.T) {
	s, err := New(Options{NameServers: "127.0.0.1:8081"})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Ping(nil); err == nil {
		t.Fatal("未启动 Ping 应报错")
	}
	ev := &model.Event{EventID: "f:1:0", Type: model.EventInsert, Database: "db1", Table: "t",
		Pk: map[string]any{"id": int64(1)}, After: map[string]any{"id": int64(1)}}
	err = s.Send(mustCtx(), []*model.Event{ev})
	if err == nil || !sink.IsPermanent(err) {
		t.Fatalf("未启动 Send 应永久错误: %v", err)
	}
	s.GracefulStop() // 未启动重复 stop 安全
	s.GracefulStop()
}

func TestClassify(t *testing.T) {
	if classify(nil) != nil {
		t.Fatal("nil 不应包装")
	}
	if !sink.IsPermanent(classify(errOf("TOPIC_NOT_EXIST error"))) {
		t.Fatal("topic 不存在应永久")
	}
	if !sink.IsPermanent(classify(errOf("CODE: 17 DESC: No topic route info"))) {
		t.Fatal("TOPIC_NOT_FOUND 应永久")
	}
	if !sink.IsPermanent(classify(errOf("current message type not match with topic accept message types"))) {
		t.Fatal("顺序消息类型不匹配应永久")
	}
	if !sink.IsPermanent(classify(errOf("authentication failed"))) {
		t.Fatal("认证拒绝应永久")
	}
	if sink.IsPermanent(classify(errOf("rpc timeout"))) {
		t.Fatal("超时应可重试")
	}
}

type strErr struct{ s string }

func (e *strErr) Error() string { return e.s }

func errOf(s string) error { return &strErr{s} }

// ── fake producer（收窄接口 producer 的包内实现；rmq.Producer 含未导出方法
// isClient，包外无法 fake——这正是 producer 接口收窄的原因）──

type fakeProducer struct {
	mu          sync.Mutex
	sent        []string // 按到达序记录 event_id（从 body 提取）
	topics      []string // 同上记录 topic（带点库名断言用）
	groups      []string // 同上记录 message group（带点库名断言用）
	hook        func(eventID string) error
	inflight    int32
	maxInflight int32
}

func (f *fakeProducer) Send(_ context.Context, msg *rmq.Message) ([]*rmq.SendReceipt, error) {
	n := atomic.AddInt32(&f.inflight, 1)
	defer atomic.AddInt32(&f.inflight, -1)
	for {
		old := atomic.LoadInt32(&f.maxInflight)
		if n <= old || atomic.CompareAndSwapInt32(&f.maxInflight, old, n) {
			break
		}
	}
	eventID := extractEventID(msg.Body)
	if f.hook != nil {
		if err := f.hook(eventID); err != nil {
			return nil, err
		}
	}
	f.mu.Lock()
	f.sent = append(f.sent, eventID)
	f.topics = append(f.topics, msg.Topic)
	if g := msg.GetMessageGroup(); g != nil {
		f.groups = append(f.groups, *g)
	}
	f.mu.Unlock()
	return []*rmq.SendReceipt{{MessageID: eventID}}, nil
}

func (f *fakeProducer) Start() error        { return nil }
func (f *fakeProducer) GracefulStop() error { return nil }

// extractEventID 从 05 §2.1 事件 JSON 抓 "event_id" 值（测试用，不引 JSON 解析）。
func extractEventID(body []byte) string {
	const key = `"event_id":"`
	s := string(body)
	i := strings.Index(s, key)
	if i < 0 {
		return ""
	}
	rest := s[i+len(key):]
	j := strings.Index(rest, `"`)
	if j < 0 {
		return ""
	}
	return rest[:j]
}

func (f *fakeProducer) sentIDs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.sent...)
}

func (f *fakeProducer) sentTopics() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.topics...)
}

func (f *fakeProducer) sentGroups() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.groups...)
}

// newStartedSink 绕过 New（rmq.NewProducer 需真实 endpoint）直接装配已启动 Sink，
// 字段初始化与 New 保持一致。
func newStartedSink(fp *fakeProducer, conc int) *Sink {
	if conc <= 0 {
		conc = defaultSendConcurrency
	}
	return &Sink{
		producer: fp,
		opts: Options{
			TopicPrefix: "CDC_", OrderBy: sink.OrderByRow,
			SendTimeout: 5 * time.Second, SendConcurrency: conc,
		},
		started:  true,
		sem:      make(chan struct{}, conc),
		inflight: make(map[string]int),
	}
}

func rowEvent(id string, pk int64) *model.Event {
	return &model.Event{
		EventID: id, Type: model.EventInsert, Database: "db1", Table: "t",
		Pk: map[string]any{"id": pk}, After: map[string]any{"id": pk},
	}
}

// TestSend_GroupSerial 组内严格串行：同组多消息的到达序 = 批内原序（顺序消息
// 保序不变式：broker 按到达序落盘）。
func TestSend_GroupSerial(t *testing.T) {
	fp := &fakeProducer{}
	s := newStartedSink(fp, 8)
	batch := []*model.Event{rowEvent("e1", 1), rowEvent("e2", 1), rowEvent("e3", 1)}
	if err := s.Send(mustCtx(), batch); err != nil {
		t.Fatalf("Send: %v", err)
	}
	got := fp.sentIDs()
	if strings.Join(got, ",") != "e1,e2,e3" {
		t.Fatalf("组内乱序: got %v", got)
	}
}

// TestSend_DottedDB_TopicAndGroup 带点库名（edu.mall.admin 是库名，admin_user 是
// 表，全名 edu.mall.admin.admin_user，多源库按 . 聚合命名）的发送路径：topic 与
// 顺序组 message group 的点一律替换为下划线——broker 字符集 ^[%|a-zA-Z0-9_-]+$
// 不收点，任一带点即被拒（topic 报 ILLEGAL_TOPIC / group 发送失败）。
func TestSend_DottedDB_TopicAndGroup(t *testing.T) {
	fp := &fakeProducer{}
	s := newStartedSink(fp, 8)
	ev := rowEvent("e1", 1)
	ev.Database = "edu.mall.admin"
	ev.Table = "admin_user"
	if err := s.Send(mustCtx(), []*model.Event{ev}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if got := fp.sentTopics(); len(got) != 1 || got[0] != "CDC_edu_mall_admin_admin_user" {
		t.Fatalf("带点库名 topic 应折叠为 CDC_edu_mall_admin_admin_user: %v", got)
	}
	// key = {db}.{table}.{pk}（order_by=row，05 §2.3）→ 组内点同样折叠
	if got := fp.sentGroups(); len(got) != 1 || got[0] != "edu_mall_admin_admin_user_1" ||
		strings.Contains(got[0], ".") {
		t.Fatalf("带点库名 message group 应折叠为 edu_mall_admin_admin_user_1: %v", got)
	}
}

// TestSend_GroupsParallel 组间并发：A 组阻塞时 B 组仍能完成（串行实现会在此
// 死锁直至超时——本测试即组间并发存在的判据）。
func TestSend_GroupsParallel(t *testing.T) {
	fp := &fakeProducer{}
	release := make(chan struct{})
	var once sync.Once
	s := newStartedSink(fp, 8)
	a := rowEvent("A1", 1)
	a.Table = "ta"
	b := rowEvent("B1", 1)
	b.Table = "tb"
	fp.hook = func(eventID string) error {
		if eventID == "A1" {
			<-release // A 组挂起
		}
		if eventID == "B1" {
			once.Do(func() { close(release) }) // B 完成即放行 A
		}
		return nil
	}
	done := make(chan error, 1)
	go func() { done <- s.Send(mustCtx(), []*model.Event{a, b}) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Send: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("B 组未能在 A 组阻塞期间完成——组间未并发（退化为串行）")
	}
}

// TestSend_ErrorFailsWholeBatch 任一组失败 → 整批错误且可重试（worker 据此退避
// 重试整批）；已发出消息不回滚（at-least-once，重复由下游幂等消化）。
func TestSend_ErrorFailsWholeBatch(t *testing.T) {
	fp := &fakeProducer{}
	s := newStartedSink(fp, 8)
	a := rowEvent("A1", 1)
	b := rowEvent("B1", 1)
	b.Table = "tb"
	fp.hook = func(eventID string) error {
		if eventID == "B1" {
			return context.DeadlineExceeded // classify → 非 Permanent；IsRetryable → true
		}
		return nil
	}
	err := s.Send(mustCtx(), []*model.Event{a, b})
	if err == nil {
		t.Fatal("B 组失败应整批报错")
	}
	if sink.IsPermanent(err) {
		t.Fatalf("瞬态失败不应标记 Permanent: %v", err)
	}
	if !sink.IsRetryable(err) {
		t.Fatalf("worker 依赖 IsRetryable 退避重试: %v", err)
	}
}

// TestSend_InflightGuard 组级串行断言：同一顺序组跨 Send 在途冲突 → 可重试错误
// （stock 重扫与增量并发同表场景），绝不静默乱序；冲突消解后恢复发送。
func TestSend_InflightGuard(t *testing.T) {
	fp := &fakeProducer{}
	release := make(chan struct{})
	entered := make(chan struct{}, 1)
	s := newStartedSink(fp, 8)
	fp.hook = func(string) error {
		select {
		case entered <- struct{}{}:
		default:
		}
		<-release
		return nil
	}
	done := make(chan error, 1)
	go func() { done <- s.Send(mustCtx(), []*model.Event{rowEvent("A1", 1)}) }()
	<-entered // 首个 Send 已越过 registerGroups 进入发送

	err := s.Send(mustCtx(), []*model.Event{rowEvent("A2", 1)})
	if err == nil || !sink.IsRetryable(err) || sink.IsPermanent(err) {
		t.Fatalf("同组在途冲突应返回可重试错误: %v", err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("首个 Send 应成功: %v", err)
	}
	if err := s.Send(mustCtx(), []*model.Event{rowEvent("A3", 1)}); err != nil {
		t.Fatalf("冲突消解后应可正常发送: %v", err)
	}
}

// TestSend_ConcurrencyBounded 组间并发受全局信号量约束（无隐式无界，09 §2）。
func TestSend_ConcurrencyBounded(t *testing.T) {
	fp := &fakeProducer{}
	s := newStartedSink(fp, 4)
	batch := make([]*model.Event, 0, 40)
	for i := 0; i < 40; i++ {
		batch = append(batch, rowEvent(fmt.Sprintf("e%d", i), int64(i))) // row 键互异 = 40 独立组
	}
	if err := s.Send(mustCtx(), batch); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if n := atomic.LoadInt32(&fp.maxInflight); n > 4 {
		t.Fatalf("并发上限失效: maxInflight=%d > 4", n)
	}
	if got := len(fp.sentIDs()); got != 40 {
		t.Fatalf("应全部送达 40 条，实际 %d", got)
	}
}

// TestSend_EmptyBatch 空批 no-op。
func TestSend_EmptyBatch(t *testing.T) {
	s := newStartedSink(&fakeProducer{}, 4)
	if err := s.Send(mustCtx(), nil); err != nil {
		t.Fatalf("空批应 nil: %v", err)
	}
}

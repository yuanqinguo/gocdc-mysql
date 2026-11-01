package source

import (
	"context"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"

	"gocdc/internal/model"
)

func TestValidateStart(t *testing.T) {
	s := New(Config{UseGTID: false}, nil)
	if err := s.validateStart(model.Position{}); err == nil {
		t.Fatal("文件模式空位点应报错")
	}
	if err := s.validateStart(model.Position{Name: "bin.000001", Pos: 4}); err != nil {
		t.Fatalf("合法文件位点: %v", err)
	}

	g := New(Config{UseGTID: true}, nil)
	if err := g.validateStart(model.Position{Name: "bin.000001"}); err == nil {
		t.Fatal("GTID 模式缺 gtid_set 应报错")
	}
	if err := g.validateStart(model.Position{GTIDSet: "not-a-set"}); err == nil {
		t.Fatal("非法 gtid_set 应报错")
	}
	if err := g.validateStart(model.Position{GTIDSet: "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5"}); err != nil {
		t.Fatalf("合法 gtid_set: %v", err)
	}
}

// TestDispatch_FiltersAndPos 用合成 binlog 事件验证分类/过滤/位点维护（无网络）。
func TestDispatch_FiltersAndPos(t *testing.T) {
	s := New(Config{}, nil)
	out := make(chan *Event, 16)
	ctx := context.Background()

	// BEGIN 查询被过滤
	begin := &replication.BinlogEvent{
		Header: &replication.EventHeader{Timestamp: 100, LogPos: 500},
		Event:  &replication.QueryEvent{Query: []byte("BEGIN")},
	}
	if err := s.dispatch(ctx, begin, out); err != nil {
		t.Fatal(err)
	}
	select {
	case ev := <-out:
		t.Fatalf("BEGIN 不应发射: %+v", ev)
	default:
	}

	// XID 推进位点 + 发射
	xid := &replication.BinlogEvent{
		Header: &replication.EventHeader{Timestamp: 101, LogPos: 900},
		Event:  &replication.XIDEvent{XID: 77},
	}
	if err := s.dispatch(ctx, xid, out); err != nil {
		t.Fatal(err)
	}
	ev := <-out
	if ev.Kind != KindXID || ev.XID != 77 || ev.LogPos != 900 {
		t.Fatalf("XID 事件: %+v", ev)
	}
	if p := s.snapshotPos(); p.Pos != 900 {
		t.Fatalf("内部位点应推进到 900: %+v", p)
	}

	// 未知事件（TableMap）静默
	tm := &replication.BinlogEvent{
		Header: &replication.EventHeader{Timestamp: 102, LogPos: 950},
		Event:  &replication.TableMapEvent{Schema: []byte("db1"), Table: []byte("t1")},
	}
	if err := s.dispatch(ctx, tm, out); err != nil {
		t.Fatal(err)
	}

	// Rotate 更新文件名；fake rotate 标记 Artificial
	rot := &replication.BinlogEvent{
		Header: &replication.EventHeader{Timestamp: 103, Flags: replication.LOG_EVENT_ARTIFICIAL_F},
		Event:  &replication.RotateEvent{NextLogName: []byte("bin.000002"), Position: 4},
	}
	if err := s.dispatch(ctx, rot, out); err != nil {
		t.Fatal(err)
	}
	ev = <-out
	if ev.Kind != KindRotate || !ev.Artificial || ev.File != "bin.000002" {
		t.Fatalf("rotate 事件: %+v", ev)
	}
	if p := s.snapshotPos(); p.Name != "bin.000002" || p.Pos != 4 {
		t.Fatalf("rotate 后内部位点: %+v", p)
	}
}

// TestDispatch_FiltersSavepointFamily SAVEPOINT 家族（事务保存点，非 DDL）必须
// 拦在 source 层——透传到下游会被误判为 DDL 且定位不到表 → pause 冻结全管线。
// 大小写不敏感（用户原文透传）。
func TestDispatch_FiltersSavepointFamily(t *testing.T) {
	s := New(Config{}, nil)
	out := make(chan *Event, 16)
	ctx := context.Background()

	filtered := []string{
		"SAVEPOINT `sp0xa1cb20`",
		"SAVEPOINT sp1",
		"savepoint sp1", // 小写原文
		"RELEASE SAVEPOINT sp1",
		"release savepoint `sp1`",
		"ROLLBACK TO SAVEPOINT sp1",
		"ROLLBACK TO sp1",
		"rollback to `sp1`",
		"BEGIN", "begin", // 边界：大小写归一后仍过滤
		"COMMIT", "ROLLBACK",
	}
	for _, q := range filtered {
		be := &replication.BinlogEvent{
			Header: &replication.EventHeader{Timestamp: 100, LogPos: 500},
			Event:  &replication.QueryEvent{Query: []byte(q)},
		}
		if err := s.dispatch(ctx, be, out); err != nil {
			t.Fatalf("%q: %v", q, err)
		}
		select {
		case ev := <-out:
			t.Fatalf("%q 不应发射: %+v", q, ev)
		default:
		}
	}

	// 真 DDL 不受影响，照常发射
	ddl := &replication.BinlogEvent{
		Header: &replication.EventHeader{Timestamp: 100, LogPos: 600},
		Event:  &replication.QueryEvent{Query: []byte("ALTER TABLE t1 ADD COLUMN c INT"), Schema: []byte("db1")},
	}
	if err := s.dispatch(ctx, ddl, out); err != nil {
		t.Fatal(err)
	}
	select {
	case ev := <-out:
		if ev.Kind != KindQuery || ev.Query != "ALTER TABLE t1 ADD COLUMN c INT" || ev.Schema != "db1" {
			t.Fatalf("真 DDL 应原样发射: %+v", ev)
		}
	default:
		t.Fatal("真 DDL 丢失")
	}
}

func TestNewDefaults(t *testing.T) {
	s := New(Config{Addr: "10.0.0.1:3307"}, nil)
	if s.cfg.Charset != "utf8mb4" || s.cfg.Flavor != "mysql" {
		t.Fatalf("charset/flavor 默认: %+v", s.cfg)
	}
	if s.cfg.HeartbeatInterval != 3*time.Second || s.cfg.ReadTimeout != 60*time.Second {
		t.Fatalf("心跳/读超时默认（防 P1-6）: %+v", s.cfg)
	}
	cfg := s.syncerConfig()
	if !cfg.DisableRetrySync {
		t.Fatal("必须禁用 go-mysql 内部重连（at-least-once 红线）")
	}
	if cfg.ParseTime {
		t.Fatal("ParseTime 必须为 false（transform 字符串透传路径）")
	}
	if cfg.TimestampStringLocation == nil {
		t.Fatal("TIMESTAMP 时区必须显式")
	}
}

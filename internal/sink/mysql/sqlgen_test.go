package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"strings"
	"sync"
	"testing"

	"gocdc/internal/model"
)

func TestQuoteIdent(t *testing.T) {
	if got := quoteIdent("we`ird"); got != "`we``ird`" {
		t.Fatalf("quoteIdent: %s", got)
	}
}

func TestBuildUpsert(t *testing.T) {
	got := buildUpsert("db", "tbl", []string{"id", "name"}, 2)
	want := "INSERT INTO `db`.`tbl` (`id`,`name`) VALUES(?,?),(?,?) " +
		"ON DUPLICATE KEY UPDATE `id`=VALUES(`id`),`name`=VALUES(`name`)"
	if got != want {
		t.Fatalf("upsert:\n%s\nwant:\n%s", got, want)
	}
}

func TestBuildDeleteComposite(t *testing.T) {
	got := buildDelete("db", "tbl", []string{"pk1", "pk2"}, 2)
	want := "DELETE FROM `db`.`tbl` WHERE (`pk1`,`pk2`) IN ((?,?),(?,?))"
	if got != want {
		t.Fatalf("delete:\n%s\nwant:\n%s", got, want)
	}
}

// ── 游程分组与执行序列（recordDriver 录制语句）──────────────────────────

// recDriver 录制 Exec 的 SQL/参数（无容器验证 Send 的分段语义）。
type recDriver struct {
	mu    sync.Mutex
	stmts []recStmt
}

type recStmt struct {
	query string
	args  []driver.Value
}

func (d *recDriver) Open(string) (driver.Conn, error) { return &recConn{d: d}, nil }

type recConn struct{ d *recDriver }

func (c *recConn) Prepare(query string) (driver.Stmt, error) {
	return &recStmtHandle{d: c.d, query: query}, nil
}
func (c *recConn) Close() error              { return nil }
func (c *recConn) Begin() (driver.Tx, error) { return recTx{}, nil }

type recStmtHandle struct {
	d     *recDriver
	query string
}

func (s *recStmtHandle) Close() error  { return nil }
func (s *recStmtHandle) NumInput() int { return -1 }
func (s *recStmtHandle) Exec(args []driver.Value) (driver.Result, error) {
	s.d.mu.Lock()
	s.d.stmts = append(s.d.stmts, recStmt{query: s.query, args: args})
	s.d.mu.Unlock()
	return driver.RowsAffected(1), nil
}
func (s *recStmtHandle) Query([]driver.Value) (driver.Rows, error) {
	return nil, fmt.Errorf("not used")
}

type recTx struct{}

func (recTx) Commit() error   { return nil }
func (recTx) Rollback() error { return nil }

var _ driver.Conn = (*recConn)(nil)

func newRecordSink(t *testing.T) (*Sink, *recDriver) {
	t.Helper()
	d := &recDriver{}
	name := "rec" + fmt.Sprint(t.Name())
	sql.Register(name, d)
	db, err := sql.Open(name, "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return &Sink{db: db, chunkRows: 500}, d
}

func evIns(db, table string, kv map[string]any) *model.Event {
	return &model.Event{Type: model.EventInsert, Database: db, Table: table, After: kv, Pk: map[string]any{"id": kv["id"]}}
}

func evUpd(db, table string, before, after map[string]any) *model.Event {
	return &model.Event{
		Type: model.EventUpdate, Database: db, Table: table,
		Before: before, After: after,
		Pk: map[string]any{"id": after["id"]},
	}
}

func evDel(db, table string, id any) *model.Event {
	return &model.Event{Type: model.EventDelete, Database: db, Table: table,
		Pk: map[string]any{"id": id}, Before: map[string]any{"id": id}}
}

// TestSend_RunGrouping 游程分组语义（05 §5.3 示例）：
// U(k1) U(k2) D(k3) U(k3) I(k4) → upsert(k1,k2) / delete(k3) / upsert(k3,k4)，
// 绝不全局重排（否则 INSERT 旧行复活已删行）。
func TestSend_RunGrouping(t *testing.T) {
	s, d := newRecordSink(t)
	batch := []*model.Event{
		evUpd("db", "t", map[string]any{"id": int64(1), "v": "a"}, map[string]any{"id": int64(1), "v": "a1"}),
		evUpd("db", "t", map[string]any{"id": int64(2), "v": "b"}, map[string]any{"id": int64(2), "v": "b1"}),
		evDel("db", "t", int64(3)),
		evUpd("db", "t", map[string]any{"id": int64(3), "v": "c"}, map[string]any{"id": int64(3), "v": "c1"}),
		evIns("db", "t", map[string]any{"id": int64(4), "v": "d"}),
	}
	if err := s.Send(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.stmts) != 3 {
		t.Fatalf("应合并为 3 条语句，实际 %d: %+v", len(d.stmts), d.stmts)
	}
	if !strings.HasPrefix(d.stmts[0].query, "INSERT INTO `db`.`t`") || len(d.stmts[0].args) != 4 {
		t.Fatalf("第 1 段应为 2 行 upsert: %s %v", d.stmts[0].query, d.stmts[0].args)
	}
	if !strings.HasPrefix(d.stmts[1].query, "DELETE FROM `db`.`t`") {
		t.Fatalf("第 2 段应为 delete: %s", d.stmts[1].query)
	}
	if !strings.HasPrefix(d.stmts[2].query, "INSERT INTO `db`.`t`") || len(d.stmts[2].args) != 4 {
		t.Fatalf("第 3 段应为 2 行 upsert: %s", d.stmts[2].query)
	}
}

// TestSend_PkChange UPDATE 改主键 → delete(旧)+upsert(新) 相邻成对。
func TestSend_PkChange(t *testing.T) {
	s, d := newRecordSink(t)
	batch := []*model.Event{
		evUpd("db", "t", map[string]any{"id": int64(9), "v": "old"}, map[string]any{"id": int64(10), "v": "new"}),
	}
	if err := s.Send(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.stmts) != 2 {
		t.Fatalf("主键变更应为 delete+upsert 两条: %+v", d.stmts)
	}
	if !strings.HasPrefix(d.stmts[0].query, "DELETE FROM") {
		t.Fatalf("先删旧行: %s", d.stmts[0].query)
	}
	if got := d.stmts[0].args; len(got) != 1 || got[0] != int64(9) {
		t.Fatalf("删除旧主键 9: %v", got)
	}
	if !strings.HasPrefix(d.stmts[1].query, "INSERT INTO") {
		t.Fatalf("再写新行: %s", d.stmts[1].query)
	}
	if got := d.stmts[1].args; len(got) != 2 || got[0] != int64(10) {
		t.Fatalf("新主键 10: %v", got)
	}
}

// TestSend_ChunkRows tx_chunk_rows 切块。
func TestSend_ChunkRows(t *testing.T) {
	s, d := newRecordSink(t)
	s.chunkRows = 2
	var batch []*model.Event
	for i := int64(0); i < 5; i++ {
		batch = append(batch, evIns("db", "t", map[string]any{"id": i}))
	}
	if err := s.Send(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.stmts) != 3 {
		t.Fatalf("5 行 chunk=2 应切 3 段(2+2+1): %d", len(d.stmts))
	}
	if len(d.stmts[0].args) != 2 || len(d.stmts[1].args) != 2 || len(d.stmts[2].args) != 1 {
		t.Fatalf("切块参数数: %d/%d/%d", len(d.stmts[0].args), len(d.stmts[1].args), len(d.stmts[2].args))
	}
}

// TestSend_UnknownEvent 非行事件显式报错（不可静默）。
func TestSend_UnknownEvent(t *testing.T) {
	s, _ := newRecordSink(t)
	err := s.Send(context.Background(), []*model.Event{{
		Type: model.EventDDL, Database: "db", Table: "t", SQL: "DROP TABLE x",
	}})
	if err == nil {
		t.Fatal("DDL 事件应报错")
	}
}

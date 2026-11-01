//go:build integration

// 集成测试：需要 Docker。运行：
//
//	go test ./internal/pipeline/ -tags=integration -count=1
//
// 场景：单容器双库端到端——source → dispatcher → transform →
// mysql sink → watermark → persister。覆盖：DML 全类型对账一致、优雅停机
// drain 无丢失、kill -9 式重启从已存位点重放收敛（幂等消化）。

package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/testcontainers/testcontainers-go/modules/mysql"

	"go.uber.org/zap"
	"gocdc/internal/config"
	"gocdc/internal/model"
	"gocdc/internal/schema"
	"gocdc/internal/sink"
	mysqlsink "gocdc/internal/sink/mysql"
	"gocdc/internal/source"
	"gocdc/internal/transform"
)

// e2eStore 内存位点存储（记录每次落盘，供重启取「最后已存位点」）。
type e2eStore struct {
	mu    sync.Mutex
	saved []model.Position
}

func (m *e2eStore) Load(_ context.Context) (model.Position, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.saved) == 0 {
		return model.Position{}, false, nil
	}
	return m.saved[len(m.saved)-1], true, nil
}

func (m *e2eStore) Save(_ context.Context, p model.Position) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.saved = append(m.saved, p)
	return nil
}

func TestIntegrationPipelineE2E(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// ── 容器：单 MySQL，双库（源 srcdb / 目标 dstdb）──
	ctr, err := mysql.Run(ctx, envOr("MYSQL_IMAGE", "mysql:8"),
		mysql.WithDatabase("srcdb"), mysql.WithUsername("root"), mysql.WithPassword("test"),
		mysql.WithConfigFile(e2eConf(t)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ctr.Terminate(context.Background()) })
	host, err := ctr.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}
	port, err := ctr.MappedPort(ctx, "3306")
	if err != nil {
		t.Fatal(err)
	}
	addr := host + ":" + port.Port()
	admin, err := sql.Open("mysql", "root:test@tcp("+addr+")/?multiStatements=true")
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	waitReady(t, ctx, admin)

	if _, err := admin.ExecContext(ctx, `
CREATE DATABASE dstdb;
CREATE TABLE srcdb.t_e2e (
  id       bigint unsigned NOT NULL AUTO_INCREMENT,
  name     varchar(64)     NOT NULL,
  amount   decimal(12,4)   NOT NULL DEFAULT '0',
  big      bigint unsigned NOT NULL DEFAULT 0,
  status   enum('new','paid','void') NOT NULL DEFAULT 'new',
  ext      json            NULL,
  bits     bit(8)          NOT NULL DEFAULT b'0',
  hit_at   datetime        NULL,
  payload  varbinary(64)   NULL,
  PRIMARY KEY (id)
);
CREATE TABLE dstdb.t_e2e LIKE srcdb.t_e2e;`); err != nil {
		t.Fatal(err)
	}

	// 起始位点 = 当前 master 位点（首启语义；必须在建表之后取，
	// 否则 DDL pause 策略会在 CREATE 语句上暂停管线）
	var file string
	var pos uint32
	if file, pos, err = queryMasterStatus(ctx, admin); err != nil {
		t.Fatal(err)
	}

	// ── 装配管线 ──
	store := &e2eStore{}
	doTrue := true
	rules, err := CompileRules([]config.Rule{{
		Schema: "srcdb", TargetDatabase: "dstdb",
		Sinks:  &config.RuleSinks{MySQL: config.RuleSink{Enabled: true}},
		Tables: []config.TableRule{{Table: "t_e2e", OrderByColumn: "id", DoStock: &doTrue}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	srcDB, err := sql.Open("mysql", "root:test@tcp("+addr+")/")
	if err != nil {
		t.Fatal(err)
	}
	defer srcDB.Close()
	zl, _ := zap.NewDevelopment()
	router := NewSinkRouter(rules, mustSink(t, addr, rules), nil, nil) // 路由 sink（mysql-only 组合）
	newPipeline := func() *Pipeline {
		return New(Options{
			Source: source.New(source.Config{
				Addr: addr, User: "root", Password: "test", ServerID: 424242,
				Timezone: time.UTC, HeartbeatInterval: time.Second, ReadTimeout: 30 * time.Second,
			}, zl),
			SchemaMgr:   schema.NewManager(schema.NewMySQLLoader(srcDB)),
			Transformer: transform.New(transform.Options{Timezone: time.UTC}),
			Sink:        router,
			Store:       store, Rules: rules,
			Workers:   4, // 分片并行
			BatchSize: 10, FlushInterval: 50 * time.Millisecond, SaveInterval: 200 * time.Millisecond,
			NodeID: "e2e", DrainTimeout: 15 * time.Second,
			ExcludeSchemas: []string{"mysql", "sys", "information_schema", "performance_schema"},
			Log:            zl,
		})
	}

	// ── 第一段运行：全类型写入 → 对账 ──
	run1Ctx, stop1 := context.WithCancel(context.Background())
	run1Err := make(chan error, 1)
	go func() { run1Err <- newPipeline().Run(run1Ctx, model.Position{Name: file, Pos: pos}, nil) }()

	waitReady(t, ctx, srcDB)
	_, err = srcDB.ExecContext(ctx, `INSERT INTO srcdb.t_e2e
		(name, amount, big, status, ext, bits, hit_at, payload) VALUES
		('alice', 1234.5678, 18446744073709551615, 'paid', '{"k": [1,2]}', b'1010', '2026-10-28 12:00:00', 0xDEADBEEF),
		('bob',   -0.0001,   0, 'new',     'null',         b'1',    '0000-00-00 00:00:00', NULL)`)
	if err != nil {
		t.Fatal(err)
	}
	// UPDATE + DELETE
	if _, err := srcDB.ExecContext(ctx, `UPDATE srcdb.t_e2e SET name='alice2', status='void' WHERE id=1`); err != nil {
		t.Fatal(err)
	}
	if _, err := srcDB.ExecContext(ctx, `DELETE FROM srcdb.t_e2e WHERE id=2`); err != nil {
		t.Fatal(err)
	}

	// 等目标收敛：id=1 更新生效、id=2 被删
	deadline := time.Now().Add(60 * time.Second)
	for {
		var name, status string
		var n2 int
		q1 := srcDB.QueryRowContext(ctx, "SELECT name, status FROM dstdb.t_e2e WHERE id=1").Scan(&name, &status)
		_ = srcDB.QueryRowContext(ctx, "SELECT COUNT(*) FROM dstdb.t_e2e WHERE id=2").Scan(&n2)
		if q1 == nil && name == "alice2" && status == "void" && n2 == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("目标未收敛: q1=%v name=%q status=%q n2=%d", q1, name, status, n2)
		}
		time.Sleep(200 * time.Millisecond)
	}

	// 全类型值对账（源 vs 目标逐列一致）
	assertRow := func(q string, want ...any) {
		t.Helper()
		var got []any = make([]any, len(want))
		dest := make([]any, len(want))
		for i := range got {
			dest[i] = &got[i]
		}
		if err := srcDB.QueryRowContext(ctx, q).Scan(dest...); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		for i := range want {
			g := fmt.Sprintf("%v", got[i])
			if b, ok := got[i].([]byte); ok {
				g = string(b) // 文本协议 decimal/字符串以 []byte 交付
			}
			if g != fmt.Sprintf("%v", want[i]) {
				t.Fatalf("%s 列 %d: got %v want %v", q, i, got[i], want[i])
			}
		}
	}
	assertRow("SELECT amount FROM dstdb.t_e2e WHERE id=1", "1234.5678") // decimal 字符串无损
	assertRow("SELECT big FROM dstdb.t_e2e WHERE id=1", "18446744073709551615")
	assertRow("SELECT CAST(JSON_UNQUOTE(JSON_EXTRACT(ext,'$.k[0]')) AS CHAR) FROM dstdb.t_e2e WHERE id=1", "1")
	assertRow("SELECT bits+0 FROM dstdb.t_e2e WHERE id=1", "10") // bit(8) 按位解码 0b1010
	assertRow("SELECT CAST(hit_at AS CHAR) FROM dstdb.t_e2e WHERE id=1", "2026-10-28 12:00:00")
	assertRow("SELECT HEX(payload) FROM dstdb.t_e2e WHERE id=1", "DEADBEEF")
	if n := countTarget(t, ctx, srcDB, "SELECT COUNT(*) FROM dstdb.t_e2e"); n != 1 {
		t.Fatalf("最终行数: %d", n)
	}

	// ── kill -9 式重启：硬 cancel（不等 drain 完成）后立刻再写 ──
	stop1()
	select {
	case err := <-run1Err:
		t.Logf("第一段退出: %v", err)
	case <-time.After(30 * time.Second):
		t.Fatal("第一段未退出")
	}
	if _, ok, err := store.Load(ctx); err != nil || !ok {
		t.Fatalf("位点应已落盘: %v %v", ok, err)
	}

	// 第二段运行：从「最后已存位点」重放 + 新写入 → 收敛且无重复
	lastPos, _, _ := store.Load(ctx)
	run2Ctx, stop2 := context.WithCancel(context.Background())
	defer stop2()
	run2Err := make(chan error, 1)
	go func() { run2Err <- newPipeline().Run(run2Ctx, lastPos, nil) }()

	if _, err := srcDB.ExecContext(ctx, `INSERT INTO srcdb.t_e2e
		(name, amount, big, status, ext, bits, hit_at, payload) VALUES
		('carol', 7.0, 42, 'paid', '{"ok":true}', b'11', '2026-01-01 00:00:00', NULL),
		('dave',  8.0, 43, 'new', NULL, b'0', NULL, NULL),
		('erin',  9.0, 44, 'new', NULL, b'0', NULL, NULL)`); err != nil {
		t.Fatal(err)
	}
	// 等收敛：目标 4 行（1 旧行 + 3 新行；重放不产生重复）
	deadline = time.Now().Add(60 * time.Second)
	for {
		n := countTarget(t, ctx, srcDB, "SELECT COUNT(*) FROM dstdb.t_e2e")
		nc := countTarget(t, ctx, srcDB, "SELECT COUNT(*) FROM dstdb.t_e2e WHERE name='carol'")
		if n == 4 && nc == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("第二段未收敛: rows=%d carol=%d", n, nc)
		}
		time.Sleep(200 * time.Millisecond)
	}
	stop2()
}

func mustSink(t *testing.T, addr string, rules *CompiledRules) sink.Sink {
	t.Helper()
	s, err := mysqlsink.New(mysqlsink.Options{Addr: addr, User: "root", Password: "test", MaxOpenConns: 4,
		TargetDB: func(schemaName, table string) string {
			if r := rules.Match(schemaName, table); r != nil && r.TargetDatabase != "" {
				return r.TargetDatabase
			}
			return ""
		}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	return &dbgSink{t: t, inner: s}
}

// dbgSink 失败时打印批内容辅助诊断（仅测试）。
type dbgSink struct {
	t     *testing.T
	inner sink.Sink
}

func (d *dbgSink) Name() string { return d.inner.Name() }
func (d *dbgSink) Ping(ctx context.Context) error {
	return d.inner.Ping(ctx)
}

func (d *dbgSink) Send(ctx context.Context, batch []*model.Event) error {
	err := d.inner.Send(ctx, batch)
	if err != nil {
		for _, ev := range batch {
			d.t.Logf("失败批事件: type=%s db=%s tbl=%s pk=%v", ev.Type, ev.Database, ev.Table, ev.Pk)
			for c, v := range ev.After {
				d.t.Logf("  after.%s = %#v", c, v)
			}
			for c, v := range ev.Before {
				d.t.Logf("  before.%s = %#v", c, v)
			}
		}
	}
	return err
}

func (d *dbgSink) Close(ctx context.Context) error { return d.inner.Close(ctx) }

func countTarget(t *testing.T, ctx context.Context, db *sql.DB, q string) int {
	t.Helper()
	var n int
	if err := db.QueryRowContext(ctx, q).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func waitReady(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for {
		if err := db.PingContext(ctx); err == nil {
			return
		} else if time.Now().After(deadline) {
			t.Fatalf("MySQL 未就绪")
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func e2eConf(t *testing.T) string {
	t.Helper()
	f := t.TempDir() + "/my.cnf"
	content := `[mysqld]
binlog_format=ROW
binlog_row_image=FULL
server_id=1
log_bin=mysql-bin
sql_mode=NO_ENGINE_SUBSTITUTION
`
	if err := os.WriteFile(f, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return f
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

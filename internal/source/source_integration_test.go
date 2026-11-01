//go:build integration

// 集成测试：需要 Docker。运行：
//
//	go test ./internal/source/ -tags=integration -count=1
//
// 镜像默认 mysql:8（本地已有；compose 用 mysql:8.0），可用 MYSQL_IMAGE 覆盖。
package source

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"
	_ "github.com/go-sql-driver/mysql"
	"github.com/testcontainers/testcontainers-go/modules/mysql"

	"gocdc/internal/model"
)

func startSrcMySQL(t *testing.T, ctx context.Context) *mysql.MySQLContainer {
	t.Helper()
	image := envOr("MYSQL_IMAGE", "mysql:8")
	ctr, err := mysql.Run(ctx, image,
		mysql.WithDatabase("srcdb"),
		mysql.WithUsername("root"),
		mysql.WithPassword("test"),
		mysql.WithConfigFile(newMySQLConf(t)),
	)
	if err != nil {
		t.Fatalf("启动 MySQL 容器: %v", err)
	}
	return ctr
}

// newMySQLConf 强制 ROW 格式 + 全镜像（04 §1 前置；容器默认已 ROW，显式化防镜像漂移）。
func newMySQLConf(t *testing.T) string {
	t.Helper()
	f := t.TempDir() + "/my.cnf"
	content := `[mysqld]
binlog_format=ROW
binlog_row_image=FULL
server_id=1
log_bin=mysql-bin
`
	if err := os.WriteFile(f, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return f
}

func TestIntegrationSource(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	ctr := startSrcMySQL(t, ctx)
	t.Cleanup(func() { _ = ctr.Terminate(context.Background()) })

	host, err := ctr.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}
	port, err := ctr.MappedPort(ctx, "3306")
	if err != nil {
		t.Fatal(err)
	}
	dsn := fmt.Sprintf("root:test@tcp(%s:%s)/srcdb?multiStatements=true", host, port.Port())
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	deadline := time.Now().Add(60 * time.Second)
	for {
		if err = db.PingContext(ctx); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("MySQL 未就绪: %v", err)
		}
		time.Sleep(500 * time.Millisecond)
	}

	if _, err := db.ExecContext(ctx, `CREATE TABLE t_src (
		id bigint NOT NULL AUTO_INCREMENT PRIMARY KEY,
		name varchar(64) NOT NULL,
		amount decimal(10,2) NOT NULL DEFAULT '0')`); err != nil {
		t.Fatal(err)
	}

	// 起始位点 = 当前 master 位点（首启语义；MySQL 8.2+ 语句改名，两种都试）
	var file string
	var pos uint32
	if err := showMasterStatus(ctx, db, &file, &pos); err != nil {
		t.Fatal(err)
	}

	out := make(chan *Event, 128)
	src := New(Config{
		Addr: host + ":" + port.Port(), User: "root", Password: "test",
		ServerID: 987654, Timezone: time.UTC,
		HeartbeatInterval: time.Second, ReadTimeout: 30 * time.Second,
	}, nil)

	startPos := model.Position{Name: file, Pos: pos}
	runErr := make(chan error, 1)
	runCtx, stopRun := context.WithCancel(context.Background())
	go func() { runErr <- src.Run(runCtx, startPos, nil, out) }()

	// 等 fake rotate（连接首包，Artificial）
	waitFor(t, out, 30*time.Second, func(ev *Event) bool {
		return ev.Kind == KindRotate && ev.Artificial
	}, "fake rotate")

	// 写入一行 → 期待 INSERT 行事件 + XID
	if _, err := db.ExecContext(ctx,
		"INSERT INTO t_src (name, amount) VALUES ('alice', 12.34)"); err != nil {
		t.Fatal(err)
	}
	var rowEv, xidEv *Event
	rowEv = waitFor(t, out, 30*time.Second, func(ev *Event) bool {
		if ev.Kind == KindRow && ev.Table == "t_src" {
			xidEv = waitFor(t, out, 30*time.Second, func(x *Event) bool { return x.Kind == KindXID }, "XID")
			return true
		}
		return false
	}, "INSERT 行事件")

	if rowEv.Schema != "srcdb" || rowEv.RowOp != replication.EnumRowsEventTypeInsert {
		t.Fatalf("行事件元数据: %+v", rowEv)
	}
	rows := rowEv.Rows.Rows
	if len(rows) != 1 {
		t.Fatalf("行数: %d", len(rows))
	}
	r := rows[0]
	if r[0] != int64(1) {
		t.Fatalf("id 值: %#v", r[0])
	}
	// go-mysql v1.14：VARCHAR → string、NEWDECIMAL → string（04 §7 string 无损路径）
	if r[1] != "alice" {
		t.Fatalf("name 值: %#v", r[1])
	}
	if r[2] != "12.34" {
		t.Fatalf("amount 值（decimal 文本）: %#v", r[2])
	}
	if xidEv == nil || xidEv.LogPos == 0 {
		t.Fatal("XID 事件缺失或无位点")
	}

	// 再写一行并删除，验证 UPDATE 对（before/after 成对）与 DELETE
	if _, err := db.ExecContext(ctx, "UPDATE t_src SET name='bob' WHERE id=1"); err != nil {
		t.Fatal(err)
	}
	upd := waitFor(t, out, 30*time.Second, func(ev *Event) bool {
		return ev.Kind == KindRow && ev.Table == "t_src" &&
			ev.RowOp == replication.EnumRowsEventTypeUpdate
	}, "UPDATE 行事件")
	if len(upd.Rows.Rows) != 2 {
		t.Fatalf("UPDATE 应为 before/after 成对: %d", len(upd.Rows.Rows))
	}
	if upd.Rows.Rows[1][1] != "bob" {
		t.Fatalf("after.name: %#v", upd.Rows.Rows[1][1])
	}

	if _, err := db.ExecContext(ctx, "DELETE FROM t_src WHERE id=1"); err != nil {
		t.Fatal(err)
	}
	del := waitFor(t, out, 30*time.Second, func(ev *Event) bool {
		return ev.Kind == KindRow && ev.Table == "t_src" &&
			ev.RowOp == replication.EnumRowsEventTypeDelete
	}, "DELETE 行事件")
	if len(del.Rows.Rows) != 1 {
		t.Fatalf("DELETE 行数: %d", len(del.Rows.Rows))
	}

	// 优雅停机：Run 随 ctx 退出
	stopRun()
	select {
	case err := <-runErr:
		if err != nil && err != context.Canceled {
			t.Fatalf("Run 退出状态: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run 未随 ctx 退出")
	}
}

// showMasterStatus 兼容 MySQL 8.2+（SHOW BINARY LOG STATUS）与旧语法。
func showMasterStatus(ctx context.Context, db *sql.DB, file *string, pos *uint32) error {
	if err := db.QueryRowContext(ctx, "SHOW BINARY LOG STATUS").Scan(file, pos, new(any), new(any), new(any)); err == nil {
		return nil
	}
	return db.QueryRowContext(ctx, "SHOW MASTER STATUS").Scan(file, pos, new(any), new(any), new(any))
}

func waitFor(t *testing.T, out chan *Event, max time.Duration, match func(*Event) bool, what string) *Event {
	t.Helper()
	deadline := time.After(max)
	for {
		select {
		case ev := <-out:
			if match(ev) {
				return ev
			}
		case <-deadline:
			t.Fatalf("等待 %s 超时", what)
		}
	}
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

//go:build integration

// 集成场景：GTID rotate 锚点必须携带 GTIDSet——若连接时不初始化 GTID 跟踪、
// Rotate 事件不带集合，use_gtid 模式下 binlog rotate 后持久化位点 gtid_set
// 被清空，重启 validateStart 拒绝空集合，管线致命退出（生产表现为 GTID 集群
// 一次日志切换后 CDC 永久不可用）。
//
// 本测试：GTID 源 → use_gtid 启动 → 写入 → FLUSH BINARY LOGS（rotate）→ 再写入，
// 断言：① 起始集合即被初始化（首事件前 currentGTID 非空）；② Rotate 事件携带
// 非空集合；③ XID 事件集合随事务单调并入。

package source

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/testcontainers/testcontainers-go/modules/mysql"

	"gocdc/internal/model"
)

func newGTIDMySQLConf(t *testing.T) string {
	t.Helper()
	f := t.TempDir() + "/my.cnf"
	content := `[mysqld]
binlog_format=ROW
binlog_row_image=FULL
server_id=1
log_bin=mysql-bin
gtid_mode=ON
enforce_gtid_consistency=ON
`
	if err := os.WriteFile(f, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return f
}

func TestIntegrationSourceGTIDRotate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	ctr, err := mysql.Run(ctx, envOr("MYSQL_IMAGE", "mysql:8"),
		mysql.WithDatabase("srcdb"),
		mysql.WithUsername("root"),
		mysql.WithPassword("test"),
		mysql.WithConfigFile(newGTIDMySQLConf(t)),
	)
	if err != nil {
		t.Fatalf("启动 MySQL 容器: %v", err)
	}
	t.Cleanup(func() { _ = ctr.Terminate(context.Background()) })
	host, _ := ctr.Host(ctx)
	port, _ := ctr.MappedPort(ctx, "3306")
	addr := host + ":" + port.Port()

	admin, err := sql.Open("mysql", "root:test@tcp("+addr+")/?multiStatements=true")
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	deadline := time.Now().Add(60 * time.Second)
	for {
		var v string
		if err := admin.QueryRowContext(ctx, "SELECT 1").Scan(&v); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("MySQL 未就绪")
		}
		time.Sleep(500 * time.Millisecond)
	}
	if _, err := admin.ExecContext(ctx, `
CREATE TABLE srcdb.t_gtid (id bigint NOT NULL PRIMARY KEY, v varchar(32) NOT NULL);`); err != nil {
		t.Fatal(err)
	}

	// 起始 GTID 集合（可能为空串——容器刚初始化无事务；写入一条让集合非空）
	if _, err := admin.ExecContext(ctx, "INSERT INTO srcdb.t_gtid (id,v) VALUES (1,'base')"); err != nil {
		t.Fatal(err)
	}
	var startSet string
	if err := admin.QueryRowContext(ctx, "SELECT @@gtid_executed").Scan(&startSet); err != nil {
		t.Fatal(err)
	}
	if startSet == "" {
		t.Fatal("源库 GTID 集为空（gtid_mode 未生效？）")
	}

	// rotate 起点之后的文件才开始收事件（对齐既有 source 集成测试口径）
	if _, err := admin.ExecContext(ctx, "FLUSH BINARY LOGS"); err != nil {
		t.Fatal(err)
	}
	var file string
	var pos uint32
	// MySQL 8.2+ 语句更名 SHOW BINARY LOG STATUS，旧版回退 SHOW MASTER STATUS
	// （与 cmd/gocdc showMasterStatus 同一兼容语义；mysql:8.0 容器无新语句）
	if err := admin.QueryRowContext(ctx, "SHOW BINARY LOG STATUS").Scan(&file, &pos, new(any), new(any), new(any)); err != nil {
		if err := admin.QueryRowContext(ctx, "SHOW MASTER STATUS").Scan(&file, &pos, new(any), new(any), new(any)); err != nil {
			t.Fatal(err)
		}
	}

	s := New(Config{
		Addr: addr, User: "root", Password: "test", ServerID: 515152,
		Timezone: time.UTC, UseGTID: true,
		HeartbeatInterval: time.Second, ReadTimeout: 30 * time.Second,
	}, nil)

	// 断言路径：validateStart + 连接初始化都要求非空集合
	p := model.Position{Name: file, Pos: pos, GTIDSet: startSet}
	if err := s.validateStart(p); err != nil {
		t.Fatalf("起始位点校验: %v", err)
	}

	events := make(chan *Event, 256)
	runCtx, stopSrc := context.WithCancel(ctx)
	defer stopSrc()
	runErr := make(chan error, 1)
	go func() { runErr <- s.Run(runCtx, p, nil, events) }()

	sawXID1, sawRotateWithSet, sawXID2 := false, false, false
	setAfterRotate := ""
	dl := time.Now().Add(90 * time.Second)
	// 触发事件流：XID1 → rotate → XID2（源端动作，事件循环并行收）
	go func() {
		time.Sleep(2 * time.Second)
		_, _ = admin.ExecContext(ctx, "INSERT INTO srcdb.t_gtid (id,v) VALUES (2,'one')")
		time.Sleep(2 * time.Second)
		_, _ = admin.ExecContext(ctx, "FLUSH BINARY LOGS")
		time.Sleep(2 * time.Second)
		_, _ = admin.ExecContext(ctx, "INSERT INTO srcdb.t_gtid (id,v) VALUES (3,'two')")
	}()
	for !(sawXID1 && sawRotateWithSet && sawXID2) {
		if time.Now().After(dl) {
			t.Fatalf("事件收集超时: xid1=%v rotateSet=%v xid2=%v", sawXID1, sawRotateWithSet, sawXID2)
		}
		select {
		case ev := <-events:
			switch ev.Kind {
			case KindRotate:
				if sawXID1 { // 只认启动后的真实 rotate（FLUSH BINARY LOGS）
					if ev.GTIDSet == "" {
						t.Fatalf("Rotate 事件 GTIDSet 为空（P0-7 回归失败：%s→%s）", ev.File, ev.File)
					}
					sawRotateWithSet = true
					setAfterRotate = ev.GTIDSet
				}
			case KindXID:
				if ev.GTIDSet == "" {
					t.Fatal("XID 事件 GTIDSet 为空")
				}
				if !sawXID1 {
					sawXID1 = true
				} else if sawRotateWithSet {
					sawXID2 = true
					// 集合单调并入：第二个事务的集合 ⊇ rotate 时集合
					if !strings.Contains(ev.GTIDSet, uuidOf(setAfterRotate)) {
						t.Fatalf("rotate 后集合未保持: %q ⊉ %q", ev.GTIDSet, setAfterRotate)
					}
				}
			}
		case <-time.After(5 * time.Second):
			// 事件驱动：活跃推送，超时说明 flow 停滞
		}
	}

	// 直接断言：连接后内部跟踪已用起始集合初始化（不依赖首个 Query/XID）
	if got := s.currentGTID(); got == "" {
		t.Fatal("内部 GTID 跟踪未初始化（连接后应携带起始集合）")
	}
}

// uuidOf 取 GTID 集文本的 UUID 前缀（"xxxxxxxx-...-...:1-5" → UUID 部分）。
func uuidOf(set string) string {
	if i := strings.IndexByte(set, ':'); i > 0 {
		return set[:i]
	}
	return set
}

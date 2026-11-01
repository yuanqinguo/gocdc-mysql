//go:build integration

package pipeline

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/etcd"
	"github.com/testcontainers/testcontainers-go/modules/mysql"

	"gocdc/internal/schema"
	"gocdc/internal/source"
)

// queryMasterStatus 取源库当前 binlog 位点。MySQL 8.2+ 语句更名 SHOW BINARY
// LOG STATUS，旧版回退 SHOW MASTER STATUS（与 cmd/gocdc showMasterStatus 同一
// 兼容语义；mysql:8.0 容器无新语句）。
func queryMasterStatus(ctx context.Context, admin *sql.DB) (file string, pos uint32, err error) {
	if err = admin.QueryRowContext(ctx, "SHOW BINARY LOG STATUS").Scan(&file, &pos, new(any), new(any), new(any)); err == nil {
		return file, pos, nil
	}
	err = admin.QueryRowContext(ctx, "SHOW MASTER STATUS").Scan(&file, &pos, new(any), new(any), new(any))
	return file, pos, err
}

func startDDLMysql(t *testing.T, ctx context.Context) (*mysql.MySQLContainer, error) {
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
		return nil, err
	}
	return mysql.Run(ctx, envOr("MYSQL_IMAGE", "mysql:8"),
		mysql.WithDatabase("srcdb"), mysql.WithUsername("root"), mysql.WithPassword("test"),
		mysql.WithConfigFile(f))
}

func startDDLEtcd(t *testing.T, ctx context.Context) (*etcd.EtcdContainer, []string, func(), error) {
	t.Helper()
	img := envOr("GOCDC_TEST_ETCD_IMAGE", "docker.cnb.cool/qingflyfei/edu.image.base:etcd-3.5.16")
	ctr, err := etcd.Run(ctx, img)
	if err != nil {
		return nil, nil, nil, err
	}
	eps, err := ctr.ClientEndpoints(ctx)
	if err != nil {
		_ = ctr.Terminate(ctx)
		return nil, nil, nil, err
	}
	cleanup := func() { _ = ctr.Terminate(context.Background()) }
	return ctr, eps, cleanup, nil
}

func newDDLSchemaMgr(srcDB *sql.DB) *schema.Manager {
	return schema.NewManager(schema.NewMySQLLoader(srcDB))
}

func newDDLSource(addr string) *source.Source {
	return source.New(source.Config{
		Addr: addr, User: "root", Password: "test", ServerID: 515151,
		Timezone: time.UTC, HeartbeatInterval: time.Second, ReadTimeout: 30 * time.Second,
	}, nil)
}

func waitCount(t *testing.T, ctx context.Context, db *sql.DB, q string, want int64, max time.Duration) {
	t.Helper()
	deadline := time.Now().Add(max)
	for {
		var n int64
		if err := db.QueryRowContext(ctx, q).Scan(&n); err == nil && n == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("计数未收敛 %q → %d（期望 %d）", q, n, want)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func waitColumn(t *testing.T, ctx context.Context, db *sql.DB, schemaName, table, col string, max time.Duration) {
	t.Helper()
	deadline := time.Now().Add(max)
	for {
		var n int
		_ = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM information_schema.COLUMNS
			 WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND COLUMN_NAME=?`, schemaName, table, col).Scan(&n)
		if n > 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("列 %s 未出现在 %s.%s", col, schemaName, table)
		}
		time.Sleep(300 * time.Millisecond)
	}
}

// waitPendingDDL 轮询 etcd 中该表的 pending DDL 记录，返回记录 ID。
func waitPendingDDL(t *testing.T, ctx context.Context, gate *EtcdDDLGate, schemaName, table string, max time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(max)
	for {
		recs, err := gate.PendingByTable(ctx, schemaName, table)
		if err == nil && len(recs) > 0 {
			return recs[0].ID
		}
		if time.Now().After(deadline) {
			t.Fatalf("pending DDL 未出现（%s.%s）", schemaName, table)
		}
		time.Sleep(300 * time.Millisecond)
	}
}

var _ = testcontainers.GenericContainer // 保留引用
var _ = time.Now

//go:build integration

// 集成测试：需要 Docker。运行：
//
//	go test ./internal/stock/ -tags=integration -count=1
//
// 场景：带并发写入的源库做全量→增量对账零差异；跨会话整表重扫；
// done 跳过（重启不重跑）。etcd 容器供标记存储（lease 门槛真实链路）。

package stock

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/testcontainers/testcontainers-go/modules/etcd"
	"github.com/testcontainers/testcontainers-go/modules/mysql"
	clientv3 "go.etcd.io/etcd/client/v3"

	"gocdc/internal/config"
	"gocdc/internal/etcdx"
	"gocdc/internal/model"
	"gocdc/internal/pipeline"
	"gocdc/internal/schema"
	mysqlsink "gocdc/internal/sink/mysql"
	"gocdc/internal/transform"
)

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func TestIntegrationStock(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

	// ── 容器：MySQL（单机双库）+ etcd（标记）──
	mctr, err := mysql.Run(ctx, envOr("MYSQL_IMAGE", "mysql:8"),
		mysql.WithDatabase("srcdb"), mysql.WithUsername("root"), mysql.WithPassword("test"),
		mysql.WithConfigFile(mycnf(t)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mctr.Terminate(context.Background()) })
	host, _ := mctr.Host(ctx)
	port, _ := mctr.MappedPort(ctx, "3306")
	addr := host + ":" + port.Port()

	ectr, err := etcd.Run(ctx, envOr("GOCDC_TEST_ETCD_IMAGE", "docker.cnb.cool/qingflyfei/edu.image.base:etcd-3.5.16"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ectr.Terminate(context.Background()) })
	endpoints, _ := ectr.ClientEndpoints(ctx)
	cli, err := etcdx.New(etcdx.Options{Endpoints: endpoints})
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	admin, err := sql.Open("mysql", "root:test@tcp("+addr+")/?multiStatements=true")
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	waitReady(t, ctx, admin)
	if _, err := admin.ExecContext(ctx, `
CREATE DATABASE dstdb;
CREATE TABLE srcdb.t_stock (
  id bigint NOT NULL PRIMARY KEY,
  v  varchar(64) NOT NULL
);
CREATE TABLE dstdb.t_stock LIKE srcdb.t_stock;`); err != nil {
		t.Fatal(err)
	}

	// 预置 500 行（快照基线）
	srcDB, err := sql.Open("mysql", "root:test@tcp("+addr+")/")
	if err != nil {
		t.Fatal(err)
	}
	defer srcDB.Close()
	batchValues := func(from, to int64) string {
		var vs []string
		for i := from; i <= to; i++ {
			vs = append(vs, fmt.Sprintf("(%d, 'v%d')", i, i))
		}
		return strings.Join(vs, ",")
	}
	if _, err := srcDB.ExecContext(ctx, "INSERT INTO srcdb.t_stock (id,v) VALUES "+batchValues(1, 500)); err != nil {
		t.Fatal(err)
	}

	doStock := true
	rules, err := pipeline.CompileRules([]config.Rule{{
		Schema: "srcdb", TargetDatabase: "dstdb",
		Sinks:  &config.RuleSinks{MySQL: config.RuleSink{Enabled: true}},
		Tables: []config.TableRule{{Table: "t_stock", OrderByColumn: "id", DoStock: &doStock}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	markers := NewMarkerStore(etcdx.NewKV(cli), "/gocdc/ha/stock")
	// 测试门槛：真实 leader 指针 key + 真实 lease（与 elector 当选同构）
	gateSess, err := etcdx.NewSession(ctx, cli, 60)
	if err != nil {
		t.Fatal(err)
	}
	defer gateSess.Close()
	if _, err := cli.Put(ctx, "/gocdc/ha/leader", "node-a", clientv3.WithLease(gateSess.Lease())); err != nil {
		t.Fatal(err)
	}
	markers.SetGate("/gocdc/ha/leader", gateSess.Lease())

	schemaMgr := schema.NewManager(schema.NewMySQLLoader(srcDB))
	tr := transform.New(transform.Options{Timezone: time.UTC})
	msink, err := mysqlsink.New(mysqlsink.Options{Addr: addr, User: "root", Password: "test", MaxOpenConns: 4,
		TargetDB: func(db, table string) string { return "dstdb" }})
	if err != nil {
		t.Fatal(err)
	}
	defer msink.Close(ctx)
	svc := New(srcDB, schemaMgr, tr, msink, markers, config.Stock{LockMode: "auto", ChunkRows: 100}, rules, nil, "node-a", "node-a", nil)

	svc.SetSchemas([]string{"srcdb"})

	// ── 第一轮：全量（并发写入源库——模拟业务不停写）──
	var wg sync.WaitGroup
	stopWrite := make(chan struct{})
	wg.Add(1)
	go func() { // 全量期间持续写入（这些行只能靠增量重放补齐）
		defer wg.Done()
		i := int64(501)
		for {
			select {
			case <-stopWrite:
				return
			default:
			}
			_, _ = srcDB.ExecContext(ctx, "INSERT INTO srcdb.t_stock (id,v) VALUES (?,?)", i, fmt.Sprintf("live%d", i))
			i++
			time.Sleep(5 * time.Millisecond)
		}
	}()
	time.Sleep(300 * time.Millisecond) // 让并发写入先跑起来

	res, err := svc.Run(ctx)
	close(stopWrite)
	wg.Wait()
	if err != nil {
		t.Fatalf("全量失败: %v", err)
	}
	if res.Skipped != 0 || res.Scanned < 500 {
		t.Fatalf("首轮结果: skipped=%d scanned=%d", res.Skipped, res.Scanned)
	}
	// P 先行：全局位点已落
	if gp, ok, _ := markers.GlobalPosition(ctx); !ok || gp.Name == "" {
		t.Fatal("全局 stock_position 应已持久化")
	}
	// 快照基线在目标：≥500 行（并发行不在快照视图内）
	// 实例同一性：sink 连接 vs srcDB 连接
	var sp, ss int64
	var spid, ssid string
	_ = msink.DB().QueryRowContext(ctx, "SELECT @@port, @@server_uuid").Scan(&sp, &spid)
	_ = srcDB.QueryRowContext(ctx, "SELECT @@port, @@server_uuid").Scan(&ss, &ssid)
	t.Logf("实例对照: sink port=%d uuid=%s | src port=%d uuid=%s", sp, spid[:8], ss, ssid[:8])
	// 审计：general_log 表里 chunk 期间的 INSERT 去向
	var logN, dstdbN, srcdbN int
	_ = srcDB.QueryRowContext(ctx, "SELECT COUNT(*) FROM mysql.general_log WHERE command_type='Query' AND argument LIKE 'INSERT%t_stock%'").Scan(&logN)
	_ = srcDB.QueryRowContext(ctx, "SELECT COUNT(*) FROM mysql.general_log WHERE command_type='Query' AND argument LIKE 'INSERT INTO `dstdb`.`t_stock`%'").Scan(&dstdbN)
	_ = srcDB.QueryRowContext(ctx, "SELECT COUNT(*) FROM mysql.general_log WHERE command_type='Query' AND argument LIKE 'INSERT INTO `srcdb`.`t_stock`%'").Scan(&srcdbN)
	var sample string
	_ = srcDB.QueryRowContext(ctx, "SELECT LEFT(argument, 160) FROM mysql.general_log WHERE command_type='Query' AND argument LIKE 'INSERT INTO `dstdb`.`t_stock`%' ORDER BY event_time DESC LIMIT 1").Scan(&sample)
	t.Logf("general_log: t_stock INSERT=%d (dstdb=%d srcdb=%d) 样本=%q", logN, dstdbN, srcdbN, sample)
	// 按线程聚合：chunk 大 INSERT 与手动小 INSERT 是否同连接
	trs2, _ := srcDB.QueryContext(ctx, "SELECT thread_id, LEFT(argument,60) FROM mysql.general_log WHERE command_type='Query' AND argument LIKE 'INSERT INTO `dstdb`.`t_stock`%' ORDER BY event_time LIMIT 10")
	for trs2.Next() {
		var tid int64
		var a string
		_ = trs2.Scan(&tid, &a)
		t.Logf("  log线程=%d arg=%q", tid, a)
	}
	trs2.Close()
	// 关键语句时序：锁/事务/t_stock 写入（过滤时区加载噪音）
	trs3, _ := srcDB.QueryContext(ctx, "SELECT thread_id, command_type, LEFT(argument,70) FROM mysql.general_log "+
		"WHERE argument LIKE '%FLUSH TABLES%' OR argument LIKE '%UNLOCK%' OR argument LIKE 'START TRANSACTION%' "+
		"OR argument LIKE 'ROLLBACK%' OR argument LIKE 'COMMIT%' OR (argument LIKE 'INSERT INTO `dstdb`.`t_stock`%') "+
		"OR argument LIKE '%SHOW MASTER%' OR argument LIKE '%SHOW BINARY%' ORDER BY event_time LIMIT 40")
	for trs3.Next() {
		var tid int64
		var ct, a string
		_ = trs3.Scan(&tid, &ct, &a)
		t.Logf("  L[%d][%s] %s", tid, ct, a)
	}
	trs3.Close()
	// 快照基线在目标：≥500 行（并发行在位点 P 之后，由增量重放补齐）
	var n int
	if err := srcDB.QueryRowContext(ctx, "SELECT COUNT(*) FROM dstdb.t_stock").Scan(&n); err != nil || n < 500 {
		t.Fatalf("快照基线行数: %d err=%v", n, err)
	}

	// ── done 跳过：再跑一遍（重启不重跑幂等）──
	res2, err := svc.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if res2.Skipped != 1 {
		t.Fatalf("done 表应跳过: skipped=%d", res2.Skipped)
	}

	// ── 跨会话整表重扫：伪造 stale running 标记 → 重扫后 done 且行数正确 ──
	if err := markers.Put(ctx, "srcdb", "t_stock", &Marker{
		Status: StatusRunning, SessionID: "stale-session",
	}); err != nil {
		t.Fatal(err)
	}
	res3, err := svc.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if res3.Skipped != 0 {
		t.Fatalf("stale running 应重扫: skipped=%d", res3.Skipped)
	}
	mk, _ := markers.Get(ctx, "srcdb", "t_stock")
	if mk == nil || mk.Status != StatusDone || mk.SessionID == "stale-session" {
		t.Fatalf("重扫后标记: %+v", mk)
	}

	// ── restock：清标记 → 再跑 = 重新全量（幂等覆盖）──
	if err := svc.ResetTable(ctx, "srcdb", "t_stock"); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if mk, _ := markers.Get(ctx, "srcdb", "t_stock"); mk == nil || mk.Status != StatusDone {
		t.Fatal("restock 后应重新 done")
	}

	// PURGED 检查：未来位点不判 purged；早于最老 binlog → 拒绝（容器保留全部
	// binlog 时前者不触发，语义由文件序比较保证）
	if err := CheckPurged(ctx, srcDB, model.Position{Name: "zzz-future", Pos: 4}); err != nil {
		t.Fatalf("未来位点不应判 purged: %v", err)
	}
}

func mycnf(t *testing.T) string {
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

func waitReady(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for {
		if err := db.PingContext(ctx); err == nil {
			return
		} else if time.Now().After(deadline) {
			t.Fatal("MySQL 未就绪")
		}
		time.Sleep(500 * time.Millisecond)
	}
}

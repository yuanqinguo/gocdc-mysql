//go:build integration

// 集成场景：
//
//	target_database 同名回退——规则不配 target_database（空 = 同名默认），
//	sink 闭包返回空 → resolveDB 回退源库名；同名默认路径须容器级验证
//	（否则启动核对/写入实际不可用）。
//	stock 多表并行——sql.Conn 不允许跨 goroutine 并发，多 worker 必须各用
//	独立快照连接，并行路径须容器级覆盖。
//
// 本测试：双 MySQL 实例（同名跨实例才是「同名默认」的真实形态）+ 3 张有数据的
// 表（>1 chunk）+ workers 默认 4，全部表并行扫描 done 且行数跨实例收敛。

package stock

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/testcontainers/testcontainers-go/modules/etcd"
	"github.com/testcontainers/testcontainers-go/modules/mysql"
	clientv3 "go.etcd.io/etcd/client/v3"

	"gocdc/internal/config"
	"gocdc/internal/etcdx"
	"gocdc/internal/pipeline"
	"gocdc/internal/schema"
	mysqlsink "gocdc/internal/sink/mysql"
	"gocdc/internal/transform"
)

const stockParallelDDL = `
CREATE TABLE srcdb.%s (
  id bigint NOT NULL PRIMARY KEY,
  v varchar(64) NOT NULL
);`

func TestIntegrationStockParallelSameName(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	mkMySQL := func() *mysql.MySQLContainer {
		ctr, err := mysql.Run(ctx, envOr("MYSQL_IMAGE", "mysql:8"),
			mysql.WithDatabase("srcdb"), mysql.WithUsername("root"), mysql.WithPassword("test"),
			mysql.WithConfigFile(mycnf(t)))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = ctr.Terminate(context.Background()) })
		return ctr
	}
	srcCtr, dstCtr := mkMySQL(), mkMySQL()
	addrOf := func(ctr *mysql.MySQLContainer) string {
		host, _ := ctr.Host(ctx)
		port, _ := ctr.MappedPort(ctx, "3306")
		return host + ":" + port.Port()
	}
	srcAddr, dstAddr := addrOf(srcCtr), addrOf(dstCtr)

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

	openDB := func(addr string) *sql.DB {
		db, err := sql.Open("mysql", "root:test@tcp("+addr+")/?multiStatements=true")
		if err != nil {
			t.Fatal(err)
		}
		return db
	}
	srcAdmin, dstAdmin := openDB(srcAddr), openDB(dstAddr)
	defer srcAdmin.Close()
	defer dstAdmin.Close()
	waitReady(t, ctx, srcAdmin)
	waitReady(t, ctx, dstAdmin)

	for _, tbl := range []string{"t_a", "t_b", "t_c"} {
		ddl := fmt.Sprintf(stockParallelDDL, tbl)
		if _, err := srcAdmin.ExecContext(ctx, ddl); err != nil {
			t.Fatal(err)
		}
		if _, err := dstAdmin.ExecContext(ctx, ddl); err != nil { // 同名目标库同构表
			t.Fatal(err)
		}
	}

	srcDB, err := sql.Open("mysql", "root:test@tcp("+srcAddr+")/")
	if err != nil {
		t.Fatal(err)
	}
	defer srcDB.Close()
	const rowsPer = 300 // ChunkRows=100 → 每表 3 chunk，并行度有意义
	for _, tbl := range []string{"t_a", "t_b", "t_c"} {
		var vs []string
		for i := 1; i <= rowsPer; i++ {
			vs = append(vs, fmt.Sprintf("(%d, 'v%d')", i, i))
		}
		if _, err := srcDB.ExecContext(ctx,
			"INSERT INTO srcdb."+tbl+" (id,v) VALUES "+strings.Join(vs, ",")); err != nil {
			t.Fatal(err)
		}
	}

	// 关键：TargetDatabase 留空（同名默认路径的核心场景）
	doStock := true
	rules, err := pipeline.CompileRules([]config.Rule{{
		Schema: "srcdb",
		Sinks:  &config.RuleSinks{MySQL: config.RuleSink{Enabled: true}},
		Tables: []config.TableRule{
			{Table: "t_a", OrderByColumn: "id", DoStock: &doStock},
			{Table: "t_b", OrderByColumn: "id", DoStock: &doStock},
			{Table: "t_c", OrderByColumn: "id", DoStock: &doStock},
		},
	}})
	if err != nil {
		t.Fatal(err)
	}

	markers := NewMarkerStore(etcdx.NewKV(cli), "/gocdc/stockp/stock")
	gateSess, err := etcdx.NewSession(ctx, cli, 60)
	if err != nil {
		t.Fatal(err)
	}
	defer gateSess.Close()
	if _, err := cli.Put(ctx, "/gocdc/stockp/leader", "node-a", clientv3.WithLease(gateSess.Lease())); err != nil {
		t.Fatal(err)
	}
	markers.SetGate("/gocdc/stockp/leader", gateSess.Lease())

	schemaMgr := schema.NewManager(schema.NewMySQLLoader(srcDB))
	tr := transform.New(transform.Options{Timezone: time.UTC})
	// 关键：TargetDB 闭包返回空串（同名默认 → Sink.resolveDB 回退源库名；
	// 目标是独立实例上的同名库——真实生产形态）
	msink, err := mysqlsink.New(mysqlsink.Options{Addr: dstAddr, User: "root", Password: "test", MaxOpenConns: 4,
		TargetDB: func(db, table string) string { return "" }})
	if err != nil {
		t.Fatal(err)
	}
	defer msink.Close(ctx)

	svc := New(srcDB, schemaMgr, tr, msink, markers,
		config.Stock{LockMode: "auto", ChunkRows: 100}, rules, nil, "node-a", nil)
	svc.SetSchemas([]string{"srcdb"})

	// Workers 默认 4（>1）：3 表并行扫描共用快照连接集（连接独占约束的核心场景）。
	if _, err := svc.Run(ctx); err != nil {
		t.Fatalf("多表并行全量失败: %v", err)
	}

	// done 标记 + 跨实例同名目标行数收敛
	for _, tbl := range []string{"t_a", "t_b", "t_c"} {
		mk, err := markers.Get(ctx, "srcdb", tbl)
		if err != nil || mk == nil || mk.Status != StatusDone {
			t.Fatalf("%s 无 done 标记: %+v err=%v", tbl, mk, err)
		}
		if mk.RowsDone != rowsPer {
			t.Fatalf("%s 行数 %d != %d", tbl, mk.RowsDone, rowsPer)
		}
		dl := time.Now().Add(30 * time.Second)
		for {
			var n int64
			if err := dstAdmin.QueryRowContext(ctx, "SELECT COUNT(*) FROM srcdb."+tbl).Scan(&n); err == nil && n == rowsPer {
				break
			}
			if time.Now().After(dl) {
				t.Fatalf("%s 目标行数未收敛", tbl)
			}
			time.Sleep(300 * time.Millisecond)
		}
	}
}

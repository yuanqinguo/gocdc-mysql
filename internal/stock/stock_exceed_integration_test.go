//go:build integration

// 集成场景（待扫表数 > stock.workers 的连接独占碰撞）：
//
//	若按 todo 下标取快照连接（i%len(conns) 取模轮转），会把「仍在扫描中」表的
//	同根连接发给后启动的表——两 goroutine 并发共用一根 sql.Conn，driver: bad
//	connection，全量初始化失败（间歇性，取决于表完成次序）。表数 ≤ workers
//	的并行场景永不越界，不会暴露该问题。
//
// 本测试构造确定性碰撞：workers=2、6 张表、第一张表重载（50 chunk 长扫），
// 其余轻表——取模轮转下轻表必然在第一张表扫完前撞上 conn0；槽位 channel
// 独占连接后全部收敛。

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

func TestIntegrationStockTablesExceedWorkers(t *testing.T) {
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

	// 6 张表 > workers=2：碰撞构造的关键。t_a 重载（5000 行 / chunk 100 = 50 chunk，
	// 长时间占住一根连接），t_b..t_f 轻表（取模轮转下必然复用 t_a 的连接）。
	tables := []string{"t_a", "t_b", "t_c", "t_d", "t_e", "t_f"}
	for _, tbl := range tables {
		ddl := fmt.Sprintf(stockParallelDDL, tbl)
		if _, err := srcAdmin.ExecContext(ctx, ddl); err != nil {
			t.Fatal(err)
		}
		if _, err := dstAdmin.ExecContext(ctx, ddl); err != nil {
			t.Fatal(err)
		}
	}

	srcDB, err := sql.Open("mysql", "root:test@tcp("+srcAddr+")/")
	if err != nil {
		t.Fatal(err)
	}
	defer srcDB.Close()
	for _, tbl := range tables {
		n := 3
		if tbl == "t_a" {
			n = 5000
		}
		var vs []string
		for i := 1; i <= n; i++ {
			vs = append(vs, fmt.Sprintf("(%d, 'v%d')", i, i))
		}
		if _, err := srcDB.ExecContext(ctx,
			"INSERT INTO srcdb."+tbl+" (id,v) VALUES "+strings.Join(vs, ",")); err != nil {
			t.Fatal(err)
		}
	}

	doStock := true
	rules, err := pipeline.CompileRules([]config.Rule{{
		Schema: "srcdb",
		Sinks:  &config.RuleSinks{MySQL: config.RuleSink{Enabled: true}},
		Tables: []config.TableRule{
			{Table: "t_a", OrderByColumn: "id", DoStock: &doStock},
			{Table: "t_b", OrderByColumn: "id", DoStock: &doStock},
			{Table: "t_c", OrderByColumn: "id", DoStock: &doStock},
			{Table: "t_d", OrderByColumn: "id", DoStock: &doStock},
			{Table: "t_e", OrderByColumn: "id", DoStock: &doStock},
			{Table: "t_f", OrderByColumn: "id", DoStock: &doStock},
		},
	}})
	if err != nil {
		t.Fatal(err)
	}

	markers := NewMarkerStore(etcdx.NewKV(cli), "/gocdc/stockex/stock")
	gateSess, err := etcdx.NewSession(ctx, cli, 60)
	if err != nil {
		t.Fatal(err)
	}
	defer gateSess.Close()
	if _, err := cli.Put(ctx, "/gocdc/stockex/leader", "node-a", clientv3.WithLease(gateSess.Lease())); err != nil {
		t.Fatal(err)
	}
	markers.SetGate("/gocdc/stockex/leader", gateSess.Lease())

	schemaMgr := schema.NewManager(schema.NewMySQLLoader(srcDB))
	tr := transform.New(transform.Options{Timezone: time.UTC})
	msink, err := mysqlsink.New(mysqlsink.Options{Addr: dstAddr, User: "root", Password: "test", MaxOpenConns: 4,
		TargetDB: func(db, table string) string { return "" }})
	if err != nil {
		t.Fatal(err)
	}
	defer msink.Close(ctx)

	svc := New(srcDB, schemaMgr, tr, msink, markers,
		config.Stock{LockMode: "auto", ChunkRows: 100, Workers: 2}, rules, nil, "node-a", nil)
	svc.SetSchemas([]string{"srcdb"})

	// 碰撞点：轻表在 t_a 长扫期间启动，取模 Conn(2%2)=Conn(0) 撞上在途扫描 → bad connection。
	if _, err := svc.Run(ctx); err != nil {
		t.Fatalf("表数>workers 全量失败: %v", err)
	}

	for _, tbl := range tables {
		want := int64(3)
		if tbl == "t_a" {
			want = 5000
		}
		mk, err := markers.Get(ctx, "srcdb", tbl)
		if err != nil || mk == nil || mk.Status != StatusDone {
			t.Fatalf("%s 无 done 标记: %+v err=%v", tbl, mk, err)
		}
		if mk.RowsDone != want {
			t.Fatalf("%s 行数 %d != %d", tbl, mk.RowsDone, want)
		}
		dl := time.Now().Add(30 * time.Second)
		for {
			var n int64
			if err := dstAdmin.QueryRowContext(ctx, "SELECT COUNT(*) FROM srcdb."+tbl).Scan(&n); err == nil && n == want {
				break
			}
			if time.Now().After(dl) {
				t.Fatalf("%s 目标行数未收敛", tbl)
			}
			time.Sleep(300 * time.Millisecond)
		}
	}
}

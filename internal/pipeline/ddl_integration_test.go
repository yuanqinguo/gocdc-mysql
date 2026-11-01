//go:build integration

// 集成测试：需要 Docker。运行：
//
//	go test ./internal/pipeline/ -tags=integration -count=1
//
// 场景（08）：加列/扩容自动收敛（auto 白名单）；DROP → pause 挂起 →
// 人工决策 skip 后 resume，全程无静默跳行。

package pipeline

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"go.uber.org/zap"
	"gocdc/internal/config"
	"gocdc/internal/etcdx"
	"gocdc/internal/model"
	mysqlsink "gocdc/internal/sink/mysql"
	"gocdc/internal/transform"
)

func TestIntegrationDDLPolicies(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

	// ── 容器：MySQL + etcd（审批 gate）──
	ctr, err := startDDLMysql(t, ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ctr.Terminate(context.Background()) })
	host, _ := ctr.Host(ctx)
	port, _ := ctr.MappedPort(ctx, "3306")
	addr := host + ":" + port.Port()

	_, eend, ecleanup, err := startDDLEtcd(t, ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ecleanup)

	// DDL 连接必须带默认库 srcdb：QueryEvent.Schema = 会话当前库，
	// 无默认库时 Schema 为空会被分发器按「无规则」过滤（08 barrier 的路由前提）
	admin, err := sql.Open("mysql", "root:test@tcp("+addr+")/srcdb?multiStatements=true")
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	waitReady(t, ctx, admin)
	if _, err := admin.ExecContext(ctx, `
CREATE DATABASE IF NOT EXISTS dstdb;
CREATE TABLE IF NOT EXISTS srcdb.t_ddl (
  id bigint NOT NULL PRIMARY KEY,
  name varchar(32) NOT NULL
);
CREATE TABLE IF NOT EXISTS dstdb.t_ddl LIKE srcdb.t_ddl;`); err != nil {
		t.Fatal(err)
	}

	// 起始位点（建表之后）
	var file string
	var pos uint32
	if file, pos, err = queryMasterStatus(ctx, admin); err != nil {
		t.Fatal(err)
	}

	srcDB, err := sql.Open("mysql", "root:test@tcp("+addr+")/")
	if err != nil {
		t.Fatal(err)
	}
	defer srcDB.Close()

	doTrue := true
	rules, err := CompileRules([]config.Rule{{
		Schema: "srcdb", TargetDatabase: "dstdb", DDLPolicy: "auto",
		Sinks:  &config.RuleSinks{MySQL: config.RuleSink{Enabled: true}},
		Tables: []config.TableRule{{Table: "t_ddl", OrderByColumn: "id", DoStock: &doTrue}},
	}})
	if err != nil {
		t.Fatal(err)
	}

	cli, err := etcdx.New(etcdx.Options{Endpoints: eend})
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	store := &e2eStore{}
	gate := NewDDLGate(etcdx.NewKV(cli), "/gocdc/ddl/events/ddl")

	schemaMgr := newDDLSchemaMgr(srcDB)
	tr := transform.New(transform.Options{Timezone: time.UTC})
	msink, err := mysqlsink.New(mysqlsink.Options{Addr: addr, User: "root", Password: "test", MaxOpenConns: 4,
		TargetDB: func(db, table string) string { return "dstdb" }})
	if err != nil {
		t.Fatal(err)
	}
	defer msink.Close(ctx)

	ddlHooks := &DDLHooks{
		Gate: gate,
		DiffTarget: func(ctx context.Context, schema, table string) ([]string, bool, string, error) {
			res, err := mysqlsink.DiffTargetTable(ctx, srcDB, msink.DB(), schema, "dstdb", table)
			if err != nil {
				return nil, false, "", err
			}
			return res.Stmts, res.Whitelisted, res.Summary, nil
		},
		ApplyTarget: func(ctx context.Context, stmts []string) error {
			return mysqlsink.ApplyTargetDDL(ctx, msink.DB(), stmts, 30*time.Second)
		},
	}

	ddlLog, _ := zap.NewDevelopment()
	ddlSrc := newDDLSource(addr)
	_ = ddlSrc.DebugQueryHook // 诊断钩子可用（默认关）
	pl := New(Options{
		Source:    ddlSrc,
		SchemaMgr: schemaMgr, Transformer: tr, Sink: msink, Store: store, Rules: rules,
		DDLHooks: ddlHooks,
		Workers:  2, BatchSize: 5, FlushInterval: 50 * time.Millisecond, SaveInterval: 200 * time.Millisecond,
		NodeID: "ddl-test", DrainTimeout: 10 * time.Second,
		ExcludeSchemas: []string{"mysql", "sys", "information_schema", "performance_schema"},
		Log:            ddlLog,
	})
	runCtx, stop := context.WithCancel(context.Background())
	defer stop()
	runErr := make(chan error, 1)
	go func() { runErr <- pl.Run(runCtx, model.Position{Name: file, Pos: pos}, nil) }()

	// ① 基线写入
	if _, err := srcDB.ExecContext(ctx, "INSERT INTO srcdb.t_ddl (id,name) VALUES (1,'a'),(2,'b')"); err != nil {
		t.Fatal(err)
	}
	waitCount(t, ctx, srcDB, "SELECT COUNT(*) FROM dstdb.t_ddl", 2, 30*time.Second)

	// ② auto 白名单：ADD COLUMN → 目标自动加列 + 后续行携带新列
	if _, err := admin.ExecContext(ctx, "ALTER TABLE srcdb.t_ddl ADD COLUMN remark varchar(64) NULL"); err != nil {
		t.Fatal(err)
	}
	waitColumn(t, ctx, srcDB, "dstdb", "t_ddl", "remark", 60*time.Second)
	if _, err := srcDB.ExecContext(ctx, "INSERT INTO srcdb.t_ddl (id,name,remark) VALUES (3,'c','r3')"); err != nil {
		t.Fatal(err)
	}
	waitCount(t, ctx, srcDB, "SELECT COUNT(*) FROM dstdb.t_ddl WHERE remark='r3'", 1, 30*time.Second)

	// ③ auto 白名单：varchar 扩容 32→128
	if _, err := admin.ExecContext(ctx, "ALTER TABLE srcdb.t_ddl MODIFY name varchar(128) NOT NULL"); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(60 * time.Second)
	for {
		var tgtLen int
		_ = srcDB.QueryRowContext(ctx, `SELECT CHARACTER_MAXIMUM_LENGTH FROM information_schema.COLUMNS
			 WHERE TABLE_SCHEMA='dstdb' AND TABLE_NAME='t_ddl' AND COLUMN_NAME='name'`).Scan(&tgtLen)
		if tgtLen == 128 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("varchar 扩容未收敛: %d", tgtLen)
		}
		time.Sleep(300 * time.Millisecond)
	}

	// ④ 白名单外（DROP COLUMN）→ pause 挂起 → skip 决策 → resume
	if _, err := admin.ExecContext(ctx, "ALTER TABLE srcdb.t_ddl DROP COLUMN remark"); err != nil {
		t.Fatal(err)
	}
	gateKey := waitPendingDDL(t, ctx, gate, "srcdb", "t_ddl", 30*time.Second)
	// 挂起期间写入（rescan 重放覆盖窗口）
	if _, err := srcDB.ExecContext(ctx, "INSERT INTO srcdb.t_ddl (id,name) VALUES (4,'d')"); err != nil {
		t.Fatal(err)
	}
	// 人工决策 skip（08 §5-C）→ worker 按新 schema 继续
	if err := gate.Settle(ctx, gateKey, DecisionSkip, "test: 接受删列"); err != nil {
		t.Fatal(err)
	}
	waitCount(t, ctx, srcDB, "SELECT COUNT(*) FROM dstdb.t_ddl WHERE id=4", 1, 30*time.Second)

	// 全程无丢行：源 4 行 → 目标 4 行
	waitCount(t, ctx, srcDB, "SELECT COUNT(*) FROM dstdb.t_ddl", 4, 30*time.Second)

	stop()
	select {
	case <-runErr:
	case <-time.After(20 * time.Second):
		t.Fatal("管线未退出")
	}
}

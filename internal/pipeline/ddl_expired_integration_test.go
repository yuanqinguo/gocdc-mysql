//go:build integration

// 集成场景：DDL approve 竞态——审批挂起期间源表被后续 DDL 删除/改名。
// 约束语义：源表已不存在 = 该 DDL 事件已过期 → 按 skip 放行（审计留痕），
// 管线继续处理后续事件；若按 fatal 处理，管线会卡死在已不可能通过复验的
// 状态，且重启后 Settled(approve) 命中同一路径复现。生产中审批慢 + DBA
// 连续操作是真实场景。
//
// 本测试断言：approve 后管线不死、后续表事件正常流动。

package pipeline

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"go.uber.org/zap"

	"gocdc/internal/config"
	"gocdc/internal/etcdx"
	"gocdc/internal/model"
	mysqlsink "gocdc/internal/sink/mysql"
	"gocdc/internal/transform"
)

func TestIntegrationDDLExpiredDuringPause(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

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

	admin, err := sql.Open("mysql", "root:test@tcp("+addr+")/srcdb?multiStatements=true")
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	waitReady(t, ctx, admin)
	if _, err := admin.ExecContext(ctx, `
CREATE DATABASE IF NOT EXISTS dstdb;
CREATE TABLE IF NOT EXISTS srcdb.t_probe (
  id bigint NOT NULL PRIMARY KEY,
  name varchar(32) NOT NULL
);
CREATE TABLE IF NOT EXISTS srcdb.t_alive (
  id bigint NOT NULL PRIMARY KEY,
  v varchar(32) NOT NULL
);
CREATE TABLE IF NOT EXISTS dstdb.t_probe LIKE srcdb.t_probe;
CREATE TABLE IF NOT EXISTS dstdb.t_alive LIKE srcdb.t_alive;`); err != nil {
		t.Fatal(err)
	}

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

	// pause 策略：CREATE/ALTER 一律挂起等审批
	doTrue := true
	rules, err := CompileRules([]config.Rule{{
		Schema: "srcdb", TargetDatabase: "dstdb", DDLPolicy: "pause",
		Sinks: &config.RuleSinks{MySQL: config.RuleSink{Enabled: true}},
		Tables: []config.TableRule{
			{Table: "t_ddl", OrderByColumn: "id", DoStock: &doTrue},
			{Table: "t_probe", OrderByColumn: "id", DoStock: &doTrue},
			{Table: "t_alive", OrderByColumn: "id", DoStock: &doTrue},
		},
	}})
	if err != nil {
		t.Fatal(err)
	}

	cli, err := etcdx.New(etcdx.Options{Endpoints: eend})
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	gate := NewDDLGate(etcdx.NewKV(cli), "/gocdc/ddlexp/events/ddl")

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

	pl := New(Options{
		Source:    newDDLSource(addr),
		SchemaMgr: schemaMgr, Transformer: tr, Sink: msink,
		Store: &e2eStore{}, Rules: rules, DDLHooks: ddlHooks,
		Workers: 2, BatchSize: 5, FlushInterval: 50 * time.Millisecond, SaveInterval: 200 * time.Millisecond,
		NodeID: "ddl-exp-test", DrainTimeout: 10 * time.Second,
		ExcludeSchemas: []string{"mysql", "sys", "information_schema", "performance_schema"},
		Log:            zap.NewNop(),
	})
	runCtx, stop := context.WithCancel(context.Background())
	defer stop()
	runErr := make(chan error, 1)
	go func() { runErr <- pl.Run(runCtx, model.Position{Name: file, Pos: pos}, nil) }()

	// ① 基线行流动
	if _, err := srcDB.ExecContext(ctx, "INSERT INTO srcdb.t_probe (id,name) VALUES (1,'a')"); err != nil {
		t.Fatal(err)
	}
	waitCount(t, ctx, srcDB, "SELECT COUNT(*) FROM dstdb.t_probe", 1, 30*time.Second)

	// ② ALTER → pause 挂起
	if _, err := admin.ExecContext(ctx, "ALTER TABLE srcdb.t_probe ADD COLUMN remark varchar(64) NULL"); err != nil {
		t.Fatal(err)
	}
	gateKey := waitPendingDDL(t, ctx, gate, "srcdb", "t_probe", 30*time.Second)

	// ③ 挂起期间源表被删除（审批慢 + DBA 连续操作的竞态）
	if _, err := admin.ExecContext(ctx, "DROP TABLE srcdb.t_probe"); err != nil {
		t.Fatal(err)
	}

	// ④ approve → 源表已不存在，过期放行（audit skip 语义），管线不 fatal
	if err := gate.Settle(ctx, gateKey, DecisionApprove, "test: approve-after-drop"); err != nil {
		t.Fatal(err)
	}

	// ⑤ DROP TABLE 事件本身也是 pause 策略 DDL → 第二次挂起 → skip 决策放行
	dropKey := waitPendingDDL(t, ctx, gate, "srcdb", "t_probe", 30*time.Second)
	if err := gate.Settle(ctx, dropKey, DecisionSkip, "test: 源表已删，跳过 DROP"); err != nil {
		t.Fatal(err)
	}

	// ⑥ 管线必须存活且后续事件正常流动：另一张表写入继续同步
	time.Sleep(3 * time.Second) // 给 fatal 路径留暴露窗口（过期放行若误判为 fatal，此处即退出）
	select {
	case err := <-runErr:
		t.Fatalf("管线在过期 DDL 放行路径上退出（P0-8 回归失败）: %v", err)
	default:
	}
	if _, err := srcDB.ExecContext(ctx, "INSERT INTO srcdb.t_alive (id,v) VALUES (7,'alive')"); err != nil {
		t.Fatal(err)
	}
	waitCount(t, ctx, srcDB, "SELECT COUNT(*) FROM dstdb.t_alive WHERE id=7", 1, 30*time.Second)

	// 再等 2s 确认无延迟 fatal
	time.Sleep(2 * time.Second)
	select {
	case err := <-runErr:
		t.Fatalf("管线延迟退出: %v", err)
	default:
	}

	stop()
	select {
	case err := <-runErr:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("退出错误: %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("管线未退出")
	}
}

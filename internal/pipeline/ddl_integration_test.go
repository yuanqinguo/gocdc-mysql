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
		DiffTarget: func(ctx context.Context, schema, table string) (*TargetDiff, error) {
			res, err := mysqlsink.DiffTargetTable(ctx, srcDB, msink.DB(), schema, "dstdb", table)
			if err != nil {
				return nil, err
			}
			return &TargetDiff{AutoStmts: res.WhitelistedStmts(), AllStmts: res.AllStmts(),
				Whitelisted: res.Whitelisted, Summary: res.Summary}, nil
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

// TestIntegrationDDLMultiChangeApprove 用户事故场景回归（2026-11-09 approve 必炸
// 陷阱修复）：同一表连续多条 DDL（加列/改名/改类型/加列/删列）→ 累计差异合并进
// 首个挂起事件 → approve = 系统自动执行全部对账语句（旧版此处复验必失败 fatal，
// 位点冻在 DDL 前、重启重放命中已决 approve 再 fatal = 死循环）→ 目标结构对齐 +
// 行数据继续同步，后续 DDL 事件复验即自动通过。
func TestIntegrationDDLMultiChangeApprove(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
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
CREATE TABLE IF NOT EXISTS srcdb.t_ddl2 (
  id bigint NOT NULL PRIMARY KEY,
  name varchar(32) NOT NULL,
  amount int NOT NULL
);
CREATE TABLE IF NOT EXISTS dstdb.t_ddl2 LIKE srcdb.t_ddl2;`); err != nil {
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

	doTrue := true
	rules, err := CompileRules([]config.Rule{{
		Schema: "srcdb", TargetDatabase: "dstdb", DDLPolicy: "auto",
		Sinks:  &config.RuleSinks{MySQL: config.RuleSink{Enabled: true}},
		Tables: []config.TableRule{{Table: "t_ddl2", OrderByColumn: "id", DoStock: &doTrue}},
	}})
	if err != nil {
		t.Fatal(err)
	}

	cli, err := etcdx.New(etcdx.Options{Endpoints: eend})
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	gate := NewDDLGate(etcdx.NewKV(cli), "/gocdc/ddl2/events/ddl")

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
		DiffTarget: func(ctx context.Context, schema, table string) (*TargetDiff, error) {
			res, err := mysqlsink.DiffTargetTable(ctx, srcDB, msink.DB(), schema, "dstdb", table)
			if err != nil {
				return nil, err
			}
			return &TargetDiff{AutoStmts: res.WhitelistedStmts(), AllStmts: res.AllStmts(),
				Whitelisted: res.Whitelisted, Summary: res.Summary}, nil
		},
		ApplyTarget: func(ctx context.Context, stmts []string) error {
			return mysqlsink.ApplyTargetDDL(ctx, msink.DB(), stmts, 30*time.Second)
		},
	}

	pl := New(Options{
		Source:    newDDLSource(addr),
		SchemaMgr: schemaMgr, Transformer: tr, Sink: msink, Store: &e2eStore{}, Rules: rules,
		DDLHooks: ddlHooks,
		Workers:  2, BatchSize: 5, FlushInterval: 50 * time.Millisecond, SaveInterval: 200 * time.Millisecond,
		NodeID: "ddl2-test", DrainTimeout: 10 * time.Second,
		ExcludeSchemas: []string{"mysql", "sys", "information_schema", "performance_schema"},
		Log:            zap.NewExample(), // 诊断：间隙行去向排查
	})
	runCtx, stop := context.WithCancel(context.Background())
	defer stop()
	runErr := make(chan error, 1)
	go func() { runErr <- pl.Run(runCtx, model.Position{Name: file, Pos: pos}, nil) }()

	// ① 基线行
	if _, err := srcDB.ExecContext(ctx, "INSERT INTO srcdb.t_ddl2 (id,name,amount) VALUES (1,'a',1),(2,'b',2)"); err != nil {
		t.Fatal(err)
	}
	waitCount(t, ctx, srcDB, "SELECT COUNT(*) FROM dstdb.t_ddl2", 2, 30*time.Second)

	// ② 5 连 DDL（用户场景：加列/改名/改类型/加列/删列）快速连发，DDL 之间夹行
	// （间隙行回归：2026-11-09 E2E 实证——审批等待期间源库 DDL 演进到终态，若
	// 对账推进目标库后不同步刷新行结构缓存，间隙行按中间态列名 upsert → 1054
	// 熔断；追加式列变更下，meta 刷新到终态后间隙行按位置对齐正确）
	type step struct {
		ddl  string
		rows []string
	}
	steps := []step{
		{"ALTER TABLE srcdb.t_ddl2 ADD COLUMN remark varchar(64) NULL", []string{
			"INSERT INTO srcdb.t_ddl2 (id,name,amount,remark) VALUES (3,'c',3,'r3')"}},
		{"ALTER TABLE srcdb.t_ddl2 RENAME COLUMN name TO buyer_name", []string{
			"INSERT INTO srcdb.t_ddl2 (id,buyer_name,amount,remark) VALUES (4,'d',4,'r4')"}},
		{"ALTER TABLE srcdb.t_ddl2 MODIFY amount decimal(12,2) NOT NULL", []string{
			"INSERT INTO srcdb.t_ddl2 (id,buyer_name,amount,remark) VALUES (5,'e',5.5,'r5')"}},
		{"ALTER TABLE srcdb.t_ddl2 ADD COLUMN legacy varchar(8) NULL", nil},
		{"ALTER TABLE srcdb.t_ddl2 DROP COLUMN legacy", []string{
			"INSERT INTO srcdb.t_ddl2 (id,buyer_name,amount,remark) VALUES (6,'f',6,'r6')"}},
	}
	for _, st := range steps {
		if _, err := admin.ExecContext(ctx, st.ddl); err != nil {
			t.Fatalf("源端 DDL 失败 [%s]: %v", st.ddl, err)
		}
		for _, ins := range st.rows {
			if _, err := srcDB.ExecContext(ctx, ins); err != nil {
				t.Fatalf("间隙行写入失败 [%s]: %v", ins, err)
			}
		}
	}

	// ④ 审批闭环：挂起（首条 DDL 携累计差异；若管线处理快于 DDL 落库节奏可能出现
	// 多条挂起——逐条 approve 直至清空，模拟运维操作）
	for i := 0; i < 6; i++ {
		id := waitPendingDDLShort(t, ctx, gate, "srcdb", "t_ddl2", 30*time.Second)
		if id == "" {
			break
		}
		if err := gate.Settle(ctx, id, DecisionApprove, "test: approve 全量对账"); err != nil {
			t.Fatal(err)
		}
	}

	// ⑤ 目标结构对齐：buyer_name 存在、name 消失、amount=decimal、remark 存在
	waitColumn(t, ctx, srcDB, "dstdb", "t_ddl2", "buyer_name", 60*time.Second)
	waitColumn(t, ctx, srcDB, "dstdb", "t_ddl2", "remark", 60*time.Second)
	waitColumnGone(t, ctx, srcDB, "dstdb", "t_ddl2", "name", 60*time.Second)
	deadline := time.Now().Add(60 * time.Second)
	for {
		var dt string
		_ = srcDB.QueryRowContext(ctx, `SELECT DATA_TYPE FROM information_schema.COLUMNS
			 WHERE TABLE_SCHEMA='dstdb' AND TABLE_NAME='t_ddl2' AND COLUMN_NAME='amount'`).Scan(&dt)
		if dt == "decimal" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("amount 类型未对齐 decimal: %q", dt)
		}
		time.Sleep(300 * time.Millisecond)
	}

	// ⑥ 间隙行逐值正确（追加式列变更下按位置对齐：改名列/decimal 值/新列值
	// 全部落位正确）+ 总行数
	if !waitCountSoft(t, ctx, srcDB, `SELECT COUNT(*) FROM dstdb.t_ddl2 WHERE
		(id=3 AND buyer_name='c' AND amount=3   AND remark='r3') OR
		(id=4 AND buyer_name='d' AND amount=4   AND remark='r4') OR
		(id=5 AND buyer_name='e' AND amount=5.5 AND remark='r5') OR
		(id=6 AND buyer_name='f' AND amount=6   AND remark='r6')`, 4, 60*time.Second) {
		dumpRows(t, ctx, srcDB)
		t.Fatal("间隙行逐值校验未收敛")
	}
	waitCount(t, ctx, srcDB, "SELECT COUNT(*) FROM dstdb.t_ddl2", 6, 30*time.Second)

	// ⑦ 后续 DDL 全部自动收敛（挂起清空）
	waitNoPending(t, ctx, gate, "srcdb", "t_ddl2", 30*time.Second)

	stop()
	select {
	case err := <-runErr:
		if err != nil && err != context.Canceled {
			t.Fatalf("管线异常退出: %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("管线未退出")
	}
}

// waitPendingDDLShort 轮询 pending；窗口内未出现返回空串（审批闭环循环出口）。
func waitPendingDDLShort(t *testing.T, ctx context.Context, gate *EtcdDDLGate, schemaName, table string, max time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(max)
	for {
		recs, err := gate.PendingByTable(ctx, schemaName, table)
		if err == nil && len(recs) > 0 {
			return recs[0].ID
		}
		if time.Now().After(deadline) {
			return ""
		}
		time.Sleep(300 * time.Millisecond)
	}
}

// waitColumnGone 轮询列从目标消失（改名/删列对齐断言）。
func waitColumnGone(t *testing.T, ctx context.Context, db *sql.DB, schemaName, table, col string, max time.Duration) {
	t.Helper()
	deadline := time.Now().Add(max)
	for {
		var n int
		_ = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM information_schema.COLUMNS
			 WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND COLUMN_NAME=?`, schemaName, table, col).Scan(&n)
		if n == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("列 %s 应已从 %s.%s 移除", col, schemaName, table)
		}
		time.Sleep(300 * time.Millisecond)
	}
}

// waitNoPending 轮询该表 pending 清空（后续 DDL 自动收敛断言）。
func waitNoPending(t *testing.T, ctx context.Context, gate *EtcdDDLGate, schemaName, table string, max time.Duration) {
	t.Helper()
	deadline := time.Now().Add(max)
	for {
		recs, err := gate.PendingByTable(ctx, schemaName, table)
		if err == nil && len(recs) == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("pending DDL 未清空（%s.%s）: %d 条", schemaName, table, len(recs))
		}
		time.Sleep(300 * time.Millisecond)
	}
}

// waitCountSoft waitCount 的非 fatal 版（超时返回 false 供调用方补诊断）。
func waitCountSoft(t *testing.T, ctx context.Context, db *sql.DB, q string, want int64, max time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(max)
	for {
		var n int64
		if err := db.QueryRowContext(ctx, q).Scan(&n); err == nil && n == want {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// dumpRows 诊断：源/目标两侧全量行打印（间隙行排查）。
func dumpRows(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()
	rows, err := db.QueryContext(ctx, `SELECT 'SRC' side, id, buyer_name, amount, remark FROM srcdb.t_ddl2
		UNION ALL SELECT 'DST', id, buyer_name, amount, remark FROM dstdb.t_ddl2 ORDER BY side, id`)
	if err != nil {
		t.Logf("dump 失败: %v", err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		var side string
		var id int64
		var bn, rm sql.NullString
		var am sql.NullFloat64
		if err := rows.Scan(&side, &id, &bn, &am, &rm); err != nil {
			t.Logf("dump scan: %v", err)
			return
		}
		t.Logf("  %s id=%d buyer_name=%q amount=%v remark=%q", side, id, bn.String, am, rm.String)
	}
}

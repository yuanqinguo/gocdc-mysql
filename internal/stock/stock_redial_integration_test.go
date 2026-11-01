//go:build integration

// 连接丢失块级重连恢复场景（公网源库流龄强杀）：
// 槽位连接被 KILL 后 ScanTable 必须换新连接从游标继续并扫完整表（行数精确），
// 且重连后的槽位连接可供后续表继续使用。

package stock

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/testcontainers/testcontainers-go/modules/mysql"

	"gocdc/internal/config"
	"gocdc/internal/pipeline"
	"gocdc/internal/schema"
	mysqlsink "gocdc/internal/sink/mysql"
	"gocdc/internal/transform"
)

func TestIntegrationStockRedialOnDeadConn(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	mctr, err := mysql.Run(ctx, envOr("MYSQL_IMAGE", "mysql:8"),
		mysql.WithDatabase("srcdb"), mysql.WithUsername("root"), mysql.WithPassword("test"),
		mysql.WithConfigFile(mycnf(t)))
	if err != nil {
		t.Skipf("docker 不可用，跳过: %v", err)
	}
	t.Cleanup(func() { _ = mctr.Terminate(context.Background()) })
	host, _ := mctr.Host(ctx)
	port, _ := mctr.MappedPort(ctx, "3306")
	addr := host + ":" + port.Port()

	admin, err := sql.Open("mysql", "root:test@tcp("+addr+")/?multiStatements=true")
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	waitReady(t, ctx, admin)
	if _, err := admin.ExecContext(ctx, `
CREATE DATABASE dstdb;
CREATE TABLE srcdb.t_redial (
  id bigint NOT NULL PRIMARY KEY,
  v  varchar(64) NOT NULL
);
CREATE TABLE dstdb.t_redial LIKE srcdb.t_redial;`); err != nil {
		t.Fatal(err)
	}

	// 预置 2.5 万行（chunk 1000 → 25 块，KILL 后重连须扫完剩余全部块）
	srcDB, err := sql.Open("mysql", "root:test@tcp("+addr+")/")
	if err != nil {
		t.Fatal(err)
	}
	defer srcDB.Close()
	var vs []string
	for i := 1; i <= 25000; i++ {
		vs = append(vs, fmt.Sprintf("(%d, 'v%d')", i, i))
	}
	if _, err := srcDB.ExecContext(ctx,
		"INSERT INTO srcdb.t_redial (id,v) VALUES "+strings.Join(vs, ",")); err != nil {
		t.Fatal(err)
	}

	// 快照会话（1 槽）
	sess, mode, err := OpenSnapshot(ctx, srcDB, "ftwrl", 1)
	if err != nil {
		t.Fatalf("快照会话: %v", err)
	}
	defer sess.Close()
	if mode != "ftwrl" {
		t.Fatalf("lock mode = %q, want ftwrl", mode)
	}

	// KILL 槽位 0 的服务端会话：首块 fetchChunk 必然连接丢失 → RedialSlot 恢复
	var connID uint64
	if err := sess.Conn(0).QueryRowContext(ctx, "SELECT CONNECTION_ID()").Scan(&connID); err != nil {
		t.Fatalf("取槽位连接 ID: %v", err)
	}
	if _, err := srcDB.ExecContext(ctx, fmt.Sprintf("KILL %d", connID)); err != nil {
		t.Fatalf("KILL 槽位会话: %v", err)
	}

	doStock := true
	rules, err := pipeline.CompileRules([]config.Rule{{
		Schema: "srcdb", TargetDatabase: "dstdb",
		Sinks:  &config.RuleSinks{MySQL: config.RuleSink{Enabled: true}},
		Tables: []config.TableRule{{Table: "t_redial", OrderByColumn: "id", DoStock: &doStock}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	rule := rules.Match("srcdb", "t_redial")
	if rule == nil {
		t.Fatal("规则匹配失败")
	}

	schemaMgr := schema.NewManager(schema.NewMySQLLoader(srcDB))
	tr := transform.New(transform.Options{Timezone: time.UTC})
	msink, err := mysqlsink.New(mysqlsink.Options{Addr: addr, User: "root", Password: "test",
		MaxOpenConns: 4, TargetDB: func(db, table string) string { return "dstdb" }})
	if err != nil {
		t.Fatal(err)
	}
	defer msink.Close(ctx)

	scanner := NewScanner(schemaMgr, tr, msink, 1000)
	redials := 0
	rows, err := scanner.ScanTable(ctx, sess, 0, "srcdb", "t_redial", rule, nil, func(mk *Marker) error {
		redials++
		return nil
	})
	if err != nil {
		t.Fatalf("ScanTable 应在重连后完成: %v", err)
	}
	if rows != 25000 {
		t.Fatalf("rows = %d, want 25000", rows)
	}
	if redials < 25 {
		t.Fatalf("progress calls = %d, want >= 25", redials)
	}

	// 重连后的槽位连接必须可用（后续表复用该槽）
	if err := sess.Conn(0).PingContext(ctx); err != nil {
		t.Fatalf("重连后的槽位连接不可用: %v", err)
	}
	// 目标行数收敛（25 块全部投递）
	var n int
	if err := srcDB.QueryRowContext(ctx, "SELECT COUNT(*) FROM dstdb.t_redial").Scan(&n); err != nil || n != 25000 {
		t.Fatalf("目标行数: %d err=%v", n, err)
	}
}
